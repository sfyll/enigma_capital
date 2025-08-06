import asyncio
import datetime
from datetime import timedelta
from os import listdir
from os.path import isfile, join, getsize
import time
import functools
import logging
from typing import Optional, List, Dict

from account_data_fetcher.exchanges.exchange_base import ExchangeBase
from infrastructure.api_secret_getter import ApiMetaData

import aiohttp
from binance.spot import Spot
import pandas as pd
from pandas import DataFrame

class DataFetcher(ExchangeBase):
    _EXCHANGE = "Binance"
    def __init__(
        self,
        secrets: ApiMetaData,
        session: aiohttp.ClientSession,
        output_queue: asyncio.Queue,
        fetch_frequency: int,
        sub_account_name: Optional[str] = None
    ) -> None:
        super().__init__(
            exchange=self._EXCHANGE,
            session=session,
            output_queue=output_queue,
            fetch_frequency=fetch_frequency
        )
        self.logger = logging.getLogger(__name__)
        self._subaccount_name = sub_account_name
        self._client = Spot(
            base_url="https://api1.binance.com",
            api_key=secrets.key,
            api_secret=secrets.secret
        )

    async def _run_in_executor(self, func, *args):
        """Helper to run a synchronous function in the default thread pool executor."""
        loop = asyncio.get_running_loop()
        blocking_task = functools.partial(func, *args)
        return await loop.run_in_executor(None, blocking_task)

    async def fetch_balance(self, accountType: Optional[str] = None) -> float:
        """
        Asynchronously fetches the total account balance by running the blocking
        API calls in a separate thread.
        """
        self.logger.info("Fetching total Binance balance (Spot + Margin).")
        return await self._run_in_executor(self._get_total_balance_sync)

    # MODIFIED: fetch_positions is now an async method.
    async def fetch_positions(self, accountType: Optional[str] = None) -> Dict:
        """
        Asynchronously fetches all positions by running the blocking
        API calls in a separate thread.
        """
        self.logger.info("Fetching all Binance positions (Spot + Margin).")
        return await self._run_in_executor(self._get_all_positions_sync)

    def _get_total_balance_sync(self) -> float:
        """Synchronous calculation of total balance. Do not call directly from async code."""
        binance_balance = self.convert_balances_to_dollars(self._client.user_asset())
        isolated_margin_balance = self.convert_isolated_margin_balance_to_dollars(self._client.isolated_margin_account())
        margin_account_balance = self.convert_cross_margin_balance_to_dollars(self._client.margin_account())

        total_balance = binance_balance + isolated_margin_balance + margin_account_balance
        return round(total_balance, 3)

    def _get_all_positions_sync(self) -> Dict:
        """Synchronous aggregation of all positions. Do not call directly from async code."""
        data_to_return = {
            "Symbol": [], "Multiplier": [], "Quantity": [], "Dollar Quantity": []
        }
        
        spot_positions = self.get_spot_positions()
        margin_positions = self.get_margin_positions()

        all_positions = [spot_positions, margin_positions]

        for pos_group in all_positions:
            for i, symbol in enumerate(pos_group["Symbol"]):
                if symbol in data_to_return["Symbol"]:
                    index = data_to_return["Symbol"].index(symbol)
                    data_to_return["Quantity"][index] += pos_group["Quantity"][i]
                    data_to_return["Dollar Quantity"][index] += pos_group["Dollar Quantity"][i]
                else:
                    data_to_return["Symbol"].append(symbol)
                    data_to_return["Multiplier"].append(1) # Spot/Margin multiplier is 1
                    data_to_return["Quantity"].append(pos_group["Quantity"][i])
                    data_to_return["Dollar Quantity"].append(pos_group["Dollar Quantity"][i])
        
        return data_to_return

    def convert_balances_to_dollars(self, binance_balances: List[dict]) -> float:
        netliq_in_dollars = 0
        for asset_information in binance_balances:
            btc_amount = float(asset_information["btcValuation"])
            asset_amount = float(asset_information["free"])
            if  btc_amount > 0.1 or asset_amount > 100:
                if asset_information['asset'] in ["BUSD", "USDC", "USDT"]:
                    netliq_in_dollars += asset_amount
                    continue
                elif "NFT" in asset_information['asset']:
                    continue
                price = float(self.get_latest_price(asset_information["asset"]+"USDT")["price"])
                netliq_in_dollars += price * asset_amount
        return round(netliq_in_dollars,3)

    def convert_isolated_margin_balance_to_dollars(self, binance_balances_isolated_margin: dict) -> float:
        netliq_in_dollars = 0
        for cross in binance_balances_isolated_margin["assets"]:
            if float(cross["baseAsset"]["netAsset"]) != 0:
                asset = cross['baseAsset']
                if asset["asset"] in ["BUSD", "USDC", "USDT"]:
                    netliq_in_dollars += float(asset['netAsset'])
                else:
                    price = float(self._client.ticker_price(symbol=f"{asset['asset']}USDT")["price"])
                    netliq_in_dollars += price * float(asset['netAsset'])
            
            if float(cross["quoteAsset"]["netAsset"]) != 0:
                asset = cross['quoteAsset']
                if asset["asset"] in ["BUSD", "USDC", "USDT"]:
                    netliq_in_dollars += float(asset['netAsset'])
                else:
                    price = float(self._client.ticker_price(symbol=f"{asset['asset']}USDT")["price"])
                    netliq_in_dollars += price * float(asset['netAsset'])
        return round(netliq_in_dollars, 3)
    
    def convert_cross_margin_balance_to_dollars(self, margin_balance_info: dict) -> float:
        btc_usdt_price = float(self._client.ticker_price(symbol="BTCUSDT")["price"])
        net_asset_btc = float(margin_balance_info["totalNetAssetOfBtc"])
        return round(net_asset_btc * btc_usdt_price, 2)

    def get_spot_positions(self) -> dict:
        user_assets = self._client.user_asset()
        data_to_return = {"Symbol": [], "Multiplier": [], "Quantity": [], "Dollar Quantity": []}
        
        for user_asset in user_assets:
            if float(user_asset["btcValuation"]) > 0.01:
                data_to_return["Symbol"].append(user_asset['asset'])
                data_to_return["Multiplier"].append(int(1))
                data_to_return["Quantity"].append(float(user_asset["free"])+ float(user_asset["locked"]) + float(user_asset["freeze"]) + float(user_asset["withdrawing"]))
                dollar_quantity = data_to_return["Quantity"][-1] if user_asset['asset'] in ["BUSD", "USDC", "USDT"] else round(float(self.get_latest_price(user_asset["asset"]+"USDT")["price"]) * data_to_return["Quantity"][-1],3)
                data_to_return["Dollar Quantity"].append(round(dollar_quantity),3)
        
        return data_to_return

    def get_margin_positions(self) -> dict:
        # This logic is complex and makes multiple API calls. Kept as is.
        user_assets = self._client.isolated_margin_account()
        data_to_return = {"Symbol": [], "Multiplier": [], "Quantity": [], "Dollar Quantity": []}
        
        for user_asset in user_assets["assets"]:
            base_asset = user_asset["baseAsset"]
            quote_asset = user_asset["quoteAsset"]
            
            for asset in [base_asset, quote_asset]:
                net_asset = float(asset['netAsset'])
                if abs(net_asset) > 0:
                    data_to_return["Symbol"].append(asset['asset'])
                    data_to_return["Multiplier"].append(1)
                    data_to_return["Quantity"].append(net_asset)
                    
                    dollar_quantity = net_asset
                    if asset['asset'] not in ["BUSD", "USDC", "USDT"]:
                        try:
                            price = float(self._client.ticker_price(symbol=f"{asset['asset']}USDT")["price"])
                            dollar_quantity = price * net_asset
                        except:
                            self.logger.warning(f"Could not fetch price for margin asset {asset['asset']}. Using net asset value.")
                            dollar_quantity = net_asset
                    data_to_return["Dollar Quantity"].append(round(dollar_quantity, 3))

        return data_to_return


    def get_files_in_folder(self, path: str) -> list:
        onlyfiles = []
        onlyfiles += [f for f in listdir(path) if isfile(join(path, f)) and f != ".DS_Store" and getsize(join(path, f)) > 0]
        return onlyfiles

    def get_account_snapshot(self, account_type: str = "SPOT") -> List[dict]:
        return self._client.account_snapshot(account_type)

    def get_funding_wallet(self) -> List[dict]:
        return self._client.funding_wallet()

    def get_user_asset(self) -> List[dict]:
        return self._client.user_asset()

    def get_portfolio_margin_account(self) -> List[dict]:
        return self._client.portfolio_margin_account()

    def get_margin_account(self) -> List[dict]:
        return self._client.margin_account()

    def get_isolated_margin_account(self) -> List[dict]:
        return self._client.isolated_margin_account()

    def get_current_avg_price(self, symbol: str) -> List[dict]:
        return self._client.avg_price(symbol)

    def get_latest_price(self, symbol: str= None, symbols: List[str]= None) -> List[dict]:
        return self._client.ticker_price(symbol if symbol else symbols)

    def save_historical_klines(self, symbol, file_loc: str, start: str = "2020/01/01", end: str = "2023/01/01", interval="1m") -> None:
        start_ts = int(time.mktime(datetime.datetime.strptime(start, "%Y/%m/%d").timetuple())) * 1000
        end_ts = int(time.mktime(datetime.datetime.strptime(end, "%Y/%m/%d").timetuple())) * 1000
        data = []
        df_old: Optional[DataFrame] = None

        #check if we have history for this pair so as not to redownload everything, assumes same interval
        saved_prices: str = self.get_files_in_folder(file_loc)
        for cross in saved_prices:
            if symbol.upper()+".csv" == cross:
                df = pd.read_csv(file_loc+cross)
                end_ts_old = int(time.mktime(datetime.datetime.strptime(df["Unnamed: 0"].iloc[-1], "%Y-%m-%d %H:%M:%S.%f").timetuple())) * 1000
                if end_ts_old > end_ts:
                    raise Exception(f"Already got this data ! {file_loc+cross}")
                elif end_ts_old > start_ts:
                    start_ts = end_ts_old + 60000 #+1mn
                    df.set_index("Unnamed: 0", inplace=True)
                    df_old = df
                
        while start_ts < end_ts:
            result = self._client.klines(symbol, interval, startTime=start_ts, limit=1000)
            data += result
            start_ts = data[-1][0]
        df = pd.DataFrame(data)
        df.columns = ['open_time',
                    'o', 'h', 'l', 'c', 'v',
                    'close_time', 'qav', 'num_trades',
                    'taker_base_vol', 'taker_quote_vol', 'ignore']
        df.index = [datetime.datetime.fromtimestamp(x / 1000.0) for x in df.close_time]

        if df_old is not None:
            df = pd.concat([df_old, df])
            df.index = pd.to_datetime(df.index)
            df.sort_index(inplace=True)

        df.to_csv(file_loc+symbol+start.replace("/", "-")+"_"+end.replace("/", "-")+".csv")

    def get_deposit_history(self, path: str, start_date: str = "01/01/2020", end_date: Optional[str] = None) -> None:
        # Convert start_date and end_date to timestamps
        start_timestamp = int(time.mktime(datetime.datetime.strptime(start_date, "%m/%d/%Y").timetuple()) * 1000)
        
        if end_date is None:
            end_timestamp = int(time.time() * 1000)
        else:
            end_timestamp = int(time.mktime(datetime.datetime.strptime(end_date, "%m/%d/%Y").timetuple()) * 1000)
        
       # Prepare to fetch deposits in rolling 90-day intervals
        interval = timedelta(days=90)
        current_start_timestamp = start_timestamp
        current_end_timestamp = min(current_start_timestamp + int(interval.total_seconds() * 1000), end_timestamp)
        
        deposits = []

        # Fetch deposits in rolling 90-day intervals
        while current_start_timestamp < end_timestamp:
            current_deposits = self._client.deposit_history(startTime=current_start_timestamp, endTime=current_end_timestamp)
            deposits.extend(current_deposits)
            
            current_start_timestamp = current_end_timestamp
            current_end_timestamp = min(current_start_timestamp + int(interval.total_seconds() * 1000), end_timestamp)

        df = pd.DataFrame(deposits)

        #for coinly...
        df.rename(columns={"insertTime":"Date(UTC)"}, inplace=True)
        df['Date(UTC)'] = [datetime.datetime.fromtimestamp(x / 1000.0) for x in df['Date(UTC)']]
        df['Status'] = ['Completed' for x in df['status']]
        df.drop(columns={"id", 'addressTag', 'transferType', 'confirmTimes', 'unlockConfirm', 'walletType'}, inplace=True)

        df.to_csv(path)

    def get_withdraw_history(self, path: str, start_date: str = "01/01/2020", end_date: Optional[str] = None) -> None:
        # Convert start_date and end_date to timestamps
        start_timestamp = int(time.mktime(datetime.datetime.strptime(start_date, "%m/%d/%Y").timetuple()) * 1000)
        
        if end_date is None:
            end_timestamp = int(time.time() * 1000)
        else:
            end_timestamp = int(time.mktime(datetime.datetime.strptime(end_date, "%m/%d/%Y").timetuple()) * 1000)
        
       # Prepare to fetch deposits in rolling 90-day intervals
        interval = timedelta(days=90)
        current_start_timestamp = start_timestamp
        current_end_timestamp = min(current_start_timestamp + int(interval.total_seconds() * 1000), end_timestamp)
        
        deposits = []

        # Fetch deposits in rolling 90-day intervals
        while current_start_timestamp < end_timestamp:
            current_deposits = self._client.withdraw_history(startTime=current_start_timestamp, endTime=current_end_timestamp)
            deposits.extend(current_deposits)
            
            current_start_timestamp = current_end_timestamp
            current_end_timestamp = min(current_start_timestamp + int(interval.total_seconds() * 1000), end_timestamp)

        df = pd.DataFrame(deposits)
        
        #for coinly...
        df.rename(columns={"completeTime":"Date(UTC)"}, inplace=True)

        df.drop(columns={"id", 'applyTime', 'transferType', 'info', 'confirmNo', 'walletType', 'txKey'}, inplace=True)

        df.to_csv(path)

