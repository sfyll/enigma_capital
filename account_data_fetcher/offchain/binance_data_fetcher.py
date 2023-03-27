import datetime
from datetime import timedelta
import csv
import logging
import os
from os import listdir
from os.path import isfile, join, getsize
import time
from typing import Optional, List

import pandas as pd
from pandas import DataFrame
from binance.spot import Spot

from utilities.account_data_fetcher_base import accountFetcherBase


class binanceDataFetcher(accountFetcherBase):
    _EXCHANGE = "Binance"
    _ENDPOINT = 'https://api.binance.com'
    def __init__(self, path: str, password: str, sub_account_name: str = None) -> None:
        super().__init__(path, password)
        self.logger = logging.getLogger(__name__) 
        self._subaccount_name = sub_account_name
        self._client = Spot(key=self.api_meta_data[self._EXCHANGE].key, secret=self.api_meta_data[self._EXCHANGE].secret)

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

    def convert_balances_to_dollars(self, binance_balances: List[dict]) -> int:
        netliq_in_dollars = 0
        for asset_information in binance_balances:
            btc_amount = float(asset_information["btcValuation"])
            asset_amount = float(asset_information["free"])
            if  btc_amount > 0.1:
                if asset_information['asset'] in ["BUSD", "USDC", "USDT"]:
                    netliq_in_dollars += asset_amount
                    continue
                price = float(self.get_latest_price(asset_information["asset"]+"USDC")["price"])
                netliq_in_dollars += price * asset_amount
        return round(netliq_in_dollars,3)

    def convert_isolated_margin_balance_to_dollars(self, binance_balances_isolated_margin: dict) -> int:
        netliq_in_dollars = 0
        for cross in binance_balances_isolated_margin["assets"]:
            if str(cross["baseAsset"]["netAsset"]) != "0" and str(cross["quoteAsset"]["netAsset"]) != "0":
                if cross['baseAsset']["asset"] in ["BUSD", "USDC", "USDT"]:
                    netliq_in_dollars += float(cross['baseAsset']['netAsset'])
                else:
                    price = float(self.get_latest_price(cross['baseAsset']["asset"]+"USDC")["price"])
                    netliq_in_dollars += price * float(cross['baseAsset']['netAsset'])
                if cross['quoteAsset']["asset"] in ["BUSD", "USDC", "USDT"]:
                    netliq_in_dollars += float(cross['quoteAsset']['netAsset'])
                else:
                    price = float(self.get_latest_price(cross['quoteAsset']["asset"]+"USDC")["price"])
                    netliq_in_dollars += price * float(cross['quoteAsset']['netAsset'])

        return round(netliq_in_dollars,3)

    def get_spot_positions(self) -> dict:

        user_assets = self.get_user_asset()
        
        data_to_return = {
            "Symbol": [],
            "Multiplier": [],
            "Quantity": [],
            "Dollar Quantity": []
        }
        
        for user_asset in user_assets:
            if float(user_asset["btcValuation"]) > 0.01:
                data_to_return["Symbol"].append(user_asset['asset'])
                data_to_return["Multiplier"].append(int(1))
                data_to_return["Quantity"].append(float(user_asset["free"])+ float(user_asset["locked"]) + float(user_asset["freeze"]) + float(user_asset["withdrawing"]))
                dollar_quantity = data_to_return["Quantity"][-1] if user_asset['asset'] in ["BUSD", "USDC", "USDT"] else round(float(self.get_latest_price(user_asset["asset"]+"USDT")["price"]) * data_to_return["Quantity"][-1],3)
                data_to_return["Dollar Quantity"].append(round(dollar_quantity),3)
        
        return data_to_return

    def get_margin_positions(self) -> dict:
        #only isolated margin pos
        user_assets = self.get_isolated_margin_account()
        
        data_to_return = {
            "Symbol": [],
            "Multiplier": [],
            "Quantity": [],
            "Dollar Quantity": []
        }
        
        for user_asset in user_assets["assets"]:
            if abs(float(user_asset["baseAsset"]["netAssetOfBtc"])) > 0.01:
                data_to_return["Symbol"] += [user_asset["baseAsset"]['asset'], user_asset["quoteAsset"]['asset']]
                data_to_return["Multiplier"] += [1, 1]
                data_to_return["Quantity"] += [user_asset["baseAsset"]['netAsset'], user_asset["quoteAsset"]['netAsset']]
                dollar_quantity_base = float(user_asset["baseAsset"]['netAsset']) if user_asset["baseAsset"]['asset'] in ["BUSD", "USDC", "USDT"] else round(float(self.get_latest_price(user_asset['baseAsset']["asset"]+"USDC")["price"]) * float(user_asset["baseAsset"]['netAsset']),3)
                dollar_quantity_quote = float(user_asset["quoteAsset"]['netAsset']) if user_asset["quoteAsset"]['asset'] in ["BUSD", "USDC", "USDT"] else round(float(self.get_latest_price(user_asset['quoteAsset']["asset"]+"USDC")["price"]) * float(user_asset["quoteAsset"]['netAsset']),3)
                data_to_return["Dollar Quantity"].append([dollar_quantity_base, dollar_quantity_quote])

        return data_to_return


if __name__ == "__main__":
    from getpass import getpass
    log_filename= os.path.expanduser("~") + "/log/test.py"
    numeric_level = getattr(logging, "DEBUG", None)
    logging.basicConfig(level=numeric_level, format='%(levelname)s:%(asctime)s:%(message)s', filename=log_filename) 
    logger: logging.Logger = logging.getLogger()
    pwd = getpass("provide password for pk:")
    current_path = os.path.realpath(os.path.dirname(__file__))
    parent_path = os.path.dirname(current_path)
    executor = binanceDataFetcher(parent_path, pwd, logger)
    # balances = executor.get_spot_positions()
    # print(balances)
    # executor.get_deposit_history(path=path)
    # executor.get_withdraw_history(path=path)
