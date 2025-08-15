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
from binance.error import ClientError
import pandas as pd
from pandas import DataFrame

class DataFetcher(ExchangeBase):
    _EXCHANGE = "Binance"
    STABLES = {"USDT", "USDC", "BUSD", "FDUSD", "TUSD"}
    BATCH_ONLY = True

    def __init__(
        self,
        secrets: ApiMetaData,
        session: aiohttp.ClientSession,
        sub_account_name: Optional[str] = None
    ) -> None:
        super().__init__(exchange=self._EXCHANGE, session=session)
        self.logger = logging.getLogger(__name__)
        self._subaccount_name = sub_account_name
        self._client = Spot(
            base_url="https://api1.binance.com",
            api_key=secrets.key,
            api_secret=secrets.secret,
            timeout=30,  # be explicit; adjust as you like
        )

        # Anti -1021 settings
        self._recv_window_ms: int = 60_000   # 60s
        self._time_offset_ms: int = 0        # server - local
        self._last_time_sync_ms: int = 0
        self._time_sync_interval_ms: int = 5 * 60_000  # resync every 5 minutes
        self._sync_time_with_binance()  # initial sync

    async def _run_in_executor(self, func, *args):
        loop = asyncio.get_running_loop()
        blocking_task = functools.partial(func, *args)
        return await loop.run_in_executor(None, blocking_task)

    async def fetch_balance(self, accountType: Optional[str] = None) -> float:
        self.logger.info("Fetching total Binance balance (Spot + Margin).")
        return await self._run_in_executor(self._get_total_balance_sync)

    async def fetch_positions(self, accountType: Optional[str] = None) -> Dict:
        self.logger.info("Fetching all Binance positions (Spot + Margin).")
        return await self._run_in_executor(self._get_all_positions_sync)

    def _now_ms(self) -> int:
        return int(time.time() * 1000)

    def _sync_time_with_binance(self) -> None:
        try:
            server_time = self._client.time()["serverTime"]
            local = self._now_ms()
            self._time_offset_ms = int(server_time) - local
            self._last_time_sync_ms = local
            self.logger.info(f"Binance time offset set to {self._time_offset_ms} ms")
        except Exception as e:
            self.logger.warning(f"Could not sync Binance server time: {e}")

    def _timestamp_ms(self) -> int:
        now = self._now_ms()
        if now - self._last_time_sync_ms > self._time_sync_interval_ms:
            try:
                self._sync_time_with_binance()
            except Exception:
                pass
        return now + self._time_offset_ms

    def _signed_call(self, fn, **kwargs):
        # Always provide recvWindow and a timestamp adjusted with server offset
        kwargs.setdefault("recvWindow", self._recv_window_ms)
        kwargs.setdefault("timestamp", self._timestamp_ms())
        try:
            return fn(**kwargs)
        except ClientError as e:
            # Auto-resync and retry once on -1021
            if getattr(e, "error_code", None) == -1021:
                self.logger.warning("Hit -1021 (recvWindow). Resyncing time and retrying once.")
                self._sync_time_with_binance()
                kwargs["timestamp"] = self._timestamp_ms()
                return fn(**kwargs)
            raise

    def _get_total_balance_sync(self) -> float:
        binance_balance = self.convert_balances_to_dollars(self.get_user_asset())
        isolated_margin_balance = self.convert_isolated_margin_balance_to_dollars(self.get_isolated_margin_account())
        margin_account_balance = self.convert_cross_margin_balance_to_dollars(self.get_margin_account())
        total_balance = binance_balance + isolated_margin_balance + margin_account_balance
        return round(total_balance, 3)

    def _get_all_positions_sync(self) -> Dict:
        data_to_return = {"Symbol": [], "Multiplier": [], "Quantity": [], "Dollar Quantity": []}
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
                    data_to_return["Multiplier"].append(1)
                    data_to_return["Quantity"].append(pos_group["Quantity"][i])
                    data_to_return["Dollar Quantity"].append(pos_group["Dollar Quantity"][i])
        return data_to_return

    def convert_balances_to_dollars(self, binance_balances: List[dict]) -> float:
        netliq_in_dollars = 0.0

        assets_to_price = {
            b["asset"]
            for b in binance_balances
            if (float(b["btcValuation"]) > 0.1 or float(b["free"]) > 100)
            and b["asset"] not in self.STABLES
            and "NFT" not in b["asset"]
        }

        price_map = self._batch_usdt_prices(assets_to_price)

        for asset_information in binance_balances:
            btc_amount = float(asset_information["btcValuation"])
            asset_amount = float(asset_information["free"])
            if btc_amount > 0.1 or asset_amount > 100:
                asset = asset_information['asset']
                if asset in self.STABLES:
                    netliq_in_dollars += asset_amount
                    continue
                if "NFT" in asset:
                    continue

                price = price_map.get(asset)
                if price is None:
                    if self.BATCH_ONLY:
                        self.logger.warning(f"No batch price for {asset}USDT; skipping.")
                        continue
                    # optional fallback:
                    price = float(self.get_latest_price(symbol=f"{asset}USDT")["price"])
                netliq_in_dollars += price * asset_amount

        return round(netliq_in_dollars, 3)

    def convert_isolated_margin_balance_to_dollars(self, bal: dict) -> float:
        netliq_in_dollars = 0.0

        assets_to_price = set()
        for cross in bal["assets"]:
            for side in ("baseAsset", "quoteAsset"):
                a = cross[side]["asset"]
                if float(cross[side]["netAsset"]) != 0 and a not in self.STABLES:
                    assets_to_price.add(a)

        price_map = self._batch_usdt_prices(assets_to_price)

        for cross in bal["assets"]:
            for side in ("baseAsset", "quoteAsset"):
                asset = cross[side]
                net = float(asset["netAsset"])
                if net == 0:
                    continue

                a = asset["asset"]
                if a in self.STABLES:
                    netliq_in_dollars += net
                else:
                    price = price_map.get(a)
                    if price is None:
                        if self.BATCH_ONLY:
                            self.logger.warning(f"No batch price for {a}USDT; skipping.")
                            continue
                        price = float(self.get_latest_price(symbol=f"{a}USDT")["price"])
                    netliq_in_dollars += price * net

        return round(netliq_in_dollars, 3)

    def convert_cross_margin_balance_to_dollars(self, margin_balance_info: dict) -> float:
        btc_usdt_price = float(self.get_latest_price(symbol="BTCUSDT")["price"])
        net_asset_btc = float(margin_balance_info["totalNetAssetOfBtc"])
        return round(net_asset_btc * btc_usdt_price, 2)

    def get_spot_positions(self) -> dict:
        user_assets = self.get_user_asset()
        data_to_return = {"Symbol": [], "Multiplier": [], "Quantity": [], "Dollar Quantity": []}

        assets_to_price = {
            ua["asset"] for ua in user_assets
            if float(ua["btcValuation"]) > 0.01 and ua["asset"] not in self.STABLES
        }
        price_map = self._batch_usdt_prices(assets_to_price)

        for ua in user_assets:
            if float(ua["btcValuation"]) > 0.01:
                asset = ua['asset']
                qty = float(ua["free"]) + float(ua["locked"]) + float(ua["freeze"]) + float(ua["withdrawing"])
                data_to_return["Symbol"].append(asset)
                data_to_return["Multiplier"].append(1)
                data_to_return["Quantity"].append(qty)

                if asset in self.STABLES:
                    dollar_quantity = qty
                else:
                    price = price_map.get(asset)
                    if price is None:
                        if self.BATCH_ONLY:
                            self.logger.warning(f"No batch price for {asset}USDT; using 0.")
                            dollar_quantity = 0.0
                        else:
                            price = float(self.get_latest_price(symbol=f"{asset}USDT")["price"])
                            dollar_quantity = price * qty
                    else:
                        dollar_quantity = price * qty

                data_to_return["Dollar Quantity"].append(round(dollar_quantity, 3))

        return data_to_return

    def get_margin_positions(self) -> dict:
        user_assets = self.get_isolated_margin_account()
        data_to_return = {"Symbol": [], "Multiplier": [], "Quantity": [], "Dollar Quantity": []}

        assets_to_price = set()
        for ua in user_assets["assets"]:
            for side in ("baseAsset", "quoteAsset"):
                a = ua[side]["asset"]
                if abs(float(ua[side]['netAsset'])) > 0 and a not in self.STABLES:
                    assets_to_price.add(a)

        price_map = self._batch_usdt_prices(assets_to_price)

        for ua in user_assets["assets"]:
            for side in ("baseAsset", "quoteAsset"):
                asset = ua[side]
                net_asset = float(asset['netAsset'])
                if abs(net_asset) == 0:
                    continue

                a = asset['asset']
                data_to_return["Symbol"].append(a)
                data_to_return["Multiplier"].append(1)
                data_to_return["Quantity"].append(net_asset)

                if a in self.STABLES:
                    dollar_quantity = net_asset
                else:
                    price = price_map.get(a)
                    if price is None:
                        if self.BATCH_ONLY:
                            self.logger.warning(f"No batch price for {a}USDT; using net units.")
                            dollar_quantity = net_asset  # keep prior behavior if you like
                        else:
                            try:
                                price = float(self.get_latest_price(symbol=f"{a}USDT")["price"])
                                dollar_quantity = price * net_asset
                            except Exception:
                                self.logger.warning(f"Could not fetch price for margin asset {a}. Using net units.")
                                dollar_quantity = net_asset
                    else:
                        dollar_quantity = price * net_asset

                data_to_return["Dollar Quantity"].append(round(dollar_quantity, 3))

        return data_to_return

    def _batch_usdt_prices(self, assets) -> Dict[str, float]:
        symbols = [f"{a}USDT" for a in assets if a not in self.STABLES and "NFT" not in a]
        if not symbols:
            return {}

        price_map: Dict[str, float] = {}
        CHUNK = 100
        for i in range(0, len(symbols), CHUNK):
            chunk = symbols[i:i+CHUNK]
            try:
                resp = self.get_latest_price(symbols=chunk)  
                for item in resp:
                    sym = item["symbol"]           # e.g., "BTCUSDT"
                    asset = sym[:-4]               # strip "USDT"
                    price_map[asset] = float(item["price"])
            except Exception as e:
                self.logger.error(f"Batch ticker_price failed for {chunk}: {e}", exc_info=True)
        return price_map

    def get_files_in_folder(self, path: str) -> list:
        onlyfiles = []
        onlyfiles += [f for f in listdir(path) if isfile(join(path, f)) and f != ".DS_Store" and getsize(join(path, f)) > 0]
        return onlyfiles

    def get_account_snapshot(self, account_type: str = "SPOT") -> List[dict]:
        return self._signed_call(self._client.account_snapshot, type=account_type)

    def get_funding_wallet(self) -> List[dict]:
        return self._signed_call(self._client.funding_wallet)

    def get_user_asset(self) -> List[dict]:
        return self._signed_call(self._client.user_asset)

    def get_portfolio_margin_account(self) -> List[dict]:
        return self._signed_call(self._client.portfolio_margin_account)

    def get_margin_account(self) -> List[dict]:
        return self._signed_call(self._client.margin_account)

    def get_isolated_margin_account(self) -> List[dict]:
        return self._signed_call(self._client.isolated_margin_account)

    def get_current_avg_price(self, symbol: str) -> List[dict]:
        return self._client.avg_price(symbol=symbol)  # public, no signing

    def get_latest_price(self, symbol: str = None, symbols: List[str] = None) -> List[dict]:
        if symbol is not None:
            return self._client.ticker_price(symbol=symbol)  # public
        if symbols is not None:
            return self._client.ticker_price(symbols=symbols)  # public bulk endpoint
        raise ValueError("Either symbol or symbols must be provided")

    def save_historical_klines(self, symbol, file_loc: str, start: str = "2020/01/01", end: str = "2023/01/01", interval="1m") -> None:
        start_ts = int(time.mktime(datetime.datetime.strptime(start, "%Y/%m/%d").timetuple())) * 1000
        end_ts = int(time.mktime(datetime.datetime.strptime(end, "%Y/%m/%d").timetuple())) * 1000
        data = []
        df_old: Optional[DataFrame] = None

        saved_prices: str = self.get_files_in_folder(file_loc)
        for cross in saved_prices:
            if symbol.upper() + ".csv" == cross:
                df = pd.read_csv(file_loc + cross)
                end_ts_old = int(time.mktime(datetime.datetime.strptime(df["Unnamed: 0"].iloc[-1], "%Y-%m-%d %H:%M:%S.%f").timetuple())) * 1000
                if end_ts_old > end_ts:
                    raise Exception(f"Already got this data ! {file_loc+cross}")
                elif end_ts_old > start_ts:
                    start_ts = end_ts_old + 60_000
                    df.set_index("Unnamed: 0", inplace=True)
                    df_old = df

        while start_ts < end_ts:
            result = self._client.klines(symbol, interval, startTime=start_ts, limit=1000)
            data += result
            start_ts = data[-1][0]
        df = pd.DataFrame(data)
        df.columns = ['open_time','o', 'h', 'l', 'c', 'v','close_time', 'qav', 'num_trades','taker_base_vol', 'taker_quote_vol', 'ignore']
        df.index = [datetime.datetime.fromtimestamp(x / 1000.0) for x in df.close_time]

        if df_old is not None:
            df = pd.concat([df_old, df])
            df.index = pd.to_datetime(df.index)
            df.sort_index(inplace=True)

        df.to_csv(file_loc + symbol + start.replace("/", "-") + "_" + end.replace("/", "-") + ".csv")

    def get_deposit_history(self, path: str, start_date: str = "01/01/2020", end_date: Optional[str] = None) -> None:
        start_timestamp = int(time.mktime(datetime.datetime.strptime(start_date, "%m/%d/%Y").timetuple()) * 1000)
        end_timestamp = int(time.time() * 1000) if end_date is None else int(time.mktime(datetime.datetime.strptime(end_date, "%m/%d/%Y").timetuple()) * 1000)

        interval = timedelta(days=90)
        current_start_timestamp = start_timestamp
        current_end_timestamp = min(current_start_timestamp + int(interval.total_seconds() * 1000), end_timestamp)

        deposits = []
        while current_start_timestamp < end_timestamp:
            current_deposits = self._signed_call(self._client.deposit_history,
                                                 startTime=current_start_timestamp,
                                                 endTime=current_end_timestamp)
            deposits.extend(current_deposits)
            current_start_timestamp = current_end_timestamp
            current_end_timestamp = min(current_start_timestamp + int(interval.total_seconds() * 1000), end_timestamp)

        df = pd.DataFrame(deposits)
        df.rename(columns={"insertTime": "Date(UTC)"}, inplace=True)
        df['Date(UTC)'] = [datetime.datetime.fromtimestamp(x / 1000.0) for x in df['Date(UTC)']]
        df['Status'] = ['Completed' for _ in df.get('status', [])]
        df.drop(columns={"id", 'addressTag', 'transferType', 'confirmTimes', 'unlockConfirm', 'walletType'}, errors="ignore", inplace=True)
        df.to_csv(path)

    def get_withdraw_history(self, path: str, start_date: str = "01/01/2020", end_date: Optional[str] = None) -> None:
        start_timestamp = int(time.mktime(datetime.datetime.strptime(start_date, "%m/%d/%Y").timetuple()) * 1000)
        end_timestamp = int(time.time() * 1000) if end_date is None else int(time.mktime(datetime.datetime.strptime(end_date, "%m/%d/%Y").timetuple()) * 1000)

        interval = timedelta(days=90)
        current_start_timestamp = start_timestamp
        current_end_timestamp = min(current_start_timestamp + int(interval.total_seconds() * 1000), end_timestamp)

        withdrawals = []
        while current_start_timestamp < end_timestamp:
            current_withdrawals = self._signed_call(self._client.withdraw_history,
                                                    startTime=current_start_timestamp,
                                                    endTime=current_end_timestamp)
            withdrawals.extend(current_withdrawals)
            current_start_timestamp = current_end_timestamp
            current_end_timestamp = min(current_start_timestamp + int(interval.total_seconds() * 1000), end_timestamp)

        df = pd.DataFrame(withdrawals)
        df.rename(columns={"completeTime": "Date(UTC)"}, inplace=True)
        df.drop(columns={"id", 'applyTime', 'transferType', 'info', 'confirmNo', 'walletType', 'txKey'}, errors="ignore", inplace=True)
        df.to_csv(path)
