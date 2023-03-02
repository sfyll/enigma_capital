from datetime import datetime
from getpass import getpass
import logging
import os
from typing import List

import pandas as pd

from account_data_fetcher.ib_data_fetcher_api import InteractiveBrokersAppAsync
from account_data_fetcher.ib_data_fetcher_flex_queries import ibDataFetcher
from account_data_fetcher.ftx_data_fetcher import ftxDataFetcher
from account_data_fetcher.binance_data_fetcher import binanceDataFetcher
from account_data_fetcher.dydx_data_fetcher import dydxDataFetcher
from account_data_fetcher.trades_station_data_fetcher import tradesStationDataFetcher
from account_data_fetcher.bybit_data_fetcher import bybitDataFetcher
from account_data_fetcher.ethereum_data_fetcher import ethereumDataFetcher
from account_data_fetcher.coingecko_data_fetcher import coingeckoDataFetcher

class AccountDataFetcher:
    def __init__(self, pwd: str, ib_fetching_method: str, exchange_list: List[str]) -> None:
        self.pwd = pwd
        self.logger = logging.getLogger(__name__)
        self.path = os.path.realpath(os.path.dirname(__file__))
        self.set_up_executors(pwd, ib_fetching_method, exchange_list)

    def set_up_executors(self, pwd, ib_fetching_method,  exchange_list) -> None :
        if "IB" in exchange_list:
            if ib_fetching_method == "API":
                self.ib_executor = InteractiveBrokersAppAsync(self.path, pwd)
            elif ib_fetching_method == "FLEX":
                self.ib_executor = ibDataFetcher(self.path, pwd)
            else:
                raise Exception(f"Unkown IB fetching method: {ib_fetching_method}")
        else:
            self.ib_executor = None
        
        if "DYDX" in exchange_list:
            self.dydx_executor = dydxDataFetcher(self.path, pwd)
        else:
            self.dydx_executor = None
        
        if "Binance" in exchange_list:
            self.binance_executor = binanceDataFetcher(self.path, pwd)
        else:
            self.binance_executor = None

        if "TradeStation" in exchange_list:
            self.tradestation_executor = tradesStationDataFetcher(self.path, pwd)
        else:
            self.tradestation_executor = None

        if "BYBIT" in exchange_list:
            self.bybit_executor = bybitDataFetcher(self.path, pwd)
        else:
            self.bybit_executor = None

        if "Ethereum" in exchange_list:
            self.ethereum_executor = ethereumDataFetcher(self.path, pwd)
        else:
            self.ethereum_executor = None

        if "FTX" in exchange_list:
            raise FileNotFoundError("You got Rekt")
        
        self.price_fetcher = coingeckoDataFetcher().get_prices

    def write_balance_to_csv(self, manual_balance: float) -> None:
        balance_dict: dict = self.get_global_balance()
        balance_dict["netliq"] += manual_balance
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        dictionary_to_write = {
            "date": now
        }
        dictionary_to_write.update(balance_dict)

        #to write in DF
        dictionary_to_write = {key:[value] for key,value in dictionary_to_write.items()}

        try:
            df = pd.read_csv(self.path+"/netliq.csv")
        except FileNotFoundError:
            df_new = pd.DataFrame.from_dict(dictionary_to_write)
            df_new.set_index("date", inplace=True)
            df_new.to_csv(self.path+"/netliq.csv")
        else:
            df_new = pd.DataFrame.from_dict(dictionary_to_write)
            df_new.set_index("date", inplace=True)
            df.set_index("date", inplace=True)
            df_last = pd.concat([df, df_new])
            df_last.to_csv(self.path+"/netliq.csv")

        self.logger.info(f"writting {dictionary_to_write=}")

    def write_positions_to_csv(self) -> None:

        position_dict: dict = self.get_global_positions()
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        dictionary_to_write: dict = {
            "date": [],
            "Exchange": [],
            "Symbol": [],
            "Multiplier": [],
            "Quantity": [],
            "Dollar Quantity": [],
        }

        for key, value in position_dict.items():
            dictionary_to_write["date"] += [now] * len(value["Symbol"])
            dictionary_to_write["Exchange"] += [key] * len(value["Symbol"])
            dictionary_to_write["Symbol"] += value["Symbol"]
            dictionary_to_write["Multiplier"] +=  value["Multiplier"]
            dictionary_to_write["Quantity"] += value["Quantity"]
            dictionary_to_write["Dollar Quantity"] += value["Dollar Quantity"]

        try:
            df = pd.read_csv(self.path+"/positions.csv")
        except FileNotFoundError:
            df_new = pd.DataFrame.from_dict(dictionary_to_write)
            df_new.set_index("date", inplace=True)
            df_new.to_csv(self.path+"/positions.csv")
        else:
            df_new = pd.DataFrame.from_dict(dictionary_to_write)
            df_new.set_index("date", inplace=True)
            df.set_index("date", inplace=True)
            df_last = pd.concat([df, df_new])
            df_last.to_csv(self.path+"/positions.csv")

        self.logger.info(f"writting {dictionary_to_write=}")

    def get_global_balance(self) -> dict:
        if self.ib_executor:
            ib_balance = self.ib_executor.get_netliq()
        else:
            ib_balance = 0.0
        
        if self.binance_executor:
            binance_balance = self.binance_executor.get_user_asset()
            binance_isolated_margin_balance = self.binance_executor.get_isolated_margin_account()
            binance_dollar_balance = self.binance_executor.convert_balances_to_dollars(binance_balance)
            binance_dollar_isolated_margin_balance = self.binance_executor.convert_isolated_margin_balance_to_dollars(binance_isolated_margin_balance)
        else:
            binance_dollar_balance = 0.0
            binance_dollar_isolated_margin_balance = 0.0

        if self.dydx_executor:
            dydx_balance = self.dydx_executor.get_account_equity()
        else:
            dydx_balance = 0.0

        if self.tradestation_executor:
            tradestation_balance = self.tradestation_executor.get_sum_of_balance()
        else:
            tradestation_balance = 0.0

        if self.bybit_executor:
            bybit_balance = self.bybit_executor.get_netliq()
        else:
            bybit_balance = 0.0

        if self.ethereum_executor:
            ethereum_balance = self.ethereum_executor.get_netliq(self.price_fetcher)
        else:
            ethereum_balance = 0.0        

        balances = {
            "binance_spot": binance_dollar_balance,
            "binance_margin": binance_dollar_isolated_margin_balance,
            "interactive_brokers": ib_balance,
            "tradestation": tradestation_balance,
            "dydx": dydx_balance,
            "bybit": bybit_balance,
            "ethereum_balance": ethereum_balance,
            "netliq": binance_dollar_balance + binance_dollar_isolated_margin_balance + ib_balance + tradestation_balance + dydx_balance + bybit_balance + ethereum_balance 
        }
        
        return balances 
    
    def get_global_positions(self) -> dict:
        if self.ib_executor:
            ib_positions: dict = self.ib_executor.get_positions()
        else:
            ib_positions = self.generate_empty_global_positions_dict()
        
        if self.binance_executor:
            binance_spot_positions = self.binance_executor.get_spot_positions()
            binance_margin_positions = self.binance_executor.get_margin_positions()
        else:
            binance_spot_positions = self.generate_empty_global_positions_dict()
            binance_margin_positions = self.generate_empty_global_positions_dict()

        if self.dydx_executor:
            dydx_positions = self.dydx_executor.get_positions()
        else:
            dydx_positions = self.generate_empty_global_positions_dict()

        if self.tradestation_executor:
            tradestation_positions = self.tradestation_executor.get_formated_positions()
        else:
            tradestation_positions = self.generate_empty_global_positions_dict()

        if self.bybit_executor:
            bybit_spot_positions = self.bybit_executor.get_positions("SPOT")
            bybit_derivative_positions = self.bybit_executor.get_positions("FUTURE")
        else:
            bybit_spot_positions = self.generate_empty_global_positions_dict()
            bybit_derivative_positions = self.generate_empty_global_positions_dict()

        if self.ethereum_executor:
            ethereum_position = self.ethereum_executor.get_position(self.price_fetcher)
        else:
            ethereum_position = self.generate_empty_global_positions_dict()

        positions = {
            "binance_spot": binance_spot_positions,
            "binance_margin": binance_margin_positions,
            "interactive_brokers": ib_positions,
            "tradestation": tradestation_positions,
            "dydx": dydx_positions,
            "bybit_spot": bybit_spot_positions,
            "bybit_derivatives" :bybit_derivative_positions,
            "ethereum_position": ethereum_position
        }
        
        return positions 

    @staticmethod
    def generate_empty_global_positions_dict() -> dict:
        return {
            "Symbol": [],
            "Multiplier": [],
            "Quantity": [],
            "Dollar Quantity": []
        }


if __name__ == '__main__':
    import argparse
    import asyncio
    import signal
    import logging
    import functools
    

    parser = argparse.ArgumentParser()
    parser.add_argument('--seconds', dest="seconds", type=str, nargs='+', default=10)
    parser.add_argument('--log-level', dest="log_level", type=str, nargs='+', default="INFO")
    parser.add_argument('--log-file', dest="log_file", type=str, nargs='+')
    args = parser.parse_args()
    numeric_level = getattr(logging, args.log_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError('Invalid log level: %s' % args.log_level)
    logging.basicConfig(level=numeric_level, format='%(levelname)s:%(asctime)s:%(message)s', filename=args.log_file[0])
    logger: logging.Logger = logging.getLogger()

    pwd = getpass("provide password for pk:")
    executor = AccountDataFetcher(pwd, logger)

    def ask_exit(signame, loop, logger):
        logger.info("got signal %s: exit" % signame)
        loop.stop()

    loop = asyncio.get_event_loop()

    print(f"{loop.is_running()=}")

    for signame in {'SIGINT', 'SIGTERM'}:
        loop.add_signal_handler(
            getattr(signal, signame),
            functools.partial(ask_exit, signame, loop, logger))

    async def periodic(seconds):
        while True:
            executor.write_positions_to_csv()
            await asyncio.sleep(seconds)

    def stop():
        task.cancel()

    task = loop.create_task(periodic(args.seconds))

    try:
        loop.run_until_complete(task)
    except asyncio.CancelledError:
        pass