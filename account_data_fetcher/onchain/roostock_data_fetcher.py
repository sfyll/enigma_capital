import dataclasses
from datetime import datetime, timedelta
import json
import logging
import os
from typing import Callable, Dict, List, Optional

from web3 import Web3

from account_data_fetcher.onchain.config import *
from utilities.account_data_fetcher_base import accountFetcherBase

@dataclasses.dataclass(init=True, eq=True, repr=True)
class balanceMetaData:
    timestamp: datetime
    balance_per_coin: Dict[str, float]

    def is_acceptable_timestamp_detla(self, delta_in_seconds_allowed) -> bool:

            dt = datetime.utcnow()

            delta: timedelta = dt - self.timestamp

            return delta.total_seconds() < delta_in_seconds_allowed


@dataclasses.dataclass(init=True, eq=True, repr=True)
class priceMetaData:
    timestamp: datetime
    prices_per_coin: Dict[str, Dict[str, float]]

    def is_acceptable_timestamp_detla(self, delta_in_seconds_allowed) -> bool:

            dt = datetime.utcnow()

            delta: timedelta = dt - self.timestamp 
                
            return delta.total_seconds() < delta_in_seconds_allowed

class rootStockDataFetcher(accountFetcherBase):
    __URL = "https://public-node.rsk.co"
    __ADDRESS_BY_COIN = {"SOV":"0xEfC78FC7D48B64958315949279bA181C2114abbD"}
    __DECIMAL_BY_COIN = {"SOV": 18, "BTC": 18}

    def __init__(self, path: str, password: str, delta_in_seconds_allowed: int = 30) -> None:
        super().__init__(path, password)
        self.logger = logging.getLogger(__name__) 
        self.price_meta_data: Optional[priceMetaData] = None
        self.balance_meta_data: Optional[balanceMetaData] = None
        self.w3 = Web3(Web3.HTTPProvider(self.__URL))
        self.contract_by_coin: dict = self.__get_contract_by_coin()
        self.address_of_interest: list = self.__get_address_of_interest(path)
        self.delta_in_seconds_allowed: int = delta_in_seconds_allowed

    def __get_contract_by_coin(self) -> dict:
        contract_by_coin: dict = {}
        
        for coin in self.__ADDRESS_BY_COIN:
            contract_by_coin[coin] = self.w3.eth.contract(Web3.toChecksumAddress(self.__ADDRESS_BY_COIN[coin]), abi=ERC_20_ABI)
        
        return contract_by_coin

    def __get_address_of_interest(self, path: str) -> list:
        """
        FORMAT OF meta_data.json:
        {"addresses_per_chain": {"Ethereum":[]}
        """

        path = path + "/onchain/meta_data.json"

        with open(path, "r") as f:
            return json.load(f)['addresses_per_chain']["RSK"]
            

    def get_netliq(self, get_price_from_coingecko: Callable) -> float:
        balance_by_coin: dict = self.get_token_balances_by_coin()

        self.logger.debug(f"{balance_by_coin=}")
        
        netliq: float = 0
        
        self.get_prices_for_coins(balance_by_coin, get_price_from_coingecko)
        
        for coin, balance in balance_by_coin.items():
            if "USD" in coin:
                netliq += balance
            else:
                price = self.price_meta_data.prices_per_coin[coin]["usd"]
                netliq += float(balance) * float(price)
        
        return round(netliq,3)

    def get_token_balances_by_coin(self, delta_in_seconds: int = 120) -> dict:
        if self.balance_meta_data:
            if self.balance_meta_data.is_acceptable_timestamp_detla(delta_in_seconds):
                return self.balance_meta_data.balance_per_coin
            
        balance_by_coin: dict[str, float] = {}

        balance_by_coin["BTC"] = self.__get_btc_balances()

        for coin, contract in self.contract_by_coin.items():
            for my_address in self.address_of_interest: 
                    result = contract.functions.balanceOf(Web3.toChecksumAddress(my_address)).call()
                    if coin in balance_by_coin.keys():
                        balance_by_coin[coin.upper()] += int(result) / 10 ** self.__DECIMAL_BY_COIN[coin.upper()]
                    else:
                        balance_by_coin[coin.upper()] = int(result) / 10 ** self.__DECIMAL_BY_COIN[coin.upper()]

        return balance_by_coin

    def __get_btc_balances(self) -> dict:
        balance: float = 0
        
        for my_address in self.address_of_interest:
            balance +=  self.w3.eth.get_balance(Web3.toChecksumAddress(my_address)) / (10 ** self.__DECIMAL_BY_COIN["BTC"])

        return balance
    
    def get_positions(self, get_price_from_coingecko: Callable) -> dict:
        balance_by_coin = self.get_token_balances_by_coin()

        data_to_return = {
            "Symbol": [],
            "Multiplier": [],
            "Quantity": [],
            "Dollar Quantity": []
        } 


        self.get_prices_for_coins(balance_by_coin, get_price_from_coingecko)

        for coin, balance in balance_by_coin.items():
            if "USD" in coin:
                data_to_return["Symbol"].append("USD")
                data_to_return["Multiplier"].append(1)
                data_to_return["Quantity"].append(round(balance, 3))
                data_to_return["Dollar Quantity"].append(round(balance,3 ))
            else:
                price = self.price_meta_data.prices_per_coin[coin]["usd"]
                data_to_return["Symbol"].append(coin)
                data_to_return["Multiplier"].append(1)
                data_to_return["Quantity"].append(round(balance, 3))
                data_to_return["Dollar Quantity"].append(round(float(balance) * float(price),3))
        
        return data_to_return

    def get_prices_for_coins(self, balance_by_coin: Dict[str, float], get_prices_from_coingecko: Callable) -> None:
        if self.price_meta_data:
            if self.price_meta_data.is_acceptable_timestamp_detla(self.delta_in_seconds_allowed):
                return self.price_meta_data.prices_per_coin
        
        #fetching all but stablecoins usd denominated
        coins_to_fetch_price_for: List[str] = [coin for coin, _ in balance_by_coin.items() if "USD" not in coin]

        price_per_coin = get_prices_from_coingecko(coins_to_fetch_price_for)

        dt = datetime.utcnow()

        self.price_meta_data = priceMetaData(
            timestamp=dt,
            prices_per_coin=price_per_coin
        )


if __name__ == '__main__':
    from getpass import getpass
    from account_data_fetcher.offchain.coingecko_data_fetcher import coingeckoDataFetcher
    log_filename= os.path.expanduser("~") + "/log/test.py"
    numeric_level = getattr(logging, "DEBUG", None)
    logging.basicConfig(level=numeric_level, format='%(levelname)s:%(asctime)s:%(message)s', filename=log_filename) 
    logger: logging.Logger = logging.getLogger()
    pwd = getpass("provide password for pk:")
    current_path = os.path.realpath(os.path.dirname(__file__))
    parent_path = os.path.dirname(current_path)
    price_fetcher = coingeckoDataFetcher().get_prices
    executor = rootStockDataFetcher(parent_path, pwd)
    # print(executor.get_token_balances_by_coin())
    # print(executor.get_token_balances_by_coin_single_calls())
    # balance = executor.get_netliq(price_fetcher)
    # print(f"{balance=}")
    balances = executor.get_token_balances_by_coin(price_fetcher)
    print(f"{balances=}")
    # print(len(calls))
    # print(len(addresses))
    # print(balances)

