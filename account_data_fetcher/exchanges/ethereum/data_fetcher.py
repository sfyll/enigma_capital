import dataclasses
from datetime import datetime, timedelta
import json
import logging
import os
from typing import Callable, Dict, List, Optional

from web3 import Web3

from account_data_fetcher.config.onchain_config import *
from account_data_fetcher.exchanges.exchange_base import ExchangeBase
from account_data_fetcher.exchanges.coingecko.data_fetcher  import DataFetcher as CoingeckoDataFetcher
from infrastructure.api_secret_getter import ApiMetaData

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

#TODO: Batch calls via multicall contracts + use helios lightweight client (need to fix eth_call loops, broken atm)
class DataFetcher(ExchangeBase):
    __URL = "https://eth-mainnet.g.alchemy.com/v2/"
    __EXCHANGE = "Ethereum"

    def __init__(self, secrets: ApiMetaData, port_number: int, delta_in_seconds_allowed: int = 30) -> None:
        super().__init__(port_number, self.__EXCHANGE)
        self.logger = logging.getLogger(__name__) 
        self.price_meta_data: Optional[priceMetaData] = None
        self.balance_meta_data: Optional[balanceMetaData] = None
        self.__ADDRESS_BY_COIN, self.__DECIMAL_BY_COIN = self.__get_coin_configs()
        self.w3 = Web3(Web3.HTTPProvider(self.__URL+secrets.key))
        self.contract_by_coin: dict = self.__get_contract_by_coin()
        self.address_of_interest: list = self.__get_address_of_interest()
        self.delta_in_seconds_allowed: int = delta_in_seconds_allowed
        self.price_fetcher: CoingeckoDataFetcher = CoingeckoDataFetcher()
    
    @staticmethod
    def __get_coin_configs():
        address_by_coin: dict = {}
        decimal_by_coin: dict = {}

        current_directory = os.path.dirname(__file__)
        path = os.path.abspath(os.path.join(current_directory, '..', '..', 'config', 'coin_meta_data.json'))
        
        with open(path, "r") as f:
            configs = json.load(f)

            for coin, config in configs.items():
                if 'address' in config:
                    address_by_coin[coin] = config['address']
                decimal_by_coin[coin] = config['decimals']

        return address_by_coin, decimal_by_coin

    def __get_contract_by_coin(self) -> dict:
        contract_by_coin: dict = {}
        
        for coin in self.__ADDRESS_BY_COIN:
            contract_by_coin[coin] = self.w3.eth.contract(Web3.to_checksum_address(self.__ADDRESS_BY_COIN[coin]), abi=ERC_20_ABI)
        
        return contract_by_coin

    def __get_address_of_interest(self) -> list:
        """FORMAT OF meta_data.json:
        {"addresses": [] }"""

        current_directory = os.path.dirname(__file__)
        path = os.path.abspath(os.path.join(current_directory, '..', '..', 'config', 'onchain_meta_data.json'))

        with open(path, "r") as f:
            return json.load(f)['addresses_per_chain']['Ethereum']
            
    def __get_total_balance_by_coin(self, delta_in_seconds: int = 120) -> float:
        if self.balance_meta_data:
            if self.balance_meta_data.is_acceptable_timestamp_detla(delta_in_seconds):
                return self.balance_meta_data.balance_per_coin

        eth_balance_gwei: float = self.__get_eth_balances()

        token_balances: dict[str, float] = self.__get_token_balances_by_coin()

        token_balances["ETH"] = eth_balance_gwei

        self.balance_meta_data = balanceMetaData(
            timestamp=datetime.utcnow(),
            balance_per_coin=token_balances
        )

        return token_balances

    def __get_eth_balances(self) -> dict:
        balance: float = 0
        
        for my_address in self.address_of_interest:
            balance +=  self.w3.eth.get_balance(Web3.to_checksum_address(my_address)) / (10 ** self.__DECIMAL_BY_COIN["ETH"])

        return balance
    
    def __get_token_balances_by_coin(self) -> dict:
        balance_by_coin: dict[str, float] = {}

        multi_call_result = self.__query_multi_call()

        counter: int = 0
        for coin, _ in self.__ADDRESS_BY_COIN.items():
            for _ in self.address_of_interest:
               balance = self.w3.to_int(multi_call_result[counter])
               if coin in balance_by_coin and int(balance) > 0:
                   balance_by_coin[coin] += balance / 10 ** self.__DECIMAL_BY_COIN[coin.upper()]
               elif coin not in balance_by_coin and int(balance) > 0:
                   balance_by_coin[coin] = balance / 10 ** self.__DECIMAL_BY_COIN[coin.upper()]
               counter += 1

        return balance_by_coin
    
    def __query_multi_call(self) -> List[bytes]:
        calls: List[tuple] = []
        
        for coin, token_address in self.__ADDRESS_BY_COIN.items():
            for address in self.address_of_interest:
                balance_call_data = self.contract_by_coin[coin].encodeABI(fn_name='balanceOf', args=[Web3.to_checksum_address(address)])  
                calls.append((token_address, balance_call_data))
                
        multicall_contract = self.w3.eth.contract(address=MULTICALL_3_ADDRESS, abi=MULTICALL3_ABI)
        return multicall_contract.functions.aggregate(tuple(calls)).call()[1]

    def fetch_balance(self, accountType = "SPOT") -> float:
        balance_by_coin: dict = self.__get_total_balance_by_coin()
        self.logger.debug(f"{balance_by_coin=}")
        
        netliq: float = 0
        
        self.get_prices_for_coins(balance_by_coin)
        
        for coin, balance in balance_by_coin.items():
            if "USD" in coin:
                netliq += balance
            else:
                price = self.price_meta_data.prices_per_coin[coin]["usd"]
                netliq += float(balance) * float(price)
        
        return round(netliq,3)
    
    def fetch_positions(self) -> dict:
        balance_by_coin = self.__get_total_balance_by_coin()

        data_to_return = {
            "Symbol": [],
            "Multiplier": [],
            "Quantity": [],
            "Dollar Quantity": []
        } 


        self.get_prices_for_coins(balance_by_coin) 

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

    def get_prices_for_coins(self, balance_by_coin: Dict[str, float]) -> None:
        if self.price_meta_data:
            if self.price_meta_data.is_acceptable_timestamp_detla(self.delta_in_seconds_allowed):
                return self.price_meta_data.prices_per_coin
        
        #fetching all but stablecoins usd denominated
        coins_to_fetch_price_for: List[str] = [coin for coin, _ in balance_by_coin.items() if "USD" not in coin]

        price_per_coin = self.price_fetcher.get_prices(coins_to_fetch_price_for)

        dt = datetime.utcnow()

        self.price_meta_data = priceMetaData(
            timestamp=dt,
            prices_per_coin=price_per_coin
        )

    #Below is to debug helios client
    def encode_token_balances_by_coin_calls(self):
        calls: List[str] = []
        addresses: List[str] = []
        for coin, contract in self.contract_by_coin.items():
            for my_address in ["0x9527465642a7015738ef24201eec1644f3755670", "0x0f366a411dc9f8a1611cad614d8f451436fc4f4b",
                               "0x630276c20064545c06360bbd3ef48025abe3316a", "0xbe6e784ad98581be1077ddf630205ac30ce8128b",
                               "0xa48aa5c696357f29a187fb408f1a5c9ecab445c5"]:
                calls.append(contract.encodeABI(fn_name="balanceOf", args=[Web3.to_checksum_address(my_address)]))
                addresses.append(Web3.to_checksum_address(self.__ADDRESS_BY_COIN[coin]))

        return calls, addresses
    
    def get_token_balances_by_coin_single_calls(self) -> dict:
        balance_by_coin: dict[str, float] = {}

        for coin, contract in self.contract_by_coin.items():
            for my_address in self.address_of_interest: 
                    result = contract.functions.balanceOf(Web3.to_checksum_address(my_address)).call()
                    if coin in balance_by_coin.keys():
                        balance_by_coin[coin.upper()] += int(result) / 10 ** self.__DECIMAL_BY_COIN[coin.upper()]
                    else:
                        balance_by_coin[coin.upper()] = int(result) / 10 ** self.__DECIMAL_BY_COIN[coin.upper()]

        return balance_by_coin


if __name__ == '__main__':
    from getpass import getpass
    from account_data_fetcher.offchain.coingecko_data_fetcher import coingeckoDataFetcher
    log_filename= os.path.expanduser("~") + "/log/test.py"
    numeric_level = getattr(logging, "DEBUG", None)
    logging.basicConfig(level=numeric_level, format='%(levelname)s:%(asctime)s:%(message)s', filename=log_filename) 
    logger: logging.Logger = logging.getLogger()
    pwd = getpass("provide password for pk:")
    current_path = os.path.realpath(os.path.dirname(__file__))
    price_fetcher = coingeckoDataFetcher().get_prices
    executor = ethereumDataFetcher(current_path, pwd)
    # print(executor.get_token_balances_by_coin())
    # print(executor.get_token_balances_by_coin_single_calls())
    # balance = executor.get_netliq(price_fetcher)
    # print(f"{balance=}")
    calls, addresses = executor.encode_token_balances_by_coin_calls()
    print(f"{calls=}")
    print(f"{addresses=}")
    # print(len(calls))
    # print(len(addresses))
    # print(balances)

