import asyncio
import dataclasses
import functools
import json
import logging
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import aiohttp
from web3 import Web3

from account_data_fetcher.config.onchain_config import *
from account_data_fetcher.exchanges.exchange_base import ExchangeBase
from account_data_fetcher.exchanges.coingecko.data_fetcher  import DataFetcher as CoingeckoDataFetcher
from infrastructure.api_secret_getter import ApiMetaData

@dataclasses.dataclass(init=True, eq=True, repr=True)
class balanceMetaData:
    timestamp: datetime
    balance_per_coin: Dict[str, float]

    def is_acceptable_timestamp_delta(self, delta_in_seconds_allowed) -> bool:
        delta: timedelta = datetime.utcnow() - self.timestamp
        return delta.total_seconds() < delta_in_seconds_allowed

@dataclasses.dataclass(init=True, eq=True, repr=True)
class priceMetaData:
    timestamp: datetime
    prices_per_coin: Dict[str, Dict[str, float]]

    def is_acceptable_timestamp_delta(self, delta_in_seconds_allowed) -> bool:
        delta: timedelta = datetime.utcnow() - self.timestamp
        return delta.total_seconds() < delta_in_seconds_allowed

class DataFetcher(ExchangeBase):
    __URL = "https://eth-mainnet.g.alchemy.com/v2/"
    __EXCHANGE = "Ethereum"

    def __init__(self, secrets: ApiMetaData, session: aiohttp.ClientSession, output_queue: asyncio.Queue, fetch_frequency: int) -> None:
        super().__init__(
            exchange=self.__EXCHANGE,
            session=session,
            output_queue=output_queue,
            fetch_frequency=fetch_frequency
        )
        self.logger = logging.getLogger(__name__)
        self.price_meta_data: Optional[priceMetaData] = None
        self.balance_meta_data: Optional[balanceMetaData] = None
        
        self.__ADDRESS_BY_COIN, self.__DECIMAL_BY_COIN = self.__get_coin_configs()
        self.w3 = Web3(Web3.HTTPProvider(self.__URL + secrets.key))
        self.contract_by_coin: dict = self.__get_contract_by_coin()
        self.address_of_interest: list = self.__get_address_of_interest()
        
        self.price_fetcher = CoingeckoDataFetcher(session)

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
    
    async def _run_in_executor(self, func, *args, **kwargs):
        """Helper to run a synchronous function in the default thread pool."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, functools.partial(func, *args, **kwargs))

    async def fetch_balance(self, accountType="SPOT") -> float:
        """Asynchronously fetches the total USD value of all assets."""
        balance_by_coin: dict = await self.__get_total_balance_by_coin_async()
        if not balance_by_coin:
            return 0.0

        await self.get_prices_for_coins_async(balance_by_coin)
        
        netliq: float = 0
        for coin, balance in balance_by_coin.items():
            if "USD" in coin.upper():
                netliq += balance
            elif self.price_meta_data and coin in self.price_meta_data.prices_per_coin:
                price = self.price_meta_data.prices_per_coin[coin].get("usd", 0)
                netliq += float(balance) * float(price)
        
        return round(netliq, 3)

    async def fetch_positions(self) -> dict:
        """Asynchronously fetches detailed positions of all assets."""
        balance_by_coin = await self.__get_total_balance_by_coin_async()

        data_to_return = {"Symbol": [], "Multiplier": [], "Quantity": [], "Dollar Quantity": []}
        if not balance_by_coin:
            return data_to_return

        await self.get_prices_for_coins_async(balance_by_coin)

        for coin, balance in balance_by_coin.items():
            dollar_quantity = 0
            price = 0
            if "USD" in coin.upper():
                dollar_quantity = balance
            elif self.price_meta_data and coin in self.price_meta_data.prices_per_coin:
                price = self.price_meta_data.prices_per_coin[coin].get("usd", 0)
                dollar_quantity = float(balance) * float(price)

            data_to_return["Symbol"].append(coin)
            data_to_return["Multiplier"].append(1)
            data_to_return["Quantity"].append(round(balance, 3))
            data_to_return["Dollar Quantity"].append(round(dollar_quantity, 3))
        
        return data_to_return

    async def get_prices_for_coins_async(self, balance_by_coin: Dict[str, float]) -> None:
        """Asynchronously gets prices for coins, using cached data if fresh."""
        if self.price_meta_data and self.price_meta_data.is_acceptable_timestamp_delta(self.fetch_frequency):
            return

        coins_to_fetch = [coin for coin, balance in balance_by_coin.items() if "USD" not in coin.upper() and balance > 0]

        price_per_coin = await self.price_fetcher.get_prices(coins_to_fetch)
        self.price_meta_data = priceMetaData(timestamp=datetime.utcnow(), prices_per_coin=price_per_coin)

    async def __get_total_balance_by_coin_async(self) -> dict:
        """Async wrapper for fetching balances, respects caching."""
        if self.balance_meta_data and self.balance_meta_data.is_acceptable_timestamp_delta(self.fetch_frequency):
            return self.balance_meta_data.balance_per_coin
        
        balances = await self._run_in_executor(self.__get_total_balance_by_coin_sync)
        if balances:
            self.balance_meta_data = balanceMetaData(timestamp=datetime.utcnow(), balance_per_coin=balances)
        return balances

    def __get_total_balance_by_coin_sync(self) -> dict:
        """Original synchronous logic for fetching all balances."""
        try:
            eth_balance = self.__get_eth_balances_sync()
            token_balances = self.__get_token_balances_by_coin_sync()
            token_balances["ETH"] = eth_balance
            return token_balances
        except Exception as e:
            self.logger.error(f"Failed to fetch on-chain balances: {e}", exc_info=True)
            return {}

    def __get_eth_balances_sync(self) -> float:
        """Original synchronous logic for fetching native ETH balance."""
        balance: float = 0
        for my_address in self.address_of_interest:
            balance += self.w3.eth.get_balance(Web3.to_checksum_address(my_address))
        return balance / (10 ** self.__DECIMAL_BY_COIN["ETH"])
    
    def __get_token_balances_by_coin_sync(self) -> dict:
        """Original synchronous logic for fetching ERC20 token balances using multicall."""
        balance_by_coin: dict = {}
        multi_call_result = self.__query_multi_call_sync()
        counter = 0
        for coin, _ in self.__ADDRESS_BY_COIN.items():
            for _ in self.address_of_interest:
                balance_wei = self.w3.to_int(multi_call_result[counter])
                if balance_wei > 0:
                    balance = balance_wei / (10 ** self.__DECIMAL_BY_COIN[coin.upper()])
                    balance_by_coin[coin] = balance_by_coin.get(coin, 0) + balance
                counter += 1
        return balance_by_coin
    
    def __query_multi_call_sync(self) -> List[bytes]:
        """Original synchronous multicall query logic."""
        calls = []
        for coin, token_address in self.__ADDRESS_BY_COIN.items():
            for address in self.address_of_interest:
                balance_call_data = self.contract_by_coin[coin].encode_abi('balanceOf', args=[Web3.to_checksum_address(address)])
                calls.append((Web3.to_checksum_address(token_address), balance_call_data))
        
        multicall_contract = self.w3.eth.contract(address=Web3.to_checksum_address(MULTICALL_3_ADDRESS), abi=MULTICALL3_ABI)
        return multicall_contract.functions.aggregate(tuple(calls)).call()[1]
            
    #Below is to debug helios client
    def encode_token_balances_by_coin_calls(self):
        calls: List[str] = []
        addresses: List[str] = []
        for coin, contract in self.contract_by_coin.items():
            for my_address in ["0x9527465642a7015738ef24201eec1644f3755670", "0x0f366a411dc9f8a1611cad614d8f451436fc4f4b",
                               "0x630276c20064545c06360bbd3ef48025abe3316a", "0xbe6e784ad98581be1077ddf630205ac30ce8128b",
                               "0xa48aa5c696357f29a187fb408f1a5c9ecab445c5"]:
                calls.append(contract.encode_abi("balanceOf", args=[Web3.to_checksum_address(my_address)]))
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
