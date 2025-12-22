import dataclasses
import json
import logging
import os
from datetime import datetime, timedelta

import aiohttp
import asyncio
from web3 import Web3

from account_data_fetcher.config.onchain_config import *
from account_data_fetcher.exchanges.coingecko.data_fetcher import DataFetcher as coingeckoDataFetcher
from account_data_fetcher.exchanges.exchange_base import ExchangeBase


@dataclasses.dataclass(init=True, eq=True, repr=True)
class balanceMetaData:
    timestamp: datetime
    balance_per_coin: dict[str, float]

    def is_acceptable_timestamp_detla(self, delta_in_seconds_allowed) -> bool:
        dt = datetime.utcnow()

        delta: timedelta = dt - self.timestamp

        return delta.total_seconds() < delta_in_seconds_allowed


@dataclasses.dataclass(init=True, eq=True, repr=True)
class priceMetaData:
    timestamp: datetime
    prices_per_coin: dict[str, dict[str, float]]

    def is_acceptable_timestamp_detla(self, delta_in_seconds_allowed) -> bool:
        dt = datetime.utcnow()

        delta: timedelta = dt - self.timestamp

        return delta.total_seconds() < delta_in_seconds_allowed


class DataFetcher(ExchangeBase):
    __URL = "https://public-node.rsk.co"
    __ADDRESS_BY_COIN = {"SOV": "0xEfC78FC7D48B64958315949279bA181C2114abbD"}
    __DECIMAL_BY_COIN = {"SOV": 18, "BTC": 18}
    __EXCHANGE = "Rsk"

    def __init__(self, session: aiohttp.ClientSession, delta_in_seconds_allowed: int = 30) -> None:
        super().__init__(exchange=self.__EXCHANGE, session=session)
        self.logger = logging.getLogger(__name__)
        self.price_meta_data: priceMetaData | None = None
        self.balance_meta_data: balanceMetaData | None = None
        self.w3 = Web3(Web3.HTTPProvider(self.__URL))
        self.contract_by_coin: dict = self.__get_contract_by_coin()
        self.address_of_interest: list = self.__get_address_of_interest()
        self.delta_in_seconds_allowed: int = delta_in_seconds_allowed
        self.price_fetcher: coingeckoDataFetcher = coingeckoDataFetcher(session)

    def __get_contract_by_coin(self) -> dict:
        contract_by_coin: dict = {}

        for coin in self.__ADDRESS_BY_COIN:
            contract_by_coin[coin] = self.w3.eth.contract(
                Web3.to_checksum_address(self.__ADDRESS_BY_COIN[coin]), abi=ERC_20_ABI
            )

        return contract_by_coin

    def __get_address_of_interest(self) -> list:
        """
        FORMAT OF meta_data.json:
        {"addresses_per_chain": {"Ethereum":[]}
        """

        current_directory = os.path.dirname(__file__)
        path = os.path.abspath(os.path.join(current_directory, "..", "..", "config", "onchain_meta_data.json"))

        with open(path) as f:
            return json.load(f)["addresses_per_chain"]["RSK"]

    async def fetch_balance(self) -> float:
        balance_by_coin: dict = await asyncio.to_thread(self.get_token_balances_by_coin)

        self.logger.debug(f"{balance_by_coin=}")

        netliq: float = 0

        await self.get_prices_for_coins(balance_by_coin)

        for coin, balance in balance_by_coin.items():
            if "USD" in coin:
                netliq += balance
            else:
                price = self.price_meta_data.prices_per_coin[coin]["usd"]
                netliq += float(balance) * float(price)

        return round(netliq, 3)

    def get_token_balances_by_coin(self, delta_in_seconds: int = 120) -> dict:
        if self.balance_meta_data:
            if self.balance_meta_data.is_acceptable_timestamp_detla(delta_in_seconds):
                return self.balance_meta_data.balance_per_coin

        balance_by_coin: dict[str, float] = {}

        balance_by_coin["BTC"] = self.__get_btc_balances()

        for coin, contract in self.contract_by_coin.items():
            for my_address in self.address_of_interest:
                result = contract.functions.balanceOf(Web3.to_checksum_address(my_address)).call()
                if coin in balance_by_coin:
                    balance_by_coin[coin.upper()] += int(result) / 10 ** self.__DECIMAL_BY_COIN[coin.upper()]
                else:
                    balance_by_coin[coin.upper()] = int(result) / 10 ** self.__DECIMAL_BY_COIN[coin.upper()]

        return balance_by_coin

    def __get_btc_balances(self) -> dict:
        balance: float = 0

        for my_address in self.address_of_interest:
            balance += self.w3.eth.get_balance(Web3.to_checksum_address(my_address)) / (
                10 ** self.__DECIMAL_BY_COIN["BTC"]
            )

        return balance

    async def fetch_positions(self) -> dict:
        balance_by_coin = await asyncio.to_thread(self.get_token_balances_by_coin)

        data_to_return = {"Symbol": [], "Multiplier": [], "Quantity": [], "Dollar Quantity": []}

        await self.get_prices_for_coins(balance_by_coin)

        for coin, balance in balance_by_coin.items():
            if "USD" in coin:
                data_to_return["Symbol"].append("USD")
                data_to_return["Multiplier"].append(1)
                data_to_return["Quantity"].append(round(balance, 3))
                data_to_return["Dollar Quantity"].append(round(balance, 3))
            else:
                price = self.price_meta_data.prices_per_coin[coin]["usd"]
                data_to_return["Symbol"].append(coin)
                data_to_return["Multiplier"].append(1)
                data_to_return["Quantity"].append(round(balance, 3))
                data_to_return["Dollar Quantity"].append(round(float(balance) * float(price), 3))

        return data_to_return

    async def get_prices_for_coins(self, balance_by_coin: dict[str, float]) -> None:
        if self.price_meta_data:
            if self.price_meta_data.is_acceptable_timestamp_detla(self.delta_in_seconds_allowed):
                return self.price_meta_data.prices_per_coin

        # fetching all but stablecoins usd denominated
        coins_to_fetch_price_for: list[str] = [coin for coin, _ in balance_by_coin.items() if "USD" not in coin]

        price_per_coin = await self.price_fetcher.get_prices(coins_to_fetch_price_for)

        dt = datetime.utcnow()

        self.price_meta_data = priceMetaData(timestamp=dt, prices_per_coin=price_per_coin)


if __name__ == "__main__":
    import asyncio

    async def _main():
        async with aiohttp.ClientSession() as session:
            executor = DataFetcher(session)
            balances = await executor.fetch_positions()
            print(f"{balances=}")

    asyncio.run(_main())
