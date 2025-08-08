import asyncio
import dataclasses
from datetime import datetime, timedelta
import logging
import os
from typing import Dict, Optional

from account_data_fetcher.exchanges.kraken.kraken_connector import krakenApiConnector
from account_data_fetcher.exchanges.exchange_base import ExchangeBase
from infrastructure.api_secret_getter import ApiMetaData
import aiohttp

@dataclasses.dataclass(init=True, eq=True, repr=True)
class balanceMetaData:
    balance_per_coin: Dict[str, str]
    balance_per_coin_in_dollars: Dict[str, float]
    
    def get_netliq(self) -> float:
        netliq: float = 0.0
        for _, balance in self.balance_per_coin_in_dollars.items():
            netliq += balance
        return round(netliq, 2)

    def get_position(self) -> dict:
        
        data_to_return = {
            "Symbol": [],
            "Multiplier": [],
            "Quantity": [],
            "Dollar Quantity": []
        }

        for coin, balance in self.balance_per_coin.items():
            
            quantity = round(float(balance),3)
            dollar_quantity = round(self.balance_per_coin_in_dollars[coin], 3)
            
            if dollar_quantity > 100:
                data_to_return["Symbol"].append(coin if coin not in ["USDC", "USDT", "DAI", "USD", "ZUSD"] else "USD")
                data_to_return["Multiplier"].append(1)
                data_to_return["Quantity"].append(quantity)
                data_to_return["Dollar Quantity"].append(dollar_quantity)

        return data_to_return

class DataFetcher(ExchangeBase):
    _EXCHANGE = "Kraken"
    _ENDPOINT = 'https://api.kraken.com'
    __KRAKEN_TICKER_TO_OTHERS = {
        'ZEUR': 'EUR',
        'USDC': 'USD',
        'GNO': 'GNO',
        'XXBT': 'BTC',
        'XICN': 'ICN',
        'XXRP': 'XRP',
        'ZUSD': 'USD',
        'XETH': 'ETH',
        'ZGBP': 'GBP',
        'XETC': 'ETC',
        'XDAO': 'DAO',
        'XREP': 'REP',
        'ADA': 'ADA',
        'XXLM': 'XLM',
        'BCH': 'BCH',
        'ETHW': 'ETHW',
        'QTUM': 'QTUM',
        'IMX': 'IMX'
    }
    __INTERNAL_KRAKEN_MAP ={
        "XXBT":"XBTC",
    }
    __NO_PRICE_MAP = ["ZGBP", "ZEUR"]
    def __init__(
        self, 
        secrets: ApiMetaData, 
        session: aiohttp.ClientSession,
        sub_account_name: Optional[str] = None
    ) -> None:
        """
        Initializes the Kraken DataFetcher.

        Args:
            secrets (ApiMetaData): The API keys and secrets for Kraken.
            session (aiohttp.ClientSession): The shared HTTP client session.
            output_queue (asyncio.Queue): The queue to send fetched data to.
            fetch_frequency (int): The interval in seconds between data fetches.
            sub_account_name (Optional[str], optional): A specific sub-account name if any.
        """
        super().__init__(
            exchange=self._EXCHANGE, 
            session=session, 
        )
        self.logger = logging.getLogger(__name__) 
        self._subaccount_name = sub_account_name
        self.kraken_connector = krakenApiConnector(api_key=secrets.key, api_secret=secrets.secret, session=session)
        self.balance_meta_data: Optional[balanceMetaData] = None

    async def fetch_balance(self, accountType: str = "SPOT") -> float:
        await self.__update_balances(accountType)
        return self.balance_meta_data.get_netliq()
    
    async def __update_balances(self, account_type: str = "SPOT") -> None:
        if account_type == "SPOT":
            await self.__check_and_update_balances()
        else:
            raise NotImplementedError

    async def __check_and_update_balances(self, delta_in_seconds: int = 120) -> balanceMetaData:
        balance_per_coin = self.filter_balance_dict(await self.kraken_connector.get_balance())
        
        balance_per_coin_dollar = await self.get_balance_per_ticker_in_dollars(balance_per_coin)
        
        self.balance_meta_data: balanceMetaData = balanceMetaData(
            balance_per_coin= balance_per_coin,
            balance_per_coin_in_dollars=balance_per_coin_dollar
        )
        
    async def get_balance_per_ticker_in_dollars(self, balances: dict) -> dict:
        dollar_balances: dict = {}
        prices: dict = await self.kraken_connector.get_ticker()

        for token, balance in balances.items():
            if token in ["USDC", "USDT", "DAI", "USD", "ZUSD"]:
                if "USD" in dollar_balances:
                    dollar_balances["USD"] += float(balance)
                else:
                    dollar_balances["USD"] = float(balance)
            elif token in self.__INTERNAL_KRAKEN_MAP:
                dollar_balances[self.__KRAKEN_TICKER_TO_OTHERS[token]] = float(balance) * self.__get_coin_price(self.__INTERNAL_KRAKEN_MAP[token], prices)
            else:
                if token not in self.__NO_PRICE_MAP:
                    dollar_balances[token] = float(balance) * self.__get_coin_price(token, prices)

        return self.filter_balance_dict(dollar_balances)

    def filter_balance_dict(self, balances) -> dict:
        key_to_modify: set = set()
        key_to_erase: set = set()

        for key, balance in balances.items():
            if key in self.__KRAKEN_TICKER_TO_OTHERS:
                key_to_modify.add(key)
            if key in self.__NO_PRICE_MAP or  float(balance) < 0.001:
                key_to_erase.add(key)
        
        for key in key_to_erase:
            balances.pop(key)                

        for key in key_to_modify:
            if key in balances:
                balance = balances.pop(key)
                balances[self.__KRAKEN_TICKER_TO_OTHERS[key]] = balance

        return balances

    async def fetch_positions(self, accountType: str = "SPOT") -> dict:
        return self.balance_meta_data.get_position()
    
    @staticmethod
    def __get_coin_price(coin: str, price_dict: dict) -> float:
        try:
            return float(price_dict[coin+"USD"]["c"][0])
        except KeyError:
            pass

        try:
            return float(price_dict[coin+"USDT"]["c"][0])
        except KeyError:
            pass

        try:
            return float(price_dict[coin+"USDC"]["c"][0])
        except KeyError:
            pass
