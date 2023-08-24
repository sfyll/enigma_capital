import dataclasses
from datetime import datetime, timedelta
import logging
import os
from typing import Dict, Optional

from account_data_fetcher.kraken.kraken_connector import krakenApiConnector
from account_data_fetcher.exchange_base.exchange_base import ExchangeBase
from infrastructure.api_secret_getter import ApiMetaData

@dataclasses.dataclass(init=True, eq=True, repr=True)
class balanceMetaData:
    timestamp: datetime
    balance_per_coin: Dict[str, str]
    balance_per_coin_in_dollars: Dict[str, float]

    def is_acceptable_timestamp_detla(self, delta_in_seconds_allowed) -> bool:

            dt = datetime.utcnow()

            delta: timedelta = dt - self.timestamp

            return delta.total_seconds() < delta_in_seconds_allowed
    
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

class krakenDataFetcher(ExchangeBase):
    _EXCHANGE = "Kraken"
    _ENDPOINT = 'https://api.kraken.com'
    __KRAKEN_TICKER_TO_OTHERS = {
        'ZEUR': 'EUR',
        'USDC': 'USDC',
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
    def __init__(self, path: str, password: str, port_number: int, sub_account_name: Optional[str] = None) -> None:
        super().__init__(port_number, self._EXCHANGE)
        secrets: ApiMetaData = self.get_secrets(path, password, self._EXCHANGE)
        self.logger = logging.getLogger(__name__) 
        self._subaccount_name = sub_account_name
        self.kraken_connector = krakenApiConnector(api_key=secrets.key, api_secret=secrets.secret)
        self.balance_meta_data: Optional[balanceMetaData] = None

    def fetch_balance(self, accountType: str = "SPOT") -> float:
        self.__update_balances(accountType)
        return self.balance_meta_data.get_netliq()
    
    def __update_balances(self, account_type: str = "SPOT") -> None:
        if account_type == "SPOT":
            self.__check_and_update_balances()
        else:
            raise NotImplementedError

    def __check_and_update_balances(self, delta_in_seconds: int = 120) -> balanceMetaData:
        if self.balance_meta_data:
            if self.balance_meta_data.is_acceptable_timestamp_detla(delta_in_seconds):
                return self.balance_meta_data.balance_per_coin

        balance_per_coin = self.filter_balance_dict(self.kraken_connector.get_balance())
        
        balance_per_coin_dollar = self.get_balance_per_ticker_in_dollars(balance_per_coin)
        
        self.balance_meta_data: balanceMetaData = balanceMetaData(
            timestamp=datetime.utcnow(),
            balance_per_coin= balance_per_coin,
            balance_per_coin_in_dollars=balance_per_coin_dollar
        )
        
    def get_positions(self, balances: dict) -> dict:
        dollar_balances: dict = {}
        prices: dict = self.kraken_connector.get_ticker()

        for token, balance in balances.items():
            if token in ["USDC", "USDT", "DAI", "USD", "ZUSD"]:
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

    def get_positions(self, accountType: str = "SPOT") -> dict:
        self.__update_balances(accountType)
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

if __name__ == "__main__":
    from getpass import getpass
    pwd = getpass("provide password for pk:")
    executor = krakenDataFetcher(os.path.realpath(os.path.dirname(__file__)), pwd)
    print(executor.get_netliq())
    print(executor.get_positions())
