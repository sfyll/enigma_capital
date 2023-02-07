import dataclasses
from datetime import timezone, datetime, timedelta
import logging
import os
from typing import Callable, Dict, List, Optional

from utilities.account_data_fetcher_base import accountFetcherBase
from utilities.request_handler import requestHandler


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


class ethereumDataFetcher(accountFetcherBase):
    __URL = "https://api.etherscan.io/api"
    __EXCHANGE = "Etherscan"
    __COIN_BY_ADDRESSES = {"USDC":"0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48", "USDT": "0xdAC17F958D2ee523a2206206994597C13D831ec7", "AMPL":"0xD46bA6D942050d489DBd938a2C909A5d5039A161", "DAI": "0x6B175474E89094C44Da98b954EedeAC495271d0F", "DYDX": "0x92D6C1e31e14520e676a687F0a93788B716BEff5"}
    __DECIMAL_BY_COIN = {"USDC": 6, "USDT": 6, "AMPL": 9, "DAI": 18, "DYDX": 18}

    def __init__(self, path: str, password: str, delta_in_seconds_allowed: int = 30) -> None:
        super().__init__(path, password)
        self.logger = logging.getLogger(__name__) 
        self.__request_handler: requestHandler = requestHandler()
        self.price_meta_data: Optional[priceMetaData] = None
        self.balance_meta_data: Optional[balanceMetaData] = None
        self.delta_in_seconds_allowed = delta_in_seconds_allowed #strong api limitations, better not to over query
            
    def __get_total_balance_by_coin(self, delta_in_seconds: int = 120) -> float:
        if self.balance_meta_data:
            if self.balance_meta_data.is_acceptable_timestamp_detla(delta_in_seconds):
                return self.balance_meta_data.balance_per_coin

        eth_balances: Dict[str, int] = self.__get_eth_balances()
        
        eth_balance_gwei: float = 0
        
        for eth_balance in eth_balances:
            eth_balance_gwei += int(eth_balance["balance"]) / 10 ** 18

        token_balances: dict[str, float] = self.__get_token_balances_by_coin()

        token_balances["ETH"] = eth_balance_gwei

        self.balance_meta_data = balanceMetaData(
            timestamp=datetime.utcnow(),
            balance_per_coin=token_balances
        )

        return token_balances

    def __get_eth_balances(self) -> dict:

        url_endpoint = self.__request_handler.api_module(url_base=self.__URL, url_extension='module=account')
        
        params: dict = {
            "action":"balancemulti",
            "address": ','.join(self.api_meta_data[self.__EXCHANGE].other_fields["Addresses"]),
            'tag':"latest",
            "apikey": self.api_meta_data[self.__EXCHANGE].key
        }

        response = self.__request_handler.handle_requests(
                   url=url_endpoint,
                   method="get",
                   args=params,
        )

        return response["result"]
    
    def __get_token_balances_by_coin(self) -> dict:
        url_endpoint = self.__request_handler.api_module(url_base=self.__URL, url_extension='module=account')

        balance_by_coin: Dict[str, float] = {}
        
        params: dict = {
            "action":"tokenbalance",
            "contractaddress": None,
            "address": None,
            'tag':"latest",
            "apikey": self.api_meta_data[self.__EXCHANGE].key
        }

        for coin, address in self.__COIN_BY_ADDRESSES.items():
            for my_address in self.api_meta_data[self.__EXCHANGE].other_fields["Addresses"]: #free API issues
                params["contractaddress"] = address
                params["address"] = my_address
                result = self.__request_handler.handle_requests(
                    url=url_endpoint,
                    method="get",
                    args=params,
                    )["result"]
                if result:
                    if coin in balance_by_coin.keys():
                        balance_by_coin[coin.upper()] += int(result) / 10 ** self.__DECIMAL_BY_COIN[coin.upper()]
                    else:
                        balance_by_coin[coin.upper()] = int(result)

        return balance_by_coin

    def get_netliq(self, get_price_from_coingecko: Callable) -> float:
        balance_by_coin: dict = self.__get_total_balance_by_coin()
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
    
    def get_position(self, get_price_from_coingecko: Callable) -> dict:
        balance_by_coin = self.__get_total_balance_by_coin()

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
    log_filename= os.path.expanduser("~") + "/log/test.py"
    numeric_level = getattr(logging, "DEBUG", None)
    logging.basicConfig(level=numeric_level, format='%(levelname)s:%(asctime)s:%(message)s', filename=log_filename) 
    logger: logging.Logger = logging.getLogger()
    pwd = getpass("provide password for pk:")
    current_path = os.path.realpath(os.path.dirname(__file__))
    executor = ethereumDataFetcher(current_path, pwd)
    balances = executor.get_netliq()
    print(balances)

