import os
import requests
from typing import Dict, Callable
import logging

from utilities.account_data_fetcher_base import accountFetcherBase


class ethereumDataFetcher(accountFetcherBase):
    __URL = "https://api.etherscan.io/api"
    __EXCHANGE = "Etherscan"
    __COIN_BY_ADDRESSES = {"USDC":"0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48", "USDT": "0xdAC17F958D2ee523a2206206994597C13D831ec7", "AMPL":"0xD46bA6D942050d489DBd938a2C909A5d5039A161", "DAI": "0x6B175474E89094C44Da98b954EedeAC495271d0F", "DYDX": "0x92D6C1e31e14520e676a687F0a93788B716BEff5"}
    __DECIMAL_BY_COIN = {"USDC": 6, "USDT": 6, "AMPL": 9, "DAI": 18, "DYDX": 18}

    def __init__(self, path: str, password: str) -> None:
        super().__init__(path, password)
        self.logger = logging.getLogger(__name__) 

    def _api_module(self, url: str) -> str:
        """Creates an API URL.
        Overview:
        ----  
        Convert relative endpoint (e.g., 'quotes') to full API endpoint.
        Arguments:
        ----
        url (str): The URL that needs conversion to a full endpoint URL.
        Returns:
        ---
        (str): A full URL.
        """

        return '?'.join([self.__URL, url])

    def _handle_requests(self, url: str, method: str, args: dict = None) -> dict:
        """[summary]
        Arguments:
        ----
        url (str): [description]
        method (str): [description]
        args (dict, optional): [description]. Defaults to None.
        stream (bool, optional): [description]. Defaults to False.
        payload (dict, optional): [description]. Defaults to None.
        Raises:
        ----
        ValueError: [description]
        Returns:
        ----
        dict: [description]
        """

        if method == 'get':

            response = requests.get(
            url=url, params=args, verify=True)

        elif method == 'post':

            response = requests.post(
                url=url, params=args, verify=True)

        elif method == 'put':

            response = requests.put(
                url=url, params=args, verify=True)

        elif method == 'delete':

            response = requests.delete(
                url=url, params=args, verify=True)

        else:
            raise ValueError(
                'The type of request you are making is incorrect.')

        # grab the status code
        status_code = response.status_code

        # grab the response. headers.
        response_headers = response.headers

        if status_code == 200:

            if response_headers['Content-Type'] in ['application/json', 'charset=utf-8', "application/json; charset=utf-8"]:
                return response.json()
            else:
                raise Exception("unhandled response type")

        else:
            # Error
            print('')
            print('-'*80)
            print("BAD REQUEST - STATUS CODE: {}".format(status_code))
            print("RESPONSE URL: {}".format(response.url))
            print("RESPONSE HEADERS: {}".format(response.headers))
            print("RESPONSE TEXT: {}".format(response.text))
            print('-'*80)
            print('')
            
    def get_total_balance_by_coin(self) -> float:
        
        eth_balances: Dict[str:int] = self.get_eth_balances()
        
        eth_balance_gwei: float = 0
        
        for eth_balance in eth_balances:
            eth_balance_gwei += int(eth_balance["balance"]) / 10 ** 18

        token_balances: dict[str: float] = self.get_token_balances_by_coin()

        token_balances["ETH"] = eth_balance_gwei

        return token_balances

    def get_eth_balances(self) -> dict:

        url_endpoint = self._api_module(url='module=account')
        
        params: dict = {
            "action":"balancemulti",
            "address": ','.join(self.api_meta_data[self.__EXCHANGE].other_fields["Addresses"]),
            'tag':"latest",
            "apikey": self.api_meta_data[self.__EXCHANGE].key
        }

        response = self._handle_requests(
                   url=url_endpoint,
                   method="get",
                   args=params,
        )

        return response["result"]
    
    def get_token_balances_by_coin(self) -> dict:
        url_endpoint = self._api_module(url='module=account')

        balance_by_coin: Dict[str: float] = {}

        
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
                result = self._handle_requests(
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

    def get_netliq(self, get_price_from_bybit: Callable) -> float:
        balance_by_coin: dict = self.get_total_balance_by_coin()
        self.logger.debug(f"{balance_by_coin=}")
        netliq: float = 0
        for coin, balance in balance_by_coin.items():
            if "USD" in coin:
                netliq += balance
            else:
                price = get_price_from_bybit(symbol=coin.upper()+"USDC")["result"]["price"]
                netliq += float(balance) * float(price)
        return round(netliq,3)
    
    def get_position(self, get_price_from_bybit: Callable) -> dict:
        balance_by_coin = self.get_total_balance_by_coin()

        data_to_return = {
            "Symbol": [],
            "Multiplier": [],
            "Quantity": [],
            "Dollar Quantity": []
        } 

        for coin, balance in balance_by_coin.items():
            if "USD" in coin:
                data_to_return["Symbol"].append("USD")
                data_to_return["Multiplier"].append(1)
                data_to_return["Quantity"].append(round(balance, 3))
                data_to_return["Dollar Quantity"].append(round(balance,3 ))
            else:
                price = get_price_from_bybit(symbol=coin.upper()+"USDC")["result"]["price"]
                data_to_return["Symbol"].append(coin)
                data_to_return["Multiplier"].append(1)
                data_to_return["Quantity"].append(round(balance, 3))
                data_to_return["Dollar Quantity"].append(round(float(balance) * float(price),3))
        
        return data_to_return


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

