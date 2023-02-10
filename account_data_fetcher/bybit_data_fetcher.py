import logging
import os

import pybit
from pybit.exceptions import InvalidRequestError

from utilities.account_data_fetcher_base import accountFetcherBase


class bybitDataFetcher(accountFetcherBase):
    _EXCHANGE = "BYBIT"
    _ENDPOINT = 'https://api.bybit.com'
    def __init__(self, path: str, password: str, sub_account_name: str = None) -> None:
        super().__init__(path, password)
        self.logger = logging.getLogger(__name__) 
        self._subaccount_name = sub_account_name
        self._spot_client = pybit.HTTP(self._ENDPOINT, api_key=self.api_meta_data[self._EXCHANGE].key, api_secret=self.api_meta_data[self._EXCHANGE].secret, spot=True)
        self._derivative_client = pybit.HTTP(self._ENDPOINT, api_key=self.api_meta_data[self._EXCHANGE].key, api_secret=self.api_meta_data[self._EXCHANGE].secret, spot=False)

    def get_netliq(self) -> float:
        spot_netliq = self.__get_spot_balance()
        derivatives_netliq = self.__get_derivatives_balance()
        return round(spot_netliq + derivatives_netliq, 2)
    
    def __get_spot_balance(self) -> float:
        dollar_netliq: float = 0

        try:
            balances = self._spot_client.get_wallet_balance()["result"]["balances"]
        except InvalidRequestError as e:
            raise e

        for balance in balances:

            coin_balance = float(balance["total"])
            name = balance["coin"]
            
            if name.upper() in ["BUSD", "USDC", "USDT"]:
                dollar_netliq += coin_balance
            
            else:
                dollar_netliq += coin_balance * self.__get_coin_price(name)


        return dollar_netliq

    def __get_derivatives_balance(self) -> float:
        dollar_netliq: float = 0

        try:
            derivatives_netliq = self._derivative_client.get_wallet_balance()["result"]
        except InvalidRequestError as e:
            raise e

        for coin_name, data in derivatives_netliq.items():
            
            if data['equity']:

                if coin_name.upper() in ["BUSD", "USDC", "USDT"]:
                    dollar_netliq += float(data["equity"])
                
                else:
                    dollar_netliq += float(data["equity"]) * self.__get_coin_price(coin_name)
    
        return dollar_netliq

    def get_positions(self, market: str) -> dict:
        if market == "SPOT":
            return self.__get_spot_positions()
        elif market == "FUTURE":
            return self.__get_derivatives_positions()
        else:
            raise NotImplemented(f"Unkown market {market}")

    def __get_spot_positions(self) -> dict:
        data_to_return = {
            "Symbol": [],
            "Multiplier": [],
            "Quantity": [],
            "Dollar Quantity": []
        }
        
        positions = self._spot_client.my_position("/spot/v1/account")["result"]["balances"]

        for position in positions:
            quantity = round(float(position["total"]),3)
            dollar_quantity = quantity if position["coin"].upper() in ["BUSD", "USDC", "USDT"] else \
                              quantity * self.__get_coin_price(position["coin"])
            if dollar_quantity > 100:
                data_to_return["Symbol"].append(position["coin"])
                data_to_return["Multiplier"].append(1)
                data_to_return["Quantity"].append(quantity)
                data_to_return["Dollar Quantity"].append(dollar_quantity)

        return data_to_return

    def __get_derivatives_positions(self) -> dict:
        data_to_return = {
            "Symbol": [],
            "Multiplier": [],
            "Quantity": [],
            "Dollar Quantity": []
        }
        
        positions = self.__get_aggregated_derivatives_positions()
        
        for position_data in positions:
            position: dict = position_data["data"]
            if position["size"]:
                data_to_return["Symbol"].append(position["symbol"])
                data_to_return["Multiplier"].append(1)
                data_to_return["Quantity"].append(round(position["size"],3))
                data_to_return["Dollar Quantity"].append(float(position["position_value"]) + float(position["cum_realised_pnl"] + float(position["unrealised_pnl"])))

        return data_to_return

    def __get_aggregated_derivatives_positions(self) -> list:
        linear_contract_positions = self._derivative_client.my_position('/private/linear/position/list')["result"]
        future_contract_positions = self._derivative_client.my_position('/futures/private/position/list')["result"]
        inverse_contract_positions = self._derivative_client.my_position('/v2/private/position/list')["result"]
        return linear_contract_positions + future_contract_positions + inverse_contract_positions

    def __get_coin_price(self, symbol: str) -> float:
        try:
            return float(self._spot_client.last_traded_price(symbol=symbol+"USD")["result"]["price"])
        except pybit.exceptions.InvalidRequestError:
            pass

        try:
            return float(self._spot_client.last_traded_price(symbol=symbol+"USDC")["result"]["price"])
        except pybit.exceptions.InvalidRequestError:
            pass

        try:
            return float(self._spot_client.last_traded_price(symbol=symbol+"USDT")["result"]["price"])
        except pybit.exceptions.InvalidRequestError:
            pass

    

if __name__ == "__main__":
    from getpass import getpass
    pwd = getpass("provide password for pk:")
    executor = bybitDataFetcher(os.path.realpath(os.path.dirname(__file__)), pwd)
    print(executor.get_netliq())