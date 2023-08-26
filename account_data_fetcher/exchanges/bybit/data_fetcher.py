import logging
import os
from typing import List, Optional

from account_data_fetcher.exchanges.bybit.bybit_connector import bybitApiConnector
from account_data_fetcher.exchanges.bybit.exception import FailedRequestError, InvalidRequestError

from account_data_fetcher.exchanges.exchange_base import ExchangeBase
from infrastructure.api_secret_getter import ApiMetaData

#TODO make a config object to be parsed so that we can modify which account type to fetch
class DataFetcher(ExchangeBase):
    _EXCHANGE = "BYBIT"
    _ENDPOINT = 'https://api.bybit.com'
    def __init__(self, secrets: ApiMetaData, port_number: int, sub_account_name: Optional[str] = None) -> None:
        super().__init__(port_number, self._EXCHANGE)
        self._subaccount_name = sub_account_name
        self.bybit_connector = bybitApiConnector(api_key=secrets.key, api_secret=secrets.secret)

    def fetch_balance(self, accountType="UNIFIED") -> float:
        netliq = self.__get_balances(accountType)
        return round(netliq, 2)
    
    def __get_balances(self, accountType="UNIFIED") -> float:
        if accountType=="UNIFIED":
            try:
                consolidated_balance = float(self.bybit_connector.get_derivative_balance()[0]["totalEquity"])
            except InvalidRequestError as e:
                raise e

            return consolidated_balance

        else:
            try:
                derivative_balance = float(self.bybit_connector.get_derivative_balance(accountType=accountType)[0]["totalEquity"])
            except InvalidRequestError as e:
                raise e

            spot_balances: List[dict] = self.bybit_connector.get_all_coin_balance(accountType="SPOT")


            spot_netliq: float = 0

            for balance in spot_balances:
                
                coin_balance = float(balance["walletBalance"])

                if not coin_balance:
                    continue
                    
                name = balance["coin"]
                
                if name.upper() in ["BUSD", "USDC", "USDT"]:
                    spot_netliq += coin_balance
                else:
                    spot_netliq += coin_balance * self.__get_coin_price(name)

            return round(spot_netliq + derivative_balance)
        
    def fetch_positions(self, accountType = "UNIFIED") -> dict:
        data_to_return = {
        "Symbol": [],
        "Multiplier": [],
        "Quantity": [],
        "Dollar Quantity": []
        }
        
        if accountType == "UNIFIED":
            future_positions = self.__get_derivatives_positions()
            unified_positions = self.__get_unified_positions()

            all_positions = [future_positions, unified_positions]

            # Aggregating positions by symbol
            for pos in all_positions:
                for i, symbol in enumerate(pos["Symbol"]):
                    if symbol in data_to_return["Symbol"]:
                        index = data_to_return["Symbol"].index(symbol)
                        data_to_return["Quantity"][index] += pos["Quantity"][i]
                        data_to_return["Dollar Quantity"][index] += pos["Dollar Quantity"][i]
                    else:
                        data_to_return["Symbol"].append(symbol)
                        data_to_return["Multiplier"].append(1)
                        data_to_return["Quantity"].append(pos["Quantity"][i])
                        data_to_return["Dollar Quantity"].append(pos["Dollar Quantity"][i])

            return data_to_return
        
        else:
            spot_positions = self.__get_spot_positions()
            future_positions = self.__get_derivatives_positions()

            all_positions = [spot_positions, future_positions]

            # Aggregating positions by symbol
            for pos in all_positions:
                for i, symbol in enumerate(pos["Symbol"]):
                    if symbol in data_to_return["Symbol"]:
                        index = data_to_return["Symbol"].index(symbol)
                        data_to_return["Quantity"][index] += pos["Quantity"][i]
                        data_to_return["Dollar Quantity"][index] += pos["Dollar Quantity"][i]
                    else:
                        data_to_return["Symbol"].append(symbol)
                        data_to_return["Multiplier"].append(1)
                        data_to_return["Quantity"].append(pos["Quantity"][i])
                        data_to_return["Dollar Quantity"].append(pos["Dollar Quantity"][i])

            return data_to_return

    def fetch_specific_positions(self, market: str) -> dict:
        if market == "SPOT":
            return self.__get_spot_positions()
        elif market == "FUTURE":
            return self.__get_derivatives_positions()
        elif market == "UNIFIED":
            return self.__get_unified_positions()
        else:
            raise NotImplemented(f"Unkown market {market}")

    def __get_spot_positions(self) -> dict:
        data_to_return = {
            "Symbol": [],
            "Multiplier": [],
            "Quantity": [],
            "Dollar Quantity": []
        }
        
        positions: List[dict] = self.bybit_connector.get_all_coin_balance(accountType="SPOT")

        for position in positions:
            quantity = round(float(position["walletBalance"]),3)
            dollar_quantity = quantity if position["coin"].upper() in ["BUSD", "USDC", "USDT"] else \
                              quantity * self.__get_coin_price(position["coin"])
            if dollar_quantity > 100:
                data_to_return["Symbol"].append(position["coin"])
                data_to_return["Multiplier"].append(1)
                data_to_return["Quantity"].append(quantity)
                data_to_return["Dollar Quantity"].append(dollar_quantity)

        return data_to_return
    
    def __get_unified_positions(self) -> dict:
        data_to_return = {
            "Symbol": [],
            "Multiplier": [],
            "Quantity": [],
            "Dollar Quantity": []
        }
        
        positions: List[dict] = self.bybit_connector.get_all_coin_balance(accountType="UNIFIED")

        for position in positions:
            quantity = round(float(position["walletBalance"]),3)
            dollar_quantity = 0
            if quantity > 0:
                dollar_quantity = 0 if position["coin"].upper() in ["BUSD", "USDC", "USDT", "DAI"] else \
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
        
        for position in positions["list"]:
            if position["size"]:
                data_to_return["Symbol"].append(position["symbol"])
                data_to_return["Multiplier"].append(1)
                data_to_return["Quantity"].append(round(float(position["size"]),3))
                data_to_return["Dollar Quantity"].append(round(float(position["positionValue"]) + float(position["cumRealisedPnl"]) + float(position["unrealisedPnl"]),3))

        return data_to_return

    def __get_aggregated_derivatives_positions(self, is_unified_account: bool = True) -> list:
        if is_unified_account:
            return self.bybit_connector.get_position(category="linear", settleCoin="USDT")
        else:
            linear_contract_positions = self.bybit_connector.get_position(category="linear", settleCoin="USDT")
            inverse_contract_positions = self.bybit_connector.get_position(category='inverse')
            return linear_contract_positions + inverse_contract_positions

    def __get_coin_price(self, symbol: str) -> float:
        "starting with most likely, USDT unfort"
        try:
            return float(self.bybit_connector.get_last_traded_price(symbol=symbol+"USDT")["price"])
        except InvalidRequestError:
            pass

        try:
            return float(self.bybit_connector.get_last_traded_price(symbol=symbol+"USD")["price"])
        except InvalidRequestError:
            pass

        try:
            return float(self.bybit_connector.get_last_traded_price(symbol=symbol+"USDC")["price"])
        except InvalidRequestError:
            pass
    

if __name__ == "__main__":
    from getpass import getpass
    pwd = getpass("provide password for pk:")
    executor = bybitDataFetcher(os.path.realpath(os.path.dirname(__file__)), pwd)
    #TODO:do below but with asset split, right now gives only derivatives view.
    # print(executor.bybit_connector.get_position(category="linear", settleCoin="USDT"))
    # print(executor.get_positions("SPOT"))
    print(executor.get_positions("UNIFIED"))
    print(executor.get_positions("FUTURE"))
    # print(executor.get_netliq())
