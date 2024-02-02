import logging
import os
from typing import List, Optional

from account_data_fetcher.exchanges.kucoin.kucoin_connector import kucoinApiConnector
from account_data_fetcher.exchanges.kucoin.exception import FailedRequestError, InvalidRequestError

from account_data_fetcher.exchanges.exchange_base import ExchangeBase
from infrastructure.api_secret_getter import ApiMetaData

#TODO make a config object to be parsed so that we can modify which account type to fetch
class DataFetcher(ExchangeBase):
    _EXCHANGE = "KUCOIN"
    _ENDPOINT="https://api.kucoin.com"
    def __init__(self, secrets: ApiMetaData, port_number: int, sub_account_name: Optional[str] = None) -> None:
        super().__init__(port_number, self._EXCHANGE)
        self._subaccount_name = sub_account_name
        self.kucoin_connector = kucoinApiConnector(api_key=secrets.key, api_secret=secrets.secret, passphrase=secrets.other_fields["Passphrase"])

    def fetch_balance(self, accountType="UNIFIED") -> float:
        netliq = self.__get_balances()
        return round(netliq, 2)
    
    def __get_balances(self) -> float:

        balances: List[dict] = self.kucoin_connector.get_wallet_balance()

        spot_netliq: float = 0

        for balance in balances:
            
            coin_balance = float(balance["balance"])

            if not coin_balance:
                continue
                
            name = balance["currency"]
            
            if name.upper() in ["BUSD", "USDC", "USDT"]:
                spot_netliq += coin_balance
            else:
                spot_netliq += coin_balance * self.__get_coin_price(name)

        return round(spot_netliq)
        
    def fetch_positions(self, accountType = "UNIFIED") -> dict:
        return  self.__get_spot_positions()

    def __get_spot_positions(self) -> dict:
        data_to_return = {
            "Symbol": [],
            "Multiplier": [],
            "Quantity": [],
            "Dollar Quantity": []
        }
        
        balances: List[dict] = self.kucoin_connector.get_wallet_balance()

        for balance in balances:
            
            quantity = round(float(balance["balance"]),3)
            dollar_quantity = quantity if balance["currency"].upper() in ["BUSD", "USDC", "USDT"] else \
                              quantity * self.__get_coin_price(balance["currency"])
            
            if dollar_quantity > 100: 
                data_to_return["Symbol"].append(balance["currency"])
                data_to_return["Multiplier"].append(1)
                data_to_return["Quantity"].append(quantity)
                data_to_return["Dollar Quantity"].append(dollar_quantity)
                
        return data_to_return    

    def __get_coin_price(self, symbol: str) -> Optional[float]:
        "starting with most likely, USDT unfort"
        try:
            return float(self.kucoin_connector.get_last_traded_price(currencies=symbol)[symbol])
        except InvalidRequestError:
            pass 

if __name__ == "__main__":
    from getpass import getpass
    pwd = getpass("provide password for pk:")
    executor = DataFetcher(os.path.realpath(os.path.dirname(__file__)), pwd)
    #TODO:do below but with asset split, right now gives only derivatives view.
    # print(executor.kucoin_connector.get_position(category="linear", settleCoin="USDT"))
    # print(executor.get_positions("SPOT"))
    print(executor.get_positions("UNIFIED"))
    print(executor.get_positions("FUTURE"))
    # print(executor.get_netliq())
