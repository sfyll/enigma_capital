from time import sleep
from typing import Dict, List
import logging

from requests.exceptions import HTTPError

from utilities.request_handler import requestHandler

class DataFetcher:
    __API_ENDPOINT = "https://api.coingecko.com/api/v3/"
    def __init__(self, include_platform: str = "false") -> None:
        self.logger = logging.getLogger(__name__) 
        self.request_handler : requestHandler = requestHandler()
        self.__get_id_per_symbol(include_platform)

    def get_prices(self, symbols: List[str], vs_currencies: List[str] = ["USD"]) -> Dict[str, Dict[str, float]]:
        url_base: str = self.__API_ENDPOINT + 'simple/price'
        
        url = self.request_handler.api_module(url_base=url_base)

        params = {
            "ids": ",".join(self.id_per_symbol[symbol].lower() for symbol in symbols),
            "vs_currencies": ",".join(vs_currency.lower() for vs_currency in vs_currencies)
        }

        result: List[dict] =  self.request_handler.handle_requests(
            url=url,
            method="get",
            args=params
        )

        price_per_symbol: Dict[str, Dict[str, float]] = {}

        for coin, price_dict in result.items():
            price_per_symbol[self.symbol_per_id[coin.upper()]] = price_dict

        self.logger.debug(f"{price_per_symbol=}")

        return price_per_symbol


    def __get_id_per_symbol(self, include_platform: str) -> None:
        url_base: str = self.__API_ENDPOINT + 'coins/list'
        
        url = self.request_handler.api_module(url_base=url_base)

        params = {
            "include_platform": include_platform
        }
        
        #very low api request tolerance...
        try:
            result: List[dict] =  self.request_handler.handle_requests(
                url=url,
                method="get",
                args=params
            )
        except HTTPError as e:
            sleep(60)
            result: List[dict] =  self.request_handler.handle_requests(
                url=url,
                method="get",
                args=params
            )

        id_per_symbol : Dict[str, str] = {}
        symbol_per_id : Dict[str, str] = {}

        for dictionary in result:
            if "wormhole" in dictionary["id"]:
                continue
            id_per_symbol[dictionary["symbol"].upper()] = dictionary["id"].upper()
            symbol_per_id[dictionary["id"].upper()] = dictionary["symbol"].upper()

        self.id_per_symbol: Dict[str, str] =  id_per_symbol
        self.symbol_per_id: Dict[str, str] =  symbol_per_id

if __name__ == "__main__":
    executor = coingeckoDataFetcher()
    print(executor.get_prices(["BTC", "ETH", "AMPL", "DYDX", "DAI"], ["USD"]))
