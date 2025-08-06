import asyncio
import logging
from typing import Dict, List, Optional

import aiohttp
from aiohttp import ClientError

class DataFetcher:
    """
    Asynchronous data fetcher for the CoinGecko API.
    Uses aiohttp for non-blocking HTTP requests.
    """
    __API_ENDPOINT = "https://api.coingecko.com/api/v3/"

    def __init__(self, session: aiohttp.ClientSession):
        """
        Initializes the fetcher with a shared aiohttp client session.
        """
        self.logger = logging.getLogger(__name__)
        self.session = session
        self.id_per_symbol: Optional[Dict[str, str]] = None
        self.symbol_per_id: Optional[Dict[str, str]] = None

    async def get_prices(self, symbols: List[str], vs_currencies: List[str] = ["USD"]) -> Dict[str, Dict[str, float]]:
        """
        Asynchronously fetches prices for a list of symbols.
        Ensures the coin list (symbol map) is loaded before making the call.
        """
        if self.id_per_symbol is None:
            await self.__get_id_per_symbol()

        ids_to_fetch = [self.id_per_symbol[symbol.upper()] for symbol in symbols if symbol.upper() in self.id_per_symbol]
        if not ids_to_fetch:
            self.logger.warning("No valid CoinGecko IDs found for the requested symbols.")
            return {}

        url = self.__API_ENDPOINT + 'simple/price'
        params = {
            "ids": ",".join(ids_to_fetch).lower(),
            "vs_currencies": ",".join(vs_currency.lower() for vs_currency in vs_currencies)
        }

        try:
            async with self.session.get(url, params=params) as response:
                response.raise_for_status()
                result = await response.json()
        except ClientError as e:
            self.logger.error(f"Error fetching prices from CoinGecko: {e}")
            return {}

        price_per_symbol: Dict[str, Dict[str, float]] = {}
        for coin_id, price_dict in result.items():
            original_symbol = self.symbol_per_id.get(coin_id.upper())
            if original_symbol:
                price_per_symbol[original_symbol] = price_dict

        self.logger.debug(f"Fetched prices: {price_per_symbol}")
        return price_per_symbol

    async def __get_id_per_symbol(self, include_platform: str = "false"):
        """
        Asynchronously fetches the complete list of coins to map symbols to CoinGecko IDs.
        Includes retry logic with async sleep.
        """
        self.logger.info("Fetching CoinGecko coin list to build symbol map...")
        url = self.__API_ENDPOINT + 'coins/list'
        params = {"include_platform": include_platform}
        
        max_retries = 2
        for attempt in range(max_retries):
            try:
                async with self.session.get(url, params=params) as response:
                    response.raise_for_status()
                    result = await response.json()
                    if result:
                        # Process the list and populate the maps
                        id_per_symbol: Dict[str, str] = {}
                        symbol_per_id: Dict[str, str] = {}
                        for item in result:
                            symbol = item["symbol"].upper()
                            coin_id = item["id"].upper()
                            # Handle special cases from original code
                            if item["id"] == "dydx":
                                id_per_symbol["DYDX"] = coin_id
                                symbol_per_id[coin_id] = "DYDX"
                            elif item["symbol"] == "vita" and item["id"] == "vitadao":
                                id_per_symbol["VITA"] = coin_id
                                symbol_per_id[coin_id] = "VITA"
                            else:
                                id_per_symbol[symbol] = coin_id
                                symbol_per_id[coin_id] = symbol
                        
                        self.id_per_symbol = id_per_symbol
                        self.symbol_per_id = symbol_per_id
                        self.logger.info("Successfully built CoinGecko symbol map.")
                        return

            except ClientError as e:
                self.logger.warning(f"Failed to fetch coin list (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(60)
        
        self.logger.error("Could not fetch coin list from CoinGecko after multiple retries.")
        self.id_per_symbol = {}
        self.symbol_per_id = {}
