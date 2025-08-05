from abc import ABC, abstractmethod
import asyncio
import logging
from typing import Optional

from setproctitle import setproctitle
import aiohttp 

from infrastructure.log_handler import fetch_logging_config
#TODO: For now, we only enforce two methods implementation, namely fetch_balance and fetch_positions. As such, process_request is quite statically defined as well. How could we untangle both so that we can define more abstract methods and have the process_request understands what to fetch dynamically.
class ExchangeBase(ABC):
    """
    Base class for exchange-related data fetching.
    It fetches data from an exchange API and puts the resulting dictionary
    onto an asyncio.Queue for downstream processing.
    """
    __PROCESS_PREFIX = "fetch_"

    def __init__(self, exchange: str, session: aiohttp.ClientSession, output_queue: asyncio.Queue, fetch_frequency: int = 60*60) -> None:
        """    
        Initializes the ExchangeBase object.

        Args:
            exchange (str): The name of the exchange.
            session (aiohttp.ClientSession): The shared HTTP client session.
            output_queue (asyncio.Queue): The queue to send fetched data to.
            fetch_frequency (int, optional): Time interval for data fetching, in seconds.
        """
        setproctitle(self.__PROCESS_PREFIX + exchange.lower())
        self.exchange: str = exchange.lower()
        self.session = session
        self.output_queue = output_queue
        self.fetch_frequency = fetch_frequency
        self.logger = self.init_logging()

    def init_logging(self):
        fetch_logging_config('/account_data_fetcher/config/logging_config.ini')
        return logging.getLogger(__name__)
    
    @abstractmethod
    async def fetch_balance(self, accountType: Optional[str] = None) -> float:
        pass

    @abstractmethod
    async def fetch_positions(self, accountType: Optional[str] = None) -> dict:
        pass

    async def process_request(self):
        """
        Continuously fetches balance and position data and puts it onto the output queue.
        This method replaces the ZMQ publisher logic.
        """
        try:
            while True:
                balance_data = await self.fetch_balance()
                positions_data = await self.fetch_positions()

                msg: dict = {
                    "exchange": self.exchange,
                    "balance": balance_data,
                    "positions": positions_data 
                }

                await self.output_queue.put(msg)

                await asyncio.sleep(self.fetch_frequency)
        except asyncio.CancelledError:
            self.logger.info(f"Exchange fetcher for {self.exchange} is shutting down.")
        except Exception as e:
            self.logger.error(f"Fetcher for {self.exchange} failed: {e}", exc_info=True)
