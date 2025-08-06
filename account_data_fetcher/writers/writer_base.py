from abc import ABC, abstractmethod
import asyncio
import logging
import logging.config
import os
from typing import Dict

class WriterBase(ABC):
    """
    Abstract base class for writing data.
    """
    def __init__(self, writer: str, input_queue: asyncio.Queue) -> None:
        """
        Initializes the WriterBase instance.

        Args:
            writer (str): The name of the writer implementation (e.g., "CSV", "GSHEET").
            input_queue (asyncio.Queue): The queue from which to receive aggregated data.
        """
        self.writer = writer
        self.input_queue = input_queue
        self.logger = self.init_logging()

    def init_logging(self):
        """
        Initialize logging for the writer.
        
        Returns:
            logging.Logger: Logger object.
        """
        return logging.getLogger(__name__)

    @staticmethod
    def get_base_path():
        """
        Returns the base path of the current file.
        
        Returns:
            str: The base directory path.
        """
        current_directory = os.path.dirname(__file__)
        return os.path.abspath(os.path.join(current_directory, '..', ".."))
    
    @abstractmethod
    async def update_balances(self, balance: Dict[str, float]):
        """
        Update balances. This method should be implemented in subclasses.
        
        Args:
            balance (Dict[str, float]): Dictionary of balances.
        """
        pass

    @abstractmethod
    async def update_positions(self, positions: Dict[str, list]):
        """
        Update positions. This method should be implemented in subclasses.
        
        Args:
            positions (Dict[str, list]): Dictionary of positions.
        """
        pass

    async def process_request(self):
        """
        Continuously listens on the input queue for aggregated data and processes it.
        This method replaces the ZMQ subscriber logic.
        """
        try:
            while True:
                balance_and_positions_dict = await self.input_queue.get()
                

                await self.update_balances(balance_and_positions_dict["balance"])
                await self.update_positions(balance_and_positions_dict["positions"])

                self.input_queue.task_done()
        except asyncio.CancelledError:
            self.logger.info(f"Writer '{self.writer}' is shutting down.")
        except Exception as e:
            self.logger.error(f"Writer '{self.writer}' failed: {e}", exc_info=True)
