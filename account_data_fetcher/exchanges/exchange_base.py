from abc import ABC, abstractmethod
import json
import logging
import time
from typing import Optional

from setproctitle import setproctitle
import zmq

from infrastructure.log_handler import fetch_logging_config

#TODO: For now, we only enforce two methods implementation, namely fetch_balance and fetch_positions. As such, process_request is quite statically defined as well. How could we untangle both so that we can define more abstract methods and have the process_request understands what to fetch dynamically.
class ExchangeBase(ABC):
    """
    Base class for exchange-related data fetching and processing. 
    It defines which methods have to be implemented by each exchanges.
    It also handles utilities such as logging as well as other functions common to all child classes.

    Attributes:
        exchange (str): The name of the exchange, converted to lowercase.
        port_number (int): The port number for the ZMQ PUB socket.
        fetch_frequency (int): Time interval for data fetching, in seconds.
    """
    __PROCESS_PREFIX = "fetch_"
    def __init__(self, port_number: int, exchange: str, fetch_frequency: int = 60*60) -> None:
        """    
        Initialize the ExchangeBase object.

        Args:
            port_number (int): The port number for the ZMQ PUB socket.
            exchange (str): The name of the exchange.
            fetch_frequency (int, optional): Time interval for data fetching, in seconds. Defaults to 60*60.
        """
        setproctitle(self.__PROCESS_PREFIX + exchange.lower())
        self.exchange: str = exchange.lower()
        self.port_number = port_number
        self.fetch_frequency = fetch_frequency
        self.logger = self.init_logging()

    def init_logging(self):
        """Initializes logging for the class.

        Returns:
            logging.Logger: Configured logger.
        """
        fetch_logging_config('/account_data_fetcher/config/logging_config.ini')
        return logging.getLogger(__name__)
    
    @abstractmethod
    def fetch_balance(self, accountType: Optional[str] = None) -> float:
        """
        Fetch the account balance from the exchange.
        Implemented by the child class.

        Args:
            accountType (Optional[str], optional): The type of account to fetch. Defaults to None.

        Returns:
            float: The account balance.
        """

        pass

    @abstractmethod
    def fetch_positions(self, accountType: Optional[str] = None) -> dict:
        """
        Fetch the account position dictionry from the exchange.
        Implemented by the child class.

        Args:
            accountType (Optional[str], optional): The type of account to fetch. Defaults to None.

        Returns:
            Dict: The account position dictionary.
        """
        pass

    def process_request(self):
        """
        Continuously fetches balance and position data from an exchange and publishes it using zmq.

        This function sets up a zmq.PUB socket and then enters an infinite loop. 
        In each iteration, it fetches the balance and positions from the exchange, 
        constructs a message, and publishes it to the specified port.
        Iteration gaps are defined by self.fetch_frequency.

        Args:
            None

        Returns:
            None

        Raises:
            Exception: Logs the exception and its details if any part of the process fails.
        """
        try:
            context = zmq.Context()
            socket = context.socket(zmq.PUB)
            self.logger.debug(f"Publishing {self.exchange=} content to tcp://*:{self.port_number}")
            socket.bind(f"tcp://*:{self.port_number}")

            while True:
                # Fetch balance and positions
                balance_data = self.fetch_balance()
                positions_data = self.fetch_positions()

                msg: dict = {
                    "exchange": self.exchange,
                    "balance": balance_data,
                    "positions": positions_data 
                }

                self.logger.debug(f"Sending {self.exchange}: {msg=}")

                socket.send_multipart([b"balance_and_positions", json.dumps(msg).encode()])

                # Sleep or wait for a signal to fetch the next data
                time.sleep(self.fetch_frequency) # 1 hours
        except Exception as e:
            self.logger.info(f"{e=}", exc_info=True)
