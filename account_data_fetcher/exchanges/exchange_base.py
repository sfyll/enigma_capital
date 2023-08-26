from abc import ABC, abstractmethod
import json
import logging
import time
from typing import Optional

from setproctitle import setproctitle
import zmq

from infrastructure.log_handler import fetch_logging_config

class ExchangeBase(ABC):
    __PROCESS_PREFIX = "fetch_"
    def __init__(self, port_number: int, exchange: str, fetch_frequency: int = 60*60) -> None:
        setproctitle(self.__PROCESS_PREFIX + exchange.lower())
        self.exchange: str = exchange
        self.port_number = port_number
        self.fetch_frequency = fetch_frequency
        self.logger = self.init_logging()

    def init_logging(self):
        fetch_logging_config('/account_data_fetcher/config/logging_config.ini')
        return logging.getLogger(__name__)
    
    @abstractmethod
    def fetch_balance(self, accountType: Optional[str] = None) -> float:
        pass

    @abstractmethod
    def fetch_positions(self, accountType: Optional[str] = None) -> dict:
        pass

    def process_request(self):
        context = zmq.Context()
        socket = context.socket(zmq.PUB)
        self.logger.debug(f"Publishing {self.exchange=} content to tcp://*:{self.port_number}")
        socket.bind(f"tcp://*:{self.port_number}")

        while True:
            # Fetch balance and positions
            balance_data = self.fetch_balance()
            positions_data = self.fetch_positions()

            msg: dict = {
                "balance": balance_data,
                "positions": positions_data 
            }

            self.logger.debug(f"Sending {self.exchange}: {msg=}")

            socket.send_multipart([b"balance_and_positions", json.dumps(msg).encode()])

            # Sleep or wait for a signal to fetch the next data
            time.sleep(self.fetch_frequency) # 1 hours
