from abc import ABC, abstractmethod
import json
import logging
import time
from typing import Optional

from setproctitle import setproctitle
import zmq

class ExchangeBase(ABC):
    __PROCESS_PREFIX = "fetch_"
    def __init__(self, port_number: int, exchange: str, fetch_frequency: int = 60*60) -> None:
        setproctitle(self.__PROCESS_PREFIX + exchange.lower())
        self.port_number = port_number
        self.fetch_frequency = fetch_frequency
        self.logger = logging.getLogger(__name__)
        self.process_request()
    
    @abstractmethod
    def fetch_balance(self, accountType: Optional[str] = None) -> float:
        pass

    @abstractmethod
    def fetch_positions(self, accountType: Optional[str] = None) -> dict:
        pass

    def process_request(self):
        context = zmq.Context()
        socket = context.socket(zmq.PUB)
        socket.bind(f"tcp://*:{self.port_number}")

        while True:
            # Fetch balance and positions
            balance_data = self.fetch_balance()
            positions_data = self.fetch_positions()

            # Publish data
            socket.send_string('balance', str(balance_data))
            socket.send_string('positions', json.dumps(positions_data))

            # Sleep or wait for a signal to fetch the next data
            time.sleep(self.fetch_frequency) # 1 hours
