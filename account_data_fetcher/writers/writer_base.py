from abc import ABC, abstractmethod
import json
import logging
import logging.config
import os
from typing import Dict

from setproctitle import setproctitle
import zmq

class WriterBase(ABC):
    __PROCESS_PREFIX = "writer_"
    def __init__(self, data_aggregator_port_number: int, writer: str) -> None:
        setproctitle(self.__PROCESS_PREFIX + writer.lower())
        self.data_aggregator_port_number = data_aggregator_port_number
        self.logger = self.init_logging()
        self.process_request()

    def init_logging(self):
        logging.config.fileConfig(self.get_base_path() + '/account_data_fetcher/config/logging_config.ini')
        return logging.getLogger(__name__)

    @staticmethod
    def get_base_path():
        current_directory = os.path.dirname(__file__)
        return os.path.abspath(os.path.join(current_directory, '..', ".."))
    
    @abstractmethod
    def update_balances(self, balance: Dict[str, float]):
        pass

    @abstractmethod
    def update_positions(self, positions: Dict[str, list]):
        pass

    def process_request(self):
        context = zmq.Context()
        socket = context.socket(zmq.SUB)
        socket.connect(f"tcp://localhost:{self.data_aggregator_port_number}") # Assuming middleman is running on the same host
        socket.subscribe('balances')
        socket.subscribe('positions')

        while True:
            topic, data = socket.recv_multipart()
            if topic == b'balances':
                self.update_balances(json.loads(data))
            elif topic == b'positions':
                self.update_positions(json.loads(data))