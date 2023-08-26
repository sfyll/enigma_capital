from abc import ABC, abstractmethod
import json
import logging
import logging.config
import os
from typing import Dict

from setproctitle import setproctitle
import zmq

from infrastructure.log_handler import fetch_logging_config

class WriterBase(ABC):
    __PROCESS_PREFIX = "writer_"
    def __init__(self, data_aggregator_port_number: int, port_number: int, writer: str) -> None:
        setproctitle(self.__PROCESS_PREFIX + writer.lower())
        self.writer = writer
        self.data_aggregator_port_number = data_aggregator_port_number
        self.port_number = port_number #in case useful in the future ! 
        self.logger = self.init_logging()

    def init_logging(self):
        fetch_logging_config('/account_data_fetcher/config/logging_config.ini')
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
        sub_socket = context.socket(zmq.SUB)
        self.logger.debug(f"Subscribing {self.writer} to tcp://localhost:{self.data_aggregator_port_number}")
        sub_socket.connect(f"tcp://localhost:{self.data_aggregator_port_number}") # Assuming middleman is running on the same host
        """TODO:
            Handle subscription on per topic basis"""
        sub_socket.setsockopt_string(zmq.SUBSCRIBE, "")

        while True:
            _, data = sub_socket.recv_multipart()
            self.logger.debug(f"Writer received {data}")
            
            balance_and_positions_dict = json.loads(data.decode())

            self.update_balances(balance_and_positions_dict["balance"])
            self.update_positions(balance_and_positions_dict["positions"])