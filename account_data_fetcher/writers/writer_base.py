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
    """
    Abstract base class for writing data.
    """
    __PROCESS_PREFIX = "writer_"
    def __init__(self, data_aggregator_port_number: int, port_number: int, writer: str) -> None:
        """
        Initialize WriterBase instance.
        
        Args:
            data_aggregator_port_number (int): Port number of the data aggregator.
            port_number (int): Port number for this writer.
            writer (str): Name of the writer.
        """
        setproctitle(self.__PROCESS_PREFIX + writer.lower())
        self.writer = writer
        self.data_aggregator_port_number = data_aggregator_port_number
        self.port_number = port_number #in case useful in the future ! 
        self.logger = self.init_logging()

    def init_logging(self):
        """
        Initialize logging for the writer.
        
        Returns:
            logging.Logger: Logger object.
        """
        fetch_logging_config('/account_data_fetcher/config/logging_config.ini')
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
    def update_balances(self, balance: Dict[str, float]):
        """
        Update balances. This method should be implemented in subclasses.
        
        Args:
            balance (Dict[str, float]): Dictionary of balances.
        """
        pass

    @abstractmethod
    def update_positions(self, positions: Dict[str, list]):
        """
        Update positions. This method should be implemented in subclasses.
        
        Args:
            positions (Dict[str, list]): Dictionary of positions.
        """
        pass

    def process_request(self):
        """
        Process incoming data requests from the data aggregator service.

        This method establishes a ZeroMQ (zmq) SUB socket, subscribes to 
        incoming data from the data aggregator, and continually listens for 
        incoming messages. Upon receiving a message, it deserializes the 
        JSON-encoded data and calls `update_balances` and `update_positions` 
        to handle the new balance and position data, respectively.

        Raises:
            Exception: Any exceptions that arise during data subscription, 
                    deserialization, or handling are logged.

        Notes:
            - The zmq context and SUB socket are initialized within this method.
            - This function runs in an infinite loop, designed to continually 
            listen for and process incoming data.
            - Subscriptions are not yet filtered; the SUB socket is set to 
            receive all messages.
        """
        try:
            context = zmq.Context()
            sub_socket = context.socket(zmq.SUB)
            self.logger.debug(f"Subscribing {self.writer} to tcp://localhost:{self.data_aggregator_port_number}")
            sub_socket.connect(f"tcp://localhost:{self.data_aggregator_port_number}") # Assuming middleman is running on the same host
            #TODO: Handle subscription on per topic basis
            sub_socket.setsockopt_string(zmq.SUBSCRIBE, "")
            while True:
                _, data = sub_socket.recv_multipart()
                self.logger.debug(f"Writer received {data}")
                
                balance_and_positions_dict = json.loads(data.decode())

                self.logger.debug(f"writing {balance_and_positions_dict=}")

                self.update_balances(balance_and_positions_dict["balance"])
                self.update_positions(balance_and_positions_dict["positions"])
        except Exception as e:
            self.logger.info(f"{e=}", exc_info=True)