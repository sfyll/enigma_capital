from abc import ABC, abstractmethod
import json
import logging
import os
from typing import Dict

from setproctitle import setproctitle
import zmq

class WriterBase(ABC):
    __PROCESS_PREFIX = "write_"
    def __init__(self, port_number: int, writer: str) -> None:
        setproctitle(self.__PROCESS_PREFIX + writer.lower())
        self.port_number = port_number
        self.logger = logging.getLogger(__name__)
        self.process_request()

    @staticmethod
    def get_base_path():
        current_directory = os.path.dirname(__file__)
        return os.path.abspath(os.path.join(current_directory, '..', ".."))
    
    @abstractmethod
    def update_balances(self, balance: Dict[str: float]):
        pass

    @abstractmethod
    def update_positions(self, positions: Dict[str: list]):
        pass

    def process_request(self):
        context = zmq.Context()
        socket = context.socket(zmq.REP)
        socket.bind(f"tcp://*:{self.port_number}")

        while True:
            request_type = socket.recv_string()
            response_data: str

            if request_type == 'balance':
                response_data = str(self.write_balances())
            elif request_type == 'positions':
                response_data = json.dumps(self.write_positions())
            else:
                response_data = f"Unknown request type: {request_type}"

            socket.send_string(response_data)
