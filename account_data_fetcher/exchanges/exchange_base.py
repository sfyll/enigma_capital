from abc import ABC, abstractmethod
import json
import logging
from typing import Optional

from setproctitle import setproctitle
import zmq

class ExchangeBase(ABC):
    def __init__(self, port_number: int, exchange: str) -> None:
        setproctitle(exchange)
        self.port_number = port_number
        self.logger = logging.getLogger(__name__)
        self.process_request()
    
    @abstractmethod
    def fetch_balance(self, accountType: Optional[str] = None):
        pass

    @abstractmethod
    def fetch_positions(self, accountType: Optional[str] = None):
        pass

    def process_request(self):
        context = zmq.Context()
        socket = context.socket(zmq.REP)
        socket.bind(f"tcp://*:{self.port_number}")

        while True:
            request_type = socket.recv_string()
            response_data: str

            if request_type == 'balance':
                response_data = str(self.fetch_balance())
            elif request_type == 'positions':
                response_data = json.dumps(self.fetch_positions())
            else:
                response_data = f"Unknown request type: {request_type}"

            socket.send_string(response_data)
