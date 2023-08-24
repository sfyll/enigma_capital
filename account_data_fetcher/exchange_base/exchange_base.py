from abc import ABC, abstractmethod
import json
from typing import Optional

import zmq

from utilities.api_secret_getter import ApiSecretGetter, ApiMetaData

class ExchangeBase(ABC):
    def __init__(self, port_number: int) -> None:
        self.port_number = port_number

    def get_secrets(self, path: str, password: str, api_to_get: str) -> ApiMetaData:
        return ApiSecretGetter.get_api_meta_data(path, password, api_to_get)
    
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
