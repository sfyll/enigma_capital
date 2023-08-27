from dataclasses import dataclass, field
import json
import logging
import logging.config
import os
import time
from typing import Dict, List, Optional, Set

from setproctitle import setproctitle
import zmq

from infrastructure.log_handler import fetch_logging_config

@dataclass
class Subscription:
    port_number: str
    topic: Optional[Set[str]] = None

@dataclass
class DataAggregatorConfig:
    fetcher_routes: Dict[str, Subscription]  # Mapping of exchange name to port_numbers for subscribing to fetchers
    writer_routes: Dict[str, Subscription]   # Mapping of writer type to port_numbers for publishing to writers
    data_aggregator_port_number: int
    aggregation_interval: int

@dataclass
class BalanceData:
    value: float

    def update(self, value: float):
        self.value = value

@dataclass
class PositionData:
    data: Dict[str, List]
    
    def update(self, value: Dict[str, List]):
        for key, val in value.items():
            self.data[key] = val

@dataclass
class ExchangeData:
    balance: BalanceData = field(default_factory=lambda: BalanceData(0))
    position: PositionData = field(default_factory=PositionData)
    last_fetch_timestamp: float = 0

    def update(self, balance_and_positions_dict: dict) -> None:
        self.update_balance(balance_and_positions_dict["balance"])
        self.update_position(balance_and_positions_dict["positions"])
        self.last_fetch_timestamp = time.time()

    def update_balance(self, value: float):
        self.balance.update(value)

    def update_position(self, value: Dict[str, List]):
        self.position.update(value)

@dataclass
class AggregatedData:
    aggregation_interval: int
    date: str
    data_routes: DataAggregatorConfig
    netliq: float = 0
    exchanges: Dict[str, ExchangeData] = field(default_factory=dict)

    def get_object_if_ready(self) -> Optional[dict]:
        if self.__can_send():
            return self.__get_object_to_send()

    def __can_send(self) -> bool:
        if set(self.exchanges.keys()) != set(self.data_routes.fetcher_routes.keys()):
            return False
        
        # Existing logic for time delay
        oldest_fetch_timestamp = min(
            exchange.last_fetch_timestamp for exchange in self.exchanges.values()
        )

        return time.time() - oldest_fetch_timestamp >= self.aggregation_interval

        
    def __get_object_to_send(self) -> dict:
        position_dict: dict = {
            "date": [],
            "Exchange": [],
            "Symbol": [],
            "Multiplier": [],
            "Quantity": [],
            "Dollar Quantity": [],
        }

        to_send_object: dict = {"balance" : {},
                                "positions": position_dict}
        
        self.date = time.strftime('%Y-%m-%d %H:%M:%S')
        self.netliq = sum(exchange.balance.value for exchange in self.exchanges.values())
        
        to_send_object["balance"]["netliq"] = self.netliq
        to_send_object["balance"]["date"] = self.date
        
        for exchange, value in self.exchanges.items():
            to_send_object["balance"][exchange] = value.balance.value

            to_send_object["positions"]["date"] += [self.date] * len(value.position.data["Symbol"])
            to_send_object["positions"]["Exchange"] += [self.date] * len(value.position.data["Symbol"])
            to_send_object["positions"]["Symbol"] += value.position.data["Symbol"]
            to_send_object["positions"]["Multiplier"] +=  value.position.data["Multiplier"]
            to_send_object["positions"]["Quantity"] += value.position.data["Quantity"]
            to_send_object["positions"]["Dollar Quantity"] += value.position.data["Dollar Quantity"]

        return to_send_object
    
class DataAggregator:
    def __init__(self, config: DataAggregatorConfig) -> None:
        setproctitle("data_aggregator")
        self.logger = self.init_logging()
        self.path = os.path.realpath(os.path.dirname(__file__))
        self.aggregated_data = self.aggregated_data = AggregatedData(aggregation_interval=config.aggregation_interval, 
                                                                     date="", 
                                                                     exchanges={}, 
                                                                     data_routes=config)

    def init_logging(self):
        fetch_logging_config('/account_data_fetcher/config/logging_config.ini')
        return logging.getLogger(__name__)
    
    def run(self):
        context = zmq.Context()

        # Create and bind the publisher for the writers
        self.logger.debug(f"Publishing data_aggregator at tcp://localhost:{self.aggregated_data.data_routes.data_aggregator_port_number}")
        pub_socket = context.socket(zmq.PUB)
        pub_socket.bind(f"tcp://*:{self.aggregated_data.data_routes.data_aggregator_port_number}")

        # Create and connect the subscriber for the fetchers
        sub_socket = context.socket(zmq.SUB)
        for exchange_name, exchange_subscription in self.aggregated_data.data_routes.fetcher_routes.items():
            self.logger.debug(f"Subscribing data_aggregator to {exchange_name} at tcp://localhost:{exchange_subscription['port_number']}")
            sub_socket.connect(f"tcp://localhost:{exchange_subscription['port_number']}")
            """TODO: Be able to subscribe to different topics, so that the data_aggregator
                     can scale easily to more usecases"""
            sub_socket.setsockopt_string(zmq.SUBSCRIBE, "")
            # for topic in exchange_subscription["topic"]:
            #     sub_socket.subscribe(topic)

        while True:
            # Receive from fetchers
            _, data = sub_socket.recv_multipart()
            balance_and_positions_dict = json.loads(data.decode())
            exchange_name = balance_and_positions_dict.pop("exchange")

            self.logger.debug(f"Received {exchange_name=}, {data=}")

            if exchange_name not in self.aggregated_data.exchanges:
                self.aggregated_data.exchanges[exchange_name] = ExchangeData(
                    balance=BalanceData(value=balance_and_positions_dict['balance']),
                    position=PositionData(data=balance_and_positions_dict['positions']),
                    last_fetch_timestamp=time.time()
                )

            else:
                self.aggregated_data.exchanges[exchange_name].update(balance_and_positions_dict)
            objects_to_send: Optional[dict] = self.aggregated_data.get_object_if_ready()
            if objects_to_send:
                self.logger.debug(f"Sending {objects_to_send=}")
                pub_socket.send_multipart([b"balance_and_positions", json.dumps(objects_to_send).encode()])

if __name__ == '__main__':
    import argparse
    import logging

    """TODO:
        Properly parse the object below so that the inner dataclass
        can be read as a dataclass, and not accessed as a dict"""
    parser = argparse.ArgumentParser()
    parser.add_argument("--kwargs")
    args = parser.parse_args()
    kwargs_dict = json.loads(args.kwargs)
    data_aggregator_config_raw = json.loads(kwargs_dict["data-aggregator-config"])
    data_aggregator_config = DataAggregatorConfig(**data_aggregator_config_raw)

    executor = DataAggregator(data_aggregator_config)

    executor.run()



