from dataclasses import dataclass, field
import json
import logging
import os
import time
from typing import Dict, List, Optional

import pandas as pd
import zmq


@dataclass
class Subscription:
    topic: set(str)
    route: str

@dataclass
class DataAggregatorConfig:
    fetcher_routes: Dict[str, Subscription]  # Mapping of exchange name to port_numbers for subscribing to fetchers
    writer_routes: Dict[str, Subscription]   # Mapping of writer type to port_numbers for publishing to writers
    aggregation_interval: int

@dataclass
class BalanceData:
    value: float
    last_fetch_timestamp: float

    def update(self, value: float):
        self.value = value
        self.last_fetch_timestamp = time.time()

@dataclass
class PositionData:
    data: Dict[str, List] = field(default_factory=lambda: {
            "Symbol": [],
            "Multiplier": [],
            "Quantity": [],
            "Dollar Quantity": []
        })
    last_fetch_timestamp: float = 0
    
    def update(self, value: Dict[str, List]):
        for key, val in value.items():
            self.data[key] = val
        self.last_fetch_timestamp = time.time()

@dataclass
class ExchangeData:
    balance: BalanceData = BalanceData(0, 0)
    position: PositionData = PositionData()

    def update_balance(self, value: float):
        self.balance.update(value)

    def update_position(self, value: Dict[str, List]):
        self.position.update(value)

@dataclass
class AggregatedData:
    aggregation_interval: int
    date: str
    netliq: float = 0
    exchanges: Dict[str, ExchangeData] = field(default_factory=dict)

    def get_object_if_ready(self, data_type: str = "balance") -> Optional[dict]:
        if self.__can_send(data_type):
            return self.__get_object_to_send(data_type)

    def __can_send(self, data_type: str) -> bool:
        if data_type == "balance":
            oldest_fetch_timestamp = min(
                self.exchanges[exchange].balance.last_fetch_timestamp for exchange in self.exchanges.values()
            )
        
        else:
            oldest_fetch_timestamp = min(
                self.exchanges[exchange].position.last_fetch_timestamp for exchange in self.exchanges.values()
            )
        
        if time.time() - oldest_fetch_timestamp >= self.aggregation_interval:
            return True
        
        else:
            return False
        
    def __get_object_to_send(self, data_type: str) -> dict:
        to_send_object: dict = {}
        if data_type == "balance":
            self.date = time.strftime('%Y-%m-%d %H:%M:%S')
            self.netliq = sum(self.exchanges[exchange].balance.value for exchange in self.exchanges.values())
            
            to_send_object["netliq"] = self.netliq
            
            for key, value in self.exchanges.items():
                to_send_object[key] = value.balance
        else:
            for key, value in self.exchanges.items():
                to_send_object[key] = value.position

        return to_send_object
    
class DataAggregator:
    def __init__(self, config: DataAggregatorConfig) -> None:
        self.logger = logging.getLogger(__name__)
        self.data_routes = config
        self.path = os.path.realpath(os.path.dirname(__file__))
        self.aggregated_data = AggregatedData(date="", exchanges={})

    def run(self):
        context = zmq.Context()

        # Create and bind the publisher for the writers
        pub_socket = context.socket(zmq.PUB)
        for _, port in self.data_routes.writer_routes.items():
            pub_socket.bind(f"tcp://*:{port}")

        # Create and connect the subscriber for the fetchers
        sub_socket = context.socket(zmq.SUB)
        for exchange_name, exchange_subscription in self.data_routes.fetcher_routes.items():
            sub_socket.connect(f"tcp://localhost:{exchange_subscription.route}")
            for topic in exchange_subscription.topic:
                sub_socket.subscribe(topic)

        while True:
            # Receive from fetchers
            topic, data = sub_socket.recv_multipart()
            exchange_name, data_type = topic.decode().split(":")
            value = json.loads(data.decode())

            if exchange_name not in self.aggregated_data.exchanges:
                self.aggregated_data.exchanges[exchange_name] = ExchangeData()

            if data_type == 'balance':
                self.aggregated_data.exchanges[exchange_name].update_balance(value)
                balance_to_send: Optional[dict] = self.aggregated_data.get_object_if_ready(data_type)
                if balance_to_send:
                    pub_socket.send_json(balance_to_send)
            elif data_type == "positions":
                self.aggregated_data.exchanges[exchange_name].update_position(value)
                position_to_send: Optional[dict] = self.aggregated_data.get_object_if_ready("position")
                if position_to_send:
                    pub_socket.send_json(position_to_send)
            else:
                raise Exception(f"Unkown {data_type=}")


if __name__ == '__main__':
    import argparse
    import asyncio
    import signal
    import logging
    import functools
    

    parser = argparse.ArgumentParser()
    parser.add_argument('--seconds', dest="seconds", type=str, nargs='+', default=10)
    parser.add_argument('--log-level', dest="log_level", type=str, nargs='+', default="INFO")
    parser.add_argument('--log-file', dest="log_file", type=str, nargs='+')
    args = parser.parse_args()
    numeric_level = getattr(logging, args.log_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError('Invalid log level: %s' % args.log_level)
    logging.basicConfig(level=numeric_level, format='%(levelname)s:%(asctime)s:%(message)s', filename=args.log_file[0])
    logger: logging.Logger = logging.getLogger()

    pwd = getpass("provide password for pk:")
    executor = AccountDataFetcher(pwd, logger)

    def ask_exit(signame, loop, logger):
        logger.info("got signal %s: exit" % signame)
        loop.stop()

    loop = asyncio.get_event_loop()

    print(f"{loop.is_running()=}")

    for signame in {'SIGINT', 'SIGTERM'}:
        loop.add_signal_handler(
            getattr(signal, signame),
            functools.partial(ask_exit, signame, loop, logger))

    async def periodic(seconds):
        while True:
            executor.write_positions_to_csv()
            await asyncio.sleep(seconds)

    def stop():
        task.cancel()

    task = loop.create_task(periodic(args.seconds))

    try:
        loop.run_until_complete(task)
    except asyncio.CancelledError:
        pass
