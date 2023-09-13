from dataclasses import dataclass, field
from datetime import datetime, timedelta
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
    """
    Class to hold the subscription data for a route.

    Attributes:
        port_number (str): The port number to use for this subscription.
        topic (Optional[Set[str]]): Optional topic for data filtering.
    """
    port_number: str
    topic: Optional[Set[str]] = None

@dataclass
class DataAggregatorConfig:
    """
    Configuration for the Data Aggregator.

    Attributes:
        fetcher_routes (Dict[str, Subscription]): Mapping of exchange names to subscriptions for fetchers.
        writer_routes (Dict[str, Subscription]): Mapping of writer types to subscriptions for writers.
        data_aggregator_port_number (int): Port number for the Data Aggregator.
        aggregation_interval (int): Time interval for data aggregation, in seconds.
    """
    fetcher_routes: Dict[str, Subscription]  # Mapping of exchange name to port_numbers for subscribing to fetchers
    writer_routes: Dict[str, Subscription]   # Mapping of writer type to port_numbers for publishing to writers
    data_aggregator_port_number: int
    aggregation_interval: int

@dataclass
class BalanceData:
    """
    Class to represent balance data.

    Attributes:
        value (float): The value of the balance.
    """
    value: float

    def update(self, value: float):
        """
        Update balance value.

        Parameters:
            value (float): New balance value.
        """
        self.value = value

@dataclass
class PositionData:
    """
    Class to represent position data.

    Attributes:
        data (Dict[str, List]): A dictionary containing the position data.
    """
    data: Dict[str, List]

    def update(self, value: Dict[str, List]):
        """
        Update position data.

        Parameters:
            value (Dict[str, List]): New position data.
        """
        for key, val in value.items():
            self.data[key] = val

@dataclass
class ExchangeData:
    """
    Class to hold data relevant to a specific exchange.

    Attributes:
        balance (BalanceData): The balance data.
        position (PositionData): The position data.
        last_fetch_timestamp (float): The last time the data was fetched, in Unix timestamp.
    """
    balance: BalanceData = field(default_factory=lambda: BalanceData(0))
    position: PositionData = field(default_factory=PositionData)
    last_fetch_timestamp: float = 0

    def update(self, balance_and_positions_dict: dict) -> None:
        """
        Update balance and position data.

        Parameters:
            balance_and_positions_dict (dict): Dictionary containing the new balance and position data.
        """
        self.update_balance(balance_and_positions_dict["balance"])
        self.update_position(balance_and_positions_dict["positions"])
        self.last_fetch_timestamp = time.time()

    def update_balance(self, value: float):
        """
        Update balance data.

        Parameters:
            value (float): New balance value.
        """
        self.balance.update(value)

    def update_position(self, value: Dict[str, List]):
        """
        Update position data.

        Parameters:
            value (Dict[str, List]): New position data.
        """
        self.position.update(value)

@dataclass
class AggregatedData:
    """
    Class to hold aggregated data across exchanges.

    Attributes:
        aggregation_interval (int): The time interval for aggregation, in seconds.
        data_routes (DataAggregatorConfig): Configuration for the Data Aggregator.
        netliq (float): The net liquidation value.
        date (str): The last date data was sent.
        exchanges (Dict[str, ExchangeData]): Data for individual exchanges.
    """
    aggregation_interval: int
    data_routes: DataAggregatorConfig
    netliq: float = 0
    date: str = None
    exchanges: Dict[str, ExchangeData] = field(default_factory=dict)

    def get_object_if_ready(self) -> Optional[dict]:
        """
        Get the aggregated data object if conditions for sending are met.

        Returns:
            Optional[dict]: The aggregated data object or None.
        """
        if self.__can_send():
            return self.__get_object_to_send()

    def __can_send(self) -> bool:
        """
        Check if conditions for sending data are met.
        Requires all exchanges to have been populated, and current timestamp - last sent timestamp > aggregation_interval.

        Returns:
            bool: True if data can be sent, False otherwise.
        """
        if set(self.data_routes.fetcher_routes.keys()) <= set(self.exchanges.keys()) :
            pass
        else:
            return False

        #if aggregation interval == 24h, we're assuming we want to post on new day as defined by IB
        if self.aggregation_interval == 86400:
            if not self.__is_new_day():
                return False
            else:
                return True

        #if process was just launched, no need to check for all fetchers to have a new date
        if not self.date:
            return True

        #last sent object was at least aggregation interval ago
        return time.time() - datetime.strptime(self.date, '%Y-%m-%d %H:%M:%S').timestamp() >= self.aggregation_interval

    def __is_new_day(self) -> bool:
        """
        Check if it is a new day based on UTC time. Twicked for Interactive Brokers for which new days (and FLEX report publishing) is around 5am UTC.
 
        Returns:
            bool: True if it is a new day, False otherwise.
        """
        utc_time = datetime.utcnow()
        
        if utc_time.hour < 6:  # Wait until 7 AM UTC
            return False

        current_date = utc_time.date()
        last_sent_date = datetime.strptime(self.date, "%Y-%m-%d %H:%M:%S").date() if self.date else None

        return current_date != last_sent_date
        
    def __get_object_to_send(self) -> dict:
        """
        Prepare the object to be sent, updating necessary fields.

        Returns:
            dict: The aggregated data object.
        """
        self.netliq = sum(exchange.balance.value for exchange in self.exchanges.values())
        self.date = time.strftime('%Y-%m-%d %H:%M:%S')

        to_send_object = {
            "balance": {"date": self.date, "netliq": self.netliq},
            "positions": {
                key: [] for key in ["date", "Exchange", "Symbol", "Multiplier", "Quantity", "Dollar Quantity"]
            }
        }
        
        to_send_object["balance"]["netliq"] = self.netliq
        to_send_object["balance"]["date"] = self.date
        
        for exchange, value in self.exchanges.items():
            to_send_object["balance"][exchange] = value.balance.value

            len_data = len(value.position.data["Symbol"])

            to_send_object["positions"]["date"].extend([self.date] * len_data)
            to_send_object["positions"]["Exchange"].extend([exchange] * len_data)

            for key in ["Symbol", "Multiplier", "Quantity", "Dollar Quantity"]:
                to_send_object["positions"][key].extend(value.position.data[key])

        return to_send_object

#TODO: Only localhost is supported for components communication. Allow support for other hosts!
class DataAggregator:
    """Main class for aggregating data from multiple exchanges and publishing it to the writers.

    Attributes:
        aggregated_data (AggregatedData): Data to be aggregated and sent.
    """
    def __init__(self, config: DataAggregatorConfig) -> None:
        """Initializes DataAggregator class with configurations.
        
        Args:
            config (DataAggregatorConfig): The configuration settings for the data aggregator.
        """
        setproctitle("data_aggregator")
        self.logger = self.init_logging()
        self.path = os.path.realpath(os.path.dirname(__file__))
        self.aggregated_data = self.aggregated_data = AggregatedData(aggregation_interval=config.aggregation_interval, 
                                                                     date="", 
                                                                     exchanges={}, 
                                                                     data_routes=config)

    def init_logging(self):
        """Initializes logging for the class.

        Returns:
            logging.Logger: Configured logger.
        """
        fetch_logging_config('/account_data_fetcher/config/logging_config.ini')
        return logging.getLogger(__name__)
    
    def run(self):
        """
        The main method that handles the data aggregation and publication.
        
        This method performs the following operations:
        1. Initializes the ZeroMQ context.
        2. Creates and binds a publisher socket for sending aggregated data.
        3. Creates and connects a subscriber socket for receiving data from various exchanges.
        4. Enters an infinite loop that:
            a. Receives data from the subscriber socket.
            b. Updates the internal aggregated data structure.
            c. Sends the aggregated data through the publisher socket when ready. That is, when all exchanges have been published and aggregation interval has been met.
        
        Note: 
        - The ZMQ PUB-SUB model is being used here. The publisher (PUB) will send data to any
        connected subscriber (SUB).
        - This method assumes that each message received from a subscriber contains both 
        balance and position data, identified by an "exchange" field.
        """
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
            #TODO: Be able to subscribe to different topics, so that the data_aggregator can scale easily to more usecases
            sub_socket.setsockopt_string(zmq.SUBSCRIBE, "")
            # for topic in exchange_subscription["topic"]:
            #     sub_socket.subscribe(topic)

        #TODO: Handle failure. What if an exchange stop publishing and we just loop forever? Need some form of heartbit logic.
        #TODO: Make the function more modular, the data_aggregator should be agnostic to what data is being aggregated as that should be abstracted away.
        while True:
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

    #TODO: Properly parse the object below so that the inner dataclass can be read as a dataclass, and not accessed as a dict
    parser = argparse.ArgumentParser()
    parser.add_argument("--kwargs")
    args = parser.parse_args()
    kwargs_dict = json.loads(args.kwargs)
    data_aggregator_config_raw = json.loads(kwargs_dict["data-aggregator-config"])
    data_aggregator_config = DataAggregatorConfig(**data_aggregator_config_raw)

    executor = DataAggregator(data_aggregator_config)

    executor.run()



