import asyncio
from dataclasses import dataclass, field
from datetime import datetime
import logging
import time
from typing import Dict, List, Optional, Set

from setproctitle import setproctitle

from infrastructure.log_handler import fetch_logging_config

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
    expected_exchanges: Set[str]
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
        if not self.expected_exchanges.issubset(self.exchanges.keys()):
            return False

        #if aggregation interval == 24h, we're assuming we want to post on new day as defined by IB
        if self.aggregation_interval == 86400:
            return self.__is_new_day()
        
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
        self.netliq = round(sum(exchange.balance.value for exchange in self.exchanges.values()),3)
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
    def __init__(self, aggregation_interval: int, input_queues: Dict[str, asyncio.Queue], output_queues: Dict[str, asyncio.Queue]) -> None:
        setproctitle("data_aggregator")
        self.logger = self.init_logging()
        self.input_queues = input_queues
        self.output_queues = output_queues
        self.aggregated_data = AggregatedData(
            aggregation_interval=aggregation_interval,
            expected_exchanges=set(input_queues.keys())
        )

    def init_logging(self):
        """Initializes logging for the class.

        Returns:
            logging.Logger: Configured logger.
        """
        fetch_logging_config('/account_data_fetcher/config/logging_config.ini')
        return logging.getLogger(__name__)

    async def _queue_consumer(self, exchange_name: str, queue: asyncio.Queue):
        """A wrapper coroutine to consume from a queue and yield the source."""
        while True:
            data = await queue.get()
            yield exchange_name, data

    async def run(self):
        """
        Main method to aggregate data from input queues and publish to output queues.
        """
        consumers = [self._queue_consumer(name, queue) for name, queue in self.input_queues.items()]
        
        try:
            while True:
                tasks: List[asyncio.Task] = [asyncio.create_task(consumer.__anext__()) for consumer in consumers]
                done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                
                for task in done:
                    exchange_name, data = task.result()
                    self.logger.debug(f"Received from {exchange_name=}, {data=}")

                    if exchange_name not in self.aggregated_data.exchanges:
                        empty_positions = {k: [] for k in ["Symbol", "Multiplier", "Quantity", "Dollar Quantity"]}
                        self.aggregated_data.exchanges[exchange_name] = ExchangeData(position=PositionData(data=empty_positions))
                    
                    self.aggregated_data.exchanges[exchange_name].update(data)
                    
                    objects_to_send = self.aggregated_data.get_object_if_ready()
                    if objects_to_send:
                        self.logger.debug(f"Aggregated object ready. Pushing to {len(self.output_queues)} writers.")
                        await asyncio.gather(*(q.put(objects_to_send) for q in self.output_queues.values()))
                
                # Re-add pending tasks for the next iteration
                tasks = [asyncio.create_task(consumer.__anext__()) for consumer in consumers if consumer.__anext__() in pending]

        except asyncio.CancelledError:
            self.logger.info("Data aggregator is shutting down.")
        except Exception as e:
            self.logger.error(f"DataAggregator failed: {e}", exc_info=True)
