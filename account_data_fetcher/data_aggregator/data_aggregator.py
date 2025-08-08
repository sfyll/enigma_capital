import asyncio
from dataclasses import dataclass, field
from datetime import datetime
import logging
import time
from typing import Dict, List, Optional, Set

from account_data_fetcher.exchanges.exchange_base import ExchangeBase

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
    report_timestamp_utc: Optional[datetime] = None

    def update(self, balance_and_positions_dict: dict) -> None:
        """
        Update balance and position data.

        Parameters:
            balance_and_positions_dict (dict): Dictionary containing the new balance and position data.
        """
        self.update_balance(balance_and_positions_dict["balance"])
        self.update_position(balance_and_positions_dict["positions"])
        self.last_fetch_timestamp = time.time()
        if 'report_timestamp_utc' in balance_and_positions_dict:
            self.report_timestamp_utc = balance_and_positions_dict['report_timestamp_utc']

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
    logger: logging.Logger
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
            return self.__are_all_reports_new_day()
        
        #if process was just launched, no need to check for all fetchers to have a new date
        if not self.date:
            return True

        #last sent object was at least aggregation interval ago
        return time.time() - datetime.strptime(self.date, '%Y-%m-%d %H:%M:%S').timestamp() >= self.aggregation_interval

    def __are_all_reports_new_day(self) -> bool:
        """
        Verifies that for a daily report, we have received data from ALL exchanges
        that was generated on the current UTC calendar day.
        """
        utc_now = datetime.utcnow()
        utc_today = utc_now.date()

        # Check if we have already sent a report for today's date.
        last_sent_date: Optional[date] = datetime.strptime(self.date, "%Y-%m-%d %H:%M:%S").date() if self.date else None
        if last_sent_date == utc_today:
            return False 

        # Now, verify the data from all exchanges.
        for ex_name, ex_data in self.exchanges.items():
            if not ex_data.report_timestamp_utc:
                self.logger.debug(f"Cannot send daily report: Exchange '{ex_name}' is missing a report timestamp.")
                return False
            if ex_data.report_timestamp_utc.date() != utc_today:
                self.logger.debug(f"Cannot send daily report: Exchange '{ex_name}' has a stale report from {ex_data.report_timestamp_utc.date()}.")
                return False
        
        self.logger.info("All exchanges have provided reports for today. Ready to send.")
        return True


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

class DataAggregator:
    """Main class for aggregating data from multiple exchanges and publishing it to the writers.

    This version uses a more robust and cleaner asyncio pattern by merging all
    input queues into one, simplifying the main processing loop.

    Attributes:
        aggregated_data (AggregatedData): Data to be aggregated and sent.
    """
    def __init__(self, aggregation_interval: int, fetcher_instances: Dict[str, ExchangeBase], output_queues: Dict[str, asyncio.Queue]) -> None:
        self.logger = self.init_logging()
        self.fetcher_instances= fetcher_instances 
        self.output_queues = output_queues
        self.aggregated_data = AggregatedData(
            logger=self.logger,
            aggregation_interval=aggregation_interval,
            expected_exchanges=set(fetcher_instances.keys())
        )
        self.loop_frequency_seconds = aggregation_interval 

    def init_logging(self):
        """Initializes logging for the class.

        Returns:
            logging.Logger: Configured logger.
        """
        return logging.getLogger(__name__)

    async def run(self):
        """
        Main orchestration loop.
        """
        self.logger.info(f"Data Aggregator starting. Polling exchanges every {self.loop_frequency_seconds}s.")
        try:
            while True:
                fetch_tasks = {name: fetcher.process_request() for name, fetcher in self.fetcher_instances.items()}
                
                for name, task in fetch_tasks.items():
                    try:
                        data = await task
                        if name not in self.aggregated_data.exchanges:
                            empty_pos = {k: [] for k in ["Symbol", "Multiplier", "Quantity", "Dollar Quantity"]}
                            self.aggregated_data.exchanges[name] = ExchangeData(position=PositionData(data=empty_pos))
                        
                        self.aggregated_data.exchanges[name].update(data)
                        self.logger.info(f"Successfully updated data for {name}.")
                    except Exception as e:
                        self.logger.error(f"Failed to fetch or update data for {name}: {e}", exc_info=True)

                objects_to_send = self.aggregated_data.get_object_if_ready()
                if objects_to_send:
                    self.logger.info(f"Data is valid and ready. Pushing aggregated object to {len(self.output_queues)} writers.")
                    await asyncio.gather(*(q.put(objects_to_send) for q in self.output_queues.values()))
                
                await asyncio.sleep(self.loop_frequency_seconds)

        except asyncio.CancelledError:
            self.logger.info("Data aggregator is shutting down.")
        except Exception as e:
            self.logger.error(f"DataAggregator failed critically: {e}", exc_info=True)

