import asyncio
import inspect
import logging
from getpass import getpass
from importlib import import_module
from logging import Logger
from typing import List, Dict, Tuple, Optional, Any

import aiohttp
from account_data_fetcher.exchanges.exchange_base import ExchangeBase
from infrastructure.runner_base import RunnerBase
from infrastructure.api_secret_getter import ApiSecretGetter
from account_data_fetcher.data_aggregator.data_aggregator import DataAggregator

class Runner(RunnerBase):
    def __init__(self, pwd: str, logger: Optional[Logger] = None):
        """
        Initializes the Runner class.

        Args:
            pwd (str): The password for authentication.
            logger (Optional[Logger]): Logger for logging info. Defaults to None.
            """
        super().__init__(logger)
        self.secrets_per_process = self.get_secrets(pwd)

    @staticmethod
    def get_lower_case_list_elements(list_elements: List[str]) -> List[str]:
        """
        Converts all elements in the list to lowercase.

        Args:
            list_elements (List[str]): List of strings to convert.

        Returns:
            List[str]: List of lowercased strings.
        """
        return [x.lower() for x in list_elements]


    def get_secrets(self, pwd: str) -> dict:
        """
        Fetches API secrets.

        Args:
            pwd (str): The password for authentication.

        Returns:
            dict: Dictionary containing secrets.
        """
        path = self.base_path + "/account_data_fetcher/secrets/"
        
        secrets: dict = ApiSecretGetter.get_api_meta_data(path, pwd)

        secrets["gsheet"] = ApiSecretGetter.get_gsheet_meta_data(path, pwd)

        return {k.lower(): v for k, v in secrets.items()}

    async def launch_all_as_tasks(self, frequency: int, exchanges: List[str], writers: List[str]):
        """
        Launches all components, instantiating fetchers to be managed by the
        central DataAggregator (pull model).
        """
        aggregator_to_writer_queues: Dict[str, asyncio.Queue] = {
            name: asyncio.Queue() for name in self.get_lower_case_list_elements(writers)
        }

        all_tasks = []
        
        async with aiohttp.ClientSession() as http_session:
            self.logger.info("Instantiating Exchange Fetcher objects...")
            fetcher_instances = self._get_fetcher_instances(http_session, self.get_lower_case_list_elements(exchanges))

            self.logger.info("Preparing DataAggregator task...")
            aggregator_instance = DataAggregator(
                aggregation_interval=frequency,
                fetcher_instances=fetcher_instances,  
                output_queues=aggregator_to_writer_queues
            )
            aggregator_task = asyncio.create_task(aggregator_instance.run())
            all_tasks.append(aggregator_task)

            self.logger.info("Preparing Writer tasks...")
            writer_tasks = self._get_writer_tasks(aggregator_to_writer_queues)
            all_tasks.extend(writer_tasks)

            self.logger.info(f"All {len(all_tasks)} tasks created. Running forever...")
            await asyncio.gather(*all_tasks)

    def _get_fetcher_instances(self, session: aiohttp.ClientSession, exchanges_list: List[str]) -> Dict[str, 'ExchangeBase']:
        """
        Instantiates and returns a dictionary of fetcher objects, without
        creating any running tasks for them.
        """
        instances = {}
        for name in exchanges_list:
            module = import_module(f"account_data_fetcher.exchanges.{name}.data_fetcher")
            FetcherClass = getattr(module, "DataFetcher")
            
            init_params = inspect.signature(FetcherClass).parameters
            secret_keys = "ib" if "ib" in name else name
            
            possible_args = {
                "secrets": self.secrets_per_process.get(secret_keys),
                "session": session,
            }

            final_args = {key: value for key, value in possible_args.items() if key in init_params}
            self.logger.debug(f"Instantiating {name} with args: {list(final_args.keys())}")
            instance = FetcherClass(**final_args)
            if name in ["ib_async", "ib_flex"]:
                instances["ib"] = instance
            else:
                instances[name] = instance
        return instances

    def _get_writer_tasks(self, queues: Dict[str, asyncio.Queue]) -> List[asyncio.Task]:
        tasks = []
        for name in queues.keys():
            module = import_module(f"account_data_fetcher.writers.{name}.writer")
            WriterClass = getattr(module, "Writer")

            init_params = inspect.signature(WriterClass).parameters

            possible_args = {
                "secrets": self.secrets_per_process.get(name),
                "input_queue": queues[name] # Pass the queue instead of ports
            }
            
            final_args = {key: value for key, value in possible_args.items() if key in init_params}
            instance = WriterClass(**final_args)
            tasks.append(asyncio.create_task(instance.process_request()))
        return tasks

if __name__ == "__main__":
    import argparse
    from infrastructure.log_handler import logging_handler
    parser = argparse.ArgumentParser(
    description="Get Data Of Interest",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--seconds', dest="seconds", type=int, nargs='?', default=10)
    parser.add_argument("-q","--quiet",action="count",default=0, help="Be more quiet.")
    parser.add_argument("-v", "--verbose",action="count",default=0, help="Be more verbose.")
    parser.add_argument('--log-file', dest="log_file", type=str, nargs='?')
    parser.add_argument('--exchanges', dest="exchanges", type=str, nargs='+', required=True, help="List of exchange to fetch for")
    parser.add_argument('--writers', dest="writers", type=str, nargs='+', required=True, help="List of writers")
    
    args = parser.parse_args()
    args = logging_handler(args)
    
    pwd = getpass("provide password for pk:")
    executor = Runner(pwd, logger=logging.getLogger())
    logging.info("Launching Process")

    try:
        asyncio.run(executor.launch_all_as_tasks(args.seconds, args.exchanges, args.writers))
    except KeyboardInterrupt:
        logging.info("Shutdown signal received. Exiting gracefully.")
    except Exception as e:
        logging.critical(f"An unhandled exception occurred in the main runner: {e}", exc_info=True)
    finally:
        logging.info("Application has been shut down.")
