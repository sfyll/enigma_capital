from dataclasses import asdict
import logging
from getpass import getpass
import json
from logging import Logger
from subprocess import Popen
from typing import Dict, List, Optional, Tuple

from account_data_fetcher.data_aggregator.data_aggregator import DataAggregatorConfig, Subscription

from infrastructure.runner_base import RunnerBase
from infrastructure.api_secret_getter import ApiSecretGetter


class Runner(RunnerBase):
        def __init__(self, pwd: str, logger: Optional[Logger] = None):
            super().__init__(logger)
            self.port_per_process, self.secrets_per_process = self.outer_set_input_builder(pwd)

        @staticmethod
        def get_lower_case_list_elements(list_elements: List[str]) -> List[str]:
            return [x.lower() for x in list_elements]

        """Build Inputs for all exchanges, and cherry-pick at the factory level. For now only handles non defaulting inputs
           Inputs Needed:
                secrets: ApiMetaData, 
                port_number: int, 
                sub_account_name: Optional[str] = None
                delta_in_seconds_allowed: int = 30
                app= GATEWAY,
                paper_trading = False,
                cache_state = True, 
                refresh_enabled = True"""
        def outer_set_input_builder(self, pwd: str) -> Tuple[dict, dict]:
            port_per_process = self.get_port_number_pairings()
            
            secrets_per_process = self.get_secrets(pwd)

            return ({k.lower(): v for k, v in port_per_process.items()},
                    {k.lower(): v for k, v in secrets_per_process.items()},
                    )

        def get_secrets(self, pwd: str) -> dict:
            path = self.base_path + "/account_data_fetcher/secrets/"
            
            secrets: dict = ApiSecretGetter.get_api_meta_data(path, pwd)

            secrets["gsheet"] = ApiSecretGetter.get_gsheet_meta_data(path, pwd)

            return secrets
            
        def launch_processes(self, pwd: str, process_type: str, process_names: List[str], factory_prefix: str) -> None:
            process_names = self.get_lower_case_list_elements(process_names)
            
            for process_name in process_names:
                #two IB implementations!
                secret_keys = "ib" if "ib" in process_name.lower() else process_name
                try:
                    secrets_json = json.dumps(asdict(self.secrets_per_process[secret_keys]))
                except KeyError as e:
                    if str(e) in ["'csv'", "'rsk'"]:
                        secrets_json = json.dumps({"key": "dummy_key", "secret": "dummy_secret", "other_fields": {}})
                        self.logger.info(f"KeyError for {e} encountered. Using dummy secrets.")
                    else:
                        raise

                command = [
                    "python3", "-m",
                    f"account_data_fetcher.{factory_prefix}.process_factory",
                    f"--{process_type}_name", process_name,
                    "--kwargs", json.dumps({
                        "port_number": self.port_per_process[process_name],
                        "secrets": secrets_json,
                        "password": pwd,
                        "data_aggregator_port_number": self.port_per_process["dataaggregator"]
                    })
                ]

                process = Popen(command)
                self.logger.info(f"Started process for {process_name} with PID {process.pid}")

            self.logger.info(f"All {process_type} processes started. Exiting Runner Process.")

        def launch_data_aggregator(self, process_name: str, time_interval: int, exchanges: List[str], writers: List[str]) -> None:
            exchanges, writers = self.get_lower_case_list_elements(exchanges), self.get_lower_case_list_elements(writers)
            exchange_subscriptions: Dict[str, int] = {
                "ib" if k in ["ib_flex", "ib_async"] else k: Subscription(port_number=self.port_per_process[k])
                for k in exchanges if k in self.port_per_process
            }
            writer_subscriptions: Dict[str, int] = {k: Subscription(port_number=self.port_per_process[k]) for k in writers if k in self.port_per_process}
            

            data_aggregator_config: DataAggregatorConfig = DataAggregatorConfig(
                fetcher_routes=exchange_subscriptions,
                writer_routes=writer_subscriptions,
                data_aggregator_port_number=self.port_per_process["dataaggregator"],
                aggregation_interval=time_interval
            )

            command = [
                    "python3", "-m",
                    f"account_data_fetcher.{process_name}.data_aggregator",
                    "--kwargs", json.dumps({
                        "data-aggregator-config": json.dumps(asdict(data_aggregator_config)),
                    })
                ]

            process = Popen(command)

            self.logger.info(f"Started process for {process_name} with PID {process.pid}")

        def get_port_number_pairings(self) -> list:
            """FORMAT OF meta_data.json:
            {"addresses": [] }"""

            path = self.base_path + "/account_data_fetcher/config/port_number_pairing.json"

            with open(path, "r") as f:
                return json.load(f)

if __name__ == "__main__":
    import argparse
    from infrastructure.log_handler import logging_handler
    parser = argparse.ArgumentParser(
    description="Get Data Of Interest",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--seconds', dest="seconds", type=int, nargs='?', default=10)
    parser.add_argument("-q","--quiet",action="count",default=0,
                    help="Be more quiet.")
    parser.add_argument("-v", "--verbose",action="count",default=0,
                    help="Be more verbose. Both -v and -q may be used multiple times.")
    parser.add_argument('--log-file', dest="log_file", type=str, nargs='?')
    parser.add_argument('--exchanges', dest="exchanges", type=str, nargs='+', help="List of exchange to fetch for")
    parser.add_argument('--writers', dest="writers", type=str, nargs='+', help="List of writers")
    
    args = parser.parse_args()

    args = logging_handler(args)
    pwd = getpass("provide password for pk:")

    executor = Runner()
    
    logging.info("Launching Process")

    # executor.launch_data_aggregator("data_aggregator", args.seconds, args.exchanges, args.writers)

    # executor.launch_processes(pwd, 'writer', args.writers, 'writers')

    executor.launch_processes(pwd, 'exchange', args.exchanges, 'exchanges')

