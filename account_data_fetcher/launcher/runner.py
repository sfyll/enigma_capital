from dataclasses import asdict
import logging
from getpass import getpass
import json
from logging import Logger
from subprocess import Popen
from typing import List, Optional, Tuple

from infrastructure.runner_base import RunnerBase
from infrastructure.api_secret_getter import ApiSecretGetter


class Runner(RunnerBase):
        def __init__(self, logger: Optional[Logger] = None) -> None:
            super().__init__(logger)
            self.port_per_process, self.secrets_per_process, exchanges = self.outer_set_input_builder(pwd, exchanges)

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
            port_per_process = self.__read_json()
            
            secrets_per_process = self.get_secrets(pwd)

            return ({k.lower(): v for k, v in port_per_process.items()},
                    {k.lower(): v for k, v in secrets_per_process.items()},
                    )

        def get_secrets(self, pwd: str, path) -> dict:
            path = self.base_path + "/account_data_fetcher/secrets/"
            
            secrets: dict = ApiSecretGetter.get_api_meta_data(path, pwd)

            return secrets
            
        def launch_exchange_processes(self, pwd: str, exchanges: List[str]) -> None:
            exchanges = self.get_lower_case_list_elements(process)
            
            for exchange_name in exchanges:
                #two IB implementations!
                exchange_key = "ib" if "ib" in exchange_name.lower() else exchange_name
                
                try:
                    secrets_json = json.dumps(asdict(self.secrets_per_process[exchange_key]))
                except KeyError as e:
                    if str(e) == "rsk":
                        # Populate secrets_json with dummy variables if the KeyError is for 'rsk'
                        secrets_json = json.dumps({"key": "dummy_key", "secret": "dummy_secret", "other_fields": {}})
                        self.logger.info(f"KeyError for 'rsk' encountered. Using dummy secrets.")
                    else:
                        # Re-raise the exception for any other KeyError
                        raise
                
                command = [
                    "python3", "-m",
                    "account_data_fetcher.exchanges.exchange_factory",
                    "--exchange_name", exchange_name,
                    "--kwargs", json.dumps({
                        "port_number": self.port_per_process[exchange_name],
                        "secrets": secrets_json,
                        "password": pwd
                    })
                ]

                process = Popen(command)

                self.logger.info(f"Started process for {exchange_name} with PID {process.pid}")

            self.logger.info("All processes started. Exiting Runner Process.")

        def launch_writer_processes(self, pwd: str, writers: List[str]) -> None:  
            writers = self.get_lower_case_list_elements(process)          
            for writer_name in writers:
                #two IB implementations!                
                try:
                    secrets_json = json.dumps(asdict(self.secrets_per_process[writer_name]))
                except KeyError as e:
                    if str(e) == "csv":
                        # Populate secrets_json with dummy variables if the KeyError is for 'rsk'
                        secrets_json = json.dumps({"key": "dummy_key", "secret": "dummy_secret", "other_fields": {}})
                        self.logger.info(f"KeyError for 'rsk' encountered. Using dummy secrets.")
                    else:
                        # Re-raise the exception for any other KeyError
                        raise
                
                command = [
                    "python3", "-m",
                    "account_data_fetcher.writers.writer_factory",
                    "--writer_name", writer_name,
                    "--kwargs", json.dumps({
                        "port_number": self.port_per_process["DataAggregator"],
                        "secrets": secrets_json,
                        "password": pwd
                    })
                ]

                process = Popen(command)

                self.logger.info(f"Started process for {writer_name} with PID {process.pid}")

            self.logger.info("All processes started. Exiting Runner Process.")
             
        def __read_json(self, path_extension: str) -> list:
            """FORMAT OF meta_data.json:
            {"addresses": [] }"""

            path = self.base_path + "/account_data_fetcher/config/port_number_pairing.json"

            with open(path, "r") as f:
                return json.load(f)

if __name__ == "__main__":
    import argparse
    from infrastructure.log_handler import args_handler
    parser = argparse.ArgumentParser(
    description="Get Data Of Interest",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--seconds', dest="seconds", type=int, nargs='?', default=10)
    parser.add_argument('--manual-balance', dest="manual_balance", type=bool, nargs='?', default=False,
                        help="If activated, will read from a file a manual balance to offset netliq, positive for adding and negative to substract values")
    parser.add_argument("-q","--quiet",action="count",default=0,
                    help="Be more quiet.")
    parser.add_argument("-v", "--verbose",action="count",default=0,
                    help="Be more verbose. Both -v and -q may be used multiple times.")
    parser.add_argument('--log-file', dest="log_file", type=str, nargs='?')
    parser.add_argument('--exchange', dest="exchange_list", type=str, nargs='+', help="List of exchange to fetch for")
    parser.add_argument('--writer', dest="writer_list", type=str, nargs='+', help="List of writers")
    
    args = parser.parse_args()

    args = args_handler(args)

    pwd = getpass("provide password for pk:")

    executor = Runner()
    
    logging.info("Launching Process")

    # executor.launch_exchange_processes(pwd, args.exchange_list)

    execut
