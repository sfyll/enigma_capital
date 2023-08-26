import logging
from dataclasses import asdict
import os
from typing import Dict

from account_data_fetcher.writers.writer_base import WriterBase

from csv import DictWriter
import pandas as pd

class Writer(WriterBase):
    __WRITER = "CSV"
    __PATH_SUFFIX = "/account_data_fetcher/csv_db/"
    def __init__(self, port_number: float) -> None:
        super().__init__(port_number, self.__WRITER)
        self.logger = logging.getLogger(__name__)
        self.path = self.get_base_path + self.__PATH_SUFFIX

    def update_balances(self, balances: Dict[str, float]) -> None:
        balance_path: str = self.path + 'balance.csv'
        
        try:
            with open(balance_path, 'a', newline='') as csvfile:
                writer = DictWriter(csvfile, fieldnames=balances.keys())
                # If the file is empty, write the headers
                if csvfile.tell() == 0:
                    writer.writeheader()
                writer.writerow(balances)
        except FileNotFoundError:
            pd.DataFrame([balances]).set_index("date").to_csv(balance_path)

        self.logger.info(f"writting {balances=}")

    def update_positions(self, positions: Dict[str, list]) -> None:
        positions_path: str = self.path + 'position.csv'
        
        with open(positions_path, 'a', newline='') as csvfile:
            writer = DictWriter(csvfile, fieldnames=positions.keys())
            if csvfile.tell() == 0:
                writer.writeheader()
            for row in zip(*positions.values()):
                writer.writerow(dict(zip(positions.keys(), row)))
        
        self.logger.info(f"writting {positions=}")


if __name__ == '__main__':
    from getpass import getpass
    log_filename= os.path.expanduser("~") + "/log/test.py"
    numeric_level = getattr(logging, "DEBUG", None)
    logging.basicConfig(level=numeric_level, format='%(levelname)s:%(asctime)s:%(message)s', filename=log_filename) 
    logger: logging.Logger = logging.getLogger()
    pwd = getpass("provide password for pk:")
