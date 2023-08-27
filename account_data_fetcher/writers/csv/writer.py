import logging
from dataclasses import asdict
from datetime import datetime
import os
from typing import Dict

from account_data_fetcher.writers.writer_base import WriterBase

from csv import DictWriter
import pandas as pd

class Writer(WriterBase):
    __WRITER = "CSV"
    __PATH_SUFFIX = "/account_data_fetcher/csv_db/"
    def __init__(self, data_aggregator_port_number: int, port_number: int) -> None:
        super().__init__(data_aggregator_port_number, port_number, self.__WRITER)
        self.logger = logging.getLogger(__name__)
        self.path = self.get_base_path() + self.__PATH_SUFFIX

    def update_balances(self, balances: Dict[str, float]) -> None:
        balance_path = self.path + 'balance.csv'

        try:
            df = pd.read_csv(balance_path, nrows=1)  # Read just the first row to get columns
            existing_columns = set(df.columns)
            new_columns = set(balances.keys())
            
            if existing_columns != new_columns:
                # Rewrite the whole CSV only when columns differ
                df = pd.read_csv(balance_path, index_col="date")  # Read the entire CSV
                for col in new_columns - existing_columns:
                    df[col] = float(0)
                
                df = pd.concat([df, pd.DataFrame([balances]).set_index("date")])
                df.to_csv(balance_path)
            else:
                # Append the new row when columns are the same
                with open(balance_path, 'a', newline='') as csvfile:
                    writer = DictWriter(csvfile, fieldnames=balances.keys())
                    writer.writerow(balances)
                    
        except FileNotFoundError:
            pd.DataFrame([balances]).set_index("date").to_csv(balance_path)

        self.logger.info(f"writting {balances=}")

    def update_positions(self, positions: Dict[str, list]) -> None:
        positions_path = f"{self.path}position.csv"
        
        try:
            df = pd.read_csv(positions_path, nrows=1)  # Read just the first row to get columns
            existing_columns = set(df.columns)
            new_columns = set(positions.keys())

            if existing_columns != new_columns:
                raise ValueError("CSV headers don't match. Exiting without writing since logic is not handled.")

            # Continue with the normal logic of writing
            with open(positions_path, 'a', newline='') as csvfile:
                writer = DictWriter(csvfile, fieldnames=positions.keys())
                for row in zip(*positions.values()):
                    writer.writerow(dict(zip(positions.keys(), row)))

        except FileNotFoundError:
            with open(positions_path, 'w', newline='') as csvfile:
                writer = DictWriter(csvfile, fieldnames=positions.keys())
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
