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
            existing_columns = list(df.columns)
            new_columns = list(balances.keys())

            if 'date' not in new_columns:
                raise ValueError("The 'date' column is missing from the balances.")
            
            set_existing_columns = set(existing_columns)
            set_new_columns = set(new_columns)
            new_col_dif = set_new_columns - set_existing_columns
            
            if set_existing_columns!= set_new_columns:
                if set_new_columns.issuperset(set_existing_columns) or new_col_dif:
                    # New columns added, rewrite the whole CSV
                    df = pd.read_csv(balance_path, index_col="date")
                    for col in set_new_columns - set_existing_columns:
                        df[col] = float(0)
                    
                    new_row = pd.DataFrame([balances]).set_index("date")
                    df = pd.concat([df, new_row])
                    df.to_csv(balance_path)
                    
                elif set_new_columns.issubset(set_existing_columns):
                    # Missing columns, append only with a new row filled appropriately
                    balances = {col: balances.get(col, float(0)) for col in existing_columns}
                    with open(balance_path, 'a', newline='') as csvfile:
                        writer = DictWriter(csvfile, fieldnames=existing_columns)
                        writer.writerow(balances)

            else:
                # Columns match, just append the row
                with open(balance_path, 'a', newline='') as csvfile:
                    writer = DictWriter(csvfile, fieldnames=balances.keys())
                    writer.writerow(balances)
                    
        except pd.errors.EmptyDataError:
            pd.DataFrame([balances]).set_index("date").to_csv(balance_path)

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

if __name__ == '__main__':
    from getpass import getpass
    log_filename= os.path.expanduser("~") + "/log/test.py"
    numeric_level = getattr(logging, "DEBUG", None)
    logging.basicConfig(level=numeric_level, format='%(levelname)s:%(asctime)s:%(message)s', filename=log_filename) 
    logger: logging.Logger = logging.getLogger()
    pwd = getpass("provide password for pk:")
