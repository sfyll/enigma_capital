import asyncio
import logging
from typing import Dict

from account_data_fetcher.writers.writer_base import WriterBase

from csv import DictWriter
import pandas as pd

class Writer(WriterBase):
    __WRITER = "CSV"
    __PATH_SUFFIX = "/account_data_fetcher/csv_db/"
    def __init__(self, input_queue: asyncio.Queue) -> None:
        super().__init__(self.__WRITER, input_queue)
        self.logger = logging.getLogger(__name__)
        self.path = self.get_base_path() + self.__PATH_SUFFIX
        self._balance_lock = asyncio.Lock()
        self._positions_lock = asyncio.Lock()

    async def update_balances(self, balances: Dict[str, float]) -> None:
        """Asynchronously updates the balance CSV file by running blocking I/O in a separate thread."""
        def blocking_io_handler(balances_data: Dict[str, float]):
            balance_path = self.path + 'balance.csv'

            try:
                df = pd.read_csv(balance_path, nrows=1)
                existing_columns = list(df.columns)
                new_columns = list(balances_data.keys())

                if 'date' not in new_columns:
                    raise ValueError("The 'date' column is missing from the balances.")
                
                set_existing_columns = set(existing_columns)
                set_new_columns = set(new_columns)
                new_col_dif = set_new_columns - set_existing_columns
                
                if set_existing_columns != set_new_columns:
                    if set_new_columns.issuperset(set_existing_columns) or new_col_dif:
                        df = pd.read_csv(balance_path, index_col="date")
                        for col in set_new_columns - set_existing_columns:
                            df[col] = float(0)
                        
                        new_row = pd.DataFrame([balances_data]).set_index("date")
                        df = pd.concat([df, new_row])
                        df.to_csv(balance_path)
                        
                    elif set_new_columns.issubset(set_existing_columns):
                        # The variable name here is now local to the function and doesn't cause a conflict
                        balances_to_write = {col: balances_data.get(col, float(0)) for col in existing_columns}
                        with open(balance_path, 'a', newline='') as csvfile:
                            writer = DictWriter(csvfile, fieldnames=existing_columns)
                            writer.writerow(balances_to_write)

                else:
                    with open(balance_path, 'a', newline='') as csvfile:
                        writer = DictWriter(csvfile, fieldnames=balances_data.keys())
                        writer.writerow(balances_data)
                        
            except (pd.errors.EmptyDataError, FileNotFoundError):
                pd.DataFrame([balances_data]).set_index("date").to_csv(balance_path)

        async with self._balance_lock:
            await asyncio.to_thread(blocking_io_handler, balances)

    async def update_positions(self, positions: Dict[str, list]) -> None:
        """Asynchronously updates the positions CSV file by running blocking I/O in a separate thread."""
        def blocking_io_handler(positions_data: Dict[str, list]):
            positions_path = f"{self.path}position.csv"
            
            try:
                df = pd.read_csv(positions_path, nrows=1)
                existing_columns = set(df.columns)
                new_columns = set(positions_data.keys())

                if existing_columns != new_columns:
                    raise ValueError("CSV headers don't match. Exiting without writing since logic is not handled.")

                with open(positions_path, 'a', newline='') as csvfile:
                    writer = DictWriter(csvfile, fieldnames=positions_data.keys())
                    for row in zip(*positions_data.values()):
                        writer.writerow(dict(zip(positions_data.keys(), row)))

            except FileNotFoundError:
                with open(positions_path, 'w', newline='') as csvfile:
                    writer = DictWriter(csvfile, fieldnames=positions_data.keys())
                    writer.writeheader()
                    for row in zip(*positions_data.values()):
                        writer.writerow(dict(zip(positions_data.keys(), row)))

        async with self._positions_lock:
            await asyncio.to_thread(blocking_io_handler, positions)
