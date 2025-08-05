import asyncio
import logging
from dataclasses import asdict
from typing import Dict

import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials

from account_data_fetcher.writers.writer_base import WriterBase
from infrastructure.api_secret_getter import ApiMetaData


class Writer(WriterBase):
    __SCOPE = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
               "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
    __WRITER = "GSHEET"
    __PATH_SUFFIX = "/account_data_fetcher/csv_db/"
    def __init__(self, secrets: ApiMetaData, input_queue: asyncio.Queue) -> None:
        super().__init__(self.__WRITER, input_queue)
        self.logger = logging.getLogger(__name__)
        self.path = self.get_base_path() + self.__PATH_SUFFIX
        self.authenticate(secrets)
        self._balance_lock = asyncio.Lock()
        self._positions_lock = asyncio.Lock()

    def authenticate(self, secrets: ApiMetaData) -> None:
        credentials = ServiceAccountCredentials.from_json_keyfile_dict(asdict(secrets)["other_fields"], scopes=self.__SCOPE)
        self.client = gspread.authorize(credentials)

    def write_balances(self, csv_path: str) -> None:
        spreadsheet = self.client.open('Balances')
        with open(csv_path, "r") as file_obj:
            content = file_obj.read()
            self.client.import_csv(spreadsheet.id, data = content)

    def write_positions(self, csv_path: str) -> None:
        spreadsheet = self.client.open("Positions")
        with open(csv_path, "r") as file_obj:
            content = file_obj.read()
            self.client.import_csv(spreadsheet.id, data = content)

    async def update_balances(self, balances: Dict[str, float]) -> None:
        """Asynchronously updates GSheet by running blocking gspread calls in a separate thread."""
        
        # 1. Accept balances as an argument
        def blocking_io_handler(balances_data: Dict[str, float]):
            spreadsheet = self.client.open('Balances')
            worksheet = spreadsheet.get_worksheet(0)
            headers = worksheet.row_values(1)
            new_columns = list(balances_data.keys())

            # If the sheet is empty, write the header and the first row directly
            if not headers:
                worksheet.append_row(new_columns)
                worksheet.append_row([balances_data.get(col) for col in new_columns])
                return

            set_headers = set(headers)
            set_new_columns = set(new_columns)

            # If columns are identical, just append the new row in the correct order
            if set_headers == set_new_columns:
                row_to_append = [balances_data.get(col) for col in headers]
                worksheet.append_row(row_to_append)
                return

            # If columns differ, handle by rewriting (this logic can be complex,
            # so reading all records and rewriting is a safe, if slow, approach)
            all_records = worksheet.get_all_records()
            df = pd.DataFrame(all_records)
            
            # Combine all possible columns
            all_cols = headers + list(set_new_columns - set_headers)
            
            # Reindex old data with new columns, filling missing with 0
            df = df.reindex(columns=all_cols, fill_value=float(0))
            
            # Create a DataFrame for the new row and reindex it as well
            new_row_df = pd.DataFrame([balances_data])
            new_row_df = new_row_df.reindex(columns=all_cols, fill_value=float(0))

            # Combine old and new data
            df = pd.concat([df, new_row_df], ignore_index=True)
            
            # Update the entire Google Sheet
            worksheet.clear()
            worksheet.update([df.columns.values.tolist()] + df.values.tolist())

        async with self._balance_lock:
            await asyncio.to_thread(blocking_io_handler, balances)

    async def update_positions(self, position_dict: dict) -> None:
        """Asynchronously updates GSheet by running blocking gspread calls in a separate thread.""" 
        
        def blocking_io_handler(positions_data: dict):
            spreadsheet = self.client.open("Positions")
            worksheet = spreadsheet.get_worksheet(0)
            headers = worksheet.row_values(1)
            
            if headers:
                if headers != list(positions_data.keys()):
                    raise ValueError("GSheet headers don't match incoming position data. Exiting without writing.")
            else:
                # If sheet is empty, write the header
                worksheet.append_row(list(positions_data.keys()))

            # Append all new position rows
            rows_to_append = list(zip(*positions_data.values()))
            if rows_to_append:
                worksheet.append_rows(values=rows_to_append)

        async with self._positions_lock:
            await asyncio.to_thread(blocking_io_handler, position_dict)
