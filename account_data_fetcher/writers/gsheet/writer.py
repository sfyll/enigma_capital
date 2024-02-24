import logging
from dataclasses import asdict
import os
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
    def __init__(self, secrets: ApiMetaData, data_aggregator_port_number: int, port_number: int) -> None:
        super().__init__(data_aggregator_port_number, port_number, self.__WRITER)
        self.logger = logging.getLogger(__name__)
        self.path = self.get_base_path() + self.__PATH_SUFFIX
        self.authenticate(secrets)

    def authenticate(self, secrets: ApiMetaData) -> None:
        credentials = ServiceAccountCredentials.from_json_keyfile_dict(asdict(secrets)["other_fields"], scopes=self.__SCOPE)
        self.client = gspread.authorize(credentials)

    def write_balances(self, csv_path: str) -> None:
        spreadsheet = self.client.open('Netliq')
        with open(csv_path, "r") as file_obj:
            content = file_obj.read()
            self.client.import_csv(spreadsheet.id, data = content)

    def write_positions(self, csv_path: str) -> None:
        spreadsheet = self.client.open("Position")
        with open(csv_path, "r") as file_obj:
            content = file_obj.read()
            self.client.import_csv(spreadsheet.id, data = content)

    def update_balances(self, balances: Dict[str, float]) -> None:
        balance_path = self.path + 'balance.csv'
        spreadsheet = self.client.open('Balances')
        worksheet = spreadsheet.get_worksheet(0)
        
        # Fetch only the headers
        headers = worksheet.row_values(1)
        
        # If the sheet is empty
        if not headers:
            # the below method has risks of race conditions if balances are being updated
            self.write_balances(balance_path) 
            return       
        
        new_columns = list(balances.keys())
        set_new_columns = set(new_columns)
        set_headers = set(headers)

        new_col_dif = set_new_columns - set_headers 
        
        if new_col_dif:
            if set_new_columns.issuperset(set_headers):
                # New columns added, rewrite the entire sheet
                records = worksheet.get_all_records()
                df = pd.DataFrame(records)
                
                for col in set_new_columns - set_headers:
                    df[col] = float(0)
                
                new_row = pd.DataFrame([balances]).set_index("date")
                df = pd.concat([df, new_row])
                
                # Update the entire Google Sheet
                worksheet.clear()
                worksheet.append_row(new_columns)
                for _, row in df.iterrows():
                    worksheet.append_row(list(row))
            
            elif set_new_columns.issubset(set_headers):
                # Missing columns, fill in zeros for the missing fields and append the row
                row_to_append = [balances.get(col, float(0)) for col in headers]
                worksheet.append_row(row_to_append)

            else:
            # Columns are different but neither set is a subset of the other / might be due to different naming convention1
            # Append the new columns on the right side and fill in zeros for old records
                new_cols_to_add = list(set_new_columns - set_headers)

                records = worksheet.get_all_records()
                df = pd.DataFrame(records)
                
                for col in new_cols_to_add:
                    df[col] = float(0)
                
                new_row = pd.DataFrame([balances]).set_index("date")
                df = pd.concat([df, new_row])
                
                # Update the entire Google Sheet
                worksheet.clear()
                worksheet.append_row(new_columns)
                for _, row in df.iterrows():
                    worksheet.append_row(list(row))
        else:
            # Columns match, just append the row in the correct order
            row_to_append = [balances.get(col, float(0)) for col in headers]
            worksheet.append_row(row_to_append)

    def update_positions(self, position_dict: dict) -> None:
        spreadsheet = self.client.open("Positions")
        worksheet = spreadsheet.get_worksheet(0)

        headers = worksheet.row_values(1)
        if headers:
            if headers != list(position_dict.keys()):
                raise ValueError("CSV headers don't match. Exiting without writing.")
        else:
            worksheet.append_row(list(position_dict.keys()))

        rows = list(zip(*position_dict.values()))
        for row in rows:
            worksheet.append_row(list(row))

if __name__ == '__main__':
    from getpass import getpass
    log_filename= os.path.expanduser("~") + "/log/test.py"
    numeric_level = getattr(logging, "DEBUG", None)
    logging.basicConfig(level=numeric_level, format='%(levelname)s:%(asctime)s:%(message)s', filename=log_filename) 
    logger: logging.Logger = logging.getLogger()
    pwd = getpass("provide password for pk:")
