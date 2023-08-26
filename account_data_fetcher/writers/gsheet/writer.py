import logging
from dataclasses import asdict
import os

import gspread
from oauth2client.service_account import ServiceAccountCredentials

from account_data_fetcher.writers.writer_base import WriterBase
from infrastructure.api_secret_getter import ApiMetaData


class Writer(WriterBase):
    __SCOPE = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
               "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
    __WRITER = "GSHEET"
    def __init__(self, secrets: ApiMetaData, data_aggregator_port_number: int, port_number: int) -> None:
        super().__init__(data_aggregator_port_number, port_number, self.__WRITER)
        self.logger = logging.getLogger(__name__)
        self.authenticate(secrets)

    def authenticate(self, secrets: ApiMetaData) -> None:
        credentials = ServiceAccountCredentials.from_json_keyfile_dict(asdict(secrets), scopes=self.__SCOPE)
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

    def update_balances(self, balance_dict: dict) -> None:
        spreadsheet = self.client.open('Netliq')
        worksheet = spreadsheet.get_worksheet(0)
        
        if worksheet.row_count == 0:
            worksheet.append_row(list(balance_dict.keys()))

        worksheet.append_row(list(balance_dict.values()))

    def update_positions(self, position_dict: dict) -> None:
        spreadsheet = self.client.open("Position")
        worksheet = spreadsheet.get_worksheet(0)

        rows = list(zip(*position_dict.values()))

        if worksheet.row_count == 0:
            worksheet.append_row(list(position_dict.keys()))

        for row in rows:
            worksheet.append_row(list(row))

if __name__ == '__main__':
    from getpass import getpass
    log_filename= os.path.expanduser("~") + "/log/test.py"
    numeric_level = getattr(logging, "DEBUG", None)
    logging.basicConfig(level=numeric_level, format='%(levelname)s:%(asctime)s:%(message)s', filename=log_filename) 
    logger: logging.Logger = logging.getLogger()
    pwd = getpass("provide password for pk:")
