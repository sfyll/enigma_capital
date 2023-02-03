import logging
import os

import gspread
from oauth2client.service_account import ServiceAccountCredentials

from utilities.account_data_fetcher_base import accountFetcherBase


class gsheetHandler(accountFetcherBase):
    __SCOPE = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
               "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
    def __init__(self, path: str, password: str) -> None:
        super().__init__(path, password, "GSHEET")
        self.logger = logging.getLogger(__name__)
        self.authenticate()

    def authenticate(self) -> None:
        credentials = ServiceAccountCredentials.from_json_keyfile_dict(self.gsheet_meta_data, scopes=self.__SCOPE)
        self.client = gspread.authorize(credentials)

    def update_netliq_gsheet(self, csv_path: str, spreadsheet_name: str = "Netliq") -> None:
        spreadsheet = self.client.open(spreadsheet_name)
        with open(csv_path, "r") as file_obj:
            content = file_obj.read()
            self.client.import_csv(spreadsheet.id, data = content)

    def update_position_gsheet(self, csv_path: str, spreadsheet_name: str = "Position") -> None:
        spreadsheet = self.client.open(spreadsheet_name)
        with open(csv_path, "r") as file_obj:
            content = file_obj.read()
            self.client.import_csv(spreadsheet.id, data = content)


if __name__ == '__main__':
    from getpass import getpass
    log_filename= os.path.expanduser("~") + "/log/test.py"
    numeric_level = getattr(logging, "DEBUG", None)
    logging.basicConfig(level=numeric_level, format='%(levelname)s:%(asctime)s:%(message)s', filename=log_filename) 
    logger: logging.Logger = logging.getLogger()
    pwd = getpass("provide password for pk:")
