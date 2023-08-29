import asyncio
import logging
import os
from pathlib import Path
from typing import Optional

import pandas as pd
from tabulate import tabulate

from utilities.telegram_handler import telegramHandler


class netliqToTelgram(telegramHandler):
    pd.options.display.float_format = "{:,.2f}".format
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)
    def __init__(self, password: str, to_telegram = True) -> None:
        current_path = Path(os.path.realpath(os.path.dirname(__file__)))
        super().__init__(str(current_path.parent), password)
        self.netliq_path = self.get_netliq_path(current_path)

        self.logger = logging.getLogger(__name__)
        logging.getLogger('matplotlib').setLevel(logging.CRITICAL) #hacky way

        self.to_telegram = to_telegram

    def get_netliq_path(self, current_path) -> str:
        self.base = os.path.dirname(os.path.dirname(current_path))
        return self.base + '/account_data_fetcher/csv_db/balance.csv'

    async def send_yesterday(self) -> None:
        df = pd.read_csv(self.netliq_path)

        total_columns_len = len(df.columns)

        numeric_columns = df.columns[1:]
        df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric, errors='coerce')
        
        df.fillna(0, inplace=True)

        df_pct_change = df.iloc[:, 1:].pct_change().multiply(100)

        df = pd.concat([df, df_pct_change.add_suffix('_pct')], axis=1)
        
        last_line_df = df.tail(1).applymap(self.format_value)

        last_line_df = last_line_df.T

        last_row_nav = last_line_df.iloc[:total_columns_len,]

        pct_change_values = last_line_df.iloc[total_columns_len:,]

        structured_df = pd.DataFrame({'Value': self.to_single_list(last_row_nav.values), 'Pct Change': [last_line_df.iloc[0,0]] + self.to_single_list(pct_change_values.values)}, index=last_line_df.index[:total_columns_len].values)
        
        table = tabulate(structured_df, headers='keys', tablefmt='pipe', showindex=True)

        await self.send_text_to_telegram(f"\n```\n{table}\n```", parse_mode="MarkdownV2")

    @staticmethod
    def to_single_list(value: list) -> list:
        return [item for sublist in value for item in sublist]

    @staticmethod
    def format_value(value):
        if pd.isna(value):
            return 0
        elif isinstance(value, float):
            return "{:,.2f}".format(value)
        else:
            return str(value)

if __name__ == "__main__":
    from getpass import getpass
    log_filename= os.path.expanduser("~") + "/log/test.py"
    numeric_level = getattr(logging, "DEBUG", None)
    logging.basicConfig(level=numeric_level, format='%(levelname)s:%(asctime)s:%(message)s', filename=log_filename) 
    logger: logging.Logger = logging.getLogger()
    pwd = getpass("provide password for pk:")
    current_path = Path(os.path.realpath(os.path.dirname(__file__)))
    executor = netliqToTelgram(pwd, False)
    asyncio.run(executor.send_yesterday())
