import asyncio
from datetime import datetime
import logging
import os
from pathlib import Path

import pandas as pd
from tabulate import tabulate

from utilities.telegram_handler import telegramHandler

#TODO: Format for phone recipient !
class netliqToTelgram(telegramHandler):
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)
    def __init__(self, password: str, to_telegram = True) -> None:
        current_path = Path(__file__).resolve().parent
        super().__init__(str(current_path.parent), password)
        self.netliq_path = self.get_netliq_path(current_path)

        self.logger = logging.getLogger(__name__)

        self.to_telegram = to_telegram

    def get_netliq_path(self, current_path) -> str:
        base = current_path.parent.parent
        return os.path.join(base, 'account_data_fetcher/csv_db/balance.csv')
    
    async def format_and_send_dataframe(self) -> None:
        df = pd.read_csv(self.netliq_path)
        
        pd.options.display.float_format = "{:,.2f}".format

        structured_df = self.transform_dataframe(df)
        table = tabulate(structured_df, headers='keys', tablefmt='fancy_grid', showindex=True)

        await self.send_text_to_telegram(f"\n```\n{table}\n```", parse_mode="MarkdownV2")

    def transform_dataframe(self, df) -> pd.DataFrame:
        total_columns_len = len(df.columns)
        df.fillna(0, inplace=True)
        df_pct_change = df.iloc[:, 1:].pct_change().multiply(100)
        df = pd.concat([df, df_pct_change.add_suffix('_pct')], axis=1)
        
        last_line_df = df.tail(1).applymap(self.format_value)
        last_row_nav = last_line_df.iloc[:, :total_columns_len].T
        pct_change_values = last_line_df.iloc[:, total_columns_len:].T

        structured_df = pd.DataFrame({
            'Val': self.to_single_list(last_row_nav.values),
            'PctChg': [last_line_df.iloc[0, 0]] + self.to_single_list(pct_change_values.values)
        }, index=last_row_nav.index.values)

        if 'date' in structured_df.index:
            original_date_str = structured_df.loc['date', 'Val']
            formatted_date = datetime.strptime(original_date_str, '%Y-%m-%d %H:%M:%S').strftime('%d/%m/%y')
            structured_df.loc['date', 'Val'] = formatted_date
            structured_df.loc['date', 'PctChg'] = formatted_date

        return structured_df

    @staticmethod
    def to_single_list(value: list) -> list:
        return [item for sublist in value for item in sublist]

    @staticmethod
    def format_value(value):
            return "{:,.2f}".format(value) if isinstance(value, float) else str(value)


if __name__ == "__main__":
    from getpass import getpass
    log_filename= os.path.expanduser("~") + "/log/test.py"
    logging.basicConfig(level=logging.DEBUG, format='%(levelname)s:%(asctime)s:%(message)s', filename=log_filename)

    pwd = getpass("provide password for pk:")
    executor = netliqToTelgram(pwd, False)
    executor.format_and_send_dataframe()