import argparse
from datetime import datetime
from math import isnan
import os
from typing import Optional

import matplotlib.pyplot as plt
import pandas as pd

class returnStudy:
    def __init__(self):
        self.base_path, self.netliq_path, self.transactions_path = self.get_netliq_path()
        
    def get_netliq_path(self) -> str:
        base = self.find_project_root()
        return base, base + '/account_data_fetcher/csv_db/balance.csv', base + '/account_data_fetcher/csv_db/deposits_and_withdraws.csv'

    def find_project_root(self) -> str:
        """
        Locate the project root by searching for the directory containing the 'env' folder.
        """
        current_path = os.path.realpath(os.path.dirname(__file__))
        while current_path != '/':
            if 'env' in os.listdir(current_path):
                return current_path
            current_path = os.path.dirname(current_path)
        raise RuntimeError("Project root not found. Ensure the 'env' directory exists at the root.")
        
    def construct_twr(self, start_date: str ='01/01/2023', exchange: Optional[str] = None):
        self.row_cache = pd.DataFrame()
        start_date = datetime.strptime(start_date, '%d/%m/%Y')

        netliq_data = pd.read_csv(self.netliq_path)
        netliq_data['date'] = pd.to_datetime(netliq_data['date'])
        transactions_data = pd.read_csv(self.transactions_path)
        transactions_data['date'] = pd.to_datetime(transactions_data['date'], format='%d/%m/%Y')
        netliq_data['date'] = pd.to_datetime(netliq_data['date'].dt.strftime('%d/%m/%Y'), format = '%d/%m/%Y')

        if exchange is not None:
            netliq_data['netliq'] = netliq_data[exchange.lower()]
            net_transactions = self.get_transaction_data_per_exchange(exchange.upper(), transactions_data)
        else:
            net_transactions = self.get_transaction_data_portfolio(transactions_data)

        netliq_data.set_index('date', inplace=True)

        merged_data = pd.merge(netliq_data[["netliq"]], net_transactions, left_index=True, right_index=True, how='left', sort=True)
        
        merged_data = merged_data[netliq_data.index >= start_date]
        merged_data.fillna(0, inplace=True)
        
        if exchange is not None:
            merged_data['daily_return'] = [self.compute_daily_returns(row) for _, row in merged_data.iterrows()]
        else:
            merged_data['daily_return'] = (merged_data['netliq'] / (merged_data['netliq'].shift(1) + merged_data['net_amount'])) - 1

        merged_data['daily_twr'] = ((1 + merged_data['daily_return']).cumprod() - 1 ) * 100

        return merged_data

    def get_transaction_data_per_exchange(self, exchange, transactions_data) -> pd.DataFrame:
        transactions_data = transactions_data[(transactions_data['from_exchange'] == exchange.upper()) | (transactions_data['to_exchange'] == exchange.upper())]
        transactions_data['exchange'] = transactions_data.apply(
            lambda row: row['to_exchange'] if row["to_exchange"] == exchange else row['from_exchange'], axis=1
            )

        transactions_data['net_amount'] = transactions_data.apply(
                lambda row: abs(row['amount']) if row['to_exchange'] == exchange else row['amount'], axis=1
            )

        net_transactions = transactions_data.groupby(['date', 'exchange'])['net_amount'].sum().reset_index()

        net_transactions.set_index('date', inplace=True)

        return net_transactions

    def get_transaction_data_portfolio(self, transactions_data) -> pd.DataFrame:
        transactions_data.fillna(0, inplace=True)
        transactions_data = transactions_data[(transactions_data['from_exchange'] == 0) | (transactions_data['to_exchange'] == 0)]

        transactions_data.rename(columns={'amount': 'net_amount'}, inplace=True)
        net_transactions = transactions_data.groupby(['date'])['net_amount'].sum().reset_index()

        net_transactions.set_index('date', inplace=True)

        return net_transactions

    def compute_daily_returns(self, row) -> float:
        if self.row_cache.empty:
            self.row_cache = row

        #case where netliq exists but was just populated by a deposit!
        if row["netliq"] > 0 and self.row_cache["netliq"] == 0 and row["net_amount"] > 0:
            daily_returns = (row["netliq"]/(abs(row["net_amount"])) - 1)
            
        #case where netliq doesn't exit anymore as was just withdrwawn
        elif row["netliq"] < 100 and row["net_amount"] != 0 :
            daily_returns = (abs(row["net_amount"]) / self.row_cache["netliq"]) - 1

        #case where both netliq and net amount don't exist, exchange was just left
        elif row["netliq"] < 100 and row["net_amount"] == 0 :
            daily_returns = 0.0
            
        else:
            daily_returns = (row["netliq"] /( self.row_cache["netliq"] + row["net_amount"])) - 1
        
        self.row_cache = row

        return daily_returns

    def plot_2d(self, date: str, data) -> None:
        plt.plot(date, data)
        plt.xlabel('Date')
        plt.ylabel('Daily Time-Weighted Return')
        plt.title('Daily TWR Over Time')
        plt.grid()
        plt.show()

    def save_to_csv(self, df: pd.DataFrame, filename):
        """
        Saves a given DataFrame to a CSV file.
        :param df: DataFrame to be saved.
        :param filename: The name of the file to save the data.
        """
        # Using the netliq path to determine the directory
        file_path = os.path.join(self.base_path, filename)
        
        df.to_csv(file_path)
    
    def load_csv(self, filename):
        """
        Load a given CSV file into a DataFrame.
        :param df: DataFrame to be saved.
        :param filename: The name of the file to save the data.
        """
        # Using the netliq path to determine the directory
        file_path = os.path.join(self.base_path, filename)
        # Read the CSV file and return the DataFrame
        return pd.read_csv(file_path, index_col='date', parse_dates=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Run the return study script.")
    parser.add_argument(
        '--start-date',
        type=str,
        default="01/01/2024",
        help="The start date for the TWR calculation in the format DD/MM/YYYY. Defaults to 01/01/2024."
    )
    args = parser.parse_args()
    executor = returnStudy()

    daily_statistics_path = "account_data_fetcher/csv_db/daily_statistics.csv"

    if os.path.exists(os.path.join(executor.base_path, daily_statistics_path)):
        print("Loading existing daily statistics...")
        daily_twr_data = executor.load_csv(daily_statistics_path)
        # Check if the loaded data includes the requested start date
        if daily_twr_data.index.min() < pd.to_datetime(args.start_date, format='%d/%m/%Y'):
            daily_twr_data = executor.construct_twr(start_date=args.start_date, exchange=None)
    else:
        print(f"Daily statistics file not found. Constructing data from {args.start_date}...")
        daily_twr_data = executor.construct_twr(start_date=args.start_date, exchange=None)

    executor.save_to_csv(daily_twr_data, daily_statistics_path)

    executor.plot_2d(daily_twr_data.index, daily_twr_data['daily_twr'])
