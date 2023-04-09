from datetime import datetime
import os
from typing import Optional

import matplotlib.pyplot as plt
import pandas as pd


class returnStudy:
    def __init__(self):
        self.current_path = os.path.realpath(os.path.dirname(__file__))
        self.parent_path = os.path.dirname(self.current_path)
        
    def construct_twr(self, start_date: str ='01/01/2023', exchange: Optional[str] = None):
        start_date = datetime.strptime(start_date, '%d/%m/%Y')

        netliq_file = self.parent_path+'/netliq.csv'
        transactions_file = self.parent_path+'/deposits_and_withdraws.csv'

        netliq_data = pd.read_csv(netliq_file)
        netliq_data['date'] = pd.to_datetime(netliq_data['date'])
        transactions_data = pd.read_csv(transactions_file)
        transactions_data['date'] = pd.to_datetime(transactions_data['date'], format='%d/%m/%Y')

        netliq_data = netliq_data[netliq_data['date'] >= start_date]
        transactions_data = transactions_data[transactions_data['date'] >= start_date]

        merged_data = pd.merge(netliq_data, transactions_data, on='date', how='outer').sort_values('date')

        merged_data['netliq'] = merged_data['netliq'].fillna(method='ffill')

        merged_data['net_transaction'] = merged_data.apply(lambda row: row['amount'] if pd.isna(row['from_exchange']) or pd.isna(row['to_exchange']) else 0, axis=1)

        merged_data['amount'] = merged_data['amount'].fillna(0)

        merged_data['net_transaction'] = merged_data['net_transaction'].fillna(0)
        
        merged_data['daily_return'] = merged_data['netliq'] / (merged_data['netliq'].shift(1) + merged_data['net_transaction'].shift(1)) - 1

        filtered_data = merged_data[(merged_data['net_transaction'].notnull()) | (merged_data['from_exchange'].isna() & merged_data['to_exchange'].isna())]
        
        filtered_data['daily_twr'] = ((1 + filtered_data['daily_return']).cumprod() - 1 ) * 100

        daily_twr_data = filtered_data[['date', 'daily_twr']]

        return daily_twr_data

    def plot_2d(self, date: str, data) -> None:
        plt.plot(date, data)
        plt.xlabel('Date')
        plt.ylabel('Daily Time-Weighted Return')
        plt.title('Daily TWR Over Time')
        plt.grid()
        plt.show()


if __name__ == '__main__':
    executor = returnStudy()
    daily_twr_data = executor.construct_twr()

    executor.plot_2d(daily_twr_data['date'], daily_twr_data['daily_twr'])