import argparse
import pandas as pd
import matplotlib.pyplot as plt
import os
import warnings
from datetime import datetime

from utils import find_project_root

warnings.filterwarnings("ignore")

class Investor:
    def __init__(self, name):
        self.name = name
        self.units = 0.0
        self.unit_history = pd.DataFrame(columns=['date', 'units', 'cumulative_units'])

    def update_units(self, date, units):
        self.units += units
        cumulative_units = self.units
        new_row = {'date': date, 'units': units, 'cumulative_units': cumulative_units}
        self.unit_history = pd.concat([self.unit_history, pd.DataFrame([new_row])], ignore_index=True)
        print(f"[Investor Update] {self.name} on {date.strftime('%Y-%m-%d')}: Units Change = {units}, Total Units = {cumulative_units}")

class Portfolio:
    def __init__(self, netliq_data, cash_flow_data, fund_owner_name):
        self.netliq_data = netliq_data.sort_values('date').reset_index(drop=True)
        self.cash_flow_data = cash_flow_data.sort_values('date').reset_index(drop=True)
        self.fund_owner_name = fund_owner_name
        self.investors = {}
        self.total_units = 0.0
        self.nav_history = pd.DataFrame(columns=['date', 'nav'])

    def add_investor(self, investor_name):
        investor_name = investor_name.lower()
        if investor_name not in self.investors:
            investor = Investor(investor_name)
            self.investors[investor_name] = investor
            print(f"[Add Investor] New investor added: {investor_name}")

    def process(self):
        all_dates = pd.concat([self.netliq_data['date'], self.cash_flow_data['date']]).drop_duplicates().sort_values().reset_index(drop=True)

        earliest_netliq_date = self.netliq_data['date'].min()

        all_dates = all_dates[all_dates >= earliest_netliq_date]

        initial_date = all_dates.iloc[0]
        initial_netliq = self.netliq_data[self.netliq_data['date'] <= initial_date]['netliq'].iloc[-1]
        initial_nav = 1.0
        self.total_units = initial_netliq / initial_nav

        self.add_investor(self.fund_owner_name)
        self.investors[self.fund_owner_name].update_units(initial_date, self.total_units)
        self.nav_history = pd.concat(
            [self.nav_history, pd.DataFrame({'date': [initial_date], 'nav': [initial_nav]})],
            ignore_index=True
        )

        multi_investor_mode = False

        for date in all_dates[1:]:
            netliq_row = self.netliq_data[self.netliq_data['date'] == date]
            if not netliq_row.empty:
                netliq = netliq_row['netliq'].values[0]
            else:
                prev_netliq_data = self.netliq_data[self.netliq_data['date'] < date]
                if not prev_netliq_data.empty:
                    netliq = prev_netliq_data['netliq'].iloc[-1]
                else:
                    print(f"No netliq data available before {date}, skipping.")
                    continue
            nav = netliq / self.total_units if self.total_units > 0 else 1.0
            self.nav_history = pd.concat(
                [self.nav_history, pd.DataFrame({'date': [date], 'nav': [nav]})],
                ignore_index=True
            )

            cash_flows = self.cash_flow_data[self.cash_flow_data['date'] == date]
            for idx, row in cash_flows.iterrows():
                investor_name = row['investor'].strip() if pd.notnull(row['investor']) else self.fund_owner_name
                amount = row['amount']

                if investor_name.lower() == 'fund':
                    continue

                if investor_name != self.fund_owner_name and investor_name not in self.investors:
                    multi_investor_mode = True
                    self.add_investor(investor_name)
                    print(f"[Mode Switch] Multiple investors detected from date {date.strftime('%Y-%m-%d')}")

                is_deposit = pd.isnull(row['from_exchange']) and pd.notnull(row['to_exchange'])
                is_withdrawal = pd.notnull(row['from_exchange']) and pd.isnull(row['to_exchange'])

                if is_deposit:
                    units = amount / nav
                elif is_withdrawal:
                    units = -amount / nav
                else:
                    # Skip invalid transactions
                    continue

                if not multi_investor_mode:
                    self.investors[self.fund_owner_name].update_units(date, units)
                else:
                    self.add_investor(investor_name)
                    self.investors[investor_name].update_units(date, units)

                self.total_units += units
                print(f"[Total Units Update] {date.strftime('%Y-%m-%d')}: Total Units = {self.total_units}")

    def calculate_shares(self):
        unit_histories = []
        for investor in self.investors.values():
            df = investor.unit_history[['date', 'cumulative_units']].copy()
            df.rename(columns={'cumulative_units': investor.name + '_cum_units'}, inplace=True)
            unit_histories.append(df)

        shares_df = pd.DataFrame()
        for df_unit in unit_histories:
            if shares_df.empty:
                shares_df = df_unit
            else:
                shares_df = pd.merge(shares_df, df_unit, on='date', how='outer')

        self.nav_history['date'] = pd.to_datetime(self.nav_history['date'])
        shares_df['date'] = pd.to_datetime(shares_df['date'])
        shares_df = pd.merge(shares_df, self.nav_history, on='date', how='outer')

        cum_unit_cols = [col for col in shares_df.columns if '_cum_units' in col]

        shares_df[cum_unit_cols] = shares_df[cum_unit_cols].ffill()
        shares_df['nav'] = shares_df['nav'].ffill()

        for col in cum_unit_cols:
            shares_df[col] = shares_df[col].fillna(0)

        shares_df['nav'] = shares_df['nav'].fillna(method='bfill')  # Backfill if nav starts with NaN
        shares_df.fillna(0, inplace=True)  # Fill any remaining NaNs

        shares_df['total_units'] = shares_df[cum_unit_cols].sum(axis=1)

        for col in cum_unit_cols:
            investor_name = col.replace('_cum_units', '')
            shares_df[investor_name + '_share'] = shares_df[col] / shares_df['total_units']

        share_cols = [inv + '_share' for inv in self.investors.keys()]
        shares_df['total_share'] = shares_df[share_cols].sum(axis=1)

        for investor in self.investors.keys():
            shares_df[investor + '_investment_value'] = shares_df[investor + '_cum_units'] * shares_df['nav']

        self.shares_df = shares_df

    def plot_shares(self):
        fig, ax1 = plt.subplots(figsize=(12, 6))

        investor_names = list(self.investors.keys())

        primary_investor = self.fund_owner_name.lower()
        share_col_primary = primary_investor + '_share'
        if share_col_primary in self.shares_df.columns:
            ax1.plot(self.shares_df['date'], self.shares_df[share_col_primary], color='blue', label=f"{primary_investor.capitalize()}'s Share")
            ax1.set_xlabel('Date')
            ax1.set_ylabel(f"{primary_investor.capitalize()}'s Share", color='blue')
            ax1.tick_params(axis='y', labelcolor='blue')
        else:
            print(f"{primary_investor.capitalize()}'s share data is not available.")

        ax2 = ax1.twinx()

        colors = ['red', 'green', 'orange', 'purple', 'brown']
        color_index = 0
        for investor in investor_names:
            if investor != primary_investor:
                share_col = investor + '_share'
                if share_col in self.shares_df.columns:
                    ax2.plot(self.shares_df['date'], self.shares_df[share_col], label=f"{investor.capitalize()}'s Share", color=colors[color_index % len(colors)])
                    color_index += 1
                else:
                    print(f"{investor.capitalize()}'s share data is not available.")
        ax2.set_ylabel('Other Investors\' Share', color='red')
        ax2.tick_params(axis='y', labelcolor='red')

        lines, labels = ax1.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax1.legend(lines + lines2, labels + labels2, loc='upper left')

        plt.title('Ownership Shares Over Time')
        plt.show()

    def plot_shares_with_inset(self):
        fig, ax_main = plt.subplots(figsize=(12, 6))

        primary_investor = self.fund_owner_name.lower()
        share_col_primary = primary_investor + '_share'
        ax_main.plot(self.shares_df['date'], self.shares_df[share_col_primary], label=f"{primary_investor.capitalize()}'s Share", color='blue')
        ax_main.set_xlabel('Date')
        ax_main.set_ylabel('Ownership Share')
        ax_main.set_title('Ownership Shares Over Time')
        ax_main.legend()

        ax_inset = ax_main.inset_axes([0.5, 0.5, 0.47, 0.47])

        for investor in self.investors.keys():
            if investor != primary_investor:
                share_col = investor + '_share'
                if share_col in self.shares_df.columns:
                    ax_inset.plot(self.shares_df['date'], self.shares_df[share_col], label=f"{investor.capitalize()}'s Share")
        ax_inset.set_ylabel('Ownership Share')
        ax_inset.legend()

        plt.show()
 
    def save_results(self, output_path):
        self.shares_df.to_csv(os.path.join(output_path, 'ownership_shares.csv'), index=False)
    
    def update_to_latest_netliq(self):
        latest_netliq_date = self.netliq_data['date'].max()
        latest_nav = self.netliq_data[self.netliq_data['date'] == latest_netliq_date]['netliq'].iloc[0] / self.total_units
        last_date_in_shares_df = self.shares_df['date'].max()
        if latest_netliq_date > last_date_in_shares_df:
            # Add the latest netliq date to shares_df
            new_row = self.shares_df[self.shares_df['date'] == last_date_in_shares_df].copy()
            new_row['date'] = latest_netliq_date
            new_row['nav'] = latest_nav
            self.shares_df = pd.concat([self.shares_df, new_row], ignore_index=True)
            self.shares_df.sort_values('date', inplace=True)
            self.shares_df.reset_index(drop=True, inplace=True)
            print(self.shares_df)
            print(f"Updated shares_df to latest netliq date: {latest_netliq_date.strftime('%Y-%m-%d')}")

    def print_current_pnl(self):
        latest_data = self.shares_df.iloc[-1]
        for investor in self.investors.keys():
            investment_value = latest_data[f"{investor}_investment_value"]
            print(f"{investor.capitalize()}'s Investment Value: {investment_value:.2f}")


def main():
    parser = argparse.ArgumentParser(description='Portfolio Ownership Calculator')
    parser.add_argument('--fund-owner-name', type=str, required=True, help='Name of the fund owner')
    args = parser.parse_args()
    fund_owner_name = args.fund_owner_name.lower()

    base_path = find_project_root()
    netliq_path = os.path.join(base_path, 'account_data_fetcher', 'csv_db', 'balance.csv')
    cash_flow_path = os.path.join(base_path, 'account_data_fetcher', 'csv_db', 'deposits_and_withdraws.csv')

    netliq_data = pd.read_csv(netliq_path, parse_dates=['date'])
    netliq_data['date'] = pd.to_datetime(netliq_data['date'])

    cash_flow_data = pd.read_csv(cash_flow_path, parse_dates=['date'], dayfirst=True)
    cash_flow_data['date'] = pd.to_datetime(cash_flow_data['date'], dayfirst=True)

    cash_flow_data['investor'] = cash_flow_data['investor'].fillna(fund_owner_name).str.strip()

    portfolio = Portfolio(netliq_data, cash_flow_data, fund_owner_name=fund_owner_name)

    portfolio.process()

    portfolio.calculate_shares()
    
    portfolio.plot_shares()
    portfolio.plot_shares_with_inset()

    portfolio.update_to_latest_netliq()

    portfolio.print_current_pnl()

    output_path = os.path.join(base_path, 'account_data_fetcher', 'csv_db')
    portfolio.save_results(output_path)

if __name__ == '__main__':
    main()

