import pandas as pd
import matplotlib.pyplot as plt
import os
import argparse
import warnings
from datetime import datetime

from utils import find_project_root

warnings.filterwarnings("ignore")

class Investor:
    def __init__(self, name):
        self.name = name
        self.units = 0
        self.unit_history = pd.DataFrame(columns=['date', 'units'])
    
    def update_units(self, date, units):
        self.units += units
        # Record the transaction
        new_row = pd.DataFrame({'date': [date], 'units': [units]})
        self.unit_history = pd.concat([self.unit_history, new_row], ignore_index=True)
        print(f"[Investor Update] {self.name} on {date}: Units Change = {units}, Total Units = {self.units}")

class Portfolio:
    def __init__(self, netliq_data, fund_owner_name):
        self.netliq_data = netliq_data.sort_values('date').reset_index(drop=True)
        self.total_units = 0
        self.investors = {}
        self.nav_history = pd.DataFrame(columns=['date', 'nav'])
        self.fund_owner_name = fund_owner_name
        self.multi_investor_mode = False  # Indicates when to start tracking units
    
    def add_investor(self, investor_name):
        investor = Investor(investor_name)
        self.investors[investor_name] = investor
        print(f"[Add Investor] New investor added: {investor_name}")
        return investor
   
    def adjust_net_liquidity(self, cash_flow_data):
        self.netliq_data['adjusted_netliq'] = self.netliq_data['netliq']
        
        # Process cash flows in date order
        cash_flow_data = cash_flow_data.sort_values('date')
        
        for idx, row in cash_flow_data.iterrows():
            date = row['date']
            amount = row['amount']
            investor_name = row.get('investor', '').strip()
            from_exchange = row['from_exchange']
            to_exchange = row['to_exchange']
            
            # Handle empty investor names
            if pd.isnull(investor_name) or investor_name == '':
                investor_name = self.fund_owner_name

            # Ignore transactions where investor is 'fund'. this leads to incorrect share calculations if done in between transfers
            if investor_name.lower() == 'fund':
                continue

            # Check if new investor appears
            if investor_name != self.fund_owner_name and investor_name not in self.investors:
                self.multi_investor_mode = True
                self.multi_investor_start_date = date  # Record the date when multi-investor mode starts
                self.add_investor(investor_name)
                print(f"[Mode Switch] Multiple investors detected from date {date}")
                self.initialize_fund_owner_units()
            
            if not self.multi_investor_mode:
                # Before multiple investors, adjust net liquidity for fund owner's transactions
                if investor_name == self.fund_owner_name:
                    if pd.isnull(to_exchange) and pd.notnull(from_exchange):
                        # Withdrawal from the fund
                        self.netliq_data.loc[self.netliq_data['date'] == date, 'adjusted_netliq'] += (-amount)
                        print(f"[Net Liquidity Adjustment] {date}: Withdrawal of {-amount}")
                    elif pd.isnull(from_exchange) and pd.notnull(to_exchange):
                        # Deposit into the fund
                        self.netliq_data.loc[self.netliq_data['date'] == date, 'adjusted_netliq'] -= amount
                        print(f"[Net Liquidity Adjustment] {date}: Deposit of {amount}")
                    else:
                        # Internal transfer; no adjustment needed
                        print(f"[Internal Transfer] {date}: No net liquidity adjustment")
                        continue
                else:
                    # Ignore other investors before multi-investor mode
                    continue
            else:
                # After multiple investors, process transactions as investor cash flows
                self.process_investor_cash_flow(date, amount, investor_name, from_exchange, to_exchange)


    def initialize_fund_owner_units(self):
        date = self.multi_investor_start_date
        netliq_row = self.netliq_data[self.netliq_data['date'] <= date].iloc[-1]
        portfolio_value = netliq_row['adjusted_netliq']
        nav = 1  # Initialize NAV to 1 for simplicity
        
        # Assign units to Santi
        santi_units = portfolio_value / nav
        self.add_investor(self.fund_owner_name)
        self.investors[self.fund_owner_name].update_units(date, santi_units)
        self.total_units += santi_units
        
        # Record NAV history
        new_row = pd.DataFrame({'date': [date], 'nav': [nav]})
        self.nav_history = pd.concat([self.nav_history, new_row], ignore_index=True)
        print(f"[Initialize Units] Fund owner '{self.fund_owner_name}' assigned {santi_units} units at NAV {nav} on {date}")


    def process_investor_cash_flow(self, date, amount, investor_name, from_exchange, to_exchange):
        # Ignore transactions where investor is 'fund'
        if investor_name.lower() == 'fund':
            return

        if investor_name not in self.investors:
            self.add_investor(investor_name)

        if pd.isnull(from_exchange) and pd.notnull(to_exchange):
            # Deposit into the fund
            is_deposit = True
        elif pd.notnull(from_exchange) and pd.isnull(to_exchange):
            # Withdrawal from the fund
            is_deposit = False
        else:
            # Internal transfer or invalid case
            return

        investor = self.investors[investor_name]

        # Get NAV at the transaction date
        nav = self.get_nav_at_date(date)
        print(f"[NAV] {date}: NAV at transaction date = {nav}")

        # Calculate units purchased or redeemed
        units = amount / nav if is_deposit else -amount / nav

        # Update investor's units and total units
        investor.update_units(date, units)
        self.total_units += units
        print(f"[Total Units] {date}: Total Units after transaction = {self.total_units}")

    def get_nav_at_date(self, date):
        # Check if NAV for this date is already calculated
        nav_row = self.nav_history[self.nav_history['date'] == date]
        if not nav_row.empty:
            nav = nav_row['nav'].values[0]
            return nav

        # Get adjusted net liquidity on the given date
        netliq_row = self.netliq_data[self.netliq_data['date'] <= date]
        if netliq_row.empty:
            # If there's no data, default NAV to 1
            portfolio_value = 1
        else:
            netliq_row = netliq_row.iloc[-1]
            portfolio_value = netliq_row['adjusted_netliq']

        nav = portfolio_value / self.total_units if self.total_units > 0 else 1

        print(f"NAV for {nav}")

        # Record NAV history
        new_row = pd.DataFrame({'date': [date], 'nav': [nav]})
        self.nav_history = pd.concat([self.nav_history, new_row], ignore_index=True)
        return nav
    
    def update_nav_history(self):
        # Update NAV for all dates in net liquidity data
        for idx, row in self.netliq_data.iterrows():
            date = row['date']
            portfolio_value = row['adjusted_netliq']
            nav = portfolio_value / self.total_units if self.total_units > 0 else 1
            # Record NAV history
            new_row = pd.DataFrame({'date': [date], 'nav': [nav]})
            self.nav_history = pd.concat([self.nav_history, new_row], ignore_index=True)
    
    def calculate_investor_values(self):
        for investor_name, investor in self.investors.items():
            print(f"[Debug] {investor_name} Cumulative Units:\n {investor.unit_history[['date', 'units']]}")
            # Merge investor unit history with NAV history
            investor.unit_history['date'] = pd.to_datetime(investor.unit_history['date'])
            nav_history = self.nav_history.copy()
            nav_history['date'] = pd.to_datetime(nav_history['date'])
            investor.unit_history = investor.unit_history.merge(
                nav_history, on='date', how='outer'
            ).sort_values('date').fillna(method='ffill')
            
            # Fill any remaining NaN values in 'units' with 0
            investor.unit_history['units'].fillna(0, inplace=True)
            
            # Calculate cumulative units over time
            investor.unit_history['cumulative_units'] = investor.unit_history['units'].cumsum()
            
            # Calculate investment value
            investor.unit_history['investment_value'] = investor.unit_history['cumulative_units'] * investor.unit_history['nav']
    
    def calculate_investor_shares(self):
        # Calculate total units over time
        total_units_over_time = pd.DataFrame()
        for investor in self.investors.values():
            investor.unit_history['date'] = pd.to_datetime(investor.unit_history['date'])
            if total_units_over_time.empty:
                total_units_over_time = investor.unit_history[['date', 'cumulative_units']].copy()
                total_units_over_time.rename(columns={'cumulative_units': investor.name}, inplace=True)
            else:
                print("unit_history: ", investor.unit_history)
                total_units_over_time = total_units_over_time.merge(
                    investor.unit_history[['date', 'cumulative_units']], on='date', how='outer'
                )
                total_units_over_time.rename(columns={'cumulative_units': investor.name}, inplace=True)
        total_units_over_time.fillna(method='ffill', inplace=True)
        total_units_over_time.fillna(0, inplace=True)
        total_units_over_time['total_units'] = total_units_over_time[self.investors.keys()].sum(axis=1)
        print("Total Units Over Time: ", total_units_over_time)
        # Calculate each investor's share
        for investor_name in self.investors.keys():
            total_units_over_time[investor_name + '_share'] = (
                total_units_over_time[investor_name] / total_units_over_time['total_units']
            )
        self.total_units_over_time = total_units_over_time
    
    def plot_investor_shares(self):
        #plt.figure(figsize=(12, 6))
        #plt.plot(self.total_units_over_time['date'], self.total_units_over_time['virgi_share'], label='Virginia')
        #plt.xlabel('Date')
        #plt.ylabel('Ownership Share')
        #plt.title("Virginia's Share of the Fund Over Time")
        #plt.legend()
        #plt.show()

        #plt.figure(figsize=(12, 6))
        #plt.plot(self.total_units_over_time['date'], self.total_units_over_time['santi_share'], label='Santi')
        #plt.xlabel('Date')
        #plt.ylabel('Ownership Share')
        #plt.title("Santi's Share of the Fund Over Time")
        #plt.legend()
        #plt.show()

        plt.figure(figsize=(12, 6))
        plt.plot(self.total_units_over_time['date'], self.total_units_over_time['total_units'], label='Total Units')
        plt.xlabel('Date')
        plt.ylabel('Total Units')
        plt.title('Total Units Over Time')
        plt.legend()
        plt.show()


    def save_investor_values(self, output_path):
        # Save investor values to CSV files
        for investor_name, investor in self.investors.items():
            filename = f"{investor_name}_investment_values.csv"
            filepath = os.path.join(output_path, filename)
            investor.unit_history.to_csv(filepath, index=False)

def main():
    parser = argparse.ArgumentParser(description="Run the portfolio unitization script.")
    parser.add_argument(
        '--start-date',
        type=str,
        default="01/01/2023",
        help="The start date for the calculation in the format DD/MM/YYYY. Defaults to 01/01/2023."
    )
    parser.add_argument(
        '--fund-owner-name',
        type=str,
        default="FundOwner",
        help="Name of the fund owner. Defaults to 'FundOwner'."
    )
    args = parser.parse_args()
    start_date = datetime.strptime(args.start_date, '%d/%m/%Y')
    fund_owner_name = args.fund_owner_name
    
    # Get base path and data paths
    base_path = find_project_root()
    netliq_path = os.path.join(base_path, 'account_data_fetcher', 'csv_db', 'balance.csv')
    cash_flow_path = os.path.join(base_path, 'account_data_fetcher', 'csv_db', 'deposits_and_withdraws.csv')
    
    # Load net liquidity data
    netliq_data = pd.read_csv(netliq_path)
    netliq_data['date'] = pd.to_datetime(netliq_data['date'])
    netliq_data = netliq_data[netliq_data['date'] >= start_date]
    
    # Load cash flow data
    cash_flow_data = pd.read_csv(cash_flow_path)
    cash_flow_data['date'] = pd.to_datetime(cash_flow_data['date'], format='%d/%m/%Y')
    cash_flow_data = cash_flow_data[cash_flow_data['date'] >= start_date]
    
    # Initialize portfolio
    portfolio = Portfolio(netliq_data, fund_owner_name)
    
    # Adjust net liquidity and process cash flows
    portfolio.adjust_net_liquidity(cash_flow_data)
    
    # Update NAV history
    portfolio.update_nav_history()
    
    # Calculate investor values
    portfolio.calculate_investor_values()
    
    # If there are multiple investors, calculate and plot shares
    if len(portfolio.investors) > 1:
        portfolio.calculate_investor_shares()
        portfolio.plot_investor_shares()
    else:
        print("\n[Info] Only the fund owner is present. Investor shares plot will not be generated.")
    print("total_units_over_time")

    # Optionally, save investor values to CSV files
    output_path = os.path.join(base_path, 'account_data_fetcher', 'csv_db')
    portfolio.save_investor_values(output_path)

if __name__ == '__main__':
    main()
