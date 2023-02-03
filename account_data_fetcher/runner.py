import argparse
import asyncio
import functools
import logging
import signal
from getpass import getpass
from datetime import datetime
from logging import Logger
from typing import Callable, Optional, List

import pandas as pd

from account_data_fetcher.account_data_fetcher import AccountDataFetcher
from utilities.gsheet_handler import gsheetHandler

class Runner:
        def __init__(self, logger: Optional[Logger] = None) -> None:
            self.logger = logging.getLogger(__name__) if not logger else logger
            self.loop = asyncio.get_event_loop()
            for signame in {'SIGINT', 'SIGTERM'}:
                self.loop.add_signal_handler(
                getattr(signal, signame),
                functools.partial(self.ask_exit, signame, self.loop, self.logger))
            self.account_data_fetcher: Optional[AccountDataFetcher] = None
            self.latest_day_fetch: Optional[datetime] = None
            self.gsheet_client: Optional[gsheetHandler] = None

        @staticmethod
        def ask_exit(signame, loop, logger) -> None:
            logger.info("got signal %s: exit" % signame)
            loop.stop()

        async def periodic_api(self, pwd, seconds, exchange_list: List[str], ib_fetching_method: str = "API", have_manual_balance: bool = False) -> None:
            self.logger.debug(f"running periodic task with following args: {seconds}, {ib_fetching_method}")
            if not self.account_data_fetcher:
                self.account_data_fetcher = AccountDataFetcher(pwd, ib_fetching_method, exchange_list)
                while not self.account_data_fetcher.ib_executor.is_connected():
                    await asyncio.sleep(5)
            if not self.gsheet_client:
                self.gsheet_client = gsheetHandler(self.account_data_fetcher.path, pwd)
            while True:
                manual_balance = self.get_manual_balance(have_manual_balance)
                self.logger.debug(f"getting netliq")
                self.account_data_fetcher.write_balance_to_csv(manual_balance)
                self.logger.debug(f"exporting csv to gsheet")
                self.gsheet_client.update_netliq_gsheet(self.account_data_fetcher.path + "/netliq.csv")
                self.logger.debug(f"sleeping {seconds=}")
                await asyncio.sleep(seconds)

        async def periodic_flex(self, pwd, exchange_list: List[str], seconds=1800, ib_fetching_method: str = "FLEX", have_manual_balance: bool = False) -> None:
            self.logger.debug(f"running periodic task with following args: {seconds}, {ib_fetching_method}")
            if not self.account_data_fetcher:
                self.account_data_fetcher = AccountDataFetcher(pwd, ib_fetching_method, exchange_list)
            if not self.gsheet_client:
                self.gsheet_client = gsheetHandler(self.account_data_fetcher.path, pwd)
            if not self.latest_day_fetch:
                if self.is_new_day("/netliq.csv"):
                    if self.is_ib_flew_new_day("/netliq.csv"):
                        manual_balance = self.get_manual_balance(have_manual_balance)
                        self.logger.debug(f"getting netliq")
                        self.account_data_fetcher.write_balance_to_csv(manual_balance)
                        self.logger.debug(f"exporting csv to gsheet")
                        self.gsheet_client.update_netliq_gsheet(self.account_data_fetcher.path + "/netliq.csv")
                else:
                    self.logger.info(f"Data for today already present, waiting for tomorrow")
            while True:
                if self.is_new_day("/netliq.csv"):
                    if self.is_ib_flew_new_day("/netliq.csv"):
                        manual_balance = self.get_manual_balance(have_manual_balance)
                        self.account_data_fetcher.write_balance_to_csv(manual_balance)
                        self.gsheet_client.update_netliq_gsheet(self.account_data_fetcher.path + "/netliq.csv")
                await asyncio.sleep(seconds)

        async def periodic_positions(self, pwd, exchange_list: List[str], seconds=1800, ib_fetching_method: str = "FLEX") -> None:
            self.logger.debug(f"running periodic task with following args: {seconds}, {ib_fetching_method}")
            if not self.account_data_fetcher:
                self.account_data_fetcher = AccountDataFetcher(pwd, ib_fetching_method, exchange_list)
            if not self.gsheet_client:
                self.gsheet_client = gsheetHandler(self.account_data_fetcher.path, pwd)
            if not self.latest_day_fetch:
                if self.is_new_day("/positions.csv"):
                    if self.is_ib_flew_new_day("/positions.csv"):               
                        self.account_data_fetcher.write_positions_to_csv()
                        self.gsheet_client.update_position_gsheet(self.account_data_fetcher.path + "/positions.csv")
                else:
                    self.logger.info(f"Data for today already present, waiting for tomorrow")
            while True:
                if self.is_new_day("/positions.csv"):
                    if self.is_ib_flew_new_day("/positions.csv"):
                        self.account_data_fetcher.write_positions_to_csv()
                        self.gsheet_client.update_position_gsheet(self.account_data_fetcher.path + "/positions.csv")
                await asyncio.sleep(seconds)

        def get_manual_balance(self, have_manual_balance: bool = False) -> None:
            """format of csv is:
            manual_balance
            0
            """
            if have_manual_balance:
                try:
                    df = pd.read_csv(self.account_data_fetcher.path+"/manual_balance.csv")
                    return df["manual_balance"][0]
                except FileNotFoundError:
                    raise FileNotFoundError("manual balance flag was set to true whereas there is manual_balance.csv file")
            else:
                return 0
            
        def is_new_day(self, for_which_file: str = "/netliq.csv", today_datetime: Optional[datetime] = None) -> bool:
            try:
                df = pd.read_csv(self.account_data_fetcher.path+for_which_file)
            except FileNotFoundError:
                return True
            if not today_datetime:
                today_datetime = datetime.today()
            today = today_datetime.weekday()            
            last_day_df = df["date"].iloc[-1]
            datetime_df = datetime.strptime(last_day_df, "%Y-%m-%d %H:%M:%S")
            today_df = datetime_df.weekday()
            self.logger.debug(f"{today_datetime=}, {last_day_df=}")
            if today_datetime.isocalendar().week != datetime_df.isocalendar().week:
                return True
            else:
                return today != today_df

        def is_ib_flew_new_day(self, for_which_file: str = "/netliq.csv") -> bool:
            if for_which_file == "/netliq.csv":
                when_generated: datetime = self.account_data_fetcher.ib_executor.update_balance_and_get_ib_datetime()
            if for_which_file == "/positions.csv":
                when_generated: datetime = self.account_data_fetcher.ib_executor.update_positions_and_get_ib_datetime()
            return self.is_new_day(for_which_file, when_generated)

        def create_task(self, function: Callable, *args) -> asyncio.Task:
            return self.loop.create_task(function(*args))

        def run_task(self) -> None:
            try:
                self.loop.run_forever()
            except asyncio.CancelledError:
                pass


if __name__ == "__main__":
    from utilities.get_process_name import get_process_name
    from setproctitle import setproctitle

    parser = argparse.ArgumentParser(
        description="Get Balances and Positions",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--seconds', dest="seconds", type=int, nargs='?', default=10)
    parser.add_argument('--manual-balance', dest="manual_balance", type=bool, nargs='?', default=False,
                        help="If activated, will read from a file a manual balance to offset netliq, positive for adding and negative to substract values")
    parser.add_argument("-q","--quiet",action="count",default=0,
                    help="Be more quiet.")
    parser.add_argument("-v", "--verbose",action="count",default=0,
                    help="Be more verbose. Both -v and -q may be used multiple times.")
    parser.add_argument('--log-file', dest="log_file", type=str, nargs='?')
    parser.add_argument('--request-type', dest="request_type", choices=["FLEX", "API", "POSITIONS"],
                    type=str, nargs='?', default="FLEX")
    parser.add_argument('--exchange', dest="exchange_list", type=str, nargs='+', help="List of exchange to fetch for")
    
    args = parser.parse_args()

    setproctitle(get_process_name(args.log_file))
    
    # Setup logging
    args.verbosity = args.verbose - args.quiet
    if args.verbosity == 0:
        logging.root.setLevel(logging.INFO)
    elif args.verbosity >= 1:
        logging.root.setLevel(logging.DEBUG)
    elif args.verbosity == -1:
        logging.root.setLevel(logging.WARNING)
    elif args.verbosity <= -2:
        logging.root.setLevel(logging.ERROR)
    
    logging.basicConfig(format='%(levelname)s - %(asctime)s - %(name)s - %(message)s', filename=args.log_file)
    
    pwd = getpass("provide password for pk:")

    executor = Runner()
    
    logging.info("Launching Process")

    if args.request_type == "API":
        executor.create_task(executor.periodic_api, pwd, args.exchange_list, "API", args.manual_balance)
    elif args.request_type == "FLEX":
        executor.create_task(executor.periodic_flex, pwd, args.exchange_list, args.seconds, "FLEX", args.manual_balance)
        executor.create_task(executor.periodic_positions, pwd, args.exchange_list, args.seconds, "FLEX")
    else:
        raise NotImplemented(f"Haven't implemented handler for {args.request_type}")


    executor.run_task()
