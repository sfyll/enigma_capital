import argparse
import asyncio
from datetime import datetime, timedelta
import functools
from getpass import getpass
import logging
import signal
from typing import Callable, Optional

import pandas as pd

from monitor.hkd_peg_breaking.data_aggr_and_sender import dataAggrAndSender
from monitor.portfolio_monitor.netliq_to_telegram import netliqToTelgram

try:
    from monitor.uranium.uranium_thesis_monitoring import uraniumThesisMonitoring
except (FileNotFoundError, ModuleNotFoundError):
    pass #hidden thesis, should it be?
    
#TODO: Take a look at account_data_fetcher, how could you refactor the entire monitor component?
class Runner:
        def __init__(self, logger: Optional[logging.Logger] = None) -> None:
            self.logger = logging.getLogger(__name__) if not logger else logger
            self.loop = asyncio.get_event_loop()
            for signame in {'SIGINT', 'SIGTERM'}:
                self.loop.add_signal_handler(
                getattr(signal, signame),
                functools.partial(self.ask_exit, signame, self.loop, self.logger))
            self.data_aggr_and_sender: Optional[dataAggrAndSender] = None
            self.netliq_to_telegram: Optional[netliqToTelgram] = None
            self.uranium_aggr_and_sender = None

        @staticmethod
        def ask_exit(signame, loop, logger) -> None:
            logger.info("got signal %s: exit" % signame)
            loop.stop()

        async def periodic_uranium_thesis(self, pwd, seconds: int) -> None:
            self.logger.debug(f"running periodic task with following second delay: {seconds}")
            if not self.uranium_aggr_and_sender:
                self.uranium_aggr_and_sender = uraniumThesisMonitoring(pwd)
                await self.uranium_aggr_and_sender.send_to_telegram_saved_text()
                await asyncio.sleep(seconds)
            while True:
                await self.uranium_aggr_and_sender.send_to_telegram_saved_text()
                await asyncio.sleep(seconds)

        async def periodic_hkd_thesis_plotter(self, pwd, seconds: int, starting_date: str) -> None:
            self.logger.debug(f"running periodic task with following second delay: {seconds}")
            if not self.data_aggr_and_sender:
                self.data_aggr_and_sender = dataAggrAndSender(pwd, starting_date)
                if self.is_new_week():
                    await self.data_aggr_and_sender.aggr_and_send_data_hkd()
                else:
                    self.logger.info(f"Data for today already present, waiting for tomorrow")
            while True:
                if self.is_new_week():
                    await self.data_aggr_and_sender.aggr_and_send_data_hkd()
                await asyncio.sleep(seconds)

        async def periodic_netliq_update(self, pwd, seconds: int) -> None:
            self.logger.debug(f"running periodic task with following second delay: {seconds}")
            #always start by sending the update
            today =  datetime.today() - timedelta(days=1)
            self.netliq_to_telegram = netliqToTelgram(pwd)
            while True:
                is_new_day, potential_today = self.is_new_day(self.netliq_to_telegram.netliq_path, today)
                self.logger.debug(f"{is_new_day=}, {potential_today=}")
                if is_new_day:
                    result = await self.netliq_to_telegram.format_and_send_dataframe()
                    if result == True:
                        today = potential_today
                await asyncio.sleep(seconds)

        def is_new_week(self) -> bool:
            if self.data_aggr_and_sender.hibor_libor_df_daily is None:
                self.data_aggr_and_sender.get_data_daily_on()
            today_datetime = datetime.today()
            last_day_df = self.data_aggr_and_sender.hibor_libor_df_daily.index[-1]
            datetime_df = datetime.strptime(last_day_df, "%Y-%m-%d")
            self.logger.debug(f"{today_datetime=}, {last_day_df=}")
            if today_datetime.isocalendar().week != datetime_df.isocalendar().week:
                return True

        def is_new_day(self, path: str, today_datetime: Optional[datetime] = None) -> tuple[bool, int]:
            try:
                df = pd.read_csv(path)
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
                return True,  datetime.today()
            else:
                return today != today_df,  datetime.today()

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
        description="Sending HKD Thesis to Telegram Channel",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--seconds', dest="seconds", type=int, nargs='?', default=1800)
    parser.add_argument("-q","--quiet",action="count",default=0,
                    help="Be more quiet.")
    parser.add_argument("-v", "--verbose",action="count",default=0,
                    help="Be more verbose. Both -v and -q may be used multiple times.")
    parser.add_argument('--log-file', dest="log_file", type=str, nargs='?')
    parser.add_argument('--request-type', dest="request_type",
                    type=str, nargs='?', default="HKD")
    parser.add_argument('--starting-date', dest="starting_date",
                    type=str, nargs='?', default="2022-01-01")
    
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

    if args.request_type == "HKD":
        executor.create_task(executor.periodic_hkd_thesis_plotter, pwd, args.seconds, args.starting_date)
    elif args.request_type == "URANIUM":
        executor.create_task(executor.periodic_uranium_thesis, pwd, args.seconds)
    elif args.request_type == "NETLIQ":
        executor.create_task(executor.periodic_netliq_update, pwd, args.seconds)
    else:
        raise NotImplementedError(f"not aware of this request type: {args.request_type}")

    executor.run_task()
