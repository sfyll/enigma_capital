import asyncio
import logging
import os
from pathlib import Path
from typing import Optional

import matplotlib.pyplot as plt
import pandas as pd

from monitor.hkd_peg_breaking.fetch_data import dataFetcher
from utilities.telegram_handler import telegramHandler

class dataAggrAndSender(telegramHandler):
    __LIBOR_CSV_PATH = "/LIBOR USD.csv" #didn't find free historical source :( , link is : http://iborate.com/usd-libor/
    def __init__(self, password: str, beginning_date: str, to_telegram = True) -> None:
        current_path = Path(os.path.realpath(os.path.dirname(__file__)))
        super().__init__(str(current_path.parent), password)
        self.current_path: str = str(current_path)

        self.logger = logging.getLogger(__name__)
        logging.getLogger('matplotlib').setLevel(logging.CRITICAL) #hacky way

        self.data_fetcher =  dataFetcher()
        self.hibor_libor_df : Optional[pd.DataFrame] = None
        self.hibor_libor_df_daily : Optional[pd.DataFrame] = None
        self.beginning_date = beginning_date
        self.to_telegram = to_telegram

    async def aggr_and_send_data_hkd(self) -> None:
        await self.plot_and_send_historical_rates()
        await self.plot_and_send_daily_rates()
        await self.get_and_process_renminbi_facility_data()
        await self.get_and_process_monetary_aggregates()
        # await self.get_economic_aggregates()

    async def plot_and_send_historical_rates(self, plot_spread=False) -> None:
        self.get_data_historical()
        maturity_list = self.hibor_libor_df.xs("USD", axis=1).columns
        f,a = plt.subplots(len(maturity_list),1)
        if not plot_spread:
            for idx, values in enumerate(maturity_list):
                self.hibor_libor_df.xs(values, level=1, axis=1).plot(ax=a[idx] if len(maturity_list)>1 else a, title=values + " Rates", sharex=True)
        else:
            for idx, values in enumerate(maturity_list):
                # df[values + " USD-HKD spread"] = df.xs("USD", axis=1)[values] - df.xs("HKD", axis=1)[values]
                self.hibor_libor_df[values + " USD-HKD spread as perc. of US Rates"] = self.hibor_libor_df.xs("USD", axis=1)[values] - self.hibor_libor_df.xs("HKD", axis=1)[values] / self.hibor_libor_df.xs("USD", axis=1)[values]
                
                # df[values + " USD-HKD spread"].plot(ax=a[idx] if len(maturity_list)>1 else a, title=values + " USD-HKD spread ", sharex=True)
                self.hibor_libor_df[values + " USD-HKD spread as perc. of US Rates"].plot(ax=a[idx] if len(maturity_list)>1 else a, title=values + " USD-HKD spread as perc. of US Rates", sharex=True)

        plt.savefig(self.current_path+"historical_rates.png")

        if self.to_telegram:
            await self.send_photo_to_telegram(self.current_path+"historical_rates.png")
        else:
            plt.show()

    async def plot_and_send_daily_rates(self, plot_spread=False) -> None:
        self.get_data_daily_on()
        maturity_list = self.hibor_libor_df_daily.xs("USD", axis=1).columns
        f,a = plt.subplots(1 if not plot_spread else 2,1)
        if not plot_spread:
            for idx, values in enumerate(maturity_list):
                self.hibor_libor_df_daily.xs(values, level=1, axis=1).plot(ax=a[idx] if len(maturity_list)>1 else a, title=values + " Rates", sharex=True)
        else:
            for idx, values in enumerate(maturity_list):
                self.hibor_libor_df_daily[values + " USD-HKD spread"] = self.hibor_libor_df_daily.xs("USD", axis=1)[values] - self.hibor_libor_df_daily.xs("HKD", axis=1)[values]
                self.hibor_libor_df_daily[values + " USD-HKD spread as perc. of US Rates"] = self.hibor_libor_df_daily.xs("USD", axis=1)[values] - self.hibor_libor_df_daily.xs("HKD", axis=1)[values] / self.hibor_libor_df_daily.xs("USD", axis=1)[values]
                
                self.hibor_libor_df_daily[values + " USD-HKD spread"].plot(ax=a[0], title=values + " USD-HKD spread ", sharex=True)
                self.hibor_libor_df_daily[values + " USD-HKD spread as perc. of US Rates"].plot(ax=a[1], title=values + " USD-HKD spread as perc. of US Rates", sharex=True)

        plt.savefig(self.current_path+"historical_rates_daily.png")

        if self.to_telegram:
            await self.send_photo_to_telegram(self.current_path+"historical_rates_daily.png")
        else:
            plt.show()

    def get_data_historical(self) -> None:
        url_base = "https://api.hkma.gov.hk/public/market-data-and-statistics/monthly-statistical-bulletin/er-ir/hk-interbank-ir-daily"
        url_extension="segment=hibor.fixing"
        hibor_df = pd.DataFrame(self.data_fetcher.fetch_data_and_export_from_hkma(url_base, url_extension, from_month=self.beginning_date))
        libor_df = pd.read_csv(os.path.realpath(os.path.dirname(__file__)) + self.__LIBOR_CSV_PATH)
        hibor_df.rename(columns = {"end_of_day": "Date",
                            "ir_overnight": "ON", 
                            "ir_1w": "1W",
                            "ir_1m": "1M",
                            "ir_3m": "3M",
                            "ir_6m": "6M",
                            "ir_12m": "12M"},
                            inplace=True)

        libor_df["Date"] = pd.to_datetime(libor_df.Date, format="%d.%m.%Y")
        libor_df["Date"] = libor_df["Date"].dt.strftime('%Y-%m-%d')

        libor_df.drop(columns=["Week day", "1W", '2M'], inplace=True)
        hibor_df.drop(columns=['1W', 'ir_9m'], inplace=True)

        merged_df = (pd.concat([libor_df.set_index('Date'), 
                    hibor_df.set_index('Date')], 
                    axis=1, 
                    keys=['USD','HKD'])
                    )

        merged_df.dropna(axis=0, inplace=True)

        merged_df.sort_index(inplace=True)

        merged_df = merged_df[~(merged_df.index < self.beginning_date)]

        self.hibor_libor_df = merged_df

    def get_data_daily_on(self) -> None:
        url_base = 'https://api.hkma.gov.hk/public/market-data-and-statistics/daily-monetary-statistics/daily-figures-interbank-liquidity'
        url_extension=""
        fields = ["end_of_date","hibor_overnight"]

        hibor_df = pd.DataFrame(self.data_fetcher.fetch_data_and_export_from_hkma(url_base, url_extension, fields, from_month=self.beginning_date))
        libor_df = pd.read_csv(os.path.realpath(os.path.dirname(__file__)) + self.__LIBOR_CSV_PATH)

        hibor_df.rename(columns = {"end_of_date": "Date",
                            "hibor_overnight": "ON"},
                            inplace=True)

        libor_df["Date"] = pd.to_datetime(libor_df.Date, format="%d.%m.%Y")
        libor_df["Date"] = libor_df["Date"].dt.strftime('%Y-%m-%d')

        libor_df.drop(columns=["Week day", "1W", '2M', "1M", '3M', '6M', '12M'], inplace=True)

        merged_df = (pd.concat([libor_df.set_index('Date'), 
                    hibor_df.set_index('Date')], 
                    axis=1, 
                    keys=['USD','HKD'])
                    )

        merged_df.dropna(axis=0, inplace=True)

        merged_df.sort_index(inplace=True)

        merged_df = merged_df[~(merged_df.index < self.beginning_date)]

        self.hibor_libor_df_daily = merged_df
    

    async def get_and_process_renminbi_facility_data(self) -> None:
        url = 'https://api.hkma.gov.hk/public/market-data-and-statistics/daily-monetary-statistics/usage-rmb-liquidity-fac'
        url_extension=""

        renminbi_liquidity_facility = self.data_fetcher.fetch_data_and_export_from_hkma(url, url_extension)

        df = pd.DataFrame(renminbi_liquidity_facility)
        df = df[~(df['end_of_date'] < self.beginning_date)]
        df.set_index("end_of_date", inplace=True)
        df.sort_index(inplace=True)
        f,a = plt.subplots(4,3)
        x_idx = 0
        y_idx = 0
        for idx, column_title in enumerate(df.columns):
            if (idx + 1) % 3 == 0:
                df[column_title].plot(ax=a[x_idx, y_idx], title=column_title)
                y_idx = 0
                x_idx += 1
            else:
                df[column_title].plot(ax=a[x_idx, y_idx], title=column_title)
                y_idx += 1

        plt.tight_layout()
            
        plt.savefig(self.current_path+"/renminbi_facility.png")

        if self.to_telegram:
            await self.send_photo_to_telegram(self.current_path+"/renminbi_facility.png")
        else:
            plt.show()

    async def get_and_process_monetary_aggregates(self) -> None:
        url = "https://api.hkma.gov.hk/public/market-data-and-statistics/daily-monetary-statistics/daily-figures-monetary-base"
        url_extension=""

        monetary_base_data = self.data_fetcher.fetch_data_and_export_from_hkma(url, url_extension)
        
        df = pd.DataFrame(monetary_base_data)
        df = df[~(df['end_of_date'] < self.beginning_date)]
        df.set_index("end_of_date", inplace=True)
        df.sort_index(inplace=True)
        f,a = plt.subplots(3,1)
        df["aggr_balance_bf_disc_win"].plot(ax=a[0], title="Aggregate Balance Before Discount Window, HK$ million")
        df["aggr_balance_af_disc_win"].plot(ax=a[1], title="Aggregate Balance After Discount Window, HK$ million")
        df["mb_bf_disc_win_total"].plot(ax=a[2], title="Total Monetary Base Bf Discount Window, HK$ million")
        
        plt.tight_layout()
        
        plt.savefig(self.current_path+"/monetary_aggregates.png")

        if self.to_telegram:
            await self.send_photo_to_telegram(self.current_path+"/monetary_aggregates.png")
        else:
            plt.show()

    async def get_economic_aggregates(self) -> None:
        df = self.get_merged_df_cpi_and_gdp(self.get_gdp(), self.get_cpi())

        f,a = plt.subplots(len(df.columns),1)
        for idx, column in enumerate(df.columns):
            if column == "Real GDP":
                plt.subplot(len(df.columns),1, idx+1)
                plt.plot(df.index, df[column], 'o-')
            df[column].plot(ax=a[idx], title=column)
            plt.xticks(df.index[0::3], rotation = "vertical")

        plt.tight_layout()
       
        plt.savefig(self.current_path+"/GDP_and_CPI.png")
        if self.to_telegram:
            await self.send_photo_to_telegram(self.current_path+"/GDP_and_CPI.png")
        else:
            plt.show()

    def get_gdp(self) -> pd.DataFrame:
        url = "https://www.censtatd.gov.hk/api/get.php?id=30&lang=en&param=N4IgxgbiBcoMJwJqJqAjDEAGHu+4HYB9E00gWn3xABoQiAXIzLW+gB2emxAF86AiqhAZuAMTaMuPOkU4s+dAOIARAApEwAewC27LQDsApgYbCpC2fO4Y6AQwAmUbgGZmvfiADOz0HOkATGxwAPIAcjAA2iCIWohEaA6cAKREBiAAunQAygCCotECWgIJSUSp6RmeADYwDABOAK5GvEA"
        return self.data_fetcher.fetch_economics_data(url, "GDP")

    def get_cpi(self) -> pd.DataFrame:
        url = "https://www.censtatd.gov.hk/api/get.php?id=52&lang=en&full_series=1"
        return self.data_fetcher.fetch_economics_data(url, "CPI")

    def get_merged_df_cpi_and_gdp(self, gdp_df: pd.DataFrame, cpi_df: pd.DataFrame) -> pd.DataFrame:

        gdp_df.rename(columns = {"GDP_component": "Real GDP",
                                "period": "date"},
                                inplace=True)

        cpi_df.rename(columns = {"freq": "CPI",
                            "period": "date"},
                            inplace=True)

        cpi_df["CPI"] = cpi_df["figure"]
        gdp_df["Real GDP"] = gdp_df["figure"]

        gdp_df.set_index("date")
        cpi_df.set_index("date")
                        
        df = pd.merge(gdp_df[["date", "Real GDP"]],cpi_df[['date', 'CPI']], on='date', how='right')

        df["date"] = pd.to_datetime(df.date, format="%Y%m")
        df["date"] = df["date"].dt.strftime('%Y-%m')

        res = df[~(df['date'] < self.beginning_date)]

        res.set_index("date", inplace=True)

        return res


if __name__ == "__main__":
    from getpass import getpass
    log_filename= os.path.expanduser("~") + "/log/test.py"
    numeric_level = getattr(logging, "DEBUG", None)
    logging.basicConfig(level=numeric_level, format='%(levelname)s:%(asctime)s:%(message)s', filename=log_filename) 
    logger: logging.Logger = logging.getLogger()
    pwd = getpass("provide password for pk:")
    current_path = Path(os.path.realpath(os.path.dirname(__file__)))
    beginning_date = "2022-06-01"
    executor = dataAggrAndSender(pwd, beginning_date, False)
    asyncio.run(executor.aggr_and_send_data_hkd())
