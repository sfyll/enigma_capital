from datetime import date
import json
import urllib.request
from typing import List, Optional

import pandas as pd

from thesis_monitoring.hkd_peg_breaking.request_handler import requestHandler

class dataFetcher:
    def __init__(self) -> None:
        self.request_handler: requestHandler = requestHandler()

    def fetch_data_and_export_from_hkma(self, url_base: str, url_extension: str, fields: Optional[List] = None, from_month: str = "2022-01-01", to_month: Optional[str] = None, pagesize: int = 1000) -> None:
        if not to_month:
            to_month = date.today().strftime("%Y-%m-%d")
        
        url = self.request_handler.api_module(url_base=url_base, url_extension=url_extension)
        
        params = {
            "from":from_month,
            "to":to_month,
            "pagesize": pagesize,
        }

        if fields:
            params["fields"] = ','.join(fields)

        return self.request_handler.handle_requests(
            url=url,
            method="get",
            args=params
        )["result"]["records"]


    def fetch_economics_data(self, url: str, to_fetch: str = "GDP") -> pd.DataFrame:
        result =  self.request_handler.handle_requests(
            url=url,
            method="get",
        )["dataSet"]

        res = []

        if to_fetch == "GDP":
            for sub_dict in result:
                if sub_dict["GDP_component"] == "GDP" and sub_dict["freq"] == "Q" and sub_dict["svDesc"] == "Year-on-year % change":
                    res.append(sub_dict)
        
        elif to_fetch == "CPI":
            for sub_dict in result:
                if sub_dict["freq"] == "M" and sub_dict["svDesc"] == "Year-on-year % change" and sub_dict["sv"] == "CC_CM_1920":
                    res.append(sub_dict)

        else:
            raise NotImplemented(f"didn't implement handler for {to_fetch}")

        return pd.DataFrame(res)


    #Below potential other endpoints of interest

    def fetch_monthly_economic_statistics(fields= None, from_month= "2021-07-01", to_month=None) -> List[dict]:
        if not to_month:
            to_month = date.today().strftime("%Y-%m-%d")
        '''Inputs Format: https://apidocs.hkma.gov.hk/documentation/'''
        "date broken post 2021-07"
        url = 'https://api.hkma.gov.hk/public/market-data-and-statistics/monthly-statistical-bulletin/financial/economic-statistics?pagesize=200'
        if fields:
            url += "&fields=" + fields
        url += "&from=" + from_month + "&to=" + to_month

        with urllib.request.urlopen(url) as req:
            result = json.loads(req.read().decode("utf-8"))["result"]["records"]
            return result

    def fetch_monthly_banking_statistics(fields= None, from_month= "2021-07-01", to_month=None) -> List[dict]:
        if not to_month:
            to_month = date.today().strftime("%Y-%m-%d")
        '''Inputs Format: https://apidocs.hkma.gov.hk/documentation/'''
        url = 'https://api.hkma.gov.hk/public/market-data-and-statistics/monthly-statistical-bulletin/financial/banking-statistics?pagesize=200'
        if fields:
            url += "&fields=" + fields
        url += "&from=" + from_month + "&to=" + to_month

        with urllib.request.urlopen(url) as req:
            result = json.loads(req.read().decode("utf-8"))["result"]["records"]
            return result

if __name__ == '__main__':
    #fetch_and_export_historical_data()
    # fields="end_of_date,hibor_overnight"
    executor = dataFetcher()
    print(executor.fetch_and_export_historical_data())