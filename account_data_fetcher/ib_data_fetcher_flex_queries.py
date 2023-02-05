from datetime import timezone, datetime, timedelta
import logging
import os
from time import sleep
from typing import Optional

from ibflex import parser as ibparser
from ibflex import client
from ibflex.Types import OpenPosition, FlexQueryResponse, FlexStatement
from ibflex.client import ResponseCodeError

from utilities.account_data_fetcher_base import accountFetcherBase


class ibDataFetcher(accountFetcherBase):
    __HOURS_DIFFERENCE_FROM_UTC = -5
    __EXCHANGE= "IB"

    def __init__(self, path, password) -> None:
        super().__init__(path, password)
        self.__get_account_and_query_ids()
        self.logger = logging.getLogger(__name__)
        self.balance_object: Optional[FlexStatement] = None
        self.positions_object: Optional[FlexStatement] = None

    def __get_account_and_query_ids(self) -> None:
        self.account_and_query_ids: dict = {
            "token" : self.api_meta_data[self.__EXCHANGE].other_fields["Token"],
            "query_id_balance" : self.api_meta_data[self.__EXCHANGE].other_fields["Balance_query_id"],
            "query_id_position" : self.api_meta_data[self.__EXCHANGE].other_fields["Position_query_id"]
        }

    def update_balance_and_get_ib_datetime(self) -> datetime:
        if not self.is_acceptable_timestamp_detla(self.balance_object, "BALANCE"):
            self.get_balance_object()
        return self.balance_object.whenGenerated

    def update_positions_and_get_ib_datetime(self) -> datetime:
        if not self.is_acceptable_timestamp_detla(self.positions_object, "POSITIONS"):
            self.get_positions_object()
        return self.positions_object.whenGenerated

    def get_netliq(self) -> float:
        if not self.is_acceptable_timestamp_detla(self.balance_object, "BALANCE"):
            self.get_balance_object()
        return round(float(self.balance_object.ChangeInNAV.endingValue),3)

    def get_positions(self) -> dict:
        if not self.is_acceptable_timestamp_detla(self.positions_object, "POSITIONS"):
            self.get_positions_object()
        #denominate in usd so multiply by FXRateToBase, get columns: Symbol, Multiplier, Quantity, MarkPrice, CostBasisPrice, FifoPnlUnrealized
        stmt: OpenPosition = self.positions_object.OpenPositions
        self.logger.debug(stmt)

        data_to_return = {
            "Symbol": [],
            "Multiplier": [],
            "Quantity": [],
            "Dollar Quantity": []
        }

        for position in stmt:
            data_to_return["Symbol"].append(position.symbol)
            data_to_return["Multiplier"].append(int(position.multiplier))
            data_to_return["Quantity"].append(int(position.position))
            data_to_return["Dollar Quantity"].append(round(float(position.markPrice) * float(position.multiplier) * int(position.position),3))
        
        return data_to_return

    def get_balance_object(self) -> None:
        data = self.get_and_parse_data(self.account_and_query_ids["token"], self.account_and_query_ids["query_id_balance"])
        self.balance_object = data.FlexStatements[0]
        self.logger.debug(self.balance_object)

    def get_positions_object(self) -> None:
        data = self.get_and_parse_data(self.account_and_query_ids["token"], self.account_and_query_ids["query_id_position"])
        self.positions_object = data.FlexStatements[0]
        self.logger.debug(self.positions_object)

    def is_acceptable_timestamp_detla(self, object_to_check: Optional[FlexStatement], query_type: str = "BALANCE") -> bool:
        if not object_to_check:
            return False

        dt = datetime.utcnow()

        if query_type == "BALANCE":
            delta: timedelta = object_to_check.whenGenerated - dt + timedelta(hours=self.__HOURS_DIFFERENCE_FROM_UTC)
            return delta.total_seconds() < 120
        if query_type == "POSITIONS":
            delta: timedelta = object_to_check.whenGenerated - dt + timedelta(hours=self.__HOURS_DIFFERENCE_FROM_UTC)
            return delta.total_seconds() < 120
    
    def get_and_parse_data(self, token, query_id):
        data = self.get_data(token, query_id)
        return self.parse_data(data)

    def get_data(self, token, query_id):
        counter: int = 0
        data: Optional[bytes] = None
        while not data:
            if counter <= 20:
                counter += 1
                try:
                    return client.download(token, query_id)
                except ResponseCodeError as e:
                    if e.code == 1018:
                        self.logger.debug(e)
                        sleep(5)
                    else:
                        raise Exception(f"Unusual error {e}")
            else:
                raise Exception("Kept on getting errors")
    
    
    def parse_data(self, data) -> FlexQueryResponse:
        cleansed_data = ibparser.parse(data)
        return cleansed_data

if __name__ == '__main__':
    from getpass import getpass
    log_filename= os.path.expanduser("~") + "/log/test.py"
    numeric_level = getattr(logging, "DEBUG", None)
    logging.basicConfig(level=numeric_level, format='%(levelname)s:%(asctime)s:%(message)s', filename=log_filename) 
    logger: logging.Logger = logging.getLogger()
    pwd = getpass("provide password for pk:")
    current_path = os.path.realpath(os.path.dirname(__file__))
    executor = ibDataFetcher(current_path, pwd)
    balances = executor.get_netliq()
    balances = executor.get_netliq()
    print(balances)

