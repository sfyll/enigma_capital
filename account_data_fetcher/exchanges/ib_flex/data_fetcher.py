from datetime import  datetime, timedelta
import logging
import os
from requests.exceptions import ReadTimeout
from time import sleep
from typing import Optional

from ibflex import parser as ibparser
from ibflex import client
from ibflex.Types import OpenPosition, FlexQueryResponse, FlexStatement
from ibflex.client import ResponseCodeError, request_statement, check_statement_response

from account_data_fetcher.exchanges.exchange_base import ExchangeBase
from infrastructure.api_secret_getter import ApiMetaData

import requests

class DataFetcher(ExchangeBase):
    __HOURS_DIFFERENCE_FROM_UTC = -5
    __EXCHANGE= "IB"

    def __init__(self, secrets: ApiMetaData, port_number: int) -> None:
        super().__init__(port_number, self.__EXCHANGE)
        self.__get_account_and_query_ids(secrets)
        self.logger = logging.getLogger(__name__)
        self.balance_object: Optional[FlexStatement] = None
        self.positions_object: Optional[FlexStatement] = None

    def __get_account_and_query_ids(self, secrets: ApiMetaData) -> None:
        self.account_and_query_ids: dict = {
            "token" : secrets.other_fields["Token"],
            "query_id_balance" : secrets.other_fields["Balance_query_id"],
            "query_id_position" : secrets.other_fields["Position_query_id"]
        }

    def update_balance_and_get_ib_datetime(self) -> datetime:
        if not self.is_acceptable_timestamp_detla(self.balance_object, "BALANCE"):
            self.get_balance_object()
        return self.balance_object.whenGenerated

    def update_positions_and_get_ib_datetime(self) -> datetime:
        if not self.is_acceptable_timestamp_detla(self.positions_object, "POSITIONS"):
            self.get_positions_object()
        return self.positions_object.whenGenerated

    def fetch_balance(self, accountType = None) -> float:
        if not self.is_acceptable_timestamp_detla(self.balance_object, "BALANCE"):
            self.get_balance_object()
        
        self.logger.debug(
            f"{self.balance_object=}"
        )
        return round(float(self.balance_object.ChangeInNAV.endingValue),3)

    def fetch_positions(self, accountType = None) -> dict:
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
            delta: timedelta = (dt + timedelta(hours=self.__HOURS_DIFFERENCE_FROM_UTC)) - object_to_check.whenGenerated 
            return delta.total_seconds() < 120
        if query_type == "POSITIONS":
            delta: timedelta = (dt + timedelta(hours=self.__HOURS_DIFFERENCE_FROM_UTC)) - object_to_check.whenGenerated 
            return delta.total_seconds() < 120
    
    def get_and_parse_data(self, token, query_id):
        data = self.get_data(token, query_id)
        return self.parse_data(data)

    def get_data(self, token, query_id):
        max_retries = 10 
        for attempt in range(1, max_retries + 1):
            try:
                return self._request_statement_and_poll(token, query_id)
            except ResponseCodeError as e:
                # e.g. code 1018 means 'Statement generation in progress', we can sleep:
                if int(e.code) == 1018:
                    self.logger.debug(f"Got 1018 on attempt {attempt}. Sleeping before retry.")
                    sleep(5 * attempt)
                    continue
                else:
                    raise Exception(f"Unusual error {e}")
            except ReadTimeout as e:
                self.logger.debug(f"ReadTimeout on attempt {attempt}, sleeping 15s: {e}")
                sleep(15)
                continue
        raise Exception("Kept on getting errors beyond max_retries") 

    def _request_statement_and_poll(self, token: str, query_id: str) -> bytes:
        """
        Implements the ibflex 2-step approach manually:
          1) request_statement => get reference code & URL
          2) poll until statement is ready
        but with your own timeouts/retries to handle large statements.
        """
        # 1) Ask IB to start generating the statement
        stmt_access = request_statement(token, query_id, url=client.REQUEST_URL)

        # 2) Repeatedly request the statement data at the 'Url' with the reference code
        poll_attempts = 0
        while True:
            poll_attempts += 1
            # Build your GET:
            url = stmt_access.Url or client.STMT_URL
            params = {"v": "3", "t": token, "q": stmt_access.ReferenceCode}
            headers = {"user-agent": "Java"}

            # Example: extend the timeout drastically for large statements
            try:
                # e.g. 30s * poll_attempts for a bit of dynamic backoff
                resp = requests.get(url, params=params, headers=headers, timeout=(30 * poll_attempts))
            except requests.exceptions.Timeout:
                # You can either break or raise, or keep trying
                if poll_attempts >= 6:
                    raise
                self.logger.warning(f"Timeout on poll attempt {poll_attempts}, re-trying in 10s.")
                sleep(10)
                continue

            # 3) Check the response
            result = check_statement_response(resp)
            if result is True:
                # means `resp.content` is the real flex data
                return resp.content
            else:
                # means IB told us "not ready yet" or "throttled" -> result is how many seconds to wait
                wait_seconds = result
                self.logger.debug(f"Statement not ready; waiting {wait_seconds}s (attempt {poll_attempts}).")
                # If you want to increase or cap wait_seconds, do so here
                sleep(wait_seconds)

    def parse_data(self, data) -> FlexQueryResponse:
        return ibparser.parse(data)

if __name__ == '__main__':
    from getpass import getpass
    log_filename= os.path.expanduser("~") + "/log/test.py"
    numeric_level = getattr(logging, "DEBUG", None)
    logging.basicConfig(level=numeric_level, format='%(levelname)s:%(asctime)s:%(message)s', filename=log_filename) 
    logger: logging.Logger = logging.getLogger()
    pwd = getpass("provide password for pk:")
    current_path = os.path.realpath(os.path.dirname(__file__))
    executor = DataFetcher(current_path, pwd)
    balances = executor.fetch_balance()
    print(balances)

