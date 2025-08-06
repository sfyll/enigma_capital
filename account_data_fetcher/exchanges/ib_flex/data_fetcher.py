import asyncio
import logging
from typing import Optional
import functools
from datetime import datetime, timedelta 

from account_data_fetcher.exchanges.exchange_base import ExchangeBase
from infrastructure.api_secret_getter import ApiMetaData

import aiohttp
from ibflex import parser as ibparser, client
from ibflex.Types import OpenPosition, FlexQueryResponse, FlexStatement
from ibflex.client import ResponseCodeError, request_statement, check_statement_response
import requests
from time import sleep

class DataFetcher(ExchangeBase):
    _EXCHANGE = "IB"
    __HOURS_DIFFERENCE_FROM_UTC = -5

    def __init__(self, secrets: ApiMetaData, session: aiohttp.ClientSession, output_queue: asyncio.Queue, fetch_frequency: int) -> None:
        super().__init__(
            exchange=self._EXCHANGE,
            session=session,
            output_queue=output_queue,
            fetch_frequency=fetch_frequency
        )
        self.__get_account_and_query_ids(secrets)
        self.logger = logging.getLogger(__name__)
        self.balance_object: Optional[FlexStatement] = None
        self.positions_object: Optional[FlexStatement] = None


    def __get_account_and_query_ids(self, secrets: ApiMetaData) -> None:
        """Extracts token and query IDs from secrets."""
        self.account_and_query_ids: dict = {
            "token": secrets.other_fields["Token"],
            "query_id_balance": secrets.other_fields["Balance_query_id"],
            "query_id_position": secrets.other_fields["Position_query_id"]
        }

    def is_acceptable_timestamp_delta(self, object_to_check: Optional[FlexStatement]) -> bool:
        """
        Checks if the cached data is fresh enough based on its timestamp and the configured fetch_frequency.
        """
        if not object_to_check:
            return False

        ib_timestamp_as_utc = object_to_check.whenGenerated - timedelta(hours=self.__HOURS_DIFFERENCE_FROM_UTC)
        time_since_generation = datetime.utcnow() - ib_timestamp_as_utc

        is_fresh = time_since_generation.total_seconds() < self.fetch_frequency
        
        return is_fresh

    async def fetch_balance(self, accountType=None) -> float:
        """
        Fetches the total account balance, using cached data if it's still fresh.
        """
        if not self.is_acceptable_timestamp_delta(self.balance_object):
            response = await self._fetch_report_async(self.account_and_query_ids["query_id_balance"])
            self.balance_object = response.FlexStatements[0]

        return round(float(self.balance_object.ChangeInNAV.endingValue), 3)

    async def fetch_positions(self, accountType=None) -> dict:
        """
        Fetches current open positions, using cached data if it's still fresh.
        """
        if not self.is_acceptable_timestamp_delta(self.positions_object):
            response = await self._fetch_report_async(self.account_and_query_ids["query_id_position"])
            self.positions_object = response.FlexStatements[0]

        open_positions: OpenPosition = self.positions_object.OpenPositions
        
        data_to_return = {
            "Symbol": [], "Multiplier": [], "Quantity": [], "Dollar Quantity": []
        }
        for position in open_positions:
            data_to_return["Symbol"].append(position.symbol)
            data_to_return["Multiplier"].append(int(position.multiplier))
            data_to_return["Quantity"].append(int(position.position))
            dollar_quantity = round(float(position.markPrice) * float(position.multiplier) * int(position.position), 3)
            data_to_return["Dollar Quantity"].append(dollar_quantity)
        
        return data_to_return

    async def _fetch_report_async(self, query_id: str) -> FlexQueryResponse:
        """
        Asynchronously requests a report by running the blocking `ibflex`
        and `requests` calls in a separate thread.
        """
        loop = asyncio.get_running_loop()
        token = self.account_and_query_ids["token"]
        func = functools.partial(self._fetch_and_parse_report_sync, token, query_id)
        response = await loop.run_in_executor(None, func)
        return response

    def _fetch_and_parse_report_sync(self, token: str, query_id: str) -> FlexQueryResponse:
        """
        Synchronous method containing the blocking logic for fetching and parsing
        a report from IB Flex. Designed to be run in a thread pool.
        """
        max_retries = 5 
        for attempt in range(1, max_retries + 1):
            try:
                self.logger.debug(f"Requesting statement for query '{query_id}' (Attempt: {attempt})")
                stmt_access = request_statement(token, query_id, url=client.REQUEST_URL)
                poll_attempts = 0
                while True:
                    poll_attempts += 1
                    url = stmt_access.Url or client.STMT_URL
                    params = {"v": "3", "t": token, "q": stmt_access.ReferenceCode}
                    headers = {"user-agent": "Java"}
                    try:
                        resp = requests.get(url, params=params, headers=headers, timeout=(30 * poll_attempts))
                        resp.raise_for_status()
                        result = check_statement_response(resp)
                        if result is True:
                            self.logger.debug(f"Successfully received report for query '{query_id}'.")
                            return ibparser.parse(resp.content)
                        else:
                            wait_seconds = result
                            self.logger.debug(f"Statement not ready; waiting {wait_seconds}s (Poll attempt {poll_attempts}).")
                            sleep(wait_seconds)
                    except requests.exceptions.Timeout:
                        if poll_attempts >= 6:
                            self.logger.error("Polling for report timed out after multiple attempts.")
                            raise
                        self.logger.warning(f"Timeout on poll attempt {poll_attempts}, re-trying in 10s.")
                        sleep(10)
            except ResponseCodeError as e:
                if int(e.code) == 1018:
                    self.logger.debug(f"IBflex code 1018 received. Retrying after backoff delay.")
                    sleep(5 * attempt)
                    continue
                else:
                    self.logger.error(f"An unexpected IBflex response code error occurred: {e}")
                    raise
            except requests.exceptions.RequestException as e:
                self.logger.warning(f"A network error occurred on attempt {attempt}: {e}. Retrying after 15s.")
                sleep(15)
                continue
        raise Exception(f"Failed to fetch report for query '{query_id}' after {max_retries} attempts.")

