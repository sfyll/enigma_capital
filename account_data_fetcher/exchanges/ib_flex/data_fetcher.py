import asyncio
import logging
from typing import Dict
import functools
from datetime import timedelta

from account_data_fetcher.exchanges.exchange_base import ExchangeBase
from infrastructure.api_secret_getter import ApiMetaData

import aiohttp
from ibflex import parser as ibparser, client
from ibflex.Types import OpenPosition, FlexQueryResponse
from ibflex.client import ResponseCodeError, request_statement, check_statement_response
import requests
from time import sleep

class DataFetcher(ExchangeBase):
    _EXCHANGE = "IB"
    __HOURS_DIFFERENCE_FROM_UTC = -5

    def __init__(self, secrets: ApiMetaData, session: aiohttp.ClientSession) -> None:
        super().__init__(exchange=self._EXCHANGE, session=session)
        self.__get_account_and_query_ids(secrets)
        self.logger = logging.getLogger(__name__)

    async def fetch_balance(self, accountType=None) -> float:
        """
        Fetches the total account balance, using cached data if it's still fresh.
        """
        response = await self._fetch_report_async(self.account_and_query_ids["query_id_balance"])

        return round(float(response.FlexStatements[0].ChangeInNAV.endingValue), 3)

    async def fetch_positions(self, accountType=None) -> dict:
        """
        Fetches current open positions, using cached data if it's still fresh.
        """
        response = await self._fetch_report_async(self.account_and_query_ids["query_id_position"])

        open_positions: OpenPosition = response.FlexStatements[0].OpenPositions
        
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

    def __get_account_and_query_ids(self, secrets: ApiMetaData) -> None:
        """Extracts token and query IDs from secrets."""
        self.account_and_query_ids: dict = {
            "token": secrets.other_fields["Token"],
            "query_id_balance": secrets.other_fields["Balance_query_id"],
            "query_id_position": secrets.other_fields["Position_query_id"]
        }

    async def process_request(self) -> Dict:
        """
        Overrides the base class method. Fetches fresh balance and position
        data from IB and returns it with the report's generation timestamp.
        This method is now completely stateless and performs a new fetch on every call.
        """
        
        balance_report_task = self._fetch_report_async(self.account_and_query_ids["query_id_balance"])
        position_report_task = self._fetch_report_async(self.account_and_query_ids["query_id_position"])
        
        balance_response, position_response = await asyncio.gather(balance_report_task, position_report_task)

        balance_statement = balance_response.FlexStatements[0]
        balance_value = round(float(balance_statement.ChangeInNAV.endingValue), 3)

        position_statement = position_response.FlexStatements[0]
        open_positions: OpenPosition = position_statement.OpenPositions
        positions_data = {"Symbol": [], "Multiplier": [], "Quantity": [], "Dollar Quantity": []}
        for pos in open_positions:
            positions_data["Symbol"].append(pos.symbol)
            positions_data["Multiplier"].append(int(pos.multiplier))
            positions_data["Quantity"].append(int(pos.position))
            dollar_qty = round(float(pos.markPrice) * float(pos.multiplier) * int(pos.position), 3)
            positions_data["Dollar Quantity"].append(dollar_qty)

        report_timestamp_utc = balance_statement.whenGenerated - timedelta(hours=self.__HOURS_DIFFERENCE_FROM_UTC)

        self.logger.info(f"Successfully fetched IB report generated at {report_timestamp_utc} UTC.")
        
        return {
            "exchange": self._EXCHANGE,
            "balance": balance_value,
            "positions": positions_data,
            "report_timestamp_utc": report_timestamp_utc
        }

    async def _fetch_report_async(self, query_id: str) -> FlexQueryResponse:
        loop = asyncio.get_running_loop()
        token = self.account_and_query_ids["token"]
        func = functools.partial(self._fetch_and_parse_report_sync, token, query_id)
        return await loop.run_in_executor(None, func)

    def _fetch_and_parse_report_sync(self, token: str, query_id: str) -> FlexQueryResponse:
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


