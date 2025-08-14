import asyncio
import logging
from typing import Dict
import functools
from datetime import datetime, timezone, date, time as dtime
from zoneinfo import ZoneInfo

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
    _ET = ZoneInfo("America/New_York")

    def __init__(self, secrets: ApiMetaData, session: aiohttp.ClientSession) -> None:
        super().__init__(exchange=self._EXCHANGE, session=session)
        self.__get_account_and_query_ids(secrets)
        self.logger = logging.getLogger(__name__)

    async def fetch_balance(self, accountType=None) -> float:
        """
        Fetches the total account balance, using the latest statement by ToDate.
        """
        response = await self._fetch_report_async(self.account_and_query_ids["query_id_balance"])
        statements = list(response.FlexStatements)

        def _to_date(stmt) -> date:
            td = getattr(stmt, "toDate", None) or getattr(stmt, "ToDate", None)
            if td is None:
                return date.min
            if isinstance(td, date):
                return td
            if isinstance(td, datetime):
                return td.date()
            return datetime.strptime(str(td), "%Y%m%d").date()

        latest_stmt = max(statements, key=_to_date)
        return round(float(latest_stmt.ChangeInNAV.endingValue), 3)

    async def fetch_positions(self, accountType=None) -> dict:
        """
        Fetches current open positions, using the latest statement by ToDate.
        """
        response = await self._fetch_report_async(self.account_and_query_ids["query_id_position"])
        statements = list(response.FlexStatements)

        def _to_date(stmt) -> date:
            td = getattr(stmt, "toDate", None) or getattr(stmt, "ToDate", None)
            if td is None:
                return date.min
            if isinstance(td, date):
                return td
            if isinstance(td, datetime):
                return td.date()
            return datetime.strptime(str(td), "%Y%m%d").date()

        latest_stmt = max(statements, key=_to_date)
        open_positions: OpenPosition = latest_stmt.OpenPositions

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
        Fetches fresh balance and position data from IB and returns it with a report timestamp
        representing the statement period end (ToDate at 23:59:59 ET, converted to UTC).
        """
        balance_report_task = self._fetch_report_async(self.account_and_query_ids["query_id_balance"])
        position_report_task = self._fetch_report_async(self.account_and_query_ids["query_id_position"])

        balance_response, position_response = await asyncio.gather(balance_report_task, position_report_task)

        def _to_date(stmt) -> date:
            td = getattr(stmt, "toDate", None) or getattr(stmt, "ToDate", None)
            if td is None:
                return date.min
            if isinstance(td, date):
                return td
            if isinstance(td, datetime):
                return td.date()
            # assume yyyymmdd
            return datetime.strptime(str(td), "%Y%m%d").date()

        # Select the latest statements by period end (ToDate)
        balance_statements = list(balance_response.FlexStatements)
        balance_statement = max(balance_statements, key=_to_date)
        balance_value = round(float(balance_statement.ChangeInNAV.endingValue), 3)

        position_statements = list(position_response.FlexStatements)
        position_statement = max(position_statements, key=_to_date)
        open_positions: OpenPosition = position_statement.OpenPositions

        positions_data = {"Symbol": [], "Multiplier": [], "Quantity": [], "Dollar Quantity": []}
        for pos in open_positions:
            positions_data["Symbol"].append(pos.symbol)
            positions_data["Multiplier"].append(int(pos.multiplier))
            positions_data["Quantity"].append(int(pos.position))
            dollar_qty = round(float(pos.markPrice) * float(pos.multiplier) * int(pos.position), 3)
            positions_data["Dollar Quantity"].append(dollar_qty)

        # Use the report's period end (ToDate) at 23:59:59 ET as the report timestamp (converted to UTC)
        period_date_et = _to_date(balance_statement)
        period_end_et = datetime.combine(period_date_et, dtime(23, 59, 59), tzinfo=self._ET)
        report_timestamp_utc = period_end_et.astimezone(timezone.utc)

        self.logger.info(
            f"Successfully fetched IB report for period {period_date_et} ET (timestamp {report_timestamp_utc} UTC)."
        )

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
