import asyncio
import logging
from typing import Dict
import functools
from datetime import timedelta, datetime
import socket
from urllib.parse import urlparse

from account_data_fetcher.exchanges.exchange_base import ExchangeBase
from infrastructure.api_secret_getter import ApiMetaData

import aiohttp
from ibflex import parser as ibparser
from ibflex.Types import OpenPosition, FlexQueryResponse
from ibflex.client import ResponseCodeError, request_statement, check_statement_response
import requests
from time import sleep
from zoneinfo import ZoneInfo

import holidays as _holidays


class DataFetcher(ExchangeBase):
    _EXCHANGE = "IB"

    FLEX_BASE_URLS = [
        "https://www.interactivebrokers.com",
        "https://www1.interactivebrokers.com",
        "https://ndcdyn.interactivebrokers.com",
        "https://gdcdyn.interactivebrokers.com",
    ]

    def __init__(self, secrets: ApiMetaData, session: aiohttp.ClientSession) -> None:
        super().__init__(exchange=self._EXCHANGE, session=session)
        self.__get_account_and_query_ids(secrets)
        self.logger = logging.getLogger(__name__)

        seen = set()
        self.flex_base_urls = []
        for base in self.FLEX_BASE_URLS:
            base = base.rstrip("/")
            if base not in seen:
                self.flex_base_urls.append(base)
                seen.add(base)

        self._tz_et = ZoneInfo("America/New_York")
        self._tz_utc = ZoneInfo("UTC")

        self._us_holidays = None
        try:
            y = datetime.now(self._tz_et).year
            self._us_holidays = _holidays.country_holidays(country="US", years=[y - 1, y, y + 1])
        except Exception:
            self._us_holidays = None

    async def fetch_balance(self, accountType=None) -> float:
        response = await self._fetch_report_async(self.account_and_query_ids["query_id_balance"])
        return round(float(response.FlexStatements[0].ChangeInNAV.endingValue), 3)

    async def fetch_positions(self, accountType=None) -> dict:
        response = await self._fetch_report_async(self.account_and_query_ids["query_id_position"])
        open_positions: OpenPosition = response.FlexStatements[0].OpenPositions

        data_to_return = {"Symbol": [], "Multiplier": [], "Quantity": [], "Dollar Quantity": []}
        for position in open_positions:
            data_to_return["Symbol"].append(position.symbol)
            data_to_return["Multiplier"].append(int(position.multiplier))
            data_to_return["Quantity"].append(int(position.position))
            dollar_quantity = round(
                float(position.markPrice) * float(position.multiplier) * int(position.position), 3
            )
            data_to_return["Dollar Quantity"].append(dollar_quantity)

        return data_to_return

    def __get_account_and_query_ids(self, secrets: ApiMetaData) -> None:
        self.account_and_query_ids: dict = {
            "token": secrets.other_fields["Token"],
            "query_id_balance": secrets.other_fields["Balance_query_id"],
            "query_id_position": secrets.other_fields["Position_query_id"],
        }

    async def process_request(self) -> Dict:
        balance_report_task = self._fetch_report_async(self.account_and_query_ids["query_id_balance"])
        position_report_task = self._fetch_report_async(self.account_and_query_ids["query_id_position"])
        balance_response, position_response = await asyncio.gather(balance_report_task, position_report_task)

        balance_statement = balance_response.FlexStatements[0]
        position_statement = position_response.FlexStatements[0]

        data_date = balance_statement.toDate

        today_et = datetime.now(self._tz_et).date()
        expected_lbd = self._last_business_day(today_et)

        is_data_current = (data_date == expected_lbd)

        report_generated_utc = self._to_utc_from_eastern(balance_statement.whenGenerated)

        balance_value = round(float(balance_statement.ChangeInNAV.endingValue), 3)

        open_positions: OpenPosition = position_statement.OpenPositions
        positions_data = {"Symbol": [], "Multiplier": [], "Quantity": [], "Dollar Quantity": []}
        for pos in open_positions:
            positions_data["Symbol"].append(pos.symbol)
            positions_data["Multiplier"].append(int(pos.multiplier))
            positions_data["Quantity"].append(int(pos.position))
            dollar_qty = round(float(pos.markPrice) * float(pos.multiplier) * int(pos.position), 3)
            positions_data["Dollar Quantity"].append(dollar_qty)

        self.logger.info(
            f"IB Flex: data_date={data_date} (expected LBD={expected_lbd}, current={is_data_current}); "
            f"generated_utc={report_generated_utc}."
        )

        return {
            "exchange": self._EXCHANGE,
            "balance": balance_value,
            "positions": positions_data,
            "report_timestamp_utc": datetime.now(self._tz_utc),
        }

    async def _fetch_report_async(self, query_id: str) -> FlexQueryResponse:
        loop = asyncio.get_running_loop()
        token = self.account_and_query_ids["token"]
        func = functools.partial(self._fetch_and_parse_report_sync, token, query_id)
        return await loop.run_in_executor(None, func)

    def _hostname_resolves(self, url_or_base: str) -> bool:
        try:
            host = urlparse(url_or_base).hostname
            if not host:
                return False
            socket.getaddrinfo(host, 443)
            return True
        except socket.gaierror:
            return False

    def _is_dns_error(self, exc: Exception) -> bool:
        seen = set()
        e = exc
        while e and e not in seen:
            seen.add(e)
            if isinstance(e, socket.gaierror):
                return True
            name = e.__class__.__name__
            msg = str(e)
            if "NameResolutionError" in name or "Failed to resolve" in msg or "nodename nor servname" in msg:
                return True
            e = getattr(e, "__cause__", None) or getattr(e, "__context__", None)
        return False

    def _to_utc_from_eastern(self, dt_local_naive: datetime) -> datetime:
        # IB Flex exposes whenGenerated as a naive datetime; interpret as ET and convert to UTC (handles DST)
        if dt_local_naive.tzinfo is None:
            dt_et = dt_local_naive.replace(tzinfo=self._tz_et)
        else:
            dt_et = dt_local_naive.astimezone(self._tz_et)
        return dt_et.astimezone(self._tz_utc)

    def _is_business_day_et(self, d) -> bool:
        if d.weekday() >= 5:  # Sat/Sun
            return False
        if self._us_holidays is not None and d in self._us_holidays:
            return False
        return True

    def _last_business_day(self, today_et) -> datetime.date:
        # Returns the most recent ET date that is a business day strictly before 'today_et'
        d = today_et - timedelta(days=1)
        while not self._is_business_day_et(d):
            d = d - timedelta(days=1)
        return d

    def _fetch_and_parse_report_sync(self, token: str, query_id: str) -> FlexQueryResponse:
        max_retries = 5
        for attempt in range(1, max_retries + 1):
            self.logger.debug(f"Requesting statement for query '{query_id}' (Attempt: {attempt})")

            # Try each base URL in priority order; skip immediately if DNS doesn't resolve
            for base_url in self.flex_base_urls:
                if not self._hostname_resolves(base_url):
                    self.logger.warning(
                        f"Skipping base URL '{base_url}' because DNS resolution failed in this environment."
                    )
                    continue

                send_url = base_url + "/Universal/servlet/FlexStatementService.SendRequest"
                stmt_url_fallback = base_url + "/Universal/servlet/FlexStatementService.GetStatement"

                try:
                    # Ask IBKR to generate the statement
                    stmt_access = request_statement(token, query_id, url=send_url)

                    # Poll candidates: use IB-provided URL only if it resolves; always include our fallback
                    poll_candidates = []
                    ib_url = getattr(stmt_access, "Url", None)
                    if ib_url and self._hostname_resolves(ib_url):
                        poll_candidates.append(ib_url)
                    elif ib_url:
                        self.logger.warning(f"IB provided poll URL '{ib_url}' does not resolve; skipping.")
                    poll_candidates.append(stmt_url_fallback)

                    poll_attempts = 0
                    while True:
                        poll_attempts += 1
                        last_exc = None
                        for poll_url in poll_candidates:
                            params = {"v": "3", "t": token, "q": stmt_access.ReferenceCode}
                            headers = {"user-agent": "Java"}
                            try:
                                resp = requests.get(
                                    poll_url, params=params, headers=headers, timeout=(30 * poll_attempts)
                                )
                                resp.raise_for_status()
                                result = check_statement_response(resp)
                                if result is True:
                                    self.logger.debug(
                                        f"Successfully received report for query '{query_id}' via {poll_url}."
                                    )
                                    return ibparser.parse(resp.content)
                                else:
                                    wait_seconds = result
                                    self.logger.debug(
                                        f"Statement not ready; waiting {wait_seconds}s "
                                        f"(Poll attempt {poll_attempts}) via {poll_url}."
                                    )
                                    sleep(wait_seconds)
                                    last_exc = None
                                    break
                            except requests.exceptions.Timeout:
                                if poll_attempts >= 6:
                                    self.logger.error("Polling for report timed out after multiple attempts.")
                                    raise
                                self.logger.warning(
                                    f"Timeout on poll attempt {poll_attempts} via {poll_url}, re-trying in 10s."
                                )
                                sleep(10)
                                last_exc = None
                                break
                            except requests.exceptions.RequestException as e:
                                if self._is_dns_error(e):
                                    self.logger.warning(
                                        f"DNS error polling via {poll_url}: {e}. Trying next poll URL candidate."
                                    )
                                    last_exc = e
                                    continue
                                else:
                                    self.logger.warning(
                                        f"Network error polling via {poll_url}: {e}. Retrying after 15s."
                                    )
                                    sleep(15)
                                    last_exc = e
                                    break
                        else:
                            if last_exc:
                                self.logger.warning(
                                    f"All poll URL candidates failed DNS for base '{base_url}'. "
                                    f"Falling back to next base URL."
                                )
                                break
                        continue

                except ResponseCodeError as e:
                    if int(e.code) == 1018:
                        self.logger.debug("IBflex code 1018 received. Retrying after backoff delay.")
                        sleep(5 * attempt)
                        continue
                    else:
                        self.logger.error(f"An unexpected IBflex response code error occurred: {e}")
                        raise
                except requests.exceptions.RequestException as e:
                    if self._is_dns_error(e):
                        self.logger.warning(
                            f"DNS error when contacting {send_url}: {e}. Trying next base URL immediately."
                        )
                        continue
                    else:
                        self.logger.warning(
                            f"A network error occurred on attempt {attempt} via {base_url}: {e}. Retrying after 15s."
                        )
                        sleep(15)
                        continue

            if attempt < max_retries:
                self.logger.warning(
                    f"All Flex base URLs failed on attempt {attempt}. Backing off 15s before retry."
                )
                sleep(15)

        raise Exception(f"Failed to fetch report for query '{query_id}' after {max_retries} attempts.")
