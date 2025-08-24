import asyncio
import logging
from typing import Dict
import functools
from datetime import timedelta
import socket
from urllib.parse import urlparse

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

    FLEX_BASE_URLS = ["https://www.interactivebrokers.com", "https://www1.interactivebrokers.com", "https://ndcdyn.interactivebrokers.com", "https://gdcdyn.interactivebrokers.com"]
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
            "report_timestamp_utc": report_timestamp_utc,
        }

    async def _fetch_report_async(self, query_id: str) -> FlexQueryResponse:
        loop = asyncio.get_running_loop()
        token = self.account_and_query_ids["token"]
        func = functools.partial(self._fetch_and_parse_report_sync, token, query_id)
        return await loop.run_in_executor(None, func)

    def _hostname_resolves(self, base_url: str) -> bool:
        try:
            host = urlparse(base_url).hostname
            if not host:
                return False
            socket.getaddrinfo(host, 443)
            return True
        except socket.gaierror:
            return False

    def _is_dns_error(self, exc: Exception) -> bool:
        # Walk the exception chain and look for DNS-related errors or messages
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
                    # First request: ask IBKR to generate the statement
                    stmt_access = request_statement(token, query_id, url=send_url)

                    # Build poll URL candidates: (1) server-provided URL, (2) base fallback
                    poll_candidates = []
                    ib_url = getattr(stmt_access, "Url", None)
                    if ib_url and self._hostname_resolves(ib_url):
                        poll_candidates.append(ib_url)
                    else:
                        if ib_url:
                            self.logger.warning(f"IB provided poll URL '{ib_url}' does not resolve; skipping.")
                    poll_candidates.append(stmt_url_fallback)
                    
                    poll_attempts = 0
                    while True:
                        poll_attempts += 1
                        # Try poll candidates in order; on DNS error, fall back to the next candidate
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
                                    # When not ready, continue polling the same candidate first
                                    last_exc = None
                                    break  # break out of candidate loop to continue polling
                            except requests.exceptions.Timeout:
                                if poll_attempts >= 6:
                                    self.logger.error("Polling for report timed out after multiple attempts.")
                                    raise
                                self.logger.warning(
                                    f"Timeout on poll attempt {poll_attempts} via {poll_url}, re-trying in 10s."
                                )
                                sleep(10)
                                last_exc = None
                                break  # retry same candidate first
                            except requests.exceptions.RequestException as e:
                                if self._is_dns_error(e):
                                    self.logger.warning(
                                        f"DNS error polling via {poll_url}: {e}. Trying next poll URL candidate."
                                    )
                                    last_exc = e
                                    continue  # try next poll candidate
                                else:
                                    self.logger.warning(
                                        f"Network error polling via {poll_url}: {e}. Retrying after 15s."
                                    )
                                    sleep(15)
                                    last_exc = e
                                    break  # break candidates; retry the loop
                        else:
                            # We exhausted all poll candidates due to DNS errors
                            if last_exc:
                                # Break to try next base_url
                                self.logger.warning(
                                    f"All poll URL candidates failed DNS for base '{base_url}'. "
                                    f"Falling back to next base URL."
                                )
                                break
                        # If we broke from inner loop due to retryable conditions, continue polling
                        # Otherwise, if we hit the 'else' above, we will try next base URL
                        continue

                except ResponseCodeError as e:
                    if int(e.code) == 1018:
                        self.logger.debug("IBflex code 1018 received. Retrying after backoff delay.")
                        sleep(5 * attempt)
                        # Try same base again after backoff
                        continue
                    else:
                        self.logger.error(f"An unexpected IBflex response code error occurred: {e}")
                        raise
                except requests.exceptions.RequestException as e:
                    if self._is_dns_error(e):
                        self.logger.warning(
                            f"DNS error when contacting {send_url}: {e}. Trying next base URL immediately."
                        )
                        # Try next base URL without the 15s sleep (DNS issue is host-specific)
                        continue
                    else:
                        self.logger.warning(
                            f"A network error occurred on attempt {attempt} via {base_url}: {e}. "
                            f"Retrying after 15s."
                        )
                        sleep(15)
                        # Retry same attempt (and base) after backoff
                        continue

            # If we reach here, all base URLs failed this attempt
            if attempt < max_retries:
                self.logger.warning(
                    f"All Flex base URLs failed on attempt {attempt}. Backing off 15s before retry."
                )
                sleep(15)

        raise Exception(f"Failed to fetch report for query '{query_id}' after {max_retries} attempts.")
