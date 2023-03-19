import base64
from datetime import datetime as dt
import hashlib
import hmac
from json.decoder import JSONDecodeError
import logging
from requests.exceptions import ReadTimeout, SSLError, ConnectionError
from requests import Response
import time
import urllib
from typing import List, Optional

from account_data_fetcher.kraken.exception import InvalidRequestError, FailedRequestError
from utilities.request_handler import requestHandler

class krakenApiConnector:
    __ENDPOINT="https://api.kraken.com"
    
    def __init__(self, api_key: str, api_secret: str, max_retries: int = 10, 
                force_retry: bool = True, retry_delay: int = 3, retry_codes: Optional[set] = None) -> None:
        self.logger = logging.getLogger(__name__) 
        self.__request_handler: requestHandler = requestHandler()
        self.api_key: str = api_key
        self.api_secret: str = api_secret
        self.max_retries: int = max_retries
        self.force_retry: bool = force_retry
        self.retry_delay: int = retry_delay

        # Set whitelist of non-fatal Bybit status codes to retry on.
        if retry_codes is None:
            start = "EOrder:"
            self.retry_codes = {start+"Rate limit exceeded",}
        else:
            self.retry_codes = retry_codes


    def get_balance(self, **kwargs) -> List[dict]:
        module = "/0/private/Balance"
            
        response = self.__prepare_and_handle_request(
            method="post",
            path_extension=module,
            req_params=kwargs,
        )
        
        return response["result"]

    def get_ticker(self, **kwargs)-> dict[str, str]:
        module = "/0/public/Ticker"
                
        response = self.__prepare_and_handle_request(
            method="get",
            path_extension=module,
            req_params=kwargs,
            is_private=False
        )

        return response["result"]

    def __generate_headers(self, params: dict, url_path: str) -> dict:
        return {
            "API-Key": self.api_key,
            "API-Sign": self.__sign(params, url_path),
            'Content-Type': 'application/x-www-form-urlencoded; charset=utf-8'
        }

    def __get_utc_timestamp_milliseconds(self) -> str:
        return str(int(time.time() * 10 ** 3))

    def __sign(self, params: dict, url_path: str) -> str:
        postdata = urllib.parse.urlencode(params)
        encoded = (str(params['nonce']) + postdata).encode()
        message = url_path.encode() + hashlib.sha256(encoded).digest()

        mac = hmac.new(base64.b64decode(self.api_secret), message, hashlib.sha512)
        sigdigest = base64.b64encode(mac.digest())
        return sigdigest.decode()

    def __prepare_and_handle_request(self, method: str, path_extension: str, req_params: dict, is_private: bool = True):

        # Send request and return headers with body. Retry if failed.
        retries_attempted = self.max_retries

        while True:
            if is_private:
                req_params["nonce"] = self.__get_utc_timestamp_milliseconds()
                headers = self.__generate_headers(req_params, path_extension)
            
            url = self.__ENDPOINT + path_extension

            retries_attempted -= 1
            if retries_attempted < 0:
                raise FailedRequestError(
                    request=f'{method} {path_extension}: {req_params}',
                    message='Bad Request. Retries exceeded maximum.',
                    status_code=400,
                    time=dt.utcnow().strftime("%H:%M:%S")
                )

            try:
                if is_private:
                    raw_response: Response = self.__request_handler.handle_requests(
                        url=url,
                        method=method,
                        args=req_params,
                        headers=headers,
                        raw_response=True
                    )
                else:
                    raw_response: Response = self.__request_handler.handle_requests(
                        url=url,
                        method=method,
                        args=req_params,
                        raw_response=True
                    )

            except (
                ReadTimeout,
                SSLError,
                ConnectionError
            ) as e:
                if self.force_retry:
                    self.logger.error(f'{e}. {retries_attempted}')
                    time.sleep(self.retry_delay)
                    continue
                else:
                    raise e

            try:
                response = raw_response.json()

            # If we have trouble converting, handle the error and retry.
            except JSONDecodeError as e:
                if self.force_retry:
                    self.logger.error(f'{e}. {retries_attempted}')
                    time.sleep(self.retry_delay)
                    continue
                else:
                    raise FailedRequestError(
                        request=f'{method} {url}: {req_params}',
                        message='Conflict. Could not decode JSON.',
                        status_code=409,
                        time=dt.utcnow().strftime("%H:%M:%S")
                    )
            if response['error']:

                    # Generate error message.
                    error_msg = (
                        f'{response["error"]})'
                    )

                    # Retry non-fatal whitelisted error requests.
                    if response['error'][0] in self.retry_codes:

                        # 10002, recv_window error; add 2.5 seconds and retry.
                        if response['error'][0] == 10002:
                            error_msg += '. Added 2.5 seconds to recv_window'
                            self.__X_BAPI_RECV_WINDOW = str(int(self.__X_BAPI_RECV_WINDOW) + 2500)

                        # 10006, ratelimit error; wait until rate_limit_reset_ms
                        # and retry.
                        elif "Rate limit exceeded" in response['error'][0]:
                            self.logger.error(
                                f'{response["error"]}. Ratelimited on current request. '
                                f'Sleeping, then trying again. Request: {url}'
                            )

                            # Calculate how long we need to wait.
                            limit_reset = response['rate_limit_reset_ms'] / 1000
                            reset_str = time.strftime(
                                '%X', time.localtime(limit_reset)
                            )
                            err_delay = int(limit_reset) - int(time.time())
                            error_msg = (
                                f'Ratelimit will reset at {reset_str}. '
                                f'Sleeping for {err_delay} seconds'
                            )

                    else:
                        raise InvalidRequestError(
                            request=f'{method} {url}: {req_params}',
                            message=response["error"][0],
                            time=dt.utcnow().strftime("%H:%M:%S")
                        )
            else:
                return response