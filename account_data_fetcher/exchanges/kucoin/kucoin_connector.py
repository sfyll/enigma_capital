import base64
from datetime import datetime as dt
import hashlib
import hmac
from json.decoder import JSONDecodeError
import logging
from requests.exceptions import ReadTimeout, SSLError, ConnectionError
from requests import Response
import time
from typing import List, Optional

from account_data_fetcher.exchanges.kucoin.exception import InvalidRequestError, FailedRequestError
from utilities.request_handler import requestHandler

class kucoinApiConnector:
    __ENDPOINT="https://api.kucoin.com"
    
    def __init__(self, api_key: str, api_secret: str, passphrase:str,  max_retries: int = 10, 
                force_retry: bool = True, retry_delay: int = 3, retry_codes: Optional[set] = None) -> None:
        self.logger = logging.getLogger(__name__) 
        self.__request_handler: requestHandler = requestHandler()
        self.api_key: str = api_key
        self.api_secret: str = api_secret
        self.api_passphrase: bytes = base64.b64encode(hmac.new(bytes(api_secret, "utf-8"), passphrase.encode("utf-8"),hashlib.sha256).digest())

 
        self.max_retries: int = max_retries
        self.force_retry: bool = force_retry
        self.retry_delay: int = retry_delay

        # Set whitelist of non-fatal Bybit status codes to retry on.
        if retry_codes is None:
            self.retry_codes = {10002, 10006, 30034, 30035, 130035, 130150}
        else:
            self.retry_codes = retry_codes

    def get_position(self, **kwargs) -> List[dict]:
        module = "api/v1/positions" 
   
        response = self.__prepare_and_handle_request(
            method="get",
            module=module,
            req_params=kwargs
        )
        
        return response["result"]

        
    def get_last_traded_price(self, **kwargs)-> dict[str, str]:
        module = "api/v1/prices"
         
        response = self.__prepare_and_handle_request(
            method="get",
            module=module,
            req_params=kwargs
        )

        return response["data"]

    def get_wallet_balance(self, **kwargs) -> List[dict]:
        module = "api/v1/accounts"
         
        response = self.__prepare_and_handle_request(
            method="get",
            module=module,
            req_params=kwargs
        )
        
        return response["data"]
    
    def __generate_headers(self, timestamp: str, signature: bytes) -> dict:
        return {
            "KC-API-SIGN": signature,
            "KC-API-TIMESTAMP": timestamp,
            "KC-API-KEY": self.api_key,
            "KC-API-PASSPHRASE": self.api_passphrase,
            "KC-API-KEY-VERSION": "2",
            'Content-Type': 'application/json'
        }

    def __get_utc_timestamp_milliseconds(self) -> str:
        return str(int(time.time() * 10 ** 3))

    def __sign(self, timestamp: str, params: dict, method: str, path:str) -> bytes:

        _val = '&'.join(
            [str(k) + '=' + str(v) for k, v in sorted(params.items()) if
             (k != 'sign') and (v is not None)]
        )

        params_str = timestamp + method.upper() + "/" +  path +  _val

        hash_hmac =  hmac.new(bytes(self.api_secret, "utf-8"), params_str.encode("utf-8"),hashlib.sha256).digest()

        return base64.b64encode(hash_hmac)

    def __prepare_and_handle_request(self, method: str, module: str, req_params: dict):

        # Send request and return headers with body. Retry if failed.
        retries_attempted = self.max_retries
        path = self.__request_handler.endpoint_extension(self.__ENDPOINT, module)

        while True:

            timestamp = self.__get_utc_timestamp_milliseconds()
            signature = self.__sign(timestamp, req_params, method, module)
            headers = self.__generate_headers(timestamp, signature)

            retries_attempted -= 1
            if retries_attempted < 0:
                raise FailedRequestError(
                    request=f'{method} {path}: {req_params}',
                    message='Bad Request. Retries exceeded maximum.',
                    status_code=400,
                    time=dt.utcnow().strftime("%H:%M:%S")
                )

            try:
                raw_response: Response = self.__request_handler.handle_requests(
                    url=path,
                    method=method,
                    args=req_params,
                    headers=headers,
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
                        request=f'{method} {path}: {req_params}',
                        message='Conflict. Could not decode JSON.',
                        status_code=409,
                        time=dt.utcnow().strftime("%H:%M:%S")
                    )

            if response['code'] != '200000':

                    # Generate error message.
                    error_msg = (
                        f'{response["retMsg"]} (ErrCode: {response["retCode"]})'
                    )

                    # Retry non-fatal whitelisted error requests.
                    if response['retCode'] in self.retry_codes:

                        # 10006, ratelimit error; wait until rate_limit_reset_ms
                        # and retry.
                        if response['retCode'] == 10006:
                            self.logger.error(
                                f'{error_msg}. Ratelimited on current request. '
                                f'Sleeping, then trying again. Request: {path}'
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
                            request=f'{method} {path}: {req_params}',
                            message=response["retMsg"],
                            status_code=response["retCode"],
                            time=dt.utcnow().strftime("%H:%M:%S")
                        )
            else:
                return response
