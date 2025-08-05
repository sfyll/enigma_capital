import asyncio
import base64
from datetime import datetime as dt
import hashlib
import hmac
from json.decoder import JSONDecodeError
import logging
import time
import urllib.parse
from typing import List, Optional
import json

import aiohttp 
from account_data_fetcher.exchanges.kraken.exception import InvalidRequestError, FailedRequestError

class krakenApiConnector:
    __ENDPOINT="https://api.kraken.com"
    
    def __init__(self, api_key: str, api_secret: str, session: aiohttp.ClientSession, max_retries: int = 10, 
                force_retry: bool = True, retry_delay: int = 3, retry_codes: Optional[set] = None) -> None:
        self.logger = logging.getLogger(__name__) 
        self.session = session
        self.api_key: str = api_key
        self.api_secret: str = api_secret
        self.max_retries: int = max_retries
        self.force_retry: bool = force_retry
        self.retry_delay: int = retry_delay

        # Set whitelist of non-fatal Bybit status codes to retry on.
        if retry_codes is None:
            self.retry_codes = {"EOrder:Rate limit exceeded"}
        else:
            self.retry_codes = retry_codes


    async def get_balance(self, **kwargs) -> List[dict]:
        module = "/0/private/Balance"
            
        response = await self.__prepare_and_handle_request(
            method="post",
            path_extension=module,
            req_params=kwargs,
        )
        
        return response["result"]

    async def get_ticker(self, **kwargs)-> dict[str, str]:
        module = "/0/public/Ticker"
                
        response = await self.__prepare_and_handle_request(
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

    async def __prepare_and_handle_request(self, method: str, path_extension: str, req_params: dict, is_private: bool = True):
        retries_attempted = self.max_retries
        url = self.__ENDPOINT + path_extension

        while True:
            headers = None
            if is_private:
                req_params["nonce"] = self.__get_utc_timestamp_milliseconds()
                headers = self.__generate_headers(req_params, path_extension)
            
            retries_attempted -= 1
            if retries_attempted < 0:
                raise FailedRequestError(
                    request=f'{method} {path_extension}: {req_params}',
                    message='Bad Request. Retries exceeded maximum.',
                    time=dt.utcnow().strftime("%H:%M:%S")
                )

            try:
                # Use aiohttp session for the request
                async with self.session.request(method, url, data=req_params if method == 'post' else None, params=req_params if method == 'get' else None, headers=headers) as raw_response:
                    response_text = await raw_response.text()
                    response = json.loads(response_text)
            
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                if self.force_retry:
                    self.logger.error(f'{e}. Retrying in {self.retry_delay}s... ({retries_attempted} retries left)')
                    await asyncio.sleep(self.retry_delay)
                    continue
                else:
                    raise e
            except JSONDecodeError as e:
                if self.force_retry:
                    self.logger.error(f'JSONDecodeError: {e}. Response: "{response_text}". Retrying...')
                    await asyncio.sleep(self.retry_delay)
                    continue
                else:
                    raise FailedRequestError(
                        request=f'{method} {url}: {req_params}',
                        message='Conflict. Could not decode JSON.',
                        time=dt.utcnow().strftime("%H:%M:%S")
                    )

            if response.get('error'):
                error_list = response['error']
                if error_list:
                    # Handle rate limits and other retryable errors
                    if any(e in self.retry_codes for e in error_list):
                        self.logger.error(f"Retryable error: {error_list}. Retrying...")
                        await asyncio.sleep(self.retry_delay) # Simple delay, can be enhanced
                        continue
                    else:
                        raise InvalidRequestError(
                            request=f'{method} {url}: {req_params}',
                            message=str(error_list),
                            time=dt.utcnow().strftime("%H:%M:%S")
                        )
            else:
                return response
