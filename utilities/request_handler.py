import logging
from typing import Optional, Union

import requests

class requestHandler:
    def ___init__(self) -> None:
        self.logger = logging.getLogger(__name__)

    def endpoint_extension(self, url_base: str, url_extension: str = "") -> str:
        return "/".join([url_base, url_extension])

    def api_module(self, url_base: str, url_extension: str = "") -> str:
        """Creates an API URL.
        Overview:
        ----  
        Convert relative endpoint (e.g., 'quotes') to full API endpoint.
        Arguments:
        ----
        url (str): The URL that needs conversion to a full endpoint URL.
        Returns:
        ---
        (str): A full URL.
        """

        return '?'.join([url_base, url_extension])

    def handle_requests(self, url: str, method: str, args: dict = None, headers: Optional[dict] = None, 
                        raw_response: bool = False) -> Union[requests.Response, dict]:
        """[summary]
        Arguments:
        ----
        url (str): [description]
        method (str): [description]
        args (dict, optional): [description]. Defaults to None.
        stream (bool, optional): [description]. Defaults to False.
        payload (dict, optional): [description]. Defaults to None.
        Raises:
        ----
        ValueError: [description]
        Returns:
        ----
        dict: [description]
        """

        if method == 'get':
            if headers:
                response = requests.get(
                url=url, params=args, verify=True, headers=headers)
            else:
                response = requests.get(
                url=url, params=args, verify=True)

        elif method == 'post':
            if headers:
                response = requests.post(
                url=url, data=args, verify=True, headers=headers)
            else:
                response = requests.post(
                    url=url, data=args, verify=True)

        elif method == 'put':

            response = requests.put(
                url=url, params=args, verify=True)

        elif method == 'delete':

            response = requests.delete(
                url=url, params=args, verify=True)

        else:
            raise ValueError(
                'The type of request you are making is incorrect.')

        if raw_response:
            return response
        
        else:
            # grab the status code
            status_code = response.status_code

            # grab the response. headers.
            response_headers = response.headers

            if status_code == 200:

                if response_headers['Content-Type'] in ['application/json', 'charset=utf-8', "application/json; charset=utf-8"]:
                    return response.json()
                else:
                    raise Exception("unhandled response type")