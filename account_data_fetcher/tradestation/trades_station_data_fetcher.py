import dataclasses
import logging
import os
import time
import json
from typing import Dict, List, Optional

import pgpy
import requests
import urllib.parse

from utilities.account_data_fetcher_base import accountFetcherBase
from utilities.encryptor import get_decrypted_ts_state, encrypt_and_write_ts_to_file


@dataclasses.dataclass(init=True, eq=True, repr=True)
class AccountMetaData:
    AccountID: str
    AccountType: str
    CashBalance: float
    Equity: float
    MarketValue: float
    TodaysProfitLoss: float
    UnclearedDeposit: float

"""https://github.com/areed1192/tradestation-python-api/blob/master/ts/client.py"""
class tradesStationDataFetcher(accountFetcherBase):
    __AUTH_ENDPOINT = "https://signin.tradestation.com/oauth/token"
    __REDIRECT_URI = "http://localhost:3000/"
    __RESOURCE = "https://api.tradestation.com"
    __EXCHANGE = "TradeStation"
    __API_VERSION = "v3"
    __PAPER_RESOURCE = "https://sim-api.tradestation.com"

    def __init__(self, path: str, password: str, paper_trading = False,
                cache_state = True, refresh_enabled = True) -> None:
        print(path)
        super().__init__(path, password)
        self.config = {
            'client_id': self.api_meta_data[self.__EXCHANGE].key,
            'client_secret': self.api_meta_data[self.__EXCHANGE].secret,
            'username': self.api_meta_data[self.__EXCHANGE].other_fields["Username"],
            'redirect_uri': self.__REDIRECT_URI,
            'resource': self.__RESOURCE,
            'paper_resource': self.__PAPER_RESOURCE,
            'api_version': self.__API_VERSION,
            'paper_api_version': self.__API_VERSION,
            'auth_endpoint': self.__AUTH_ENDPOINT,
            'cache_state': cache_state,
            'refresh_enabled': refresh_enabled,
            'paper_trading': paper_trading
        }

        self.logger = logging.getLogger(__name__)

        self.account_meta_data: Dict[str, AccountMetaData] = {}

        self.decryption_password: str = password
        self.key, _ = pgpy.PGPKey.from_file(path + "/.pk.txt")

        # initalize the client to either use paper trading account or regular account.
        if self.config['paper_trading']:
            self.paper_trading_mode = True
        else:
            self.paper_trading_mode = False

        # call the _state_manager method and update the state to init (initalized)
        self._state_manager('init')

        # define a new attribute called 'authstate' and initalize it to '' (Blank). This will be used by our login function.
        self.authstate = False

        self.login()

    def __repr__(self) -> str:
        """Defines the string representation of our TD Ameritrade Class instance.
        Returns:
        ----
        (str): A string representation of the client.
        """

        # Define the string representation.
        str_representation = '<TradeStation Client (logged_in={log_in}, authorized={auth_state})>'.format(
            log_in=self.state['logged_in'],
            auth_state=self.authstate
        )

        return str_representation

    def headers(self, mode: str = None) -> Dict:
        """Sets the headers for the request.
        Overview:
        ----
        Returns a dictionary of default HTTP headers for calls to TradeStation API,
        in the headers we defined the Authorization and access token.
        Arguments:
        ----
        mode (str): Defines the content-type for the headers dictionary.
        Returns:
        ----
        (dict): The headers dictionary to be used in the request.
        """

        # Grab the Access Token.
        token = self.state['access_token']

        # Create the headers dictionary
        headers = {
            'Authorization': 'Bearer {access_token}'.format(access_token=token)
        }

        # Set the Mode.
        if mode == 'application/json':
            headers['Content-type'] = 'application/json'
        elif mode == 'chunked':
            headers['Transfer-Encoding'] = 'Chunked'

        return headers

    def _api_endpoint(self, url: str) -> str:
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

        # paper trading uses a different base url compared to regular trading.
        if self.paper_trading_mode:
            full_url = '/'.join([self.config['paper_resource'],
                                 self.config['paper_api_version'], url])
        else:
            full_url = '/'.join([self.config['resource'],
                                 self.config['api_version'], url])

        return full_url

    def _state_manager(self, action: str) -> None:
        """Handles the state.
        Overview:
        ----
        Manages the self.state dictionary. Initalize State will set
        the properties to their default value. Save will save the 
        current state if 'cache_state' is set to TRUE.
        Arguments:
        ----
        name (str): action argument must of one of the following:
            'init' -- Initalize State.
            'save' -- Save the current state.         
        """

        # Grab the current directory of the client file, that way we can store the JSON file in the same folder.
        dir_path = os.path.dirname(os.path.realpath(__file__))
        filename = '.ts_state_enc.txt'
        file_path = os.path.join(dir_path, filename)

        try:
            # Define the initalized state, these are the default values.
            previous_state = get_decrypted_ts_state(file_path, self.decryption_password, self.key)
        except FileNotFoundError:
            # Define the initalized state, these are the default values.
            previous_state = {
                'access_token': None,
                'refresh_token': None,
                'access_token_expires_at': 0,
                'access_token_expires_in': 0,
                'logged_in': False
            }
        
        # If the state is initalized.
        if action == 'init':

            # Initalize the state.
            self.state = previous_state

            # If they allowed for caching and the file exist, load the file.
            if self.config['cache_state'] and os.path.isfile(file_path):
                self.state.update(previous_state)

            # If they didnt allow for caching delete the file.
            elif not self.config['cache_state'] and os.path.isfile(file_path):
                os.remove(file_path)

        # if they want to save it and have allowed for caching then load the file.
        elif action == 'save' and self.config['cache_state']:
            encrypt_and_write_ts_to_file(file_path, self.state, self.key)

    def login(self) -> bool:
        """Logs the user into a new session.
        Overview:
        ---
        Ask the user to authenticate  themselves via the TD Ameritrade Authentication Portal. This will
        create a URL, display it for the User to go to and request that they paste the final URL into
        command window.
        Once the user is authenticated the API key is valide for 90 days, so refresh tokens may be used
        from this point, up to the 90 days.
        Returns:
        ----
        (bool): `True` if the session was logged in, `False` otherwise.
        """

        # if caching is enabled then attempt silent authentication.
        if self.config['cache_state']:

            # if it was successful, the user is authenticated.
            if self._silent_sso():

                # update the authentication state
                self.authstate = True
                return True

        # Go through the authorization process.
        self._authorize()

        # Grab the access token.
        self._grab_access_token()

        # update the authentication state
        self.authstate = True

        return True

    def logout(self) -> None:
        """Clears the current TradeStation Connection state."""

        # change state to initalized so they will have to either get a
        # new access token or refresh token next time they use the API
        self._state_manager('init')

    def _grab_access_token(self) -> bool:
        """Grabs an access token.
        Overview:
        ----
        Access token handler for AuthCode Workflow. This takes the
        authorization code parsed from the auth endpoint to call the
        token endpoint and obtain an access token.
        Returns:
        ----
        (bool): `True` if grabbing the access token was successful. `False` otherwise.
        """

        # Parse the URL
        url_dict = urllib.parse.parse_qs(self.state['redirect_code'])

        # Convert the values to a list.
        url_values = list(url_dict.values())

        # Grab the Code, which is stored in a list.
        url_code = url_values[0][0]

        # define the parameters of our access token post.
        data = {
            'grant_type': 'authorization_code',
            'client_id': self.config['client_id'],
            'client_secret': self.config['client_secret'],
            'code': url_code,
            'redirect_uri': self.config['redirect_uri']
        }

        # Post the data to the token endpoint and store the response.
        token_response = requests.post(
            url=self.config['auth_endpoint'],
            data=data,
            verify=True
        )

        # Call the `_token_save` method to save the access token.
        if token_response.ok:
            self._token_save(response=token_response)
            return True
        else:
            return False

    def _silent_sso(self) -> bool:
        """Handles the silent authentication workflow.
        Overview:
        ----
        Attempt a silent authentication, by checking whether current access token
        is valid and/or attempting to refresh it. Returns True if we have successfully 
        stored a valid access token.
        Returns:
        ----
        (bool): `True` if grabbing the silent authentication was successful. `False` otherwise.
        """

        # if it's not expired we don't care.
        if self._token_validation():
            return True

        # if the current access token is expired then try and refresh access token.
        elif self.state['refresh_token'] and self._grab_refresh_token():
            return True

        # More than likely a first time login, so can't do silent authenticaiton.
        else:
            return False

    def _grab_refresh_token(self) -> bool:
        """Refreshes the current access token if it's expired.
        Returns:
        ----
        (bool): `True` if grabbing the refresh token was successful. `False` otherwise.
        """

        # Build the parameters of our request.
        data = {
            'client_id': self.config['client_id'],
            'client_secret': self.config['client_secret'],
            'grant_type': 'refresh_token',
            'response_type': 'token',
            'refresh_token': self.state['refresh_token']
        }

        # Make a post request to the token endpoint.
        response = requests.post(
            url=self.config['auth_endpoint'],
            data=data,
            verify=True
        )

        # Save the token if the response was okay.
        if response.ok:
            self._token_save(response=response)
            return True
        else:
            return False

    def _token_save(self, response: requests.Response):
        """Saves an access token or refresh token.
        Overview:
        ----
        Parses an access token from the response of a POST request and saves it
        in the state dictionary for future use. Additionally, it will store the
        expiration time and the refresh token.
        Arguments:
        ----
        response (requests.Response): A response object recieved from the `token_refresh` or `_grab_access_token`
            methods.
        Returns:
        ----
        (bool): `True` if saving the token was successful. `False` otherwise.
        """

        # Parse the data.
        json_data = response.json()

        # Save the access token.
        if 'access_token' in json_data:
            self.state['access_token'] = json_data['access_token']
        else:
            self.logout()
            return False

        # If there is a refresh token then grab it.
        if 'refresh_token' in json_data:
            self.state['refresh_token'] = json_data['refresh_token']

        # Set the login state.
        self.state['logged_in'] = True

        # Store token expiration time.
        self.state['access_token_expires_in'] = json_data['expires_in']
        self.state['access_token_expires_at'] = time.time() + \
            int(json_data['expires_in'])

        self._state_manager('save')

        return True

    def _token_seconds(self) -> int:
        """Calculates when the token will expire.
        Overview:
        ----
        Return the number of seconds until the current access token or refresh token
        will expire. The default value is access token because this is the most commonly used
        token during requests.
        Returns:
        ----
        (int): The number of seconds till expiration
        """

        # Calculate the token expire time.
        token_exp = time.time() >= self.state['access_token_expires_at']

        # if the time to expiration is less than or equal to 0, return 0.
        if not self.state['refresh_token'] or token_exp:
            token_exp = 0
        else:
            token_exp = int(token_exp)

        return token_exp

    def _token_validation(self, nseconds: int = 5) -> None:
        """Validates the Access Token.
        Overview:
        ----
        Verify the current access token is valid for at least N seconds, and
        if not then attempt to refresh it. Can be used to assure a valid token
        before making a call to the Tradestation API.
        Arguments:
        ----
        nseconds (int): The minimum number of seconds the token has to be valid for before
            attempting to get a refresh token.
        """

        if self._token_seconds() < nseconds and self.config['refresh_enabled']:
            self._grab_refresh_token()

    def _authorize(self) -> None:
        """Authorizes the session.
        Overview:
        ----
        Initalizes the oAuth Workflow by creating the URL that
        allows the user to login to the Tradestation API using their credentials
        and then will parse the URL that they paste back into the terminal.
        """

        # prepare the payload to login
        data = {
            'response_type': 'code',
            'client_id': self.config['client_id'],
            'audience':'https://api.tradestation.com',
            'redirect_uri': self.config['redirect_uri'],
            "scope":"MarketData ReadAccount Crypto openid offline_access"
        }

        # url encode the data.
        params = urllib.parse.urlencode(data, safe="/:")
        # build the full URL for the authentication endpoint.
        url = 'https://signin.tradestation.com/authorize?' + params

        # aks the user to go to the URL provided, they will be prompted to authenticate themsevles.
        print('')
        print('='*80)
        print('')
        print('Please go to URL provided authorize your account: {}'.format(url))
        print('')
        print('-'*80)

        # ask the user to take the final URL after authentication and paste here so we can parse.
        my_response = input('Paste the full URL redirect here: ')

        # store the redirect URL
        self.state['redirect_code'] = my_response

    def _handle_requests(self, url: str, method: str, headers: dict = {}, args: dict = None, stream: bool = False, payload: dict = None) -> dict:
        """[summary]
        Arguments:
        ----
        url (str): [description]
        method (str): [description]
        headers (dict): [description]
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

        streamed_content = []
        if method == 'get':

            # handles the non-streaming GET requests.
            if stream == False:
                response = requests.get(
                    url=url, headers=headers, params=args, verify=True)

            # handles the Streaming request.
            else:
                response = requests.get(
                    url=url, headers=headers, params=args, verify=True, stream=True)
                for line in response.iter_lines(chunk_size=300):

                    if 'END' not in line.decode() and line.decode() != '':
                        try:
                            streamed_content.append(json.loads(line))
                        except:
                            print(line)

        elif method == 'post':

            if payload is None:
                response = requests.post(
                    url=url, headers=headers, params=args, verify=True)
            else:
                response = requests.post(
                    url=url, headers=headers, params=args, verify=True, json=payload)

        elif method == 'put':

            if payload is None:
                response = requests.put(
                    url=url, headers=headers, params=args, verify=True)
            else:
                response = requests.put(
                    url=url, headers=headers, params=args, verify=True, json=payload)

        elif method == 'delete':

            response = requests.delete(
                url=url, headers=headers, params=args, verify=True)

        else:
            raise ValueError(
                'The type of request you are making is incorrect.')

        # grab the status code
        status_code = response.status_code

        # grab the response. headers.
        response_headers = response.headers

        if status_code == 200:

            if response_headers['Content-Type'] in ['application/json', 'charset=utf-8']:
                return response.json()
            elif response_headers['Transfer-Encoding'] == 'chunked':

                return streamed_content

        else:
            # Error
            print('')
            print('-'*80)
            print("BAD REQUEST - STATUS CODE: {}".format(status_code))
            print("RESPONSE URL: {}".format(response.url))
            print("RESPONSE HEADERS: {}".format(response.headers))
            print("RESPONSE TEXT: {}".format(response.text))
            print('-'*80)
            print('')
            
    def user_info(self) -> dict:

        # validate the token.
        self._token_validation()

        url = "https://signin.tradestation.com/userinfo"

        response = self._handle_requests(
                   url=url,
                   method="get",
                   headers=self.headers()
        )

        return response


    def get_user_accounts(self) -> dict:
        """Grabs all the accounts associated with the User.
        Arguments:
        ----
        user_id (str): The Username of the account holder.
        Returns:
        ----
        (dict): All the user accounts.
        """

        # validate the token.
        self._token_validation()

        # define the endpoint.
        url_endpoint = self._api_endpoint(url='brokerage/accounts')

        # grab the response.
        response = self._handle_requests(
            url=url_endpoint,
            method='get',
            headers=self.headers()
        )

        for dictionnary in response["Accounts"]:
            self.account_meta_data[dictionnary["AccountID"]] = AccountMetaData(
                AccountID=dictionnary["AccountID"],
                AccountType=dictionnary["AccountType"],
                CashBalance=0.0,
                Equity=0.0,
                MarketValue=0.0,
                TodaysProfitLoss=0.0,
                UnclearedDeposit=0.0
            )

        return response

    def account_balances(self) -> dict:
        """Grabs all the balances for each account provided.
        Args:
        ----
        account_keys (List[str]): A list of account numbers. Can only be a max
            of 25 account numbers
        Raises:
        ----
        ValueError: If the list is more than 25 account numbers will raise an error.
        Returns:
        ----
        dict: A list of account balances for each of the accounts.
        """


        # validate the token.
        self._token_validation()


        if not self.account_meta_data:
            self.get_user_accounts()
        
        account_keys = [key for key, _ in self.account_meta_data.items()]

        # argument validation
        if not account_keys:
            raise ValueError(
                "Non existing list")
        elif len(account_keys) == 0:
            raise ValueError(
                "You cannot pass through an empty list for account keys.")
        elif len(account_keys) > 0 and len(account_keys) <= 25:
            account_keys = ','.join(account_keys)
        elif len(account_keys) > 25:
            raise ValueError(
                "You cannot pass through more than 25 account keys.")

        # define the endpoint.
        url_endpoint = self._api_endpoint(
            url='brokerage/accounts/{account_numbers}/balances'.format(
                account_numbers=account_keys)
        )

        # define the arguments
        params = {
            'access_token': self.state['access_token']
        }

        # grab the response.
        response = self._handle_requests(
            url=url_endpoint,
            method='get',
            args=params,
            headers=self.headers()
        )


        for balance in response["Balances"]:
            self.account_meta_data[balance["AccountID"]].CashBalance = float(balance["CashBalance"])
            self.account_meta_data[balance["AccountID"]].Equity = float(balance["Equity"])
            self.account_meta_data[balance["AccountID"]].MarketValue = float(balance["MarketValue"])
            self.account_meta_data[balance["AccountID"]].TodaysProfitLoss = float(balance["TodaysProfitLoss"])
            self.account_meta_data[balance["AccountID"]].UnclearedDeposit = float(balance["UnclearedDeposit"])

        return response

    def account_wallets(self) -> dict:
        """Grabs all the crypto wallets for each account provided.
        Args:
        ----
        account_keys (List[str]): A list of account numbers. Can only be a max
            of 25 account numbers
        Raises:
        ----
        ValueError: If the list is more than 25 account numbers will raise an error.
        Returns:
        ----
        dict: update self.account_meta_data but takes into consideration only USD balances
        """


        # validate the token.
        self._token_validation()


        if not self.account_meta_data:
            self.get_user_accounts()
        
        account_keys = [key for key, _ in self.account_meta_data.items() if self.account_meta_data[key].AccountType == "Crypto"]

        # argument validation
        if not account_keys:
            raise ValueError(
                "Non existing list")
        elif len(account_keys) == 0:
            raise ValueError(
                "You cannot pass through an empty list for account keys.")
        elif len(account_keys) > 0 and len(account_keys) <= 25:
            account_keys = ','.join(account_keys)
        elif len(account_keys) > 25:
            raise ValueError(
                "You cannot pass through more than 25 account keys.")

        # define the endpoint.
        url_endpoint = self._api_endpoint(
            url='brokerage/accounts/{account_numbers}/wallets'.format(
                account_numbers=account_keys)
        )

        # define the arguments
        params = {
            'access_token': self.state['access_token']
        }

        # grab the response.
        response = self._handle_requests(
            url=url_endpoint,
            method='get',
            args=params,
            headers=self.headers()
        )

        for wallet in response["Wallets"]:
            if "USD" in wallet["Currency"] == "USDC":
                self.account_meta_data[wallet["AccountID"]].CashBalance += float(wallet["Balance"])
                self.account_meta_data[wallet["AccountID"]].Equity += float(wallet["Balance"])
                self.account_meta_data[wallet["AccountID"]].MarketValue += float(wallet["Balance"])
                self.account_meta_data[wallet["AccountID"]].TodaysProfitLoss += float(wallet["UnrealizedProfitLossAccountCurrency"])

        return response

    def get_sum_of_balance(self) -> int:
        self.account_balances()
        self.account_wallets()
        account_total_balance: float = 0.0
        
        for account_id in self.account_meta_data:
            if self.account_meta_data[account_id].Equity:
                account_total_balance += self.account_meta_data[account_id].Equity
        
        return round(account_total_balance,3)


    def account_positions(self, account_keys: Optional[List[str]] = None, symbols: Optional[List[str]] = None) -> dict:
        """Grabs all the account positions.
        Arguments:
        ----
        account_keys (List[str]): A list of account numbers..
        symbols (List[str]): A list of ticker symbols, you want to return.
        Raises:
        ----
        ValueError: If the list is more than 25 account numbers will raise an error.
        Returns:
        ----
        dict: A list of account balances for each of the accounts.
        """

        # validate the token.
        self._token_validation()

        if not self.account_meta_data:
            self.get_user_accounts()
        
        if not account_keys:
            account_keys = [key for key, _ in self.account_meta_data.items()]


        # argument validation, account keys.
        if len(account_keys) == 0:
            raise ValueError(
                "You cannot pass through an empty list for account keys.")
        elif len(account_keys) > 0 and len(account_keys) <= 25:
            account_keys = ','.join(account_keys)
        elif len(account_keys) > 25:
            raise ValueError(
                "You cannot pass through more than 25 account keys.")

        # argument validation, symbols.
        if symbols is not None:

            if len(symbols) == 0:
                raise ValueError(
                    "You cannot pass through an empty symbols list for the filter.")
            else:

                symbols_formatted = []
                for symbol in symbols:
                    symbols_formatted.append(
                        "Symbol eq '{}'".format(symbol)
                    )

                symbols = 'or '.join(symbols_formatted)
                params = {
                    'access_token': self.state['access_token'],
                    '$filter': symbols
                }

        else:
            params = {
                'access_token': self.state['access_token']
            }

        # define the endpoint.
        url_endpoint = self._api_endpoint(
            url='brokerage/accounts/{account_numbers}/positions'.format(
                account_numbers=account_keys
            )
        )

        # grab the response.
        response = self._handle_requests(
            url=url_endpoint,
            method='get',
            args=params,
            headers=self.headers()
        )

        return response

    def get_formated_positions(self):

        positions: dict = self.account_positions()

        data_to_return = {
            "Symbol": [],
            "Multiplier": [],
            "Quantity": [],
            "Dollar Quantity": []
        }

        for position in positions["Positions"]:
            data_to_return["Symbol"].append(position["Symbol"])
            #extrapolate multiplier
            multiplier = float(position["MarketValue"]) / abs(int(position["Quantity"])) / ((float(position["Ask"]) + float(position['Bid'])) / 2)
            data_to_return["Multiplier"].append(round(multiplier))
            data_to_return["Quantity"].append(int(position["Quantity"]))
            data_to_return["Dollar Quantity"].append(round(float(position["MarketValue"]),3))
        
        return data_to_return


if __name__ == '__main__':
    from getpass import getpass
    pwd = getpass("provide password for pk:")
    current_path = os.path.realpath(os.path.dirname(__file__))
    executor = tradesStationDataFetcher(current_path, pwd)
    balances = executor.get_sum_of_balance()
    print(balances)

