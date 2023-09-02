import dataclasses
import logging
from time import sleep
import os
from typing import Dict, Optional

from dependencies.dydxv3python.dydx3 import Client
from dependencies.dydxv3python.dydx3.constants import API_HOST_MAINNET
from dependencies.dydxv3python.dydx3.constants import NETWORK_ID_MAINNET

from account_data_fetcher.exchanges.exchange_base import ExchangeBase
from infrastructure.api_secret_getter import ApiMetaData

@dataclasses.dataclass(init=True, eq=True, repr=True)
class openPositions:
    market: str
    status: str
    side: str
    size: str
    maxSize: str
    entryPrice: str
    exitPrice: Optional[str]
    unrealizedPnl: str
    realizedPnl: str
    createdAt: str
    closedAt: Optional[str]
    sumOpen: str
    sumClose: str
    netFunding: str

@dataclasses.dataclass(init=True, eq=True, repr=True)
class marketData:
    market: str
    status: str
    baseAsset: str
    quoteAsset: str
    stepSize: str
    tickSize: str
    indexPrice: str
    oraclePrice: str
    priceChange24H: str
    nextFundingRate: str
    nextFundingAt: str
    minOrderSize: str
    type: str
    initialMarginFraction: str
    maintenanceMarginFraction: str
    transferMarginFraction: str
    volume24H: str
    trades24H: str
    openInterest: str
    incrementalInitialMarginFraction: str
    incrementalPositionSize: str
    maxPositionSize: str
    baselinePositionSize: str
    assetResolution: str
    syntheticAssetId: str

@dataclasses.dataclass(init=True, eq=True, repr=True)
class marginRequirements:
    initialMarginRequirements: float
    maintenanceMarginRequirements: float


#TODO: Fix arbitrary ConnectionResetError bug requests.exceptions.ConnectionError: ('Connection aborted.', ConnectionResetError(104, 'Connection reset by peer'))
class DataFetcher(ExchangeBase):
    _EXCHANGE = "DYDX"
    def __init__(self, secrets: ApiMetaData, port_number: int) -> None:
        super().__init__(port_number, self._EXCHANGE)
        self.logger = logging.getLogger(__name__) 
        self.client = Client(
            network_id=NETWORK_ID_MAINNET,
            host=API_HOST_MAINNET,
            api_key_credentials={
                    'secret': secrets.secret,
                    'key': secrets.key,
                    'passphrase': secrets.other_fields["Passphrase"],
                }
        )

    def get_account_info(self) -> Optional[dict]:
        data: Optional[dict] = None
        counter: int = 0
        while not data:
            if counter <= 20:
                counter +=1
                try:
                    return self.client.private.get_accounts().data
                except ConnectionError as e:
                    self.logger.debug(e)
                    sleep(5)
                    return self.client.private.get_accounts().data
            else:
                raise Exception("Kept on getting ConnectionResetErrors")

    def fetch_balance(self) -> float:
        account_info = self.get_account_info()
        return float(account_info["accounts"][0]["equity"])

    def fetch_positions(self) -> dict:
        open_positions: Dict[str, openPositions] = {}
        account_info = self.get_account_info()["accounts"][0]
        open_positions_meta_data = account_info["openPositions"]
        market_meta_data: dict[marketData] = self.get_markets()["markets"]
        market_data: dict[str, marketData] = {}

        for market in open_positions_meta_data:
            open_positions[market] = openPositions(**open_positions_meta_data[market])
            market_data[market] = marketData(**market_meta_data[market])

        data_to_return = {
            "Symbol": [],
            "Multiplier": [],
            "Quantity": [],
            "Dollar Quantity": []
        }

        for _, value in open_positions.items():
            data_to_return["Symbol"].append(value.market)
            data_to_return["Multiplier"].append(1)
            data_to_return["Quantity"].append(value.size)
            data_to_return["Dollar Quantity"].append(round(float(value.size) * float(market_data[value.market].indexPrice),3))
        
        return data_to_return

    def get_markets(self) -> dict:
        markets: Optional[dict] = None
        counter: int = 0
        while not markets:
            if counter <= 20:
                counter +=1
                try:
                    return self.client.public.get_markets().data
                except ConnectionError as e:
                    self.logger.debug(e)
                    sleep(5)
            else:
                raise Exception("Kept on getting ConnectionResetErrors")

            

if __name__ == "__main__":
    from getpass import getpass
    log_filename= os.path.expanduser("~") + "/log/test.py"
    numeric_level = getattr(logging, "DEBUG", None)
    logging.basicConfig(level=numeric_level, format='%(levelname)s:%(asctime)s:%(message)s', filename=log_filename) 
    logger: logging.Logger = logging.getLogger()
    pwd = getpass("provide password for pk:")
    current_path = os.path.realpath(os.path.dirname(__file__))
    executor = dydxDataFetcher(current_path, pwd, logger)
    # balances = executor.get_account_info()
    for i in range(30):
        balances = executor.get_positions()
        print(f"{balances=}")
        sleep(1)
