import logging
from typing import Optional

from ib_insync import *

from utilities.account_data_fetcher_base import accountFetcherBase


class InteractiveBrokersAppAsync(accountFetcherBase):
    __EXCHANGE = "IB"
    __GATEWAY_V = "10.20"
    __GATEWAY_PORT = 4001
    __TWS_PORT = 7496
    __HOST = "127.0.0.1"
    def __init__(self, path: str, password: str, app: str = "GATEWAY"):
        super().__init__(path, password)
        self.logger = logging.getLogger(__name__)
        self.__on_start(app)
        self.netliq: Optional[float] = None

    def __on_start(self, app : str) -> None:
        if app == "TWS":
            self.__initialize_client_and_watchdog(self.__GATEWAY_V, False, self.__HOST, self.__TWS_PORT)
        elif app == "GATEWAY":
            self.__initialize_client_and_watchdog(self.__GATEWAY_V, True, self.__HOST, self.__GATEWAY_PORT)
        else:
            raise Exception("Unkown app")
        self.watchdog.start()
        self.ib.run()

    def __initialize_client_and_watchdog(self, gateway_v: str, is_gateway:bool, host: str, port: int) -> None:
        ibc = IBC(gateway_v, gateway=is_gateway, tradingMode='live', userid=self.api_meta_data[self.__EXCHANGE].key,
                        password=self.api_meta_data[self.__EXCHANGE].secret, ibcIni="/opt/ibc/config.ini")
        self.ib = IB()
        self.ib.accountValueEvent += self.__account_value_event
        self.watchdog = Watchdog(ibc, self.ib, host, port, readonly=True)

    def __account_value_event(self, account_value: AccountValue) -> None:
        if account_value.tag == "NetLiquidation":
            self.netliq = float(account_value.value)

    def is_connected(self) -> bool:
        return self.ib.isConnected()

    def get_netliq(self) -> float:
        while self.netliq is None:
            self.ib.sleep(1)
        else:
            return self.netliq


if __name__ == '__main__':
    import os
    path = os.path.realpath(os.path.dirname(__file__))
    pwd = ""
    executor = InteractiveBrokersAppAsync(path, pwd, app="GATEWAY")
    summary = executor.get_netliq()
