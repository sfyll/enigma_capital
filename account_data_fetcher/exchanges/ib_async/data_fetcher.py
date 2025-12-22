import asyncio
import aiohttp
from ib_insync import *

from account_data_fetcher.exchanges.exchange_base import ExchangeBase
from infrastructure.api_secret_getter import ApiMetaData


class DataFetcher(ExchangeBase):
    __EXCHANGE = "IB"
    __GATEWAY_V = "10.20"
    __GATEWAY_PORT = 4001
    __TWS_PORT = 7496
    __HOST = "127.0.0.1"

    def __init__(self, secrets: ApiMetaData, session: aiohttp.ClientSession, app: str = "GATEWAY"):
        super().__init__(exchange=self.__EXCHANGE, session=session)
        self._secrets = secrets
        self.__on_start(app)
        self.netliq: float | None = None

    def __on_start(self, app: str) -> None:
        if app == "TWS":
            self.__initialize_client_and_watchdog(self.__GATEWAY_V, False, self.__HOST, self.__TWS_PORT)
        elif app == "GATEWAY":
            self.__initialize_client_and_watchdog(self.__GATEWAY_V, True, self.__HOST, self.__GATEWAY_PORT)
        else:
            raise Exception("Unkown app")
        self.watchdog.start()
        self.ib.run()

    def __initialize_client_and_watchdog(self, gateway_v: str, is_gateway: bool, host: str, port: int) -> None:
        ibc = IBC(
            gateway_v,
            gateway=is_gateway,
            tradingMode="live",
            userid=self._secrets.key,
            password=self._secrets.secret,
            ibcIni="/opt/ibc/config.ini",
        )
        self.ib = IB()
        self.ib.accountValueEvent += self.__account_value_event
        self.watchdog = Watchdog(ibc, self.ib, host, port, readonly=True)

    def __account_value_event(self, account_value: AccountValue) -> None:
        if account_value.tag == "NetLiquidation":
            self.netliq = float(account_value.value)

    def is_connected(self) -> bool:
        return self.ib.isConnected()

    def _fetch_balance_sync(self) -> float:
        while self.netliq is None:
            self.ib.sleep(1)
        return self.netliq

    async def fetch_balance(self) -> float:
        return await asyncio.to_thread(self._fetch_balance_sync)

    # TODO: Fetch positions
    async def fetch_positions(self) -> float:
        raise NotImplementedError


if __name__ == "__main__":
    import asyncio
    import os
    from getpass import getpass

    from account_data_fetcher.launcher.runner import Runner

    async def _main():
        pwd = getpass("provide password for pk:")
        runner = Runner(pwd)
        async with aiohttp.ClientSession() as session:
            executor = DataFetcher(runner.secrets_per_process["ib"], session, app="GATEWAY")
            print(await executor.fetch_balance())

    asyncio.run(_main())
