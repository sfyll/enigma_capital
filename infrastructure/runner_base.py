import asyncio
import functools
import os
import logging
import traceback
import signal
from logging import Logger
from typing import Callable, Optional

from infrastructure.api_secret_getter import ApiSecretGetter, ApiMetaData

class RunnerBase:
        def __init__(self, logger: Optional[Logger] = None) -> None:
            self.path = os.path.realpath(os.path.dirname(__file__))
            self.logger = logging.getLogger(__name__) if not logger else logger
            self.loop = asyncio.get_event_loop()
            for signame in {'SIGINT', 'SIGTERM'}:
                self.loop.add_signal_handler(
                getattr(signal, signame),
                functools.partial(self.ask_exit, signame, self.loop, self.logger))
            
        @staticmethod
        def ask_exit(signame, loop, logger) -> None:
            logger.info("got signal %s: exit" % signame)
            loop.stop()

        @staticmethod
        def get_secrets(path: str, password: str, api_to_get: str) -> ApiMetaData:
            return ApiSecretGetter.get_api_meta_data(path, password, api_to_get)
        
        def create_task(self, function: Callable, *args) -> asyncio.Task:
            return self.loop.create_task(function(*args))

        def run_task(self) -> None:
            try:
                self.loop.run_forever()
            except asyncio.CancelledError:
                pass

        def handle_exceptions(self, task: asyncio.Task):
            if task.cancelled():
                return
            if task.exception():
                e = task.exception()
                self.logger.error(f"An error occurred: {e}")
                self.logger.error("".join(traceback.format_tb(e.__traceback__)))