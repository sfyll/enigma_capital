import os
import logging
from logging import Logger
from typing import Optional

from infrastructure.api_secret_getter import ApiSecretGetter, ApiMetaData

class RunnerBase:
        def __init__(self, logger: Optional[Logger] = None) -> None:
            self.base_path = self.get_base_path()
            self.logger = logging.getLogger(__name__) if not logger else logger
        
        @staticmethod
        def get_base_path():
            current_directory = os.path.dirname(__file__)
            return os.path.abspath(os.path.join(current_directory, '..'))

        @staticmethod
        def get_secrets(path: str, password: str, api_to_get: str) -> ApiMetaData:
            return ApiSecretGetter.get_api_meta_data(path, password, api_to_get)
