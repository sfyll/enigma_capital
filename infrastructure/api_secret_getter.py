import dataclasses
from getpass import getpass
import json
from typing import Dict, Optional

import utilities.encryptor as encryptor


@dataclasses.dataclass(init=True, eq=True, repr=True)
class ApiMetaData:
    key: str
    secret: str
    other_fields: Optional[dict]


class ApiSecretGetter:
    def __init__(self):
        pass    

    @staticmethod
    def get_api_meta_data(path: str, pwd: str) -> ApiMetaData:
        api_meta_data: dict = {}
        try:
            key = encryptor.get_key_from_current_file(path)
            encrypted_meta_data = encryptor.get_encrypted_meta_data(path)
        except FileNotFoundError or NotADirectoryError as e:
            raise e
        with key.unlock(pwd):
            meta_data: str = encryptor.pgpy_decrypt(key, encrypted_meta_data).replace('\'', '\"')
            meta_data_dict: dict =  json.loads(meta_data)
            for key in meta_data_dict:
                api_meta_data[key] = ApiMetaData(
                    key=meta_data_dict[key]["Key"],
                    secret=meta_data_dict[key]["Secret"],
                    other_fields=meta_data_dict[key]["Other_fields"] if "Other_fields" in meta_data_dict[key] else None
                )
        return api_meta_data

    @staticmethod
    def get_gsheet_meta_data(path: str, pwd: str) -> dict:
        try:
            key = encryptor.get_key_from_current_file(path)
            encrypted_meta_data = encryptor.get_encrypted_gsheet_meta_data(path)
        except FileNotFoundError or NotADirectoryError as e:
            raise e
        with key.unlock(pwd):
            meta_data: str = encryptor.pgpy_decrypt(key, encrypted_meta_data).replace('\'', '\"')
            return json.loads(meta_data)
