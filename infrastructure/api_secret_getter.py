import dataclasses
from getpass import getpass
import json
from typing import Dict, Optional

import utilities.encryptor as encryptor


@dataclasses.dataclass(init=True, eq=True, repr=True)
class ApiMetaData:
    """
    Dataclass for storing API metadata.
    
    Attributes:
        key (str): API key.
        secret (str): API secret.
        other_fields (Optional[dict]): Optional dictionary to store additional fields.
    """
    key: str
    secret: str
    other_fields: Optional[dict]


class ApiSecretGetter:
    """
    A class for retrieving encrypted API metadata.

    This class provides methods to decrypt API metadata from
    given files using a provided password. Supports retrieval
    of API metadata for generic APIs and Google Sheets.
    """
    def __init__(self):
        pass    

    @staticmethod
    def get_api_meta_data(path: str, pwd: str) -> ApiMetaData:
        """
        Retrieves and decrypts API metadata from a file.

        Args:
            path (str): Path to the encrypted metadata file.
            pwd (str): Password for decrypting the metadata.

        Returns:
            ApiMetaData: A dataclass instance containing the decrypted API metadata.

        Raises:
            FileNotFoundError, NotADirectoryError: Raises if the provided file path is invalid.
        """
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
    def get_gsheet_meta_data(path: str, pwd: str) -> ApiMetaData:
        """
        Retrieves and decrypts Google Sheets API metadata from a file.

        Args:
            path (str): Path to the encrypted metadata file.
            pwd (str): Password for decrypting the metadata.

        Returns:
            ApiMetaData: A dataclass instance containing the decrypted API metadata for Google Sheets.

        Raises:
            FileNotFoundError, NotADirectoryError: Raises if the provided file path is invalid.

        Notes:
            - The 'key' and 'secret' fields in ApiMetaData will be empty as they are not applicable for Google Sheets.
        """
        try:
            key = encryptor.get_key_from_current_file(path)
            encrypted_meta_data = encryptor.get_encrypted_gsheet_meta_data(path)
        except FileNotFoundError or NotADirectoryError as e:
            raise e
        with key.unlock(pwd):
            meta_data: str = encryptor.pgpy_decrypt(key, encrypted_meta_data).replace('\'', '\"')
            return ApiMetaData(
                key="",
                secret="",
                other_fields=json.loads(meta_data))
