
import argparse
from importlib import import_module
import inspect
import json

from account_data_fetcher.exchanges.exchange_base import ExchangeBase
from infrastructure.api_secret_getter import ApiMetaData


class ExchangeFactory:
    __FILE_PREFIX = "account_data_fetcher.exchanges"
    __STANDARDIZED_FILE_NAME = "data_fetcher" 
    __STANDARDIZED_CLASS_NAME = "DataFetcher" 
    
    @classmethod
    def get_exchange_class(cls, exchange_name: str) -> ExchangeBase:
        
        module_name = cls.get_module_name(exchange_name)
        
        try:
            exchange_module = import_module(module_name)
            return getattr(exchange_module, cls.__STANDARDIZED_CLASS_NAME)
        except (ImportError, AttributeError, ModuleNotFoundError) as e:
            raise Exception(f"{e=}")

    @classmethod
    def get_module_name(cls, exchange_name):
        return f"{cls.__FILE_PREFIX}.{exchange_name.lower()}.{cls.__STANDARDIZED_FILE_NAME}"

    @classmethod
    def launch_exchange_and_run_request_processor(cls, exchange_name: str, *args, **kwargs) -> None:
        exchange_instance: ExchangeBase = cls.get_exchange_class(exchange_name)

        signature = inspect.signature(exchange_instance.__init__)
        filtered_kwargs = {key: value for key, value in kwargs.items() if key in signature.parameters}
        
        launched_instance: ExchangeBase = exchange_instance(*args, **filtered_kwargs)

        launched_instance.process_request()

    @classmethod
    def main(cls):
        parser = argparse.ArgumentParser()
        parser.add_argument("--exchange_name", required=True)
        parser.add_argument("--args", default="{}")
        parser.add_argument("--kwargs", default="{}")

        args = parser.parse_args()

        # Deserialize the JSON strings
        args_dict = json.loads(args.args)
        kwargs_dict = json.loads(args.kwargs)

        # Deserialize the secrets JSON string to a dictionary
        secrets_dict = json.loads(kwargs_dict["secrets"])

        # Convert the dictionary to the SecretsDataClass
        secrets_data_class_instance = ApiMetaData(**secrets_dict)

        # Replace the secrets entry in kwargs_dict with the deserialized data class
        kwargs_dict["secrets"] = secrets_data_class_instance


        cls.launch_exchange_and_run_request_processor(args.exchange_name, **args_dict, **kwargs_dict)

if __name__ == "__main__":
    ExchangeFactory.main()