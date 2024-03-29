
import argparse
from importlib import import_module
import json

from account_data_fetcher.launcher.process_factory_base import ProcessFactoryBase
from infrastructure.api_secret_getter import ApiMetaData


class ProcessFactory(ProcessFactoryBase):
    __FILE_PREFIX = "account_data_fetcher.exchanges"
    _STANDARDIZED_CLASS_NAME = "DataFetcher" 
    __STANDARDIZED_FILE_NAME = "data_fetcher" 

    @classmethod
    def get_module_name(cls, process_name):
        """
        Gets the module name for a given process name.

        Args:
            process_name (str): Name of the process.
            
        Returns:
            str: The full module name.
        """
        return f"{cls.__FILE_PREFIX}.{process_name.lower()}.{cls.__STANDARDIZED_FILE_NAME}"

    @classmethod
    def main(cls):
        """
        Main method to launch the CLI parser and execute the process.

        Note:
            Uses argparse for parsing CLI arguments. Deserializes JSON strings to dicts.
            Converts the dictionary to SecretsDataClass and replaces in kwargs_dict.
            Arguments of interest will get picked-up in launch_process_and_run_request_processor().
        """
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


        cls.launch_process_and_run_request_processor(args.exchange_name, **args_dict, **kwargs_dict)

if __name__ == "__main__":
    ProcessFactory.main()