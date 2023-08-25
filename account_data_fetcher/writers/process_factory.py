
import argparse
import json

from account_data_fetcher.launcher.process_factory_base import ProcessFactoryBase
from infrastructure.api_secret_getter import ApiMetaData


class ProcessFactory(ProcessFactoryBase):
    __FILE_PREFIX = "account_data_fetcher.exchanges"
    __STANDARDIZED_FILE_NAME = "data_fetcher" 

    @classmethod
    def get_module_name(cls, exchange_name):
        return f"{cls.__FILE_PREFIX}.{exchange_name.lower()}.{cls.__STANDARDIZED_FILE_NAME}"

    @classmethod
    def main(cls):
        parser = argparse.ArgumentParser()
        parser.add_argument("--writer_name", required=True)
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