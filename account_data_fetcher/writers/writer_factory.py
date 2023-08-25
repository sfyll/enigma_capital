import argparse
from importlib import import_module
import inspect
import json

from account_data_fetcher.writers.writer_base import WriterBase
from infrastructure.api_secret_getter import ApiMetaData


class WriterFactory:
    __FILE_PREFIX = "account_data_fetcher.writers"
    __STANDARDIZED_FILE_NAME = "writer" 
    __STANDARDIZED_CLASS_NAME = "Writer" 
    
    @classmethod
    def get_writer_class(cls, writer_name: str) -> WriterBase:
        
        module_name = cls.get_module_name(writer_name)
        
        try:
            writer_module = import_module(module_name)
            return getattr(writer_module, cls.__STANDARDIZED_CLASS_NAME)
        except (ImportError, AttributeError, ModuleNotFoundError) as e:
            raise Exception(f"{e=}")

    @classmethod
    def get_module_name(cls, writer_name):
        return f"{cls.__FILE_PREFIX}.{writer_name.lower()}.{cls.__STANDARDIZED_FILE_NAME}"

    @classmethod
    def launch_writer_and_run_request_processor(cls, writer_name: str, *args, **kwargs) -> None:
        writer_instance: WriterBase = cls.get_writer_class(writer_name)

        signature = inspect.signature(writer_instance.__init__)
        filtered_kwargs = {key: value for key, value in kwargs.items() if key in signature.parameters}
        
        launched_instance: WriterBase = writer_instance(*args, **filtered_kwargs)

        launched_instance.process_request()

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


        cls.launch_writer_and_run_request_processor(args.writer_name, **args_dict, **kwargs_dict)

if __name__ == "__main__":
    WriterFactory.main()