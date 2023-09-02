
import argparse
import json

from account_data_fetcher.launcher.process_factory_base import ProcessFactoryBase
from infrastructure.api_secret_getter import ApiMetaData


class ProcessFactory(ProcessFactoryBase):
    """
    Factory class to launch data writing processes.
    
    Attributes:
        __FILE_PREFIX (str): Prefix for the module import path.
        _STANDARDIZED_CLASS_NAME (str): Standardized name for the class being imported.
        __STANDARDIZED_FILE_NAME (str): Standardized name for the file being imported.
    """
     
    __FILE_PREFIX = "account_data_fetcher.writers"
    _STANDARDIZED_CLASS_NAME = "Writer" 
    __STANDARDIZED_FILE_NAME = "writer" 

    @classmethod
    def get_module_name(cls, process_name):
        """
        Generates the module name for a given process name.
        
        Args:
            process_name (str): The name of the process.
            
        Returns:
            str: Full module path based on the process name.
        """
        return f"{cls.__FILE_PREFIX}.{process_name.lower()}.{cls.__STANDARDIZED_FILE_NAME}"

    @classmethod
    def main(cls):
        """
        Main entry point to initiate the writer process.
        
        Command line arguments:
            --writer_name (str): Name of the writer process, required.
            --args (JSON str): JSON-formatted string for arguments, optional.
            --kwargs (JSON str): JSON-formatted string for keyword arguments, optional.
        """
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


        cls.launch_process_and_run_request_processor(args.writer_name, **args_dict, **kwargs_dict)

if __name__ == "__main__":
    ProcessFactory.main()