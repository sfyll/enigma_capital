from account_data_fetcher.launcher.process_factory_base import ProcessFactoryBase


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

if __name__ == "__main__":
    ProcessFactory.main()
