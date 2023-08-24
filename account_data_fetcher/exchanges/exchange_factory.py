
from importlib import import_module
import inspect
from account_data_fetcher.exchanges.exchange_base import ExchangeBase


class ExchangeFactory:
    __STANDARDIZED_FILE_NAME = "data_fetcher" 
    __STANDARDIZED_CLASS_NAME = "DataFetcher" 
    
    @classmethod
    def get_exchange_class(cls, prefix: str, exchange_name: str, *args, **kwargs) -> ExchangeBase:
        
        if "ib" in exchange_name.lower():
            if exchange_name.endswith("async"):
                module_name = f"{prefix}.{exchange_name.lower()}.{cls.__STANDARDIZED_FILE_NAME} + _async"
            if exchange_name.endswith("flex_queries"):
                module_name = f"{prefix}.{exchange_name.lower()}.{cls.__STANDARDIZED_FILE_NAME} + _flex_queries"
        else:
            module_name = f"{prefix}.{exchange_name.lower()}.{cls.__STANDARDIZED_FILE_NAME}"
        
        try:
            print(f"{cls.__STANDARDIZED_FILE_NAME=}")
            print(f"{module_name=}")
            exchange_module = import_module(module_name)
            print(f"{exchange_module=}")
            exchange_instance = getattr(exchange_module, cls.__STANDARDIZED_CLASS_NAME)
            print(f"{exchange_instance=}")
        except (ImportError, AttributeError, ModuleNotFoundError) as e:
            raise Exception(f"{e=}")


        signature = inspect.signature(exchange_instance.__init__)
        filtered_kwargs = {key: value for key, value in kwargs.items() if key in signature.parameters}
        
        return exchange_instance(*args, **filtered_kwargs)