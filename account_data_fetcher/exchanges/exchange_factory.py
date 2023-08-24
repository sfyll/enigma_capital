
from importlib import import_module
import inspect
from account_data_fetcher.exchanges.exchange_base import ExchangeBase


class ExchangeFactory:
    __STANDARDIZED_FILE_NAME = "data_fetcher" 
    __STANDARDIZED_CLASS_NAME = "DataFetcher" 
    
    @classmethod
    def get_exchange_class(cls, exchange_name, *args, **kwargs) -> ExchangeBase:
        
        if "ib" in exchange_name.lower():
            if exchange_name.endswith("async"):
                module_name = f"{exchange_name.lower()}.{cls.__STANDARDIZED_FILE_NAME} + _async"
            if exchange_name.endswith("flex_queries"):
                module_name = f"{exchange_name.lower()}.{cls.__STANDARDIZED_FILE_NAME} + _flex_queries"
        else:
            module_name = f"{exchange_name.lower()}.{cls.__STANDARDIZED_FILE_NAME}"
        
        try:
            exchange_module = import_module(module_name)
            exchange_instance = getattr(exchange_module, cls.__STANDARDIZED_CLASS_NAME)
        except (ImportError, AttributeError):
            raise ValueError(f"Unknown or incorrect exchange: {exchange_name}")


        signature = inspect.signature(exchange_instance.__init__)
        filtered_kwargs = {key: value for key, value in kwargs.items() if key in signature.parameters}
        
        return exchange_instance(*args, **filtered_kwargs)