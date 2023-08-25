
from abc import ABC, abstractmethod
from importlib import import_module
import inspect

from infrastructure.api_secret_getter import ApiMetaData


class ProcessFactoryBase(ABC):

    @classmethod
    def get_process_class(cls, process_name: str):
        
        module_name = cls.get_module_name(process_name)
        
        try:
            exchange_module = import_module(module_name)
            return getattr(exchange_module, cls.__STANDARDIZED_CLASS_NAME)
        except (ImportError, AttributeError, ModuleNotFoundError) as e:
            raise Exception(f"{e=}")

    @classmethod
    @abstractmethod
    def get_module_name(cls, process_name) -> str:
        pass

    @classmethod
    def launch_process_and_run_request_processor(cls, process_name: str, *args, **kwargs) -> None:
        process_instance = cls.get_process_class(process_name)

        signature = inspect.signature(process_instance.__init__)
        filtered_kwargs = {key: value for key, value in kwargs.items() if key in signature.parameters}
        
        launched_instance = process_instance(*args, **filtered_kwargs)

        launched_instance.process_request()

    """the below is the CLI parser
       refer to an implementation for details"""
    @classmethod
    @abstractmethod
    def main(cls):
        pass