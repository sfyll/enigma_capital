from abc import ABC, abstractmethod
from importlib import import_module
import inspect

class ProcessFactoryBase(ABC):

    @classmethod
    def get_process_class(cls, process_name: str):
        """
        Retrieves the class object for a given process name.

        Args:
            process_name (str): Name of the process.
            
        Returns:
            Class: The class object corresponding to the process name.

        Raises:
            Exception: If importing the module or getting the attribute fails.
        """
        module_name = cls.get_module_name(process_name)
        
        try:
            exchange_module = import_module(module_name)
            return getattr(exchange_module, cls._STANDARDIZED_CLASS_NAME)
        except (ImportError, AttributeError, ModuleNotFoundError) as e:
            raise Exception(f"{e=}")

    @classmethod
    @abstractmethod
    def get_module_name(cls, process_name) -> str:
        """
        Abstract method to get the module name for a given process name.

        Args:
            process_name (str): Name of the process.
            
        Returns:
            str: The module name.
        """
        pass

    #TODO: Process request is the only entry-point at the factory level. This could be made more generic to accomodate for other entry-points and inputs as the application scales.
    @classmethod
    def launch_process_and_run_request_processor(cls, process_name: str, *args, **kwargs) -> None:
        """
        Instantiates the process class and runs its request processor method.

        Args:
            process_name (str): Name of the process.
            *args: Variable-length argument list.
            **kwargs: Arbitrary keyword arguments.
        """
        process_instance = cls.get_process_class(process_name)

        signature = inspect.signature(process_instance.__init__)
        filtered_kwargs = {key: value for key, value in kwargs.items() if key in signature.parameters}
        
        launched_instance = process_instance(*args, **filtered_kwargs)

        launched_instance.process_request()

    @classmethod
    @abstractmethod
    def main(cls):
        """
        Abstract method for the main CLI parser implementation. Refer to an implementation for enlightening examples.
        """
        pass