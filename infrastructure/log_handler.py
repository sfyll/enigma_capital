import ast
import logging.config
import os
from configparser import ConfigParser
from io import StringIO


def get_base_path():
    """Returns the absolute path to the project's root directory."""
    current_directory = os.path.dirname(__file__)
    return os.path.abspath(os.path.join(current_directory, ".."))

def _normalize_log_path(path: str) -> str:
    path = os.path.expanduser(path)
    if not os.path.isabs(path):
        path = os.path.abspath(os.path.join(get_base_path(), path))
    os.makedirs(os.path.dirname(path), exist_ok=True)
    return path


def logging_handler(args):
    """
    Configures logging by reading a base config file, modifying it with
    runtime arguments, and loading it.
    """
    verbosity = args.verbose - args.quiet
    log_level_name = "INFO"  # Default
    if verbosity >= 1:
        log_level_name = "DEBUG"
    elif verbosity == -1:
        log_level_name = "WARNING"
    elif verbosity <= -2:
        log_level_name = "ERROR"

    config_path = os.path.join(get_base_path(), "account_data_fetcher/config/logging_config.ini")

    config = ConfigParser()
    config.read(config_path)

    if "handler_fileHandler" in config:
        if args.log_file:
            log_path = _normalize_log_path(args.log_file)
            config["handler_fileHandler"]["args"] = f"('{log_path}',)"
        elif "args" in config["handler_fileHandler"]:
            try:
                handler_args = ast.literal_eval(config["handler_fileHandler"]["args"])
                if isinstance(handler_args, tuple) and handler_args:
                    log_path = _normalize_log_path(str(handler_args[0]))
                    config["handler_fileHandler"]["args"] = f"('{log_path}',)"
            except (ValueError, SyntaxError):
                pass

    if "logger_root" in config:
        config["logger_root"]["level"] = log_level_name

    config_buffer = StringIO()
    config.write(config_buffer)
    config_buffer.seek(0)  # Rewind the buffer to the beginning
    logging.config.fileConfig(config_buffer)

    return args


def create_exchange_specific_logger(exchange_name: str) -> logging.Logger:
    """
    Creates a dedicated logger for an exchange, which logs to its own file.
    This function should work correctly with the new setup as it reuses the
    formatter from the already-configured root logger.
    """
    logger = logging.getLogger(exchange_name)
    if not logger.handlers:
        log_filename = os.path.expanduser(f"~/log/{exchange_name}_log.txt")
        os.makedirs(os.path.dirname(log_filename), exist_ok=True)

        file_handler = logging.FileHandler(log_filename)

        root_logger = logging.getLogger()
        if root_logger.handlers:
            file_handler.setFormatter(root_logger.handlers[0].formatter)
            file_handler.setLevel(root_logger.level)

        logger.addHandler(file_handler)
        logger.propagate = False

    return logger
