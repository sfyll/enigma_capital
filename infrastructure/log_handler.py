from configparser import ConfigParser
import logging.config
import os

def logging_handler(args):
    log_file = args.log_file  # obtained from CLI

    # Load from config file
    logging.config.fileConfig(get_base_path() + '/account_data_fetcher/config/logging_config_launch.ini')


    # Dynamically update log file name
    file_handler = logging.FileHandler(filename=log_file)
    
    # Update root logger
    logging.getLogger().addHandler(file_handler)

    # Set log level based on CLI
    args.verbosity = args.verbose - args.quiet
    log_level = logging.INFO  # default
    if args.verbosity == 0:
        log_level = logging.INFO
    elif args.verbosity >= 1:
        log_level = logging.DEBUG
    elif args.verbosity == -1:
        log_level = logging.WARNING
    elif args.verbosity <= -2:
        log_level = logging.ERROR

    logging.getLogger().setLevel(log_level)

    modify_logging_config(args.log_file,log_level)

    return args

def modify_logging_config(log_file, log_level):
    config = ConfigParser()
    config.read(get_base_path() + '/account_data_fetcher/config/logging_config_launch.ini')

    if 'handler_fileHandler' in config:
        config['handler_fileHandler']['args'] = f"('{log_file}',)"
        config['handler_fileHandler']['level'] = logging.getLevelName(log_level)

    with open(get_base_path() + '/account_data_fetcher/config/logging_config.ini', 'w') as configfile:
        config.write(configfile)


def create_exchange_specific_logger(exchange_name: str) -> logging.Logger:
    logger = logging.getLogger(exchange_name)
    log_filename = os.path.expanduser(f"~/log/{exchange_name}_log.txt")
    
    if not logger.handlers:
        file_handler = logging.FileHandler(log_filename)
        # Reuse the same formatter and level from the root logger
        root_logger = logging.getLogger()
        file_handler.setFormatter(root_logger.handlers[0].formatter)
        file_handler.setLevel(root_logger.level)
        logger.addHandler(file_handler)

    return logger

def get_base_path():
    current_directory = os.path.dirname(__file__)
    return os.path.abspath(os.path.join(current_directory, '..'))

def fetch_logging_config(config_path_extension: str):
    logging.config.fileConfig(get_base_path() + config_path_extension)

