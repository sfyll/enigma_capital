import logging

def args_handler(args):    
    # Setup logging
    args.verbosity = args.verbose - args.quiet
    if args.verbosity == 0:
        logging.root.setLevel(logging.INFO)
    elif args.verbosity >= 1:
        logging.root.setLevel(logging.DEBUG)
    elif args.verbosity == -1:
        logging.root.setLevel(logging.WARNING)
    elif args.verbosity <= -2:
        logging.root.setLevel(logging.ERROR)
    
    logging.basicConfig(format='%(levelname)s - %(asctime)s - %(name)s - %(message)s', filename=args.log_file)

    return args

def create_exchange_specific_logger(exchange_name: str) -> logging.Logger:
    logger = logging.getLogger(exchange_name)
    log_filename = f"{exchange_name}_log.txt"
    
    if not logger.handlers:
        file_handler = logging.FileHandler(log_filename)
        # Reuse the same formatter and level from the root logger
        root_logger = logging.getLogger()
        file_handler.setFormatter(root_logger.handlers[0].formatter)
        file_handler.setLevel(root_logger.level)
        logger.addHandler(file_handler)

    return logger
