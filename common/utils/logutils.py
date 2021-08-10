import logging


def create_logger(logger_name: str, log_level: int = None):
    '''
    Creates a Logger with default level at INFO
    '''
    
    if not log_level:
        log_level = logging.INFO
    
    logger = logging.getLogger(logger_name)
    logger.setLevel(log_level)
    log_handler = logging.StreamHandler()
    log_handler.setLevel(log_level)
    log_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    log_handler.setFormatter(log_formatter)
    logger.addHandler(log_handler)

    return logger