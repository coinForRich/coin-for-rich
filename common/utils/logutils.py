import logging
import inspect


def create_logger(
    logger_name: str,
    log_level: int = logging.INFO,
    stream_handler: bool = True,
    log_filename: str = None,
    **kwargs
):
    '''
    Creates a Logger with default level at INFO;
    The default handler is StreamHandler - the only other option is FileHandler

    :params:
        @log_filename: full path to log filename
        @kwargs: additional keyword arguments
    '''

    check_file_handler = \
        (stream_handler is False or stream_handler is None) and log_filename is None
    if check_file_handler:
        raise ValueError(
            "log_filename must be provided if stream handler is not selected"
        )

    logger = logging.getLogger(logger_name)
    logger.setLevel(log_level)

    if stream_handler:
        log_handler = logging.StreamHandler()
    else:
        # Look into the kwargs provided above, and grab any arg
        # that FileHandler object uses
        file_handler_kws = {
            key: value for key, value in kwargs.items()
            if key in (inspect.getfullargspec(logging.FileHandler).args)
        }
        log_handler = logging.FileHandler(log_filename, **file_handler_kws)
    log_handler.setLevel(log_level)
    log_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    log_handler.setFormatter(log_formatter)

    logger.addHandler(log_handler)
    return logger
