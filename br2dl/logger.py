import os
import logging
import logging.handlers


def init_logger(log_level: str, log_format: str, log_file_size: int, log_file_count: int, log_file_dir: None, logger_name: str = "Application"):
    """initialize logger

    Args:
        log_level (str): loglevel 'DEBUG', 'INFO', 'WARN' or 'ERROR'
        log_format (str): logformat
        log_file_size (int): size of each file
        log_file_count (int): count of files are kept in dir
        log_file_dir (None): path to log directory
        logger_name (str, optional): name of logger. Defaults to "Application".

    Returns:
        logger: logger instance is set up.
    """
    # Logger initialize
    logging.basicConfig(level=log_level, format=log_format)

    if log_file_dir is not None:
        dir = os.path.dirname(log_file_dir)
        if not os.path.exists(dir):
            os.makedirs(dir)

        fh = logging.handlers.RotatingFileHandler(log_file_dir, maxBytes=log_file_size, backupCount=log_file_count)
        fh.setFormatter(logging.Formatter(log_format))
        fh.setLevel(log_level)
        logging.getLogger().addHandler(fh)

    return logging.getLogger(logger_name)
