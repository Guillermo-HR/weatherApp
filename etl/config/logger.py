import os
import logging
from logging.handlers import RotatingFileHandler

def check_log_file(log_dir: str, log_file: str) -> None:
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    if not os.path.isfile(log_file):
        with open(log_file, 'w'):
            pass


def setup_logger(name: str = "etl_logger",
                 log_level: int = logging.INFO,
                 max_bytes: int = 10 * 1024 * 1024,
                 backup_count: int = 5) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(log_level)
    
    if logger.handlers:
        return logger
    
    log_dir = "logs"
    log_file = os.path.join(log_dir, "etl.log")
    check_log_file(log_dir, log_file)

    FORMAT = ("[%(asctime)s %(threadName)s %(filename)s > %(funcName)s()]"
              " %(levelname)s:\n\t%(message)s")
    date_format = "%Y-%m-%d %H:%M:%S"
    formatter = logging.Formatter(fmt=FORMAT, datefmt=date_format)

    file_handler = RotatingFileHandler(
        filename=log_file,
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger
    
