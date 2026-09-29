import logging
import os
import sys
from logging.handlers import RotatingFileHandler


def setup_logger(log_file_path: str = "logs/ai_worker.log", level: int = logging.INFO) -> logging.Logger:
    os.makedirs(os.path.dirname(log_file_path), exist_ok=True)

    logger = logging.getLogger("ai_worker")
    logger.setLevel(level)

    if logger.handlers:
        return logger

    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] [%(name)s]: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # 1. Stdout console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # 2. Rotating file handler (10MB limit, 3 backups like lumberjack)
    file_handler = RotatingFileHandler(
        filename=log_file_path,
        maxBytes=10 * 1024 * 1024,
        backupCount=3,
        encoding="utf-8"
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger
