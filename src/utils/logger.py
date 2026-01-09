"""
Logging configuration module
"""

import logging
import sys
from pathlib import Path
from typing import Optional
from datetime import datetime
import time


class LocalTimeFormatter(logging.Formatter):
    """Custom formatter that uses local timezone."""
    
    def formatTime(self, record, datefmt=None):
        """Format time using local timezone."""
        # Convert timestamp to local timezone
        # datetime.fromtimestamp() uses system's local timezone
        ct = datetime.fromtimestamp(record.created)
        # If system timezone is not set, try to use TZ environment variable
        # or default to local time
        if datefmt:
            s = ct.strftime(datefmt)
        else:
            s = ct.strftime(self.default_time_format)
        return s


def setup_logger(
    name: str = "cep_processor",
    log_level: str = "INFO",
    log_file: Optional[Path] = None
) -> logging.Logger:
    """
    Configure and return a logger instance.

    Args:
        name: Logger name
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Optional file path for file logging

    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))

    # Remove existing handlers to avoid duplicates
    logger.handlers.clear()

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, log_level.upper(), logging.INFO))

    # Formatter with local timezone
    formatter = LocalTimeFormatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # File handler (if specified)
    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(getattr(logging, log_level.upper(), logging.INFO))
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger
