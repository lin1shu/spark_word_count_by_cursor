"""
Logging configuration for Spark Word Count.

This module handles setting up logging for various components of the application.
It provides a consistent format and centralized configuration for all logging.
"""

import logging
import logging.config
import os
import sys
from typing import Any, Dict, Optional, Union

# Default log levels
DEFAULT_LOG_LEVEL = "INFO"
DEFAULT_PYSPARK_LOG_LEVEL = "WARN"

# Format strings for different levels of verbosity
SIMPLE_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
DETAILED_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(pathname)s:%(lineno)d - %(message)s"


def get_logger(name: str) -> logging.Logger:
    """Get a logger with the specified name.

    Args:
        name: The name of the logger, typically __name__ of the calling module.

    Returns:
        A configured logger instance.
    """
    return logging.getLogger(name)


def configure_logging(
    level: Optional[Union[str, int]] = None,
    pyspark_level: Optional[Union[str, int]] = None,
    format_string: Optional[str] = None,
    log_file: Optional[str] = None,
) -> None:
    """Configure logging for the application.

    Args:
        level: The log level for the application loggers.
        pyspark_level: The log level specifically for PySpark loggers.
        format_string: The format string to use for logging.
        log_file: Optional path to a log file, if None logs to stderr only.
    """
    # Use defaults if not specified
    level = level or os.environ.get("LOG_LEVEL", DEFAULT_LOG_LEVEL)
    pyspark_level = pyspark_level or os.environ.get("PYSPARK_LOG_LEVEL", DEFAULT_PYSPARK_LOG_LEVEL)
    format_string = format_string or (
        DETAILED_FORMAT
        if os.environ.get("LOG_DETAILED", "").lower() in ("true", "1", "t")
        else SIMPLE_FORMAT
    )

    # Convert string log levels to their numeric values
    if isinstance(level, str):
        level = getattr(logging, level.upper())
    if isinstance(pyspark_level, str):
        pyspark_level = getattr(logging, pyspark_level.upper())

    # Basic configuration for root logger
    handlers: Dict[str, Any] = {}

    # Always add a console handler
    handlers["console"] = {
        "class": "logging.StreamHandler",
        "level": level,
        "formatter": "default",
        "stream": sys.stderr,
    }

    # Add a file handler if log_file is specified
    if log_file:
        handlers["file"] = {
            "class": "logging.FileHandler",
            "level": level,
            "formatter": "default",
            "filename": log_file,
            "mode": "a",
        }

    # Configure logging
    logging.config.dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "default": {
                    "format": format_string,
                },
            },
            "handlers": handlers,
            "loggers": {
                "": {  # Root logger
                    "level": level,
                    "handlers": list(handlers.keys()),
                    "propagate": True,
                },
                "pyspark": {
                    "level": pyspark_level,
                    "handlers": list(handlers.keys()),
                    "propagate": False,
                },
                "py4j": {
                    "level": pyspark_level,
                    "handlers": list(handlers.keys()),
                    "propagate": False,
                },
            },
        }
    )

    logging.getLogger(__name__).debug("Logging configured successfully.")
