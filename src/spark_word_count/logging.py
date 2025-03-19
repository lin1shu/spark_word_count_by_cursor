"""
Logging configuration for the application.
"""

import logging
import logging.config
import os
import sys
from typing import Any, Dict, List, Optional, TypedDict, Union


# Define handler configuration type
class HandlerDict(TypedDict, total=False):
    """TypedDict for handler configuration."""
    class_: str  # Will be mapped to 'class' in the configuration
    formatter: str
    level: Union[str, int]
    filename: Optional[str]
    stream: Optional[Any]


class LoggerDict(TypedDict):
    """TypedDict for logger configuration."""
    level: Union[str, int]
    handlers: List[str]
    propagate: bool


class LoggingConfig(TypedDict):
    """TypedDict for logging configuration."""
    version: int
    formatters: Dict[str, Dict[str, Any]]
    handlers: Dict[str, Dict[str, Any]]
    loggers: Dict[str, LoggerDict]
    root: LoggerDict


def get_logging_config(
    level: Union[str, int] = "INFO",
    log_file: Optional[str] = None,
    log_to_console: bool = True,
) -> Dict[str, Any]:
    """
    Get the logging configuration.
    
    Args:
        level: Logging level (default: INFO)
        log_file: Path to log file (default: None)
        log_to_console: Whether to log to console (default: True)
        
    Returns:
        Dict[str, Any]: Logging configuration dictionary
    """
    handlers: List[str] = []
    handlers_config: Dict[str, Dict[str, Any]] = {}
    
    # Console handler
    if log_to_console:
        handlers.append("console")
        handlers_config["console"] = {
            "class": "logging.StreamHandler",
            "level": level,
            "formatter": "standard",
            "stream": sys.stdout,
        }
    
    # File handler
    if log_file:
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        handlers.append("file")
        handlers_config["file"] = {
            "class": "logging.FileHandler",
            "level": level,
            "formatter": "standard",
            "filename": log_file,
        }
    
    # Root logger configuration
    root_logger: Dict[str, Any] = {
        "level": level,
        "handlers": handlers,
        "propagate": False,
    }
    
    # Full logging configuration
    config: Dict[str, Any] = {
        "version": 1,
        "formatters": {
            "standard": {
                "format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S",
            },
            "simple": {
                "format": "%(levelname)s %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S",
            },
        },
        "handlers": handlers_config,
        "loggers": {
            "spark_word_count": {
                "level": level,
                "handlers": handlers,
                "propagate": False,
            },
            "pyspark": {
                "level": "WARN",
                "handlers": handlers,
                "propagate": False,
            },
        },
        "root": root_logger,
    }
    
    return config


def setup_logging(
    level: Union[str, int] = "INFO",
    log_file: Optional[str] = None,
    log_to_console: bool = True,
) -> None:
    """
    Set up logging configuration.
    
    Args:
        level: Logging level (default: INFO)
        log_file: Path to log file (default: None)
        log_to_console: Whether to log to console (default: True)
    """
    config = get_logging_config(level, log_file, log_to_console)
    logging.config.dictConfig(config)
    
    # Log startup message
    logger = logging.getLogger("spark_word_count")
    logger.info("Logging configured with level %s", level)
    if log_file:
        logger.info("Logging to file: %s", log_file)


def main() -> None:
    """Test the logging configuration."""
    setup_logging(level="DEBUG", log_file="logs/test.log")
    
    logger = logging.getLogger("spark_word_count")
    logger.debug("This is a debug message")
    logger.info("This is an info message")
    logger.warning("This is a warning message")
    logger.error("This is an error message")
    logger.critical("This is a critical message")


if __name__ == "__main__":
    main()
