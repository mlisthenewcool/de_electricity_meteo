"""Logging configuration and utilities.

This module provides a centralized logging setup using YAML configuration.
It supports JSON-formatted structured logging for better log aggregation.

Usage:
    from de_electricity_meteo.logger import logger

    logger.info("Message", extra={"key": "value"})
"""

import logging
import logging.config
from functools import lru_cache
from pathlib import Path

import yaml

from de_electricity_meteo.config.paths import LOGGER_CONFIG
from de_electricity_meteo.config.settings import LOGGER_NAME
from de_electricity_meteo.enums import LoggerChoice


@lru_cache(maxsize=1)
def load_config(path: Path) -> None:
    """Loads the logging configuration from a YAML file.

    The @lru_cache decorator ensures the file is read and parsed only once
    during the application lifecycle to optimize performance.

    Args:
        path: The filesystem path to the YAML configuration file.

    Raises:
        FileNotFoundError: If the provided path does not exist.
        ValueError: If the YAML file contains syntax errors.
        RuntimeError: If any other unexpected error occurs during configuration.
    """
    if not path.exists():
        raise FileNotFoundError(f"Logging configuration file not found at: {path}")

    try:
        with path.open(mode="rt", encoding="utf-8") as f:
            config = yaml.safe_load(f)
            logging.config.dictConfig(config)
    except yaml.YAMLError as yaml_error:
        raise ValueError(f"Invalid YAML configuration in file at {path}: {yaml_error}")
    except Exception as general_error:
        raise RuntimeError(f"Unexpected error loading logging config: {general_error}")


def is_logger_name_defined(name: str) -> bool:
    """Checks if a specific logger name exists in the logging manager's registry.

    This prevents instantiating a default logger if it wasn't explicitly
    defined in the YAML configuration.

    Args:
        name: The string name of the logger to check in the registry.

    Returns:
        True if the logger name is defined in the registry, False otherwise.
    """
    # logging.root.manager.loggerDict contains all instantiated loggers (except root)
    return name in logging.Logger.manager.loggerDict


def get_safe_logger(config_path: Path, name: LoggerChoice) -> logging.Logger:
    """Retrieves a configured logger instance after ensuring the configuration is loaded.

    Args:
        config_path: Path to the YAML configuration file.
        name: The Enum member representing the desired logger from LoggerChoice.

    Returns:
        The initialized and configured logger object.

    Raises:
        FileNotFoundError: If the config file is missing (via load_config).
        ValueError: If the logger name is not found in the loaded configuration
            or if the YAML is invalid.
        RuntimeError: If configuration fails unexpectedly.
    """
    load_config(path=config_path)

    # to choose a specific handler in Python code
    # handler = logging.getHandlerByName(x)
    # logger = logging.getLogger(x)
    # if handler:
    #   logger.addHandler(handler)

    logger_name_as_str = name.value
    if is_logger_name_defined(logger_name_as_str):
        return logging.getLogger(logger_name_as_str)

    raise ValueError(
        f"Logger {logger_name_as_str} is not defined or was not created in the config. "
        f"Use one of: {[choice.value for choice in LoggerChoice]}"
    )


# todo: should maybe move that to a function & improve default configuration
try:
    logger = get_safe_logger(config_path=LOGGER_CONFIG, name=LOGGER_NAME)
except Exception as e:
    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger("fallback")
    logger.error("Logging configuration failed, using fallback logger.")
    logger.error(f"Error message was: {e}")

if __name__ == "__main__":
    extras = {"status": "working"}
    logger.debug("Testing DEBUG level...", extra=extras)
    logger.info("Testing INFO level...", extra=extras)
    logger.warning("Testing WARNING level...", extra=extras)
    logger.error("Testing ERROR level...", extra=extras)
    logger.critical("Testing CRITICAL level...", extra=extras)
