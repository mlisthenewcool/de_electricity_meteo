"""Enumerations for application configuration.

This module defines enums used throughout the application for type-safe
configuration values, avoiding magic strings.
"""

from enum import StrEnum


class LoggerChoice(StrEnum):
    """Available logger configurations defined in config/logger.yaml.

    Each value corresponds to a logger name in the YAML configuration file.
    Use these values with get_safe_logger() to retrieve a configured logger.

    Attributes:
        CONSOLE: JSON-formatted logger that outputs to stdout.
        FILE: JSON-formatted logger that outputs to a rotating file (if configured).
    """

    CONSOLE = "jsonConsoleLogger"
    FILE = "jsonFileLogger"
