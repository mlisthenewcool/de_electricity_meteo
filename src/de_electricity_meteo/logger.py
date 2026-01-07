"""Logging configuration using Loguru.

This module provides colored terminal logging with support for the standard
library's extra={} pattern for compatibility with existing code.

Usage:
    from de_electricity_meteo.logger import logger

    logger.info("Download started", extra={"url": "https://example.com", "size": 1024})
    logger.error("Failed to connect", extra={"attempt": 3, "max_retries": 5})

    try:
        risky_operation()
    except Exception:
        logger.exception("Operation failed", extra={"context": "data_pipeline"})
"""

import sys
from typing import Any

from loguru import logger as _loguru_logger

from de_electricity_meteo.config.settings import LOG_LEVEL


class LoguruAdapter:
    """Adapter to support standard library's extra={} pattern with Loguru.

    Loguru uses bind() for extra context, but many codebases use the standard
    library's `logger.info("msg", extra={...})` syntax. This adapter bridges
    the two approaches, allowing existing code to work without modification.

    Attributes:
        ANSI color codes for formatting extra fields in terminal output.
    """

    _MAGENTA = "\x1b[35m"
    _WHITE = "\x1b[37m"
    _RESET = "\x1b[0m"

    _FORMAT = (
        "<green>{time:HH:mm:ss}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
        "<level>{message}</level>"
        "{extra_str}"
    )

    def __init__(self, level: str = LOG_LEVEL) -> None:
        """Initialize the adapter and configure Loguru.

        Args:
            level: Minimum log level to display. Defaults to LOG_LEVEL from settings.
        """
        _loguru_logger.remove()

        patched = _loguru_logger.patch(LoguruAdapter._format_extra)  # type: ignore[arg-type]
        patched.add(
            sys.stderr,
            level=level,
            format=self._FORMAT,
            colorize=True,
            backtrace=True,
            diagnose=True,
        )
        self._logger = patched

    @staticmethod
    def _format_extra(record: dict[str, Any]) -> None:
        """Patch function to format extra fields for colored display.

        Transforms the extra dict into a formatted string with ANSI colors
        that will be appended to each log line.

        Args:
            record: Loguru record dict containing the 'extra' field.
        """
        extra = record["extra"]
        if extra:
            formatted = " | ".join(
                f"{LoguruAdapter._MAGENTA}{k}={LoguruAdapter._WHITE}{v}{LoguruAdapter._RESET}"
                for k, v in extra.items()
            )
            record["extra_str"] = f" | {formatted}"
        else:
            record["extra_str"] = ""

    def _log(self, level: str, message: str, **kwargs: Any) -> None:
        """Internal logging method that handles extra={} conversion.

        Args:
            level: Log level name (debug, info, warning, error, critical).
            message: Log message string.
            **kwargs: Optional 'extra' dict and 'exc_info' bool.
        """
        extra: dict[str, Any] = kwargs.pop("extra", {})
        exc_info: bool = kwargs.pop("exc_info", False)

        bound = self._logger.bind(**extra)
        log_method = getattr(bound.opt(depth=2, exception=exc_info), level)
        log_method(message)

    def debug(self, message: str, **kwargs: Any) -> None:
        """Log a debug message."""
        self._log("debug", message, **kwargs)

    def info(self, message: str, **kwargs: Any) -> None:
        """Log an info message."""
        self._log("info", message, **kwargs)

    def warning(self, message: str, **kwargs: Any) -> None:
        """Log a warning message."""
        self._log("warning", message, **kwargs)

    def error(self, message: str, **kwargs: Any) -> None:
        """Log an error message."""
        self._log("error", message, **kwargs)

    def critical(self, message: str, **kwargs: Any) -> None:
        """Log a critical message."""
        self._log("critical", message, **kwargs)

    def exception(self, message: str, **kwargs: Any) -> None:
        """Log an error message with exception traceback.

        Should be called from within an exception handler.
        """
        kwargs["exc_info"] = True
        self._log("error", message, **kwargs)


logger = LoguruAdapter()


if __name__ == "__main__":
    extras = {"status": "working", "user_id": 42}
    logger.debug("Testing DEBUG level...", extra=extras)
    logger.info("Testing INFO level...", extra=extras)
    logger.warning("Testing WARNING level...", extra=extras)
    logger.error("Testing ERROR level...", extra=extras)
    logger.critical("Testing CRITICAL level...", extra=extras)

    logger.info("Message without extras")

    try:
        result = 1 / 0
    except Exception:
        logger.exception("Testing exception traceback", extra={"context": "division"})
