"""Logging configuration using Loguru.

This module provides colored terminal logging with support for the standard
library's extra={} pattern for compatibility with existing code.

Usage:
    from de_electricity_meteo.core import logger

    logger.info("Download started", extra={"url": "https://example.com", "size": 1024})
    logger.error("Failed to connect", extra={"attempt": 3, "max_retries": 5})

    try:
        risky_operation()
    except Exception:
        logger.exception("Operation failed", extra={"context": "data_pipeline"})
"""

from __future__ import annotations

import re
import sys
from typing import Any

from loguru import logger as _loguru_logger

from de_electricity_meteo.config.settings import LOG_LEVEL


class LoguruAdapter:
    """Adapter bridging standard library's extra={} pattern with Loguru's bind().

    This is a singleton: multiple instantiations return the same instance.
    Loguru is configured once on first instantiation.

    Attributes:
        _instance: The singleton instance (class-level).
        _ANSI_PATTERN: Compiled regex matching ANSI escape sequences.
    """

    _instance: LoguruAdapter | None = None

    _MAGENTA = "\x1b[35m"
    _WHITE = "\x1b[37m"
    _RESET = "\x1b[0m"
    _ANSI_PATTERN = re.compile(r"\x1b\[[0-9;]*m")

    _FORMAT = (
        "<green>{time:HH:mm:ss}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{name}</cyan>:<cyan>{function}</cyan> | "
        "<level>{message}</level>"
        "{extra_str}"
    )

    def __new__(cls, level: str = LOG_LEVEL) -> LoguruAdapter:
        """Return the singleton instance, creating it on first call."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._configure(level)
        return cls._instance

    def _configure(self, level: str) -> None:
        """Configure Loguru (called once on singleton creation)."""
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

    @classmethod
    def _reset_for_tests(cls) -> None:
        """Reset singleton state. For testing purposes only."""
        cls._instance = None

    @staticmethod
    def _strip_ansi(text: str) -> str:
        """Strip ANSI escape sequences from text to prevent injection.

        Args:
            text: Input string that may contain ANSI escape codes.

        Returns:
            String with all ANSI escape sequences removed.
        """
        return LoguruAdapter._ANSI_PATTERN.sub("", text)

    @staticmethod
    def _format_extra(record: dict[str, Any]) -> None:
        """Patch function to format extra fields for colored display.

        Transforms the extra dict into a formatted string with ANSI colors
        that will be appended to each log line. User-provided values are
        sanitized to prevent ANSI escape sequence injection.

        Args:
            record: Loguru record dict containing the 'extra' field.
        """
        extra = record["extra"]
        if not extra:
            record["extra_str"] = ""
            return

        parts: list[str] = []

        for k, v in extra.items():
            try:
                v_safe = LoguruAdapter._strip_ansi(str(v))
            except Exception:  # maybe only ValueError, AttributeError ?
                v_safe = "<REPR_ERROR>"

            try:
                k_safe = LoguruAdapter._strip_ansi(str(k))
            except Exception:  # maybe only ValueError, AttributeError ?
                k_safe = "<REPR_ERROR>"

            parts.append(
                f"{LoguruAdapter._MAGENTA}{k_safe}{LoguruAdapter._RESET}"
                f"={LoguruAdapter._WHITE}{v_safe}{LoguruAdapter._RESET}"
            )

        record["extra_str"] = " | " + " | ".join(parts)

    def _log(self, level: str, message: str, **kwargs: Any) -> None:
        """Internal logging method that handles extra={} conversion.

        Args:
            level: Log level name (debug, info, warning, error, critical).
            message: Log message string.
            **kwargs: Optional 'extra' dict and 'exc_info' bool.
        """
        extra: dict[str, Any] = kwargs.pop("extra", {})
        exc_info = kwargs.pop("exc_info", False)

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
        if sys.exc_info()[0] is None:
            kwargs["exc_info"] = False
            extra: dict[str, Any] = kwargs.get("extra", {})
            extra["note"] = "logger.exception() called without active exception"
            kwargs["extra"] = extra
        else:
            kwargs["exc_info"] = True

        self._log("error", message, **kwargs)


logger = LoguruAdapter()


if __name__ == "__main__":
    # Visual demo of logger output - run with: python -m de_electricity_meteo.core.logger
    extras = {"status": "working", "user_id": 42}

    logger.debug("Debug level message", extra=extras)
    logger.info("Info level message", extra=extras)
    logger.warning("Warning level message", extra=extras)
    logger.error("Error level message", extra=extras)
    logger.critical("Critical level message", extra=extras)

    logger.info("Message without extras")

    logger.info("msg", extra={"message": "<red>should not be interpreted</red>"})
    logger.info("msg", extra={"\x1b[31mkey": "should not be interpreted"})
    logger.info("msg", extra={"should not be interpreted": "\x1b[31mkey"})

    logger.exception("Raised without active exception")

    try:
        result = 1 / 0
    except ZeroDivisionError:
        logger.exception("Exception with traceback", extra={"context": "division"})
