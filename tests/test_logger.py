"""Tests for the Loguru-based logger module."""

import io
import sys

import pytest

from de_electricity_meteo.logger import LoguruAdapter


class TestLoguruAdapter:
    """Tests for the LoguruAdapter class."""

    @pytest.fixture
    def capture_stderr(self) -> io.StringIO:
        """Fixture to capture stderr output."""
        return io.StringIO()

    @pytest.fixture
    def adapter(self, capture_stderr: io.StringIO) -> LoguruAdapter:
        """Create an adapter that writes to captured stderr."""
        # Redirect stderr temporarily
        original_stderr = sys.stderr
        sys.stderr = capture_stderr

        adapter = LoguruAdapter(level="DEBUG")

        # Restore stderr for other operations
        sys.stderr = original_stderr
        return adapter

    def test_adapter_initialization(self) -> None:
        """LoguruAdapter initializes without errors."""
        adapter = LoguruAdapter()
        assert adapter._logger is not None

    def test_adapter_custom_level(self) -> None:
        """LoguruAdapter accepts custom log level."""
        adapter = LoguruAdapter(level="WARNING")
        assert adapter._logger is not None

    def test_log_methods_exist(self) -> None:
        """All standard log methods are available."""
        adapter = LoguruAdapter()

        assert hasattr(adapter, "debug")
        assert hasattr(adapter, "info")
        assert hasattr(adapter, "warning")
        assert hasattr(adapter, "error")
        assert hasattr(adapter, "critical")
        assert hasattr(adapter, "exception")

    def test_log_methods_callable_without_extra(self) -> None:
        """Log methods work without extra parameter."""
        adapter = LoguruAdapter()

        # Should not raise
        adapter.debug("Debug message")
        adapter.info("Info message")
        adapter.warning("Warning message")
        adapter.error("Error message")
        adapter.critical("Critical message")

    def test_log_methods_callable_with_extra(self) -> None:
        """Log methods work with extra parameter."""
        adapter = LoguruAdapter()
        extras = {"key": "value", "count": 42}

        # Should not raise
        adapter.debug("Debug message", extra=extras)
        adapter.info("Info message", extra=extras)
        adapter.warning("Warning message", extra=extras)
        adapter.error("Error message", extra=extras)
        adapter.critical("Critical message", extra=extras)

    def test_exception_method_in_handler(self) -> None:
        """Exception method logs traceback when called in exception handler."""
        adapter = LoguruAdapter()

        try:
            raise ValueError("Test error")
        except ValueError:
            # Should not raise
            adapter.exception("Caught an error", extra={"context": "test"})

    def test_format_extra_with_values(self) -> None:
        """_format_extra correctly formats extra fields."""
        adapter = LoguruAdapter()
        record: dict = {"extra": {"status": "ok", "count": 5}}

        adapter._format_extra(record)

        assert "extra_str" in record
        assert "status" in record["extra_str"]
        assert "ok" in record["extra_str"]
        assert "count" in record["extra_str"]
        assert "5" in record["extra_str"]

    def test_format_extra_empty(self) -> None:
        """_format_extra returns empty string when no extras."""
        adapter = LoguruAdapter()
        record: dict = {"extra": {}}

        adapter._format_extra(record)

        assert record["extra_str"] == ""

    def test_format_extra_contains_ansi_colors(self) -> None:
        """_format_extra includes ANSI color codes."""
        adapter = LoguruAdapter()
        record: dict = {"extra": {"key": "value"}}

        adapter._format_extra(record)

        # Check for ANSI escape sequences
        assert "\x1b[" in record["extra_str"]
        assert adapter._MAGENTA in record["extra_str"]
        assert adapter._RESET in record["extra_str"]


class TestLoggerModuleExport:
    """Tests for the module-level logger export."""

    def test_logger_is_exported(self) -> None:
        """Module exports a logger instance."""
        from de_electricity_meteo.logger import logger  # noqa: PLC0415

        assert logger is not None
        assert isinstance(logger, LoguruAdapter)

    def test_exported_logger_is_functional(self) -> None:
        """Exported logger can log messages."""
        from de_electricity_meteo.logger import logger  # noqa: PLC0415

        # Should not raise
        logger.info("Test message from exported logger", extra={"test": True})
