"""Tests for the Loguru-based logger module."""

import pytest

from de_electricity_meteo.core.logger import LoguruAdapter, logger


class TestLoguruAdapter:
    """Tests for LoguruAdapter initialization and log methods."""

    def test_initialization_default_and_custom_level(self) -> None:
        """LoguruAdapter initializes with default and custom levels."""
        assert LoguruAdapter()._logger is not None
        assert LoguruAdapter(level="WARNING")._logger is not None

    def test_invalid_level_raises(self) -> None:
        """Invalid log level raises ValueError."""
        with pytest.raises(ValueError, match="(?i)level"):
            LoguruAdapter(level="INVALID")

    @pytest.mark.parametrize("method", ["debug", "info", "warning", "error", "critical"])
    def test_log_methods(self, method: str) -> None:
        """All log methods work with and without extra."""
        adapter = LoguruAdapter()
        log_fn = getattr(adapter, method)
        log_fn("message")
        log_fn("message", extra={"key": "value"})

    def test_exception_with_active_exception(self, capsys: pytest.CaptureFixture) -> None:
        """exception() logs traceback when called in handler."""
        adapter = LoguruAdapter(level="DEBUG")
        try:
            raise ValueError("test error")
        except ValueError:
            adapter.exception("caught", extra={"ctx": "test"})
        assert "ValueError" in capsys.readouterr().err

    def test_exception_without_active_exception(self, capsys: pytest.CaptureFixture) -> None:
        """exception() adds note when called outside handler."""
        LoguruAdapter(level="DEBUG").exception("no exception")
        output = capsys.readouterr().err
        assert "no exception" in output
        assert "logger.exception() called without active exception" in output


class TestFormatExtra:
    """Tests for _format_extra static method."""

    def test_empty_extra(self) -> None:
        """Empty extra produces empty string."""
        record: dict = {"extra": {}}
        LoguruAdapter._format_extra(record)
        assert record["extra_str"] == ""

    def test_formats_key_value_with_ansi(self) -> None:
        """Extra fields are formatted with ANSI colors."""
        record: dict = {"extra": {"status": "ok", "count": 5}}
        LoguruAdapter._format_extra(record)
        assert all(s in record["extra_str"] for s in ["status", "ok", "count", "5", "\x1b["])

    def test_strips_ansi_from_user_input(self) -> None:
        """ANSI codes in keys and values are stripped."""
        record: dict = {"extra": {"\x1b[31mkey\x1b[0m": "\x1b[32mvalue\x1b[0m"}}
        LoguruAdapter._format_extra(record)
        assert "key" in record["extra_str"] and "value" in record["extra_str"]
        # Injected codes (red \x1b[31m, green \x1b[32m) must be stripped
        assert "\x1b[31m" not in record["extra_str"]
        assert "\x1b[32m" not in record["extra_str"]

    def test_handles_str_conversion_error_in_value(self) -> None:
        """Objects raising on str() as values show <REPR_ERROR>."""

        class BadStr:
            def __str__(self) -> str:
                raise ValueError

        record: dict = {"extra": {"key": BadStr()}}
        LoguruAdapter._format_extra(record)
        assert "<REPR_ERROR>" in record["extra_str"]

    def test_handles_str_conversion_error_in_key(self) -> None:
        """Objects raising on str() as keys show <REPR_ERROR>."""

        class BadStrKey:
            def __str__(self) -> str:
                raise ValueError

            def __hash__(self) -> int:
                return 42

            def __eq__(self, other: object) -> bool:
                return isinstance(other, BadStrKey)

        record: dict = {"extra": {BadStrKey(): "value"}}
        LoguruAdapter._format_extra(record)
        assert "<REPR_ERROR>" in record["extra_str"]

    def test_preserves_loguru_markup_literally(self) -> None:
        """Loguru markup tags are not interpreted."""
        record: dict = {"extra": {"x": "<red>text</red>"}}
        LoguruAdapter._format_extra(record)
        assert "<red>text</red>" in record["extra_str"]


class TestStripAnsi:
    """Tests for _strip_ansi static method."""

    @pytest.mark.parametrize(
        ("input_text", "expected"),
        [
            ("normal text", "normal text"),
            ("\x1b[31mred\x1b[0m", "red"),
            ("\x1b[1m\x1b[32mbold green\x1b[0m", "bold green"),
        ],
    )
    def test_strip_ansi(self, input_text: str, expected: str) -> None:
        """ANSI sequences are removed, normal text preserved."""
        assert LoguruAdapter._strip_ansi(input_text) == expected


class TestModuleExport:
    """Tests for module-level logger export."""

    def test_logger_exported_and_functional(self) -> None:
        """Module exports a functional LoguruAdapter instance."""
        assert isinstance(logger, LoguruAdapter)
        logger.info("test", extra={"x": 1})  # Should not raise
