"""Tests for the Loguru-based logger module."""

from typing import Any, Generator

import pytest

from de_electricity_meteo.core.logger import LoguruAdapter, logger


@pytest.fixture(autouse=True)
def isolate_singleton() -> Generator[None, None, None]:
    """Reset singleton after each test for isolation."""
    yield
    LoguruAdapter._reset_for_tests()


class TestLoguruAdapter:
    """Tests for LoguruAdapter initialization and log methods."""

    def test_singleton_returns_same_instance(self) -> None:
        """Multiple instantiations return the same instance."""
        adapter1 = LoguruAdapter()
        adapter2 = LoguruAdapter()
        assert adapter1 is adapter2

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

    def test_depth_shows_caller_location(self, capsys: pytest.CaptureFixture) -> None:
        """Log output shows caller location, not internal logger.py functions."""
        LoguruAdapter(level="DEBUG").info("depth test")
        output = capsys.readouterr().err
        # Should show test file/function, not logger internals
        assert "test_logger" in output
        assert "logger:_log" not in output


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

    def test_handles_str_conversion_error(self) -> None:
        """Objects raising on str() show <REPR_ERROR> for both keys and values."""

        class BadStr:
            def __str__(self) -> str:
                raise Exception

            def __hash__(self) -> int:
                return 42

            def __eq__(self, other: object) -> bool:
                return isinstance(other, BadStr)

        # Test value error
        record: dict = {"extra": {"key": BadStr()}}
        LoguruAdapter._format_extra(record)
        assert "<REPR_ERROR>" in record["extra_str"]

        # Test key error
        record: dict[str, Any] = {"extra": {BadStr(): "value"}}
        LoguruAdapter._format_extra(record)
        assert "<REPR_ERROR>" in record["extra_str"]

    def test_handles_various_data_types(self) -> None:
        """Extra handles None, lists, and nested dicts."""
        record: dict = {
            "extra": {
                "none_val": None,
                "list_val": [1, 2, 3],
                "dict_val": {"nested": "value"},
            }
        }
        LoguruAdapter._format_extra(record)
        assert "None" in record["extra_str"]
        assert "[1, 2, 3]" in record["extra_str"]
        assert "nested" in record["extra_str"]

    def test_all_keys_are_present(self) -> None:
        """All extra keys are included in output."""
        keys: list[str] = ["alpha", "beta", "gamma", "delta"]
        record: dict[str, Any] = {"extra": {k: f"val_{k}" for k in keys}}
        LoguruAdapter._format_extra(record)
        for key in keys:
            assert key in record["extra_str"]
            assert f"val_{key}" in record["extra_str"]


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
