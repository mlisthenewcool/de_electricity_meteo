import importlib
import logging
import sys
from pathlib import Path
from typing import Generator

import pytest
import yaml
from pytest_mock import MockerFixture

from de_electricity_meteo.enums import LoggerChoice
from de_electricity_meteo.logger import (
    get_safe_logger,
    is_logger_name_defined,
    load_config,
)


@pytest.fixture(autouse=True)
def reset_lru_cache() -> Generator[None, None, None]:
    """Fixture to clear the LRU cache before each test to ensure isolation."""
    # the following code ensures: clean (before test) -> test -> clean (after test)
    load_config.cache_clear()
    yield
    load_config.cache_clear()


class TestLogger:
    """Group of tests for the logging utility module."""

    def test_load_config_file_not_found(self) -> None:
        """Verify that FileNotFoundError is raised if the config path does not exist."""
        with pytest.raises(FileNotFoundError, match="Logging configuration file not found"):
            load_config(Path("non_existent_path.yaml"))

    def test_load_config_invalid_yaml(self, tmp_path: Path) -> None:
        """Verify that ValueError is raised if the YAML configuration is malformed."""
        config_file = tmp_path / "bad_config.yaml"
        config_file.write_text("invalid: yaml: : :")

        with pytest.raises(ValueError, match="Invalid YAML configuration"):
            load_config(config_file)

    def test_is_logger_name_defined(self, mocker: MockerFixture) -> None:
        """Check the detection of loggers in the logging manager's registry."""
        mocker.patch.dict("logging.Logger.manager.loggerDict", {"test_logger": mocker.Mock()})
        assert is_logger_name_defined("test_logger") is True
        assert is_logger_name_defined("unknown_logger") is False

    def test_get_safe_logger_success(self, tmp_path: Path, mocker: MockerFixture) -> None:
        """Verify that get_safe_logger returns a valid logger when everything is correct."""
        # 1. Create a valid YAML config
        config_file = tmp_path / "valid_config.yaml"
        config_content = {
            "version": 1,
            "loggers": {"dev": {"level": "DEBUG"}},
        }
        config_file.write_text(yaml.dump(config_content))

        # 2. Mock LoggerChoice to have a known value
        mock_choice = mocker.Mock(spec=LoggerChoice)
        mock_choice.value = "dev"

        # 3. Execute
        logger = get_safe_logger(config_file, mock_choice)

        assert isinstance(logger, logging.Logger)
        assert logger.name == "dev"

    def test_get_safe_logger_undefined_in_config(
        self, tmp_path: Path, mocker: MockerFixture
    ) -> None:
        """Verify that ValueError is raised if the requested logger is missing from YAML."""
        config_file = tmp_path / "valid_config.yaml"
        config_content = {"version": 1, "loggers": {"other": {}}}
        config_file.write_text(yaml.dump(config_content))

        mock_choice = mocker.Mock(spec=LoggerChoice)
        mock_choice.value = "missing_logger"

        with pytest.raises(ValueError, match="is not defined or was not created"):
            get_safe_logger(config_file, mock_choice)

    def test_lru_cache_efficiency(self, tmp_path: Path, mocker: MockerFixture) -> None:
        """Verify that the configuration file is read only once per test."""
        config_file = tmp_path / "cache_test.yaml"
        config_file.write_text(yaml.dump({"version": 1}))

        spy_load = mocker.spy(yaml, "safe_load")

        # First call: should read the file and call safe_load
        load_config(config_file)
        # Next calls: should use the cache (function body is not executed)
        load_config(config_file)
        load_config(config_file)
        load_config(config_file)

        assert spy_load.call_count == 1

    def test_logger_fallback_on_import_error(
        self, mocker: MockerFixture, request: pytest.FixtureRequest
    ) -> None:
        """Verify that a fallback logger is created if the initial configuration fails.

        This test targets the module-level try/except block by forcing a failure of
        `get_safe_logger` during a module reload.
        """

        # 1. Remove the reloaded module from sys.modules
        # This prevents the "fallback" state from leaking to other tests
        def cleanup() -> None:
            if "de_electricity_meteo.logger" in sys.modules:
                del sys.modules["de_electricity_meteo.logger"]

        request.addfinalizer(cleanup)

        # 2. Force get_safe_logger to fail
        mocker.patch(
            "de_electricity_meteo.logger.get_safe_logger",
            side_effect=Exception("Simulated failure"),
        )

        # 3. Mock logging functions within the target module's namespace
        # This ensures we catch calls made during the module reload
        mock_basic_config = mocker.patch("de_electricity_meteo.logger.logging.basicConfig")

        # Prepare a mock for the fallback logger
        mock_fallback_logger = mocker.Mock()
        mock_fallback_logger.name = "fallback"
        mock_get_logger = mocker.patch(
            "de_electricity_meteo.logger.logging.getLogger",
            return_value=mock_fallback_logger,
        )

        # 4. Force a module reload to trigger the top-level code execution
        module_name = "de_electricity_meteo.logger"
        if module_name in sys.modules:
            importlib.reload(sys.modules[module_name])
        else:
            importlib.import_module(module_name)

        # Import the module locally after reload to inspect its state
        import de_electricity_meteo.logger as logger_module  # noqa: PLC0415

        # 5. Verifications
        assert mock_basic_config.called, "basicConfig should be called on failure"
        mock_get_logger.assert_any_call("fallback")
        assert logger_module.logger.name == "fallback"
