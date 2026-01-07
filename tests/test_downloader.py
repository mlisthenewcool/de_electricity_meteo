"""Tests for the downloader module.

This module tests async download utilities, retry logic, file validation,
and 7z archive extraction functions.
"""

import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import aiohttp
import pytest
from pytest_mock import MockerFixture

from de_electricity_meteo.core import (
    ArchiveNotFoundError,
    ExistingFileAction,
    FileIntegrityError,
    FileNotFoundInArchiveError,
    RetryExhaustedError,
    download_to_file,
    extract_7z_async,
    extract_7z_sync,
    stream_retry,
    validate_sqlite_header,
)

# Test constants to avoid magic numbers
EXPECTED_CALLS_TWO = 2
EXPECTED_CALLS_THREE = 3


class TestStreamRetry:
    """Tests for the stream_retry decorator."""

    @pytest.mark.asyncio
    async def test_success_on_first_attempt(self) -> None:
        """Function succeeds on first attempt without retrying."""
        call_count = 0

        @stream_retry(max_retries=3)
        async def successful_func() -> str:
            nonlocal call_count
            call_count += 1
            return "success"

        result = await successful_func()

        assert result == "success"
        assert call_count == 1

    @pytest.mark.asyncio
    async def test_success_after_retries(self) -> None:
        """Function succeeds after a few failed attempts."""
        call_count = 0

        @stream_retry(max_retries=3, start_delay=0.01)
        async def flaky_func() -> str:
            nonlocal call_count
            call_count += 1
            if call_count < EXPECTED_CALLS_THREE:
                raise aiohttp.ClientError("Temporary failure")
            return "success"

        result = await flaky_func()

        assert result == "success"
        assert call_count == EXPECTED_CALLS_THREE

    @pytest.mark.asyncio
    async def test_retry_exhausted_raises_error(self) -> None:
        """RetryExhaustedError is raised when all retries fail."""
        call_count = 0

        @stream_retry(max_retries=2, start_delay=0.01)
        async def always_fails(session: object, url: str) -> None:
            nonlocal call_count
            call_count += 1
            raise aiohttp.ClientError("Permanent failure")

        with pytest.raises(RetryExhaustedError) as exc_info:
            await always_fails(None, "https://example.com/file")

        assert exc_info.value.attempts == EXPECTED_CALLS_THREE  # initial + 2 retries
        assert "https://example.com/file" in exc_info.value.url
        assert call_count == EXPECTED_CALLS_THREE

    @pytest.mark.asyncio
    async def test_timeout_error_triggers_retry(self) -> None:
        """asyncio.TimeoutError triggers retry mechanism."""
        call_count = 0

        @stream_retry(max_retries=2, start_delay=0.01)
        async def timeout_func() -> str:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise asyncio.TimeoutError("Connection timed out")
            return "recovered"

        result = await timeout_func()

        assert result == "recovered"
        assert call_count == EXPECTED_CALLS_TWO

    @pytest.mark.asyncio
    async def test_non_retryable_exception_propagates(self) -> None:
        """Exceptions not in the retry list propagate immediately."""

        @stream_retry(max_retries=3, exceptions=(aiohttp.ClientError,))
        async def value_error_func() -> None:
            raise ValueError("This should not be retried")

        with pytest.raises(ValueError, match="This should not be retried"):
            await value_error_func()

    @pytest.mark.asyncio
    async def test_custom_exceptions_parameter(self) -> None:
        """Custom exception types can be specified for retry."""
        call_count = 0

        @stream_retry(max_retries=2, start_delay=0.01, exceptions=(KeyError,))
        async def key_error_func() -> str:
            nonlocal call_count
            call_count += 1
            if call_count < EXPECTED_CALLS_TWO:
                raise KeyError("missing key")
            return "success"

        result = await key_error_func()

        assert result == "success"
        assert call_count == EXPECTED_CALLS_TWO


class TestValidateSqliteHeader:
    """Tests for the validate_sqlite_header function."""

    def test_valid_sqlite_file(self, tmp_path: Path) -> None:
        """Valid SQLite file passes validation."""
        sqlite_file = tmp_path / "valid.db"
        # SQLite header: "SQLite format 3\0"
        sqlite_file.write_bytes(b"SQLite format 3\x00" + b"\x00" * 100)

        # Should not raise
        validate_sqlite_header(sqlite_file)

    def test_file_does_not_exist(self, tmp_path: Path) -> None:
        """FileIntegrityError raised when file does not exist."""
        missing_file = tmp_path / "missing.db"

        with pytest.raises(FileIntegrityError) as exc_info:
            validate_sqlite_header(missing_file)

        assert "does not exist" in exc_info.value.reason

    def test_empty_file(self, tmp_path: Path) -> None:
        """FileIntegrityError raised when file is empty."""
        empty_file = tmp_path / "empty.db"
        empty_file.touch()

        with pytest.raises(FileIntegrityError) as exc_info:
            validate_sqlite_header(empty_file)

        assert "empty" in exc_info.value.reason

    def test_invalid_header(self, tmp_path: Path) -> None:
        """FileIntegrityError raised when header is invalid."""
        invalid_file = tmp_path / "invalid.db"
        invalid_file.write_bytes(b"Not a SQLite file header")

        with pytest.raises(FileIntegrityError) as exc_info:
            validate_sqlite_header(invalid_file)

        assert "Invalid SQLite" in exc_info.value.reason

    def test_file_too_short(self, tmp_path: Path) -> None:
        """FileIntegrityError raised when file is shorter than header."""
        short_file = tmp_path / "short.db"
        short_file.write_bytes(b"SQLite")  # Only 6 bytes

        with pytest.raises(FileIntegrityError) as exc_info:
            validate_sqlite_header(short_file)

        assert "Invalid SQLite" in exc_info.value.reason


class TestExtract7zSync:
    """Tests for the extract_7z_sync function."""

    def test_archive_not_found(self, tmp_path: Path) -> None:
        """ArchiveNotFoundError raised when archive does not exist."""
        missing_archive = tmp_path / "missing.7z"
        dest = tmp_path / "output.gpkg"

        with pytest.raises(ArchiveNotFoundError) as exc_info:
            extract_7z_sync(missing_archive, "file.gpkg", dest)

        assert exc_info.value.archive_path == missing_archive

    def test_file_not_found_in_archive(self, tmp_path: Path, mocker: MockerFixture) -> None:
        """FileNotFoundInArchiveError raised when target file is not in archive."""
        archive = tmp_path / "archive.7z"
        archive.touch()
        dest = tmp_path / "output.gpkg"

        # Mock py7zr to return empty file list
        mock_archive = MagicMock()
        mock_archive.getnames.return_value = ["other_file.txt", "data/another.csv"]
        mock_archive.__enter__ = MagicMock(return_value=mock_archive)
        mock_archive.__exit__ = MagicMock(return_value=False)

        mocker.patch(
            "de_electricity_meteo.core.downloader.py7zr.SevenZipFile", return_value=mock_archive
        )

        with pytest.raises(FileNotFoundInArchiveError) as exc_info:
            extract_7z_sync(archive, "missing.gpkg", dest)

        assert exc_info.value.target_filename == "missing.gpkg"
        assert exc_info.value.archive_path == archive

    def test_skip_when_exists_and_skip_action(self, tmp_path: Path, mocker: MockerFixture) -> None:
        """Extraction is skipped when file exists and if_exists=SKIP."""
        archive = tmp_path / "archive.7z"
        archive.touch()
        dest = tmp_path / "output.gpkg"
        dest.touch()  # File already exists

        # py7zr should never be called
        mock_sevenzipfile = mocker.patch("de_electricity_meteo.core.downloader.py7zr.SevenZipFile")

        extract_7z_sync(archive, "file.gpkg", dest, if_exists=ExistingFileAction.SKIP)

        mock_sevenzipfile.assert_not_called()

    def test_error_when_exists_and_error_action(self, tmp_path: Path) -> None:
        """FileExistsError is raised when file exists and if_exists=ERROR."""
        archive = tmp_path / "archive.7z"
        archive.touch()
        dest = tmp_path / "output.gpkg"
        dest.touch()  # File already exists

        with pytest.raises(FileExistsError, match="already exists"):
            extract_7z_sync(archive, "file.gpkg", dest, if_exists=ExistingFileAction.ERROR)

    def test_successful_extraction(self, tmp_path: Path, mocker: MockerFixture) -> None:
        """Successful extraction of a file from archive."""
        archive = tmp_path / "archive.7z"
        archive.touch()
        dest = tmp_path / "output.gpkg"

        # Create a mock extracted file with valid SQLite header
        def mock_extract(path: Path, targets: list[str]) -> None:
            extracted = path / "nested/dir/iris.gpkg"
            extracted.parent.mkdir(parents=True, exist_ok=True)
            extracted.write_bytes(b"SQLite format 3\x00" + b"\x00" * 100)

        mock_archive = MagicMock()
        mock_archive.getnames.return_value = ["nested/dir/iris.gpkg"]
        mock_archive.extract.side_effect = mock_extract
        mock_archive.__enter__ = MagicMock(return_value=mock_archive)
        mock_archive.__exit__ = MagicMock(return_value=False)

        mocker.patch(
            "de_electricity_meteo.core.downloader.py7zr.SevenZipFile", return_value=mock_archive
        )

        extract_7z_sync(archive, "iris.gpkg", dest)

        assert dest.exists()
        assert dest.read_bytes().startswith(b"SQLite format 3")

    def test_validation_failure_removes_file(self, tmp_path: Path, mocker: MockerFixture) -> None:
        """Invalid extracted file is removed and error is raised."""
        archive = tmp_path / "archive.7z"
        archive.touch()
        dest = tmp_path / "output.gpkg"

        # Create a mock extracted file with invalid header
        def mock_extract(path: Path, targets: list[str]) -> None:
            extracted = path / "iris.gpkg"
            extracted.write_bytes(b"Invalid content")

        mock_archive = MagicMock()
        mock_archive.getnames.return_value = ["iris.gpkg"]
        mock_archive.extract.side_effect = mock_extract
        mock_archive.__enter__ = MagicMock(return_value=mock_archive)
        mock_archive.__exit__ = MagicMock(return_value=False)

        mocker.patch(
            "de_electricity_meteo.core.downloader.py7zr.SevenZipFile", return_value=mock_archive
        )

        with pytest.raises(FileIntegrityError):
            extract_7z_sync(archive, "iris.gpkg", dest, validate_sqlite=True)

        # File should be cleaned up after validation failure
        assert not dest.exists()

    def test_skip_validation_when_disabled(self, tmp_path: Path, mocker: MockerFixture) -> None:
        """No validation when validate_sqlite=False."""
        archive = tmp_path / "archive.7z"
        archive.touch()
        dest = tmp_path / "output.txt"

        # Create a mock extracted non-SQLite file
        def mock_extract(path: Path, targets: list[str]) -> None:
            extracted = path / "data.txt"
            extracted.write_text("Just some text data")

        mock_archive = MagicMock()
        mock_archive.getnames.return_value = ["data.txt"]
        mock_archive.extract.side_effect = mock_extract
        mock_archive.__enter__ = MagicMock(return_value=mock_archive)
        mock_archive.__exit__ = MagicMock(return_value=False)

        mocker.patch(
            "de_electricity_meteo.core.downloader.py7zr.SevenZipFile", return_value=mock_archive
        )

        # Should not raise even though it's not a SQLite file
        extract_7z_sync(archive, "data.txt", dest, validate_sqlite=False)

        assert dest.exists()


class TestExtract7zAsync:
    """Tests for the extract_7z_async function."""

    @pytest.mark.asyncio
    async def test_delegates_to_sync(self, tmp_path: Path, mocker: MockerFixture) -> None:
        """Async function delegates to sync function in executor."""
        archive = tmp_path / "archive.7z"
        dest = tmp_path / "output.gpkg"

        mock_sync = mocker.patch(
            "de_electricity_meteo.core.downloader.extract_7z_sync",
            return_value=None,
        )

        await extract_7z_async(
            archive,
            "iris.gpkg",
            dest,
            if_exists=ExistingFileAction.OVERWRITE,
            validate_sqlite=False,
        )

        mock_sync.assert_called_once_with(
            archive, "iris.gpkg", dest, ExistingFileAction.OVERWRITE, False
        )

    @pytest.mark.asyncio
    async def test_propagates_exceptions(self, tmp_path: Path) -> None:
        """Exceptions from sync function are propagated."""
        missing_archive = tmp_path / "missing.7z"
        dest = tmp_path / "output.gpkg"

        with pytest.raises(ArchiveNotFoundError):
            await extract_7z_async(missing_archive, "file.gpkg", dest)


class TestDownloadToFile:
    """Tests for the download_to_file function."""

    @pytest.mark.asyncio
    async def test_successful_download(self, tmp_path: Path, mocker: MockerFixture) -> None:
        """Successful download writes content to file."""
        dest = tmp_path / "downloaded.bin"
        content = b"Test file content " * 100

        # Mock response
        mock_response = AsyncMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.headers = {"content-length": str(len(content))}

        async def mock_iter_chunked(chunk_size: int):
            for i in range(0, len(content), chunk_size):
                yield content[i : i + chunk_size]

        mock_response.content.iter_chunked = mock_iter_chunked
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        # Mock session
        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_response)

        # Disable tqdm for cleaner test output
        mocker.patch("de_electricity_meteo.core.downloader.tqdm", return_value=MagicMock())

        result = await download_to_file(mock_session, "https://example.com/file", dest)

        assert result == len(content)
        assert dest.exists()
        assert dest.read_bytes() == content

    @pytest.mark.asyncio
    async def test_creates_parent_directories(self, tmp_path: Path, mocker: MockerFixture) -> None:
        """Parent directories are created if they don't exist."""
        dest = tmp_path / "nested" / "dirs" / "file.bin"
        content = b"content"

        mock_response = AsyncMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.headers = {"content-length": str(len(content))}

        async def mock_iter_chunked(chunk_size: int):
            yield content

        mock_response.content.iter_chunked = mock_iter_chunked
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)

        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_response)

        mocker.patch("de_electricity_meteo.core.downloader.tqdm", return_value=MagicMock())

        await download_to_file(mock_session, "https://example.com/file", dest)

        assert dest.parent.exists()
        assert dest.exists()

    @pytest.mark.asyncio
    async def test_http_error_propagates(self, tmp_path: Path) -> None:
        """HTTP errors are propagated after retries are exhausted."""
        dest = tmp_path / "file.bin"

        # Test the retry decorator directly with custom settings for fast test
        @stream_retry(max_retries=0, start_delay=0.01)
        async def mock_download(session: object, url: str, dest_path: Path) -> int:
            raise aiohttp.ClientResponseError(
                request_info=MagicMock(),
                history=(),
                status=404,
                message="Not Found",
            )

        with pytest.raises(RetryExhaustedError) as exc_info:
            await mock_download(None, "https://example.com/missing", dest)

        assert exc_info.value.attempts == 1
        assert "https://example.com/missing" in exc_info.value.url
        assert isinstance(exc_info.value.last_error, aiohttp.ClientResponseError)

    @pytest.mark.asyncio
    async def test_skip_when_file_exists(self, tmp_path: Path, mocker: MockerFixture) -> None:
        """Download is skipped when file exists and if_exists=SKIP."""
        dest = tmp_path / "existing.bin"
        dest.write_bytes(b"existing content")

        mock_session = MagicMock()

        result = await download_to_file(
            mock_session, "https://example.com/file", dest, if_exists=ExistingFileAction.SKIP
        )

        assert result == 0
        assert dest.read_bytes() == b"existing content"
        mock_session.get.assert_not_called()

    @pytest.mark.asyncio
    async def test_error_when_file_exists(self, tmp_path: Path) -> None:
        """FileExistsError is raised when file exists and if_exists=ERROR."""
        dest = tmp_path / "existing.bin"
        dest.touch()

        mock_session = MagicMock()

        with pytest.raises(FileExistsError, match="already exists"):
            await download_to_file(
                mock_session, "https://example.com/file", dest, if_exists=ExistingFileAction.ERROR
            )
