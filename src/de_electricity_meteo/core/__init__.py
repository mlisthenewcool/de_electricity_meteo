"""Core infrastructure modules.

This package contains shared utilities used across all domain packages:
- downloader: Async HTTP download with retry logic
- logger: Loguru-based logging with colored output
- enums: Shared enumerations
- exceptions: Custom exception classes
"""

from de_electricity_meteo.core.downloader import (
    download_to_file,
    extract_7z_async,
    extract_7z_sync,
    stream_retry,
    validate_sqlite_header,
)
from de_electricity_meteo.core.enums import ExistingFileAction
from de_electricity_meteo.core.exceptions import (
    ArchiveNotFoundError,
    FileIntegrityError,
    FileNotFoundInArchiveError,
    RetryExhaustedError,
)
from de_electricity_meteo.core.logger import logger

__all__ = [
    # downloader
    "download_to_file",
    "extract_7z_async",
    "extract_7z_sync",
    "stream_retry",
    "validate_sqlite_header",
    # enums
    "ExistingFileAction",
    # exceptions
    "ArchiveNotFoundError",
    "FileIntegrityError",
    "FileNotFoundInArchiveError",
    "RetryExhaustedError",
    # logger
    "logger",
]
