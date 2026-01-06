"""Custom exceptions for the data engineering pipeline.

This module defines domain-specific exceptions that provide clear error messages
and structured error information for debugging and error handling.
"""

from pathlib import Path


class DownloadError(Exception):
    """Base exception for download-related failures."""


class RetryExhaustedError(DownloadError):
    """Raised when all retry attempts have been exhausted.

    Attributes:
        url: The URL that failed to download.
        attempts: Total number of attempts made.
        last_error: The last exception that occurred.
    """

    def __init__(self, url: str, attempts: int, last_error: Exception) -> None:
        """Initializes RetryExhaustedError with failure details.

        Args:
            url: The URL that failed to download.
            attempts: Total number of attempts made.
            last_error: The last exception that occurred.
        """
        self.url = url
        self.attempts = attempts
        self.last_error = last_error
        super().__init__(
            f"Download failed after {attempts} attempts for URL: {url}. Last error: {last_error}"
        )


class ExtractionError(Exception):
    """Base exception for archive extraction failures."""


class ArchiveNotFoundError(ExtractionError):
    """Raised when the archive file does not exist.

    Attributes:
        archive_path: Path to the missing archive.
    """

    def __init__(self, archive_path: Path) -> None:
        """Initializes ArchiveNotFoundError.

        Args:
            archive_path: Path to the missing archive.
        """
        self.archive_path = archive_path
        super().__init__(f"Archive not found: {archive_path}")


class FileNotFoundInArchiveError(ExtractionError):
    """Raised when the target file is not found within the archive.

    Attributes:
        target_filename: Name of the file that was not found.
        archive_path: Path to the archive that was searched.
    """

    def __init__(self, target_filename: str, archive_path: Path) -> None:
        """Initializes FileNotFoundInArchiveError.

        Args:
            target_filename: Name of the file that was not found.
            archive_path: Path to the archive that was searched.
        """
        self.target_filename = target_filename
        self.archive_path = archive_path
        super().__init__(f"File {target_filename} not found in archive: {archive_path.name}")


class FileIntegrityError(Exception):
    """Raised when file validation fails.

    Attributes:
        file_path: Path to the file that failed validation.
        reason: Description of why validation failed.
    """

    def __init__(self, file_path: Path, reason: str) -> None:
        """Initializes FileIntegrityError.

        Args:
            file_path: Path to the file that failed validation.
            reason: Description of why validation failed.
        """
        self.file_path = file_path
        self.reason = reason
        super().__init__(f"File integrity check failed for {file_path.name}: {reason}")
