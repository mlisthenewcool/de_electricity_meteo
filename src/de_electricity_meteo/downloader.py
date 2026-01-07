"""Async download and archive extraction utilities.

This module provides robust utilities for downloading large files over HTTP
with automatic retry logic and extracting files from 7z archives.
"""

import asyncio
import shutil
import tempfile
from concurrent.futures import ThreadPoolExecutor
from functools import wraps
from pathlib import Path
from typing import Any, Callable, Coroutine, TypeVar

import aiofiles
import aiohttp
import py7zr
from tqdm.asyncio import tqdm

from de_electricity_meteo.config.paths import DATA_BRONZE
from de_electricity_meteo.config.settings import (
    DOWNLOAD_CHUNK_SIZE,
    DOWNLOAD_TIMEOUT_CONNECT,
    DOWNLOAD_TIMEOUT_SOCK_READ,
    DOWNLOAD_TIMEOUT_TOTAL,
    RETRY_BACKOFF_FACTOR,
    RETRY_INITIAL_DELAY,
    RETRY_MAX_ATTEMPTS,
)
from de_electricity_meteo.enums import ExistingFileAction
from de_electricity_meteo.exceptions import (
    ArchiveNotFoundError,
    FileIntegrityError,
    FileNotFoundInArchiveError,
    RetryExhaustedError,
)
from de_electricity_meteo.logger import logger

# Type alias for generic async functions
T = TypeVar("T")
AsyncFunc = Callable[..., Coroutine[Any, Any, T]]


def stream_retry(
    max_retries: int = RETRY_MAX_ATTEMPTS,
    start_delay: float = RETRY_INITIAL_DELAY,
    backoff_factor: float = RETRY_BACKOFF_FACTOR,
    exceptions: tuple[type[Exception], ...] = (aiohttp.ClientError, asyncio.TimeoutError),
) -> Callable[[AsyncFunc[T]], AsyncFunc[T]]:
    """A decorator that retries an asynchronous function with exponential backoff.

    This decorator catches specified exceptions and retries the decorated function
    until the maximum number of attempts is reached. The delay between retries
    increases exponentially according to the backoff factor.

    Args:
        max_retries: The maximum number of retry attempts before giving up.
            Defaults to RETRY_MAX_ATTEMPTS.
        start_delay: The initial delay between retries in seconds.
            Defaults to RETRY_INITIAL_DELAY.
        backoff_factor: The multiplier applied to the delay after each
            failed attempt. Defaults to RETRY_BACKOFF_FACTOR.
        exceptions: Tuple of exception types to catch and retry.
            Defaults to (aiohttp.ClientError, asyncio.TimeoutError).

    Returns:
        A wrapped asynchronous function that implements the retry logic.

    Raises:
        RetryExhaustedError: If all retry attempts have been exhausted.
    """

    def decorator(func: AsyncFunc[T]) -> AsyncFunc[T]:
        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> T:
            attempt = 0
            current_delay = start_delay
            last_exception: Exception | None = None

            # Extract URL from args/kwargs for error reporting
            url = kwargs.get("url") or (args[1] if len(args) > 1 else "unknown")

            while attempt <= max_retries:
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    attempt += 1
                    if attempt > max_retries:
                        logger.error(
                            "Download failed after all retries",
                            extra={"url": url, "attempts": attempt, "error": str(e)},
                        )
                        break

                    logger.warning(
                        "Retrying after error",
                        extra={
                            "attempt": attempt,
                            "max_attempts": max_retries + 1,
                            "delay_sec": round(current_delay, 1),
                            "error": str(e),
                        },
                    )
                    await asyncio.sleep(current_delay)
                    current_delay *= backoff_factor

            if last_exception:
                raise RetryExhaustedError(url=str(url), attempts=attempt, last_error=last_exception)

            raise RetryExhaustedError(
                url=str(url), attempts=attempt, last_error=RuntimeError("Unknown failure")
            )

        return wrapper

    return decorator


@stream_retry()
async def download_to_file(
    session: aiohttp.ClientSession,
    url: str,
    dest_path: Path,
    chunk_size: int = DOWNLOAD_CHUNK_SIZE,
    if_exists: ExistingFileAction = ExistingFileAction.SKIP,
) -> int:
    """Downloads a stream from a URL and saves it to disk using async buffers.

    This function performs a memory-efficient download by streaming the response
    content in chunks and writing them asynchronously to the destination path.
    It automatically creates parent directories if they do not exist.

    Args:
        session: An active aiohttp ClientSession used to perform the request.
        url: The absolute URL of the file to download.
        dest_path: The local filesystem path where the file will be stored.
        chunk_size: The size of each data chunk to read from the network and
            write to disk, in bytes. Defaults to DOWNLOAD_CHUNK_SIZE.
        if_exists: Action when dest_path already exists. Defaults to OVERWRITE.

    Returns:
        The total number of bytes successfully written to the file, or 0 if skipped.

    Raises:
        FileExistsError: If if_exists is ERROR and the file already exists.
        RetryExhaustedError: If all retry attempts fail due to network errors.
        aiohttp.ClientResponseError: If the server returns an error status
            code (4xx or 5xx).
        OSError: If a filesystem error occurs while creating directories
            or writing the file.
    """
    if dest_path.exists():
        match if_exists:
            case ExistingFileAction.SKIP:
                logger.debug(
                    "Skipping download: file exists",
                    extra={"path": str(dest_path)},
                )
                return 0
            case ExistingFileAction.ERROR:
                raise FileExistsError(f"File already exists: {dest_path}")
            case ExistingFileAction.OVERWRITE:
                logger.debug(
                    "Overwriting existing file",
                    extra={"path": str(dest_path)},
                )

    logger.info("Starting download", extra={"url": url, "dest": dest_path.name})

    timeout = aiohttp.ClientTimeout(
        total=DOWNLOAD_TIMEOUT_TOTAL,
        connect=DOWNLOAD_TIMEOUT_CONNECT,
        sock_read=DOWNLOAD_TIMEOUT_SOCK_READ,
    )

    async with session.get(url, timeout=timeout) as response:
        response.raise_for_status()

        total_bytes = 0
        dest_path.parent.mkdir(parents=True, exist_ok=True)

        total_size = int(response.headers.get("content-length", 0))

        progress_bar = tqdm(
            total=total_size,
            unit="iB",
            unit_scale=True,
            unit_divisor=1024,
            desc=f"Downloading {dest_path.name}",
            leave=False,  # the bar disappears once finished
        )

        try:
            async with aiofiles.open(dest_path, mode="wb") as f:
                async for chunk in response.content.iter_chunked(chunk_size):
                    await f.write(chunk)
                    chunk_len = len(chunk)
                    total_bytes += chunk_len
                    progress_bar.update(chunk_len)
        finally:
            progress_bar.close()

        logger.info(
            "Download completed",
            extra={"path": dest_path.name, "size_mb": round(total_bytes / (1024**2), 2)},
        )
        return total_bytes


def validate_sqlite_header(path: Path) -> None:
    """Validates that a file has a valid SQLite/GeoPackage header.

    GeoPackage files are SQLite databases and must start with the standard
    SQLite file header. This function performs a quick validation by checking
    the first 16 bytes of the file.

    Args:
        path: Path to the file to validate.

    Raises:
        FileIntegrityError: If the file is missing, empty, or has an invalid header.
    """
    if not path.exists():
        raise FileIntegrityError(path, "File does not exist")

    if path.stat().st_size == 0:
        raise FileIntegrityError(path, "File is empty")

    try:
        with path.open(mode="rb") as f:
            header = f.read(16)
            if header != b"SQLite format 3\x00":
                raise FileIntegrityError(path, "Invalid SQLite/GeoPackage header")
    except OSError as e:
        raise FileIntegrityError(path, f"Could not read file header: {e}")


def extract_7z_sync(
    archive_path: Path,
    target_filename: str,
    dest_path: Path,
    if_exists: ExistingFileAction = ExistingFileAction.SKIP,
    validate_sqlite: bool = True,
) -> None:
    """Extracts a specific file from a 7z archive atomically.

    Searches for the file within the archive's internal structure, extracts it
    to a temporary directory, and then moves it to its final destination.

    Args:
        archive_path: Path to the source .7z archive.
        target_filename: Name or suffix of the file to extract.
        dest_path: Final destination path for the extracted file.
        if_exists: Action when dest_path already exists. Defaults to OVERWRITE.
        validate_sqlite: If True, validates the extracted file has a valid
            SQLite/GeoPackage header. Defaults to True.

    Raises:
        ArchiveNotFoundError: If the archive_path does not exist.
        FileNotFoundInArchiveError: If target_filename is not found in the archive.
        FileExistsError: If if_exists is ERROR and the file already exists.
        FileIntegrityError: If validation is enabled and the file is invalid.
        py7zr.exceptions.ArchiveError: If the archive is corrupted or invalid.
        OSError: If a filesystem error occurs during extraction or file moving.
    """
    if not archive_path.exists():
        raise ArchiveNotFoundError(archive_path)

    if dest_path.exists():
        match if_exists:
            case ExistingFileAction.SKIP:
                logger.debug(
                    "Skipping extraction: file exists",
                    extra={"path": str(dest_path)},
                )
                return
            case ExistingFileAction.ERROR:
                raise FileExistsError(f"File already exists: {dest_path}")
            case ExistingFileAction.OVERWRITE:
                logger.debug(
                    "Overwriting existing file",
                    extra={"path": str(dest_path)},
                )

    logger.info(
        "Starting extraction",
        extra={"archive": archive_path.name, "target": target_filename},
    )

    with tempfile.TemporaryDirectory(prefix="tmp_dir_7z_extract") as tmp_dir:
        tmp_dir_path = Path(tmp_dir)

        with py7zr.SevenZipFile(archive_path, mode="r") as archive:
            all_files = archive.getnames()

            # Flexible search to handle nested internal directories
            # IGN data archives have inconsistent internal structures
            try:
                target_internal_path = next(f for f in all_files if f.endswith(target_filename))
            except StopIteration:
                raise FileNotFoundInArchiveError(target_filename, archive_path)

            logger.debug(
                "Found target in archive",
                extra={"internal_path": target_internal_path},
            )
            archive.extract(path=tmp_dir_path, targets=[target_internal_path])
            extracted_file = tmp_dir_path / target_internal_path

            if dest_path.exists():
                dest_path.unlink()

            shutil.move(src=extracted_file, dst=dest_path)

            if validate_sqlite:
                try:
                    validate_sqlite_header(dest_path)
                except FileIntegrityError:
                    if dest_path.exists():
                        dest_path.unlink()
                    raise

            size_mb = round(dest_path.stat().st_size / 1024**2, 2)
            logger.info(
                "Extraction completed",
                extra={"path": dest_path.name, "size_mb": size_mb},
            )


async def extract_7z_async(
    archive_path: Path,
    target_filename: str,
    dest_path: Path,
    if_exists: ExistingFileAction = ExistingFileAction.SKIP,
    validate_sqlite: bool = True,
) -> None:
    """Wraps the 7z extraction in an executor to avoid blocking the event loop.

    This function serves as the asynchronous entry point for extraction. It delegates
    the CPU-intensive decompression work to a separate thread pool.

    Args:
        archive_path: Path to the source .7z archive.
        target_filename: Exact name or suffix of the file to extract.
        dest_path: Final destination path for the extracted file.
        if_exists: Action when dest_path already exists. Defaults to OVERWRITE.
        validate_sqlite: If True, validates the extracted file has a valid
            SQLite/GeoPackage header. Defaults to True.

    Raises:
        ArchiveNotFoundError: If the archive_path does not exist.
        FileNotFoundInArchiveError: If target_filename is not found in the archive.
        FileExistsError: If if_exists is ERROR and the file already exists.
        FileIntegrityError: If validation is enabled and the file is invalid.
    """
    loop = asyncio.get_running_loop()

    # Use a ThreadPoolExecutor for CPU-bound tasks like decompression
    # max_workers=1 ensures thread safety for file operations (unlink, move)
    with ThreadPoolExecutor(max_workers=1) as pool:
        await loop.run_in_executor(
            pool,
            extract_7z_sync,
            archive_path,
            target_filename,
            dest_path,
            if_exists,
            validate_sqlite,
        )


if __name__ == "__main__":
    # Manual test functions for development - use pytest for actual testing

    async def download_contours_iris(if_exists: ExistingFileAction) -> None:
        """Download and extract CONTOURS-IRIS GeoPackage from IGN."""
        url = (
            "https://data.geopf.fr/telechargement/download/CONTOURS-IRIS/"
            "CONTOURS-IRIS_3-0__GPKG_LAMB93_FXX_2025-01-01/"
            "CONTOURS-IRIS_3-0__GPKG_LAMB93_FXX_2025-01-01.7z"
        )
        archive_path = DATA_BRONZE / "CONTOURS-IRIS_3-0__GPKG_LAMB93_FXX_2025-01-01.7z"
        dest_path = DATA_BRONZE / "iris.gpkg"

        async with aiohttp.ClientSession() as session:
            try:
                await download_to_file(session, url, archive_path, if_exists=if_exists)
            except RetryExhaustedError as e:
                logger.error("Download failed", extra={"error": str(e)})
                return

        try:
            await extract_7z_async(
                archive_path=archive_path,
                target_filename="iris.gpkg",
                dest_path=dest_path,
                if_exists=if_exists,
            )
        except (ArchiveNotFoundError, FileNotFoundInArchiveError, FileIntegrityError) as e:
            logger.error("Extraction failed", extra={"error": str(e)})

    async def download_admin_express_cog(if_exists: ExistingFileAction) -> None:
        """Download and extract ADMIN-EXPRESS-COG GeoPackage from IGN."""
        url = (
            "https://data.geopf.fr/telechargement/download/ADMIN-EXPRESS-COG/"
            "ADMIN-EXPRESS-COG_4-0__GPKG_WGS84G_FRA_2025-01-01/"
            "ADMIN-EXPRESS-COG_4-0__GPKG_WGS84G_FRA_2025-01-01.7z"
        )
        archive_path = DATA_BRONZE / "ADMIN-EXPRESS-COG_4-0__GPKG_WGS84G_FRA_2025-01-01.7z"
        target_filename = "ADE-COG_4-0_GPKG_WGS84G_FRA-ED2025-01-01.gpkg"
        dest_path = DATA_BRONZE / "admin_express_cog.gpkg"

        async with aiohttp.ClientSession() as session:
            try:
                await download_to_file(session, url, archive_path, if_exists=if_exists)
            except RetryExhaustedError as e:
                logger.error("Download failed", extra={"error": str(e)})
                return

        try:
            await extract_7z_async(
                archive_path=archive_path,
                target_filename=target_filename,
                dest_path=dest_path,
                if_exists=if_exists,
            )
        except (ArchiveNotFoundError, FileNotFoundInArchiveError, FileIntegrityError) as e:
            logger.error("Extraction failed", extra={"error": str(e)})

    # Run the desired download function
    asyncio.run(download_contours_iris(ExistingFileAction.SKIP))
    asyncio.run(download_admin_express_cog(ExistingFileAction.SKIP))
