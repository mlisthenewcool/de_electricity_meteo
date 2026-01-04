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

from de_electricity_meteo.config.paths import DATA, DATA_BRONZE
from de_electricity_meteo.logger import logger

# Type alias for generic async functions
T = TypeVar("T")
AsyncFunc = Callable[..., Coroutine[Any, Any, T]]


def stream_retry(
    max_retries: int = 3, start_delay: float = 1.0, backoff_factor: float = 2.0
) -> Callable[[AsyncFunc[T]], AsyncFunc[T]]:
    """A decorator that retries an asynchronous function with exponential backoff.

    This decorator catches network-related exceptions (`aiohttp.ClientError` and
    `asyncio.TimeoutError`) and retries the decorated function until the
    maximum number of attempts is reached.

    Args:
        max_retries: The maximum number of retry attempts before giving up.
            Defaults to 3.
        start_delay: The initial delay between retries in seconds.
            Defaults to 1.0.
        backoff_factor: The multiplier applied to the delay after each
            failed attempt. Defaults to 2.0.

    Returns:
        A wrapped asynchronous function that implements the retry logic.

    Raises:
        aiohttp.ClientError: If the request fails after all retry attempts
            due to client-side or network issues.
        asyncio.TimeoutError: If the request times out after all retry attempts.
        RuntimeError: If an unexpected failure occurs during the retry loop.
    """

    def decorator(func: AsyncFunc[T]) -> AsyncFunc[T]:
        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> T:
            attempt = 0
            current_delay = start_delay
            last_exception: Exception | None = None

            while attempt <= max_retries:
                try:
                    return await func(*args, **kwargs)
                except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                    last_exception = e
                    attempt += 1
                    if attempt > max_retries:
                        logger.error("Final failure", extra={"error": str(e)})
                        break

                    logger.warning(
                        "Retry attempt...",
                        extra={"attempt": attempt, "delay": current_delay},
                    )
                    await asyncio.sleep(current_delay)
                    current_delay *= backoff_factor

            if last_exception:
                raise last_exception
            raise RuntimeError("Unexpected retry failure")

        return wrapper

    return decorator


@stream_retry(max_retries=3)
async def download_to_file(
    session: aiohttp.ClientSession,
    url: str,
    dest_path: Path,
    chunk_size: int = 1024 * 1024,  # todo: add that to constants
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
            write to disk, in bytes. Defaults to 1,048,576 (1MB).

    Returns:
        The total number of bytes successfully written to the file.

    Raises:
        aiohttp.ClientResponseError: If the server returns an error status
            code (4xx or 5xx).
        aiohttp.ClientPayloadError: If the connection is broken or the data
            payload is corrupted during streaming.
        asyncio.TimeoutError: If the operation exceeds the 10-minute total
            timeout or the 30-second socket read timeout.
        OSError: If a filesystem error occurs while creating directories
            or writing the file.
    """
    # todo: use range between retries
    # todo: add timers to constants

    # Setting a reasonable timeout:
    # 10m total, 10s for connection, 30s to read any data from server
    timeout = aiohttp.ClientTimeout(total=600, connect=10, sock_read=30)

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
            leave=False,  # La barre disparaît une fois fini pour ne pas polluer le terminal
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
            extra={
                "path": str(dest_path),
                "size_mb": round(total_bytes / (1024**2), 2),
            },
        )
        return total_bytes


def validate_geopackage_file_integrity(path: Path) -> None:
    """Performs physical integrity checks on the given file.

    Args:
        path: Path to the file to validate.

    Raises:
        ValueError: If the file is empty or structurally invalid.
    """
    if not path.exists() or path.stat().st_size == 0:
        raise ValueError(f"File {path.name} is empty or missing.")

    # Basic GeoPackage/SQLite header check
    # A GeoPackage is a SQLite file, it must start with the 'SQLite format 3' string
    try:
        with path.open(mode="rb") as f:
            header = f.read(16)
            if header != b"SQLite format 3\x00":
                raise ValueError(f"File {path.name} is not a valid GeoPackage (invalid header).")
    except OSError as e:
        raise ValueError(f"Could not read file header: {e}")


def extract_7z_sync(
    archive_path: Path, target_filename: str, dest_path: Path, overwrite: bool = True
) -> None:
    """Extracts a specific file from a 7z archive atomically.

    Searches for the file within the archive's internal structure, extracts it
    to a temporary directory, and then moves it to its final destination.

    Args:
        archive_path: Path to the source .7z archive.
        target_filename: Name or suffix of the file to extract (e.g., 'iris.gpkg').
        dest_path: Final destination path for the extracted file.
        overwrite: If True, replaces existing file at dest_path. Defaults to True.

    Raises:
        FileNotFoundError: If the archive_path does not exist or the target_filename
            is not found within the archive.
        py7zr.exceptions.ArchiveError: If the archive is corrupted or invalid.
        OSError: If a filesystem error occurs during directory creation or file moving.
    """
    # todo: install 7zip on OS and use 'asyncio.create_subprocess_exec' to speed up

    if not archive_path.exists():
        raise FileNotFoundError(f"Archive {archive_path} not found")

    if dest_path.exists():
        if not overwrite:
            logger.debug(f"File {dest_path.name} already exists. Skipping (overwrite=False).")
            return
        else:
            logger.debug(f"File {dest_path.name} already exists. Overwriting (overwrite=True).")

    # use a system temporary directory to isolate the extraction process
    with tempfile.TemporaryDirectory(prefix="tmp_dir_7z_extract") as tmp_dir:
        tmp_dir_path = Path(tmp_dir)

        with py7zr.SevenZipFile(archive_path, mode="r") as archive:
            all_files = archive.getnames()

            # flexible search to handle nested internal directories
            # IGN data are a mess, so we need this method
            try:
                target_internal_path = next(f for f in all_files if f.endswith(target_filename))
            except StopIteration:
                raise FileNotFoundError(
                    f"File {target_filename} not found in archive {archive_path.name}"
                )

            # extract only the targeted file
            logger.debug(f"Extracting {target_internal_path} ...")
            archive.extract(path=tmp_dir_path, targets=[target_internal_path])
            extracted_file = tmp_dir_path / target_internal_path

            if dest_path.exists():
                dest_path.unlink()

            # ensure destination parent directory exists
            # dest_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(src=extracted_file, dst=dest_path)

            # validate the extracted file
            try:
                validate_geopackage_file_integrity(dest_path)
                logger.info(f"File successfully extracted to {dest_path}")
            except ValueError as e:
                if dest_path.exists():
                    dest_path.unlink()
                logger.error(e)
                raise  # todo: custom error ?


async def extract_7z_async(
    archive_path: Path, target_filename: str, dest_path: Path, overwrite: bool = True
) -> None:
    """Wraps the 7z extraction in an executor to avoid blocking the event loop.

    This function serves as the asynchronous entry point for extraction. It delegates
    the CPU-intensive decompression work to a separate thread pool.

    Args:
        archive_path: Path to the source .7z archive.
        target_filename: Exact name or suffix of the file to extract (e.g., 'iris.gpkg').
        dest_path: Final destination path for the extracted file.
        overwrite: If True, replaces existing file at dest_path. Defaults to True.
    """
    loop = asyncio.get_running_loop()

    # use a ThreadPoolExecutor for CPU-bound tasks like decompression
    # note: max_workers=1 is important because the sync function is neither thread and process safe
    #       could be problematic if several processes request the same dest_path (unlink, move, ...)
    with ThreadPoolExecutor(max_workers=1) as pool:
        await loop.run_in_executor(
            pool, extract_7z_sync, archive_path, target_filename, dest_path, overwrite
        )


if __name__ == "__main__":

    async def mock():
        """Quick function to test speed."""
        # URL d'un fichier de 100MB pour bien voir la barre de progression
        file = "1GB.bin"
        test_url = f"https://ash-speed.hetzner.com/{file}"
        dest = DATA / file

        async with aiohttp.ClientSession() as session:
            try:
                print(f"Démarrage du test vers {dest}...")
                start_time = asyncio.get_event_loop().time()

                size = await download_to_file(session, test_url, dest)

                end_time = asyncio.get_event_loop().time()
                duration = end_time - start_time
                speed_mbps = (size / (1024**2)) / duration

                print("\nTest terminé !")
                print(f"Vitesse moyenne : {speed_mbps:.2f} MB/s")

            except Exception as e:
                print(f"Le test a échoué : {e}")

    async def mock_contours_iris():
        """Quick function to test downlaod -> extraction."""
        _url = "https://data.geopf.fr/telechargement/download/CONTOURS-IRIS/CONTOURS-IRIS_3-0__GPKG_LAMB93_FXX_2025-01-01/CONTOURS-IRIS_3-0__GPKG_LAMB93_FXX_2025-01-01.7z"

        _archive_path = DATA_BRONZE / "CONTOURS-IRIS_3-0__GPKG_LAMB93_FXX_2025-01-01.7z"
        _target_filename = "iris.gpkg"
        _dest_path = DATA_BRONZE / "iris.gpkg"

        async with aiohttp.ClientSession() as session:
            try:
                _ = await download_to_file(session, _url, _archive_path)
            except Exception as e:
                logger.error(e)

        try:
            await extract_7z_async(
                archive_path=_archive_path,
                target_filename=_target_filename,
                dest_path=_dest_path,
                overwrite=True,
            )
        except Exception as e:
            logger.error(e)

    async def mock_admin_express_cogs():
        """Quick function to test download -> extraction."""
        _url = "https://data.geopf.fr/telechargement/download/ADMIN-EXPRESS-COG/ADMIN-EXPRESS-COG_4-0__GPKG_WGS84G_FRA_2025-01-01/ADMIN-EXPRESS-COG_4-0__GPKG_WGS84G_FRA_2025-01-01.7z"

        _archive_path = DATA_BRONZE / "ADMIN-EXPRESS-COG_4-0__GPKG_WGS84G_FRA_2025-01-01.7z"
        _target_filename = "ADE-COG_4-0_GPKG_WGS84G_FRA-ED2025-01-01.gpkg"
        _dest_path = DATA_BRONZE / "admin_express_cog.gpkg"

        async with aiohttp.ClientSession() as session:
            try:
                _ = await download_to_file(session, _url, _archive_path)
            except Exception as e:
                logger.error(e)

        try:
            await extract_7z_async(
                archive_path=_archive_path,
                target_filename=_target_filename,
                dest_path=_dest_path,
                overwrite=True,
            )
        except Exception as e:
            logger.error(e)

    asyncio.run(mock_contours_iris())
