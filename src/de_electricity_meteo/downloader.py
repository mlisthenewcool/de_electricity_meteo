import asyncio
from functools import wraps
from pathlib import Path
from typing import Any, Callable, Coroutine, TypeVar

import aiofiles
import aiohttp
from tqdm.asyncio import tqdm

from de_electricity_meteo.config.paths import DATA
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


if __name__ == "__main__":

    async def mock():
        """Quick function to test speed."""
        # URL d'un fichier de 100MB pour bien voir la barre de progression
        file = "1GB.bin"
        TEST_URL = f"https://ash-speed.hetzner.com/{file}"
        dest = DATA / file

        async with aiohttp.ClientSession() as session:
            try:
                print(f"Démarrage du test vers {dest}...")
                start_time = asyncio.get_event_loop().time()

                size = await download_to_file(session, TEST_URL, dest)

                end_time = asyncio.get_event_loop().time()
                duration = end_time - start_time
                speed_mbps = (size / (1024**2)) / duration

                print("\nTest terminé !")
                print(f"Vitesse moyenne : {speed_mbps:.2f} MB/s")

            except Exception as e:
                print(f"Le test a échoué : {e}")

    asyncio.run(mock())
