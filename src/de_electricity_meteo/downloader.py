import asyncio
import io
from functools import wraps
from pathlib import Path
from typing import Any, Callable

import aiofiles
import aiohttp

from de_electricity_meteo.logger import logger


def stream_retry(
    max_retries: int = 3, start_delay: float = 1.0, backoff_factor: float = 2.0
):
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(url: str, *args, **kwargs) -> Any:
            attempt = 0
            current_delay = start_delay
            last_exception = None

            # todo: is it really the fastest way to download ?
            buffer_size = io.DEFAULT_BUFFER_SIZE

            while attempt <= max_retries:
                try:
                    async with aiohttp.ClientSession() as session:
                        async with session.get(
                            url,
                            timeout=aiohttp.ClientTimeout(
                                total=None, sock_read=300
                            ),  # todo: ?
                        ) as response:
                            response.raise_for_status()

                            logger.info(
                                "Connection established",
                                extra={
                                    "url": url,
                                    "buffer_size": buffer_size,
                                    "attempt": attempt + 1,
                                },
                            )

                            # have to forward buffer_size to the saving function
                            return await func(
                                response, *args, buffer_size=buffer_size, **kwargs
                            )

                except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                    last_exception = e
                    attempt += 1
                    if attempt > max_retries:
                        logger.error(
                            "Échec définitif", extra={"url": url, "error": str(e)}
                        )
                        break

                    logger.warning(
                        "Retry...", extra={"url": url, "delay": current_delay}
                    )
                    await asyncio.sleep(current_delay)
                    current_delay *= backoff_factor

            raise last_exception

        return wrapper

    return decorator


@stream_retry(max_retries=3)
async def save_file(response: aiohttp.ClientResponse, path: Path, buffer_size: int):
    """Téléchargement asynchrone pur : Réseau (aiohttp) -> Disque (aiofiles)."""
    total_bytes_written = 0

    # Utilisation de aiofiles pour ne pas bloquer l'event loop
    async with aiofiles.open(path, mode="wb") as f:
        async for chunk in response.content.iter_chunked(buffer_size):
            await f.write(chunk)
            total_bytes_written += len(chunk)

    logger.info(
        "Téléchargement terminé",
        extra={
            "path": path,
            "size_mb": round(total_bytes_written / (1024 * 1024), 2),
        },
    )


if __name__ == "__main__":
    # tasks = await [asyncio.create_task() for _ in range(10)]
    # await asyncio.gather(*tasks)

    async def main():
        # https://docs.aiohttp.org/en/stable/streams.html
        ODRE_PARQUET_URL = (
            "https://odre.opendatasoft.com/api/explore/v2.1/catalog/datasets/"
            "registre-national-installation-production-stockage-electricite-agrege/"
            "exports/parquet?lang=fr&timezone=Europe%2FBerlin"
        )
        try:
            data_dir = Path("data")
            parquet_path = data_dir / "bronze/installations.parquet"
            await save_file(ODRE_PARQUET_URL, dest_path=parquet_path)
        except Exception as e:
            print(f"Erreur fatale : {e}")

    asyncio.run(main())
