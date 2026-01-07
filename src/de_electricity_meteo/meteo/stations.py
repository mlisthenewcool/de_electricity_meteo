# --- informations sur les stations
# → https://www.data.gouv.fr/datasets/informations-sur-les-stations-metadonnees/
# → https://meteo.data.gouv.fr/datasets/656dab84db1bdf627a40eaae
# 180Mo, toutes les stations (métropole + outre-mer)
# fréquence (annuelle ??)
# 1 seul fichier
# téléchargement : https://www.data.gouv.fr/api/1/datasets/r/1fe544d8-4615-4642-a307-5956a7d90922

# --- observations (temps réel)
# données "brutes", temps réel sur 24h, des stations en activité
# toutes les stations par département
# par heure donc OK
# bcp de fichiers (par département + par décennie)


# --- climatologiques (données qualifiées, mais aussi temps réel ?)
# → https://www.data.gouv.fr/datasets/donnees-climatologiques-de-base-horaires
# → https://meteo.data.gouv.fr/datasets/6569b4473bedf2e7abad3b72
# données qualifiées, archivées, des stations historisées, encore actives ou non
# https://confluence-meteofrance.atlassian.net/wiki/spaces/OpenDataMeteoFrance/pages/854261785/API+Donn+es+Climatologiques
# documentation des champs : https://www.data.gouv.fr/datasets/donnees-climatologiques-de-base-horaires?resource_id=48b7e4aa-3ca1-4a63-9ab1-a723357336ff
# granularité d'accès = 1 station
# maximum 1 an de données à la fois
import asyncio
from pathlib import Path

import aiohttp
import polars as pl

from de_electricity_meteo.config.paths import METEO_FRANCE_INFO_STATIONS_BRONZE
from de_electricity_meteo.core import download_to_file, logger

DOWNLOAD_INFO_STATIONS_URL = (
    "https://www.data.gouv.fr/api/1/datasets/r/1fe544d8-4615-4642-a307-5956a7d90922"
)


async def download_infos_stations(url: str, dest_path: Path) -> None:  # noqa: D103
    async with aiohttp.ClientSession() as session:
        try:
            await download_to_file(session=session, url=url, dest_path=dest_path)
        except Exception as e:
            logger.error(f"Failed to download {DOWNLOAD_INFO_STATIONS_URL}. Error: {e}")


if __name__ == "__main__":
    if METEO_FRANCE_INFO_STATIONS_BRONZE.exists():
        logger.warning(f"{METEO_FRANCE_INFO_STATIONS_BRONZE} already exists. Skipping.")
    else:
        asyncio.run(
            download_infos_stations(
                DOWNLOAD_INFO_STATIONS_URL, dest_path=METEO_FRANCE_INFO_STATIONS_BRONZE
            )
        )

    df = pl.read_json(METEO_FRANCE_INFO_STATIONS_BRONZE)

    print(df.shape)
    print(df.columns)
    print(df.head(10))
