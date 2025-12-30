from pathlib import Path

import polars as pl

from de_electricity_meteo.config.paths import (
    ODRE_REGISTRE_NATIONAL_INSTALLATIONS_BRONZE,
)
from de_electricity_meteo.downloader import save_file
from de_electricity_meteo.logger import logger

DOWNLOAD_URL = (
    "https://odre.opendatasoft.com/api/explore/v2.1/catalog/datasets/"
    "registre-national-installation-production-stockage-electricite-agrege/"
    "exports/parquet?lang=fr&timezone=Europe%2FBerlin"
)


async def download(url: str, path: Path) -> None:
    try:
        await save_file(url=url, path=path)
    except Exception as e:
        logger.error(f"Failed to download {DOWNLOAD_URL}. Error: {e}")


async def extract(path: Path) -> pl.LazyFrame:
    df = pl.scan_parquet(path)

    columns_to_extract = {
        "idpeps": "",
        "nominstallation": "nom_installation",
        "codeeicresourceobject": "",
        # geolocalisation
        "codeiris": "code_iris",  # best for geolocalisation
        "codeinseecommune": "code_insee_commune",  # second best for geolocalisation
        "codeepci": "code_epci",  # third
        "codedepartement": "code_departement",  # forth
        "coderegion": "code_region",  # fifth
        # details about production
        "codefiliere": "code_filiere",
        "codecombustible": "code_combustible",
        "codescombustiblessecondaires": "codes_combustibles_secondaires",
        "codetechnologie": "code_technologie",
        # details about stockage
        "typestockage": "type_stockage",
        # details about power
        "maxpuis": "max_puis",
        # other
        "regime": "regime",
        "gestionnaire": "gestionnaire",  # todo: codegestionnaire?
        # dates
        "datemiseenservice_date": "date_mise_en_service",
        # todo: vérifier si différences entre
        #  dateraccordement <> datemiseenservice <> datedebutversion
        # existe t-il des datederaccordement
    }

    print(columns_to_extract)

    # df.collect()

    return df


async def load() -> None:
    return None


async def transform() -> None:
    return None


async def pipeline() -> None:
    await download(
        url=DOWNLOAD_URL,
        path=ODRE_REGISTRE_NATIONAL_INSTALLATIONS_BRONZE,
    )

    await extract(path=ODRE_REGISTRE_NATIONAL_INSTALLATIONS_BRONZE)
    await load()
    await transform()


if __name__ == "__main__":
    # asyncio.run(pipeline())
    print(DOWNLOAD_URL)
