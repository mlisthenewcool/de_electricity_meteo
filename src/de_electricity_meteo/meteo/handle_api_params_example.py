import asyncio
from datetime import datetime
from pathlib import Path

import aiohttp

from de_electricity_meteo.logger import logger


async def fetch_electricity_data(
    session: aiohttp.ClientSession, start_dt: datetime, end_dt: datetime, dest_path: Path
):
    """Télécharge les données RTE pour une plage horaire donnée."""
    base_url = "https://odre.opendatasoft.com/api/explore/v2.1/catalog/datasets/installations-registre-national/exports/json"

    # Construction de la clause WHERE (Lisible, aiohttp s'occupe de l'encodage)
    where_query = (
        f"date_heure >= '{start_dt.isoformat()}Z' AND date_heure <= '{end_dt.isoformat()}Z'"
    )

    params = {
        "where": where_query,
        "timezone": "Europe/Berlin",
        "limit": -1,  # Pour récupérer tous les enregistrements de la plage
    }

    try:
        async with session.get(base_url, params=params, timeout=30) as response:
            if response.status == 200:  # noqa: PLR2004
                data = await response.read()

                # Sauvegarde en Bronze
                dest_path.parent.mkdir(parents=True, exist_ok=True)
                with open(dest_path, "wb") as f:
                    f.write(data)

                logger.info(f"✅ Données sauvegardées dans {dest_path.name}")
            else:
                logger.error(f"❌ Erreur API {response.status} pour {start_dt}")

    except Exception as e:
        logger.error(f"⚠️ Erreur lors du téléchargement : {e}")


if __name__ == "__main__":

    async def main():  # noqa: D103
        # Exemple de plage : le 2 janvier 2026 de 10h à 11h
        start = datetime(2026, 1, 2, 10, 0, 0)
        end = datetime(2026, 1, 2, 10, 59, 59)

        file_name = Path(f"data/02_bronze/elec_prod_{start.strftime('%Y%m%d_%H')}.json")

        async with aiohttp.ClientSession() as session:
            await fetch_electricity_data(session, start, end, file_name)

    asyncio.run(main())
