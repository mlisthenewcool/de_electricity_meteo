from pathlib import Path

ROOT_DIR = Path(__file__).parent.parent.parent.parent

DATA = ROOT_DIR / Path("data")
DATA_BRONZE = DATA / Path("bronze")

ODRE_REGISTRE_NATIONAL_INSTALLATIONS_BRONZE = (
    DATA_BRONZE / "odre_registre_national_installations.parquet"
)
METEO_FRANCE_INFO_STATIONS_BRONZE = DATA_BRONZE / "meteo_france_info_stations.json"
