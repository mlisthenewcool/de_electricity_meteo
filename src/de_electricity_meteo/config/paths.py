from pathlib import Path

ROOT_DIR = Path(__file__).parent.parent.parent.parent

DATA = ROOT_DIR / Path("data")

CONFIG = ROOT_DIR / Path("src/de_electricity_meteo/config")
LOGGER_CONFIG = CONFIG / "logger.yaml"
