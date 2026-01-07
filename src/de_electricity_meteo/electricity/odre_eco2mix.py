import polars as pl

from de_electricity_meteo.config.paths import DATA_BRONZE

url = (
    "https://odre.opendatasoft.com/api/explore/v2.1/catalog/datasets/eco2mix-regional-tr/exports/"
    "parquet?lang=fr&qv1=(date_heure:[2026-01-02T10:00:00Z TO 2026-01-02T10:59:59Z])"
    "&timezone=Europe/Berlin"
)

# or use API
# date_heure >= '2026-01-02T10:00:00Z' AND date_heure < '2026-01-02T10:59:59Z'

url_api = (
    "https://odre.opendatasoft.com/api/explore/v2.1/catalog/datasets/eco2mix-regional-tr/"
    "records?where=date_heure >= '2026-01-02T10:00:00Z' AND date_heure < '2026-01-02T10:59:59Z'"
    "&limit=-1"
)

MY_PATH = DATA_BRONZE / "eco2mix-regional-tr_test.parquet"

df = pl.read_parquet(MY_PATH)

print(df.shape)
print(df.columns)
print(df)

print("min", df.select(pl.col("date_heure").min()))
print("max", df.select(pl.col("date_heure").max()))
