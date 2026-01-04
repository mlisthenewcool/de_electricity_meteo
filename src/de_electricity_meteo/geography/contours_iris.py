import duckdb
import polars as pl

from de_electricity_meteo.config.paths import DATA_BRONZE

URL: str = "https://data.geopf.fr/telechargement/download/CONTOURS-IRIS/CONTOURS-IRIS_3-0__GPKG_LAMB93_FXX_2025-01-01/CONTOURS-IRIS_3-0__GPKG_LAMB93_FXX_2025-01-01.7z"

contours_iris_path = DATA_BRONZE / "iris.gpkg"

con = duckdb.connect()
con.install_extension("spatial")
con.load_extension("spatial")

# Liste les couches disponibles
layers = (
    con.execute(query="SELECT layers FROM st_read_meta(?)", parameters=[str(contours_iris_path)])
    .pl()
    .explode("layers")
    .unnest("layers")
)
pl.Config.set_tbl_width_chars(width=300)
pl.Config.set_tbl_rows(n=30)
print(layers)

#########################

query = """
    SELECT *, ST_AsGeoJSON(geometrie)
    FROM st_read(?, layer='contours_iris')
"""
iris = con.execute(query=query, parameters=[str(contours_iris_path)]).pl()
print(iris.shape)
print(iris.columns)
print(iris.sort(by="code_iris"))
