import duckdb
import polars as pl

from de_electricity_meteo.config.paths import DATA_BRONZE

URL: str = "https://data.geopf.fr/telechargement/download/ADMIN-EXPRESS-COG/ADMIN-EXPRESS-COG_4-0__GPKG_WGS84G_FRA_2025-01-01/ADMIN-EXPRESS-COG_4-0__GPKG_WGS84G_FRA_2025-01-01.7z"

"ADMIN-EXPRESS-COG_4-0__GPKG_WGS84G_FRA_2025-01-01"
"ADMIN-EXPRESS-COG"
"1_DONNEES_LIVRAISON_2025-06-00114"
"ADE-COG_4-0_GPKG_WGS84G_FRA-ED2025-01-01"
"ADE-COG_4-0_GPKG_WGS84G_FRA-ED2025-01-01.gpkg"


admin_express_cog_path = DATA_BRONZE / "admin_express_cog.gpkg"

con = duckdb.connect()
con.install_extension("spatial")
con.load_extension("spatial")

# Liste les couches disponibles
layers = (
    con.execute(
        query="SELECT layers FROM st_read_meta(?)", parameters=[str(admin_express_cog_path)]
    )
    .pl()
    .explode("layers")
    .unnest("layers")
)
pl.Config.set_tbl_width_chars(width=300)
pl.Config.set_tbl_rows(n=30)
print(layers)


# commune_layer = layers.filter(pl.col("name") == "commune")
# region_layer = layers.filter(pl.col("name") == "region")
# print(commune_layer.explode("fields"))
# print(region_layer.explode("fields"))

query = """
    SELECT *, ST_AsGeoJSON(geometrie)
    FROM st_read(?, layer='commune')
"""
communes = con.execute(query=query, parameters=[str(admin_express_cog_path)]).pl()
print(communes.shape)
print(communes.columns)
print(communes)
