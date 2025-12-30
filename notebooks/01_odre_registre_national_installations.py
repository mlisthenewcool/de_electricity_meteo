import marimo

__generated_with = "0.18.4"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    return mo, pl


@app.cell
def _():
    from de_electricity_meteo.config.paths import (
        ODRE_REGISTRE_NATIONAL_INSTALLATIONS_BRONZE,
    )
    return (ODRE_REGISTRE_NATIONAL_INSTALLATIONS_BRONZE,)


@app.cell
def _(ODRE_REGISTRE_NATIONAL_INSTALLATIONS_BRONZE, pl):
    df = pl.scan_parquet(source=ODRE_REGISTRE_NATIONAL_INSTALLATIONS_BRONZE).collect()
    return (df,)


@app.cell
def _(df, mo):
    mo.ui.dataframe(df)
    return


@app.cell
def _(df, pl):
    pk_cols = ["idpeps", "codeeicresourceobject"]

    def check_duplicated_on_pk(cols: list[str]):
        for col in cols:
            print(
                f"duplicated rows on '{col}':",
                df.filter(
                    pl.col(col).is_not_null() & pl.col(col).is_duplicated()
                ).shape[0],
            )

    check_duplicated_on_pk(cols=pk_cols)
    return


@app.cell
def _():
    return


@app.cell
def _(df, pl):
    # check datederaccordement
    df.filter(
        pl.col("datederaccordement").is_not_null()
    )    # datederaccordement     2022-02-01
    # datemiseenservice      2010-02-26
    # datedebutversion       2022-09-21
    # datemiseenservice_date 2010-02-26
    return


@app.cell
def _(df, pl):
    # check datemiseenservice <> datedebutversion
    df.filter(
        pl.col("datedebutversion").is_not_null()
        & pl.col("datemiseenservice").ne_missing(other=pl.col("datedebutversion"))
    )
    return


@app.cell
def _():
    # todo: datemiseenservice_date == 1897-03-21 ???
    return


@app.cell
def _(mo):
    mo.md(r"""
    -> check power values to select the best one(s)s
    """)
    return


@app.cell
def _(df, pl):
    df.select(pl.col("puismaxinstallee")).to_series().equals(
        other=df.select(pl.col("maxpuis")).to_series()
    )
    return


@app.cell
def _(df, pl):
    df.filter(pl.col("puismaxinstallee").ne_missing(other=pl.col("maxpuis")))
    return


@app.cell
def _(df, pl):
    for col in ["puismaxinstallee", "maxpuis"]:
        print(col, df.select(pl.col(col).sum()).item())
    return


@app.cell
def _(df, pl):
    df.select(pl.selectors.contains("puis").sum().round(decimals=3))
    return


@app.cell
def _(df, pl):
    df.select(pl.selectors.contains("energie").sum().round(decimals=3))
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
