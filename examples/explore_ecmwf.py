import marimo

__generated_with = "0.15.0"
app = marimo.App()


@app.cell
def _():
    import marimo as mo

    return (mo,)


@app.cell
def _(mo):
    mo.center(mo.md("# Exploring ECMWF data"))
    return


@app.cell
def _(mo):
    mo.md(
        r"""This notebook offers an exploration into historical cyclone forecasts from ECMWF, accessed from the [NCAR TIGGE dataset](https://rda.ucar.edu/datasets/d330003/#). All `cxml` files are processed into a tabular format and stored in an internal PostGIS database. Further documentation on data processing can be found in the [ocha-lens](https://ocha-lens.readthedocs.io/en/latest/) package documentation. This processing code also pulls examples from the [CLIMADA Petals](https://github.com/CLIMADA-project/climada_petals) project."""
    )
    return


@app.cell
def _():
    import pandas as pd
    import geopandas as gpd
    from sqlalchemy import text
    from dotenv import load_dotenv
    import plotly.express as px

    load_dotenv(dotenv_path="../ds-cyclones-pipeline/.env")

    import ocha_stratus as stratus

    STAGE = "dev"
    STORM_TABLE = "ecmwf_storms"
    TRACK_TABLE = "ecmwf_tracks_geo"
    return STAGE, STORM_TABLE, TRACK_TABLE, gpd, pd, px, stratus, text


@app.cell
def _(stratus):
    engine = stratus.get_engine(stage="dev")
    return (engine,)


@app.cell
def _(STORM_TABLE, TRACK_TABLE, engine, pd, text):
    with engine.connect() as conn:
        df_storms = pd.read_sql(
            text(f"select * from storms.{STORM_TABLE}"), con=conn
        )
        max_date = pd.read_sql(
            f"SELECT MAX(valid_time) as max_date FROM storms.{TRACK_TABLE}",
            conn,
        )
        most_recent_date = max_date["max_date"].iloc[0].strftime("%b %d, %Y")
        min_date = pd.read_sql(
            f"SELECT MIN(valid_time) as max_date FROM storms.{TRACK_TABLE}",
            conn,
        )
        first_date = min_date["max_date"].iloc[0].strftime("%b %d, %Y")
    return df_storms, first_date, most_recent_date


@app.cell
def _(STAGE, df_storms, first_date, mo, most_recent_date):
    database_stage = mo.stat(value=STAGE, label="Source database")

    total_storms = mo.stat(value=len(df_storms), label="Unique named storms")

    latest = mo.stat(value=most_recent_date, label="Most recent track point")
    first = mo.stat(value=first_date, label="Oldest track point")

    mo.hstack([latest, first, total_storms, database_stage], justify="center")
    return


@app.cell
def _(df_storms, mo, px):
    df_basin = (
        df_storms.groupby(["genesis_basin"])["index"].count().reset_index()
    )
    fig_basin = px.bar(
        df_basin,
        x="genesis_basin",
        y="index",
        template="simple_white",
        title="Count of storms per basin",
    )
    fig_basin.update_layout(
        margin=dict(l=0, r=0, t=40, b=0),
        yaxis=dict(showgrid=True, gridcolor="lightgrey", title="", ticks=""),
    )

    df_season = df_storms.groupby(["season"])["index"].count().reset_index()
    fig_season = px.line(
        df_season,
        x="season",
        y="index",
        template="simple_white",
        title="Count of storms per season",
    )
    fig_season.update_layout(
        margin=dict(l=0, r=0, t=40, b=0),
        yaxis=dict(showgrid=True, gridcolor="lightgrey", title="", ticks=""),
    )

    mo.hstack([mo.ui.plotly(fig_season), mo.ui.plotly(fig_basin)])
    return


@app.cell
def _(df_storms, mo):
    mo.accordion({"### See storm data": df_storms})
    return


@app.cell
def _(mo):
    mo.md(r"""## Track - level exploration""")
    return


@app.cell
def _(mo):
    mo.md(
        r"""Use the selector to explore track-level data with wind speed and pressure forecasts. A single storm may have multiple forecast tracks. The database contains additional tracks for non-named storms (and so are missing a `storm_id`). More complex queries to the database (ie. by spatial and temporal bounding boxes) may be necessary to include forecasts for a given storm before it was given a name. Storms that cross basins may also receive separate `storm_id`s -- see `sarai_sp_2020` and `sarai_si_2020`. """
    )
    return


@app.cell
def _(df_storms, mo):
    sid_selector = mo.ui.dropdown(
        options=df_storms.storm_id.unique()[::-1],
        value=df_storms.storm_id.unique()[-1],
        label="Select a storm SID:",
    )
    return (sid_selector,)


@app.cell
def _(sid_selector):
    sid_selector
    return


@app.cell
def _(TRACK_TABLE, engine, gpd, sid_selector):
    with engine.connect() as _conn:
        gdf_tracks = gpd.read_postgis(
            f"select * from storms.{TRACK_TABLE} where storm_id='{sid_selector.value}'",
            geom_col="geometry",
            con=_conn,
        )
    return (gdf_tracks,)


@app.cell
def _(df_storms, sid_selector):
    storm_name = df_storms[df_storms.storm_id == sid_selector.value][
        "name"
    ].iloc[0]
    storm_season = df_storms[df_storms.storm_id == sid_selector.value][
        "season"
    ].iloc[0]
    storm_basin = df_storms[df_storms.storm_id == sid_selector.value][
        "genesis_basin"
    ].iloc[0]
    return storm_basin, storm_name, storm_season


@app.cell
def _(gdf_tracks, mo, storm_basin, storm_name, storm_season):
    name = mo.stat(value=storm_name, label="Storm name")

    season = mo.stat(value=str(storm_season), label="Storm season")

    genesis_basin = mo.stat(
        value=storm_basin,
        label="Genesis basin",
    )

    n_forecasts = mo.stat(
        value=gdf_tracks.forecast_id.nunique(),
        label="Number of forecasts",
    )

    mo.hstack([name, genesis_basin, season, n_forecasts], justify="center")
    return


@app.cell
def _(gdf_tracks, px):
    fig = px.scatter_map(
        gdf_tracks,
        lat=gdf_tracks.geometry.y,
        lon=gdf_tracks.geometry.x,
        color="forecast_id",
        hover_data=["wind_speed", "pressure", "basin", "valid_time"],
        zoom=3,
    )
    fig.update_layout(
        margin=dict(l=0, r=0, t=0, b=20),
    )
    return


@app.cell
def _(gdf_tracks, mo):
    mo.accordion({"### See track data": gdf_tracks})
    return


if __name__ == "__main__":
    app.run()
