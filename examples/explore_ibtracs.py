import marimo

__generated_with = "0.15.2"
app = marimo.App()


@app.cell
def _():
    import marimo as mo

    return (mo,)


@app.cell
def _(mo):
    mo.center(mo.md("# Exploring IBTrACS data"))
    return


@app.cell
def _(mo):
    mo.md(
        r"""This notebook offers a basic exploration of [IBTrACS](https://www.ncei.noaa.gov/products/international-best-track-archive) best-track cyclone data from NOAA. All data is stored in an internal database, and processed from the source IBTrACS NetCDF files as is defined in the [ocha-lens](https://ocha-lens.readthedocs.io/en/latest/datasets/ibtracs.html) package. Further technical documentation can also be found [here](https://www.ncei.noaa.gov/sites/g/files/anmtlf171/files/2025-04/IBTrACS_version4r01_Technical_Details.pdf). Following IBTrACS, the data in this database is updated three times a week (on Sunday, Tuesday, and Thursday)."""
    )
    return


@app.cell
def _():
    import pandas as pd
    import geopandas as gpd
    from sqlalchemy import text
    from dotenv import load_dotenv
    import plotly.express as px
    import io
    import numpy as np

    load_dotenv(dotenv_path="../ds-cyclones-pipeline/.env")

    import ocha_stratus as stratus

    STAGE = "dev"
    return STAGE, gpd, io, np, pd, px, stratus, text


@app.cell
def _(stratus):
    engine = stratus.get_engine(stage="dev")
    return (engine,)


@app.cell
def _(engine, pd, text):
    with engine.connect() as conn:
        df_storms = pd.read_sql(
            text("select * from storms.ibtracs_storms"), con=conn
        )
        max_date = pd.read_sql(
            "SELECT MAX(valid_time) as max_date FROM storms.ibtracs_tracks_geo",
            conn,
        )
        most_recent_date = max_date["max_date"].iloc[0].strftime("%b %d, %Y")
    return df_storms, most_recent_date


@app.cell
def _(STAGE, df_storms, mo, most_recent_date):
    df_provisional = df_storms[df_storms.provisional == True]  # noqa
    df_best = df_storms[df_storms.provisional == False]  # noqa

    database_stage = mo.stat(value=STAGE, label="Source database")

    total_storms = mo.stat(value=len(df_storms), label="Unique storms")

    provisional = mo.stat(
        value=len(df_provisional),
        label="Provisional storms",
    )

    best = mo.stat(
        value=len(df_best),
        label="Best track storms",
    )
    latest = mo.stat(value=most_recent_date, label="Most recent track point")

    mo.hstack(
        [latest, total_storms, provisional, best, database_stage],
        justify="center",
    )
    return


@app.cell
def _(df_storms, mo, px):
    df_basin = df_storms.groupby(["genesis_basin"]).count().reset_index()
    fig_basin = px.bar(
        df_basin,
        x="genesis_basin",
        y="sid",
        template="simple_white",
        title="Storms per basin",
    )
    fig_basin.update_layout(
        margin=dict(l=0, r=0, t=40, b=0),
        width=300,
        height=300,
        yaxis=dict(showgrid=True, gridcolor="lightgrey", title="", ticks=""),
    )

    df_season = df_storms.groupby(["season"]).count().reset_index()
    fig_season = px.line(
        df_season,
        x="season",
        y="sid",
        template="simple_white",
        title="Storms per season",
    )
    fig_season.update_layout(
        margin=dict(l=0, r=0, t=40, b=0),
        width=300,
        height=300,
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
        r"""We can also drill in more closely to look at the individual tracks for a given storm. The storm intensity measurements (such as wind speed, pressure, etc.) are retrieved differently depending on whether the storm is provisional or not. Provisional storms pull this data from the relevant USA Agency, while the official “best track” storms use the values reported by the relevant WMO Agency. **Note that different providers also have different averaging periods for wind speed reporting.** As such, comparisons of wind speed from different providers may not be accurate. Additional documentation forthcoming."""
    )
    return


@app.cell
def _(df_storms, mo):
    sid_selector = mo.ui.dropdown(
        options=df_storms.sid.unique()[::-1],
        value=df_storms.sid.unique()[-1],
        label="Select a storm SID:",
    )
    return (sid_selector,)


@app.cell
def _(df_storms, sid_selector):
    storm_name = df_storms[df_storms.sid == sid_selector.value]["name"].iloc[0]
    storm_season = df_storms[df_storms.sid == sid_selector.value][
        "season"
    ].iloc[0]
    storm_provisional = df_storms[df_storms.sid == sid_selector.value][
        "provisional"
    ].iloc[0]
    storm_atcf = df_storms[df_storms.sid == sid_selector.value][
        "atcf_id"
    ].iloc[0]
    storm_status = "Provisional" if storm_provisional else "Best"

    sid_selector
    return storm_atcf, storm_name, storm_season, storm_status


@app.cell
def _(mo, sid_selector):
    mo.md(
        f"""You can check [this IBTrACS source](https://ncics.org/ibtracs/index.php?name=v04r01-{sid_selector.value}) to validate the data."""
    )
    return


@app.cell
def _(engine, gpd, sid_selector):
    with engine.connect() as _conn:
        gdf_tracks = gpd.read_postgis(
            f"select * from storms.ibtracs_tracks_geo where sid='{sid_selector.value}'",
            geom_col="geometry",
            con=_conn,
        )
    return (gdf_tracks,)


@app.cell
def _(gdf_tracks, mo, np):
    cols = gdf_tracks.select_dtypes(include=[np.number]).columns.tolist()
    display_selector = mo.ui.dropdown(
        options=cols, value=cols[0], label="Select a column to display:"
    )
    return (display_selector,)


@app.cell
def _(display_selector):
    display_selector
    return


@app.cell
def _():
    return


@app.cell
def _(gdf_tracks, mo, storm_atcf, storm_name, storm_season, storm_status):
    name = mo.stat(value=storm_name, label="Storm name")

    season = mo.stat(value=str(storm_season), label="Storm season")

    status = mo.stat(
        value=storm_status,
        label="Track status",
    )

    n_points = mo.stat(
        value=len(gdf_tracks),
        label="Number of points",
    )

    atcf = mo.stat(value=storm_atcf, label="ATCF ID")

    mo.hstack([name, atcf, season, status, n_points], justify="center")
    return


@app.cell
def _(display_selector, gdf_tracks, px):
    fig = px.scatter_map(
        gdf_tracks,
        lat=gdf_tracks.geometry.y,
        lon=gdf_tracks.geometry.x,
        color=display_selector.value,
        color_continuous_scale="redor",
        hover_data=["wind_speed", "provider", "basin", "nature", "valid_time"],
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


@app.cell
def _(mo):
    mo.md(
        r"""
    ## Compare against HDX IBTrACS data

    Upload the latest [IBTrACS CSV from HDX](https://data.humdata.org/dataset/ibtracs-global-tropical-storm-tracks) to compare outputs.
    """
    )
    return


@app.cell
def _(mo):
    upload = mo.ui.file(filetypes=[".csv"])
    return (upload,)


@app.cell
def _(upload):
    upload
    return


@app.cell
def _(io, mo, pd, upload):
    if upload.value:
        file_contents = upload.value[0].contents.decode("utf-8")
        file_obj = io.StringIO(file_contents)
        df_hdx = pd.read_csv(file_obj, low_memory=False).drop(index=0)
        mo.output.clear()
    return (df_hdx,)


@app.cell
def _(df_hdx, df_storms, mo):
    missing_db = list(set(df_hdx.SID.unique()) - set(df_storms.sid.unique()))

    mo.accordion(
        {
            "Are there any storms reported on HDX that aren't in our database?": df_hdx[
                df_hdx.SID.isin(missing_db)
            ].drop_duplicates(subset=["SID"], keep="first")
        }
    )
    return


@app.cell
def _(df_hdx, df_storms, mo):
    missing_hdx = list(set(df_storms.sid.unique()) - set(df_hdx.SID.unique()))

    mo.accordion(
        {
            "Are there any storms in our database that aren't reported on HDX?": df_storms[
                df_storms.sid.isin(missing_hdx)
            ].drop_duplicates(subset=["SID"], keep="first")
        }
    )
    return


@app.cell
def _(df_hdx, mo, pd, sid_selector):
    df_hdx_ = df_hdx[df_hdx.SID == sid_selector.value]
    df_hdx_["ISO_TIME"] = pd.to_datetime(df_hdx_["ISO_TIME"])

    df_hdx_clean = df_hdx_.rename(
        columns={
            "SID": "sid",
            "BASIN": "basin",
            "ISO_TIME": "valid_time",
            "NATURE": "nature",
            "WMO_WIND": "wind_speed",
            "WMO_PRES": "pressure",
        }
    )
    df_hdx_clean.drop(["NUMBER", "SUBBASIN"], inplace=True, axis=1)
    mo.output.clear()
    return (df_hdx_clean,)


@app.cell
def _(df_hdx_clean, gdf_tracks, mo):
    mo.accordion(
        {
            "For a given storm selected above, do the intensity measurements and total number of points align?": gdf_tracks[
                [
                    "sid",
                    "valid_time",
                    "wind_speed",
                    "pressure",
                    "nature",
                    "basin",
                ]
            ].merge(
                df_hdx_clean,
                how="outer",
                on=["sid", "valid_time"],
                suffixes=["_db", "_hdx"],
            )
        }
    )
    return


if __name__ == "__main__":
    app.run()
