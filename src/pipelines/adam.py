"""WFP ADAM ETL pipeline.

Two modes (mirroring the GDACS pipeline pattern):

1. Current — real-time ADAM events from a recent window (days_back).
2. Archive — historical backfill across a date range.

Both modes call ``ocha_lens.datasources.adam`` to list events and download
per-event population CSVs, then write rows to ``storms.adam_exposure`` in
long format covering admin_level 0/1/2. The library returns one DataFrame
per event with all three levels already cumulative-≥-threshold; this module
just adds event/episode/valid_time and persists.

ADAM event_id is the same identifier GDACS uses for the same physical event
(verified empirically — e.g. MILTON-24 is event_id 1001111 in both systems).
So the pipeline also upserts ``storms.storm_id_lookup`` with
``adam_eventid = event_id`` for each event it ingests, recording that we
have ADAM data for the event. Whatever the GDACS pipeline (or later runs of
ADAM) writes to other columns of that row is preserved.

Idempotency: before the event loop, we pre-load the set of
(adam_eventid, adam_episodeid) pairs already in ``storms.adam_exposure``
and skip the CSV download for any event whose latest episode is in that
set. First backfill downloads everything; subsequent runs only fetch new
events or events whose latest episode has advanced.
"""

import logging
from datetime import datetime, timezone
from functools import partial
from typing import Optional, Set, Tuple

import coloredlogs
import ocha_stratus as stratus
import pandas as pd
import requests
from dotenv import load_dotenv
from sqlalchemy import text

from ocha_lens.datasources import adam as adam_api


load_dotenv()


logger = logging.getLogger(__name__)


def _load_ingested_episodes(engine) -> Set[Tuple[int, int]]:
    """Set of (adam_eventid, adam_episodeid) pairs already in
    storms.adam_exposure. Used to skip the CSV download for events whose
    latest episode we've already persisted."""
    with engine.connect() as conn:
        df = pd.read_sql(
            text(
                "SELECT DISTINCT adam_eventid, adam_episodeid "
                "FROM storms.adam_exposure"
            ),
            conn,
        )
    return {(int(r.adam_eventid), int(r.adam_episodeid)) for r in df.itertuples()}


def _emit_rows(
    event_id: int,
    episode_id: int,
    valid_time: pd.Timestamp,
    exposure: pd.DataFrame,
) -> list:
    """Long-form exposure DataFrame (from adam_api.get_exposure) → list of
    row dicts matching storms.adam_exposure schema."""
    rows = []
    for _, r in exposure.iterrows():
        pop = r["pop_exposed"]
        rows.append({
            "adam_eventid": event_id,
            "adam_episodeid": episode_id,
            "valid_time": valid_time,
            "wind_speed_kt": int(r["wind_speed_kt"]),
            "admin_level": int(r["admin_level"]),
            "iso3": r["iso3"] if pd.notna(r["iso3"]) else None,
            "admin_name": r["admin_name"],
            "parent_admin_name": (
                r["parent_admin_name"] if pd.notna(r["parent_admin_name"]) else None
            ),
            # pcode = iso3 at admin_level=0; null at adm1/adm2 (no public
            # GAUL→OCHA COD crosswalk yet — deferred to a future geometric
            # join pipeline that uses ADAM's per-event wind-footprint shp).
            "pcode": (
                r["iso3"] if int(r["admin_level"]) == 0 and pd.notna(r["iso3"]) else None
            ),
            "pop_exposed": None if pd.isna(pop) else int(pop),
        })
    return rows


def _ingest_event_range(
    from_date: Optional[str],
    to_date: Optional[str],
    source: Optional[str],
    mode: str,
    chunksize: int,
) -> None:
    """Shared core: walk ADAM events in a date window, write exposure rows
    + storm_id_lookup linkage."""
    logger.info(
        "Fetching ADAM events %s -> %s (source=%s)",
        from_date, to_date, source,
    )
    events = adam_api.get_events(
        from_date=from_date, to_date=to_date, source=source,
    )
    logger.info("Got %d events (latest episode per event_id)", len(events))

    engine = stratus.get_engine(mode, write=True)
    already_ingested = _load_ingested_episodes(engine)
    logger.info(
        "Skip set: %d (event_id, episode_id) pairs already in adam_exposure",
        len(already_ingested),
    )

    all_rows = []
    storm_links = []
    n_skipped = 0
    n_no_csv = 0

    for i, ev in events.iterrows():
        event_id = int(ev["event_id"])
        episode_id = int(ev["episode_id"])
        logger.info(
            "[%d/%d] event_id=%s episode_id=%s name=%s",
            i + 1, len(events), event_id, episode_id, ev["name"],
        )

        if (event_id, episode_id) in already_ingested:
            logger.info("  already ingested, skipping CSV download")
            n_skipped += 1
            continue

        try:
            exposure = adam_api.get_exposure(event_id, ev["population_csv_url"])
        except adam_api.NoExposureCSVError as e:
            logger.info("  no exposure CSV: %s", e)
            n_no_csv += 1
            continue
        except requests.exceptions.HTTPError as e:
            # 403 is the common case for ADAM: WFP publishes the CSV URL in
            # the API but the underlying file is ACL'd private. Not a
            # pipeline issue, not transient — demote to INFO so it doesn't
            # spam WARNING log noise. Other HTTP error codes (4xx/5xx) are
            # genuine problems worth a warning.
            status = e.response.status_code if e.response is not None else None
            if status == 403:
                logger.info("  CSV access denied by WFP (403), skipping")
            else:
                logger.warning("  HTTP error fetching CSV: %s", e)
            n_no_csv += 1
            continue
        except requests.exceptions.RequestException as e:
            # Transient network failure — skip, retry next run. Any other
            # exception (pandera ValidationError, etc.) propagates loudly.
            logger.warning("  network error: %s", e)
            n_no_csv += 1
            continue

        valid_time = pd.to_datetime(ev["to_date"])
        rows = _emit_rows(event_id, episode_id, valid_time, exposure)
        all_rows.extend(rows)
        logger.info(
            "  +%d rows (adm0:%d, adm1:%d, adm2:%d)",
            len(rows),
            sum(1 for r in rows if r["admin_level"] == 0),
            sum(1 for r in rows if r["admin_level"] == 1),
            sum(1 for r in rows if r["admin_level"] == 2),
        )

        # ADAM event_id IS the GDACS gdacs_eventid (shared identifier space).
        # Record the linkage; the row's atcf_id stays untouched if GDACS
        # pipeline already populated it.
        storm_links.append({
            "gdacs_eventid": event_id,
            "adam_eventid": event_id,
        })

    logger.info(
        "Done: %d events ingested, %d skipped (already in DB), %d skipped (no CSV)",
        len(events) - n_skipped - n_no_csv, n_skipped, n_no_csv,
    )

    if all_rows:
        df = pd.DataFrame(all_rows)
        upsert = partial(
            stratus.postgres_upsert, constraint="adam_exposure_unique",
        )
        logger.info(
            "Upserting %d rows -> storms.adam_exposure (%s)", len(df), mode,
        )
        df.to_sql(
            "adam_exposure", engine, schema="storms",
            if_exists="append", index=False, method=upsert, chunksize=chunksize,
        )
        logger.info("Wrote %d rows", len(df))

    if storm_links:
        df_links = pd.DataFrame(storm_links)
        upsert = partial(
            stratus.postgres_upsert, constraint="storm_id_lookup_pkey",
        )
        logger.info(
            "Upserting %d storm_id_lookup rows (adam_eventid)", len(df_links),
        )
        df_links.to_sql(
            "storm_id_lookup", engine, schema="storms",
            if_exists="append", index=False, method=upsert,
        )


def run_adam_current(
    mode: str = "dev",
    days_back: int = 14,
    source: Optional[str] = "NOAA",
    chunksize: int = 1000,
) -> None:
    """Real-time ADAM events from the last ``days_back`` days.

    Default lookback is 14 days (wider than GDACS's 7) because ADAM's
    publication lag has a long tail — events whose CSV gets published days
    after the storm passed should still be caught.
    """
    coloredlogs.install(
        logger=logger,
        fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    logger.info("Starting ADAM Current ETL pipeline...")

    now = datetime.now(timezone.utc)
    from_date = (now - pd.Timedelta(days=days_back)).strftime("%Y-%m-%d")
    to_date = now.strftime("%Y-%m-%d")
    _ingest_event_range(from_date, to_date, source, mode, chunksize)
    logger.info("Pipeline successfully finished!")


def run_adam_archive(
    from_date: str = "2010-01-01",
    to_date: Optional[str] = None,
    source: Optional[str] = "NOAA",
    mode: str = "dev",
    chunksize: int = 1000,
) -> None:
    """Historical ADAM backfill across a date range. Idempotent: re-runs
    only download CSVs for events whose latest episode isn't already in
    storms.adam_exposure."""
    coloredlogs.install(
        logger=logger,
        fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    logger.info("Starting ADAM Archive ETL pipeline...")

    if to_date is None:
        to_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    _ingest_event_range(from_date, to_date, source, mode, chunksize)
    logger.info("Pipeline successfully finished!")
