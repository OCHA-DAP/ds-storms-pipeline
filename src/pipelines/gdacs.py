"""GDACS ETL pipeline.

Two modes (mirroring NHC pipeline pattern):

1. Current: Real-time GDACS Tropical Cyclone events (recent window)
2. Archive: Historical backfill across a date range

Both modes use ocha_lens.datasources.gdacs for API access + parsing
and write to storms.gdacs_exposure in long format. Wind threshold
standardization (GDACS buffer39 → 34 kt, buffer74 → 64 kt) and the
wide→long pivot live here; ocha-lens stays GDACS-faithful.

Inline matching: for each newly-ingested event we attempt to
resolve its NHC atcf_id via .match.attempt_match, so the GDACS
timeline is fetched once per event lifetime instead of paying a
second API trip in a separate match pass.
"""

import logging
from datetime import datetime, timezone
from functools import partial
from typing import Optional

import coloredlogs
import ocha_stratus as stratus
import pandas as pd
import requests
from dotenv import load_dotenv

from ocha_lens.datasources import gdacs as gdacs_api

from .match import (
    attempt_match,
    load_freshest_nhc_tracks,
    load_matched_eventids,
    upsert_matches,
)


load_dotenv()


logger = logging.getLogger(__name__)


# GDACS publishes buffer thresholds in mph (Saffir-Simpson public-facing
# convention: 39 mph = tropical storm onset, 74 mph = Cat 1 hurricane).
# We store wind_speed_kt in knots to match NHC operational products
# (34 kt ≈ 39 mph, 64 kt ≈ 74 mph). Conversion lives here, not in
# ocha-lens, so the library stays GDACS-faithful.
_BUFFER_TO_WIND_KT = {
    "buffer39": 34,
    "buffer74": 64,
}


def _nullable_int(v):
    """pop_affected from ocha-lens is nullable ("Int64"); SQL
    pop_exposed is nullable too. Pass through pd.NA/None as None;
    cast real numbers to Python int (pandera Int64 isn't a vanilla
    int and psycopg2 sometimes mishandles it)."""
    return None if pd.isna(v) else int(v)


def _emit_rows(eventid, episode_id, valid_time, adm0_by_buffer, adm1_by_buffer):
    """Wide-by-buffer dicts of DataFrames → long-format row dicts
    matching the storms.gdacs_exposure schema.

    Two transformations happen here:
      1. iso3 is standardized via gdacs_api.to_iso3() — e.g. XJE → JEY
      2. gdacs_admin_code preserves the raw GDACS native admin code
         (GMI_CNTRY at adm0; GMI_ADMIN at adm1)
    """
    rows = []
    for buffer_key, wind_kt in _BUFFER_TO_WIND_KT.items():
        adm0 = adm0_by_buffer.get(buffer_key)
        if adm0 is not None:
            for _, r in adm0.iterrows():
                raw_gdacs_code = r["iso3"]  # at adm0 this is GMI_CNTRY
                std_iso3 = gdacs_api.to_iso3(raw_gdacs_code)
                rows.append({
                    "gdacs_eventid": eventid,
                    "gdacs_episodeid": episode_id,
                    "valid_time": valid_time,
                    "wind_speed_kt": wind_kt,
                    "admin_level": 0,
                    "iso3": std_iso3,
                    "admin_name": r["country"],
                    "gdacs_admin_code": raw_gdacs_code,
                    # iso3 IS the pcode at admin_level=0
                    "pcode": std_iso3,
                    "pop_exposed": _nullable_int(r["pop_affected"]),
                })

        adm1 = adm1_by_buffer.get(buffer_key)
        if adm1 is not None:
            for _, r in adm1.iterrows():
                rows.append({
                    "gdacs_eventid": eventid,
                    "gdacs_episodeid": episode_id,
                    "valid_time": valid_time,
                    "wind_speed_kt": wind_kt,
                    "admin_level": 1,
                    "iso3": gdacs_api.to_iso3(r["iso3"]),
                    "admin_name": r["admin_name"],
                    "gdacs_admin_code": r["gmi_admin"],
                    # pcode null at admin_level=1 until a downstream
                    # enrichment step (FieldMaps lookup or similar)
                    # fills it
                    "pcode": None,
                    "pop_exposed": _nullable_int(r["pop_affected"]),
                })
    return rows


def _ingest_event_range(
    from_date: str,
    to_date: str,
    source: Optional[str],
    mode: str,
    chunksize: int,
) -> None:
    """Shared core: walk events in a date range, write exposure rows.
    Called by both run_gdacs_archive and run_gdacs_current."""
    logger.info(
        "Fetching GDACS events %s -> %s (source=%s)",
        from_date, to_date, source,
    )
    events = gdacs_api.get_events(
        from_date=from_date,
        to_date=to_date,
        source=source,
    )
    logger.info("Got %d events", len(events))

    engine = stratus.get_engine(mode, write=True)

    nhc_tracks = load_freshest_nhc_tracks(engine)
    already_matched = load_matched_eventids(engine)
    logger.info(
        "Loaded %d NHC track rows; %d events already matched",
        len(nhc_tracks), len(already_matched),
    )

    all_rows = []
    matches = []
    n_skipped = 0

    for i, ev in events.iterrows():
        eventid = int(ev["eventid"])
        logger.info(
            "[%d/%d] eventid=%s name=%s",
            i + 1, len(events), eventid, ev["name"],
        )
        try:
            detail = gdacs_api.get_event_detail(eventid)
            episode_id = gdacs_api.latest_episode_id(detail)
            valid_time = pd.to_datetime(ev["to_date"])
            adm0 = gdacs_api.get_exposure_adm0(eventid, detail=detail)
            adm1 = gdacs_api.get_exposure_adm1(eventid, detail=detail)
        except gdacs_api.NoEpisodesError:
            # Legitimate "event has no episodes yet" — skip, retry
            # next run when GDACS has caught up.
            logger.info("  no episodes yet for %s, skipping", eventid)
            n_skipped += 1
            continue
        except requests.exceptions.RequestException as e:
            # Transient network failure — skip, retry next run.
            # Any other exception (KeyError on missing API field,
            # pandera ValidationError on bad data, etc.) propagates
            # and aborts the run loudly.
            logger.warning("  network error for %s: %s", eventid, e)
            n_skipped += 1
            continue

        rows = _emit_rows(eventid, episode_id, valid_time, adm0, adm1)
        all_rows.extend(rows)
        logger.info("  +%d rows", len(rows))

        if eventid not in already_matched:
            atcf_id = attempt_match(eventid, nhc_tracks, detail=detail)
            if atcf_id is not None:
                matches.append(
                    {"gdacs_eventid": eventid, "atcf_id": atcf_id}
                )
                logger.info("  matched → atcf_id=%s", atcf_id)

    logger.info(
        "Done fetching: %d rows from %d events (%d skipped); "
        "%d inline matches resolved",
        len(all_rows), len(events) - n_skipped, n_skipped, len(matches),
    )

    if all_rows:
        df = pd.DataFrame(all_rows)
        upsert = partial(
            stratus.postgres_upsert, constraint="gdacs_exposure_unique"
        )
        logger.info("Upserting %d rows -> storms.gdacs_exposure (%s)",
                    len(df), mode)
        df.to_sql(
            "gdacs_exposure",
            engine,
            schema="storms",
            if_exists="append",
            index=False,
            method=upsert,
            chunksize=chunksize,
        )
        logger.info("Wrote %d rows", len(df))
    else:
        logger.info("No exposure rows to write")

    upsert_matches(matches, engine)


def run_gdacs_current(
    mode: str = "dev",
    days_back: int = 7,
    source: Optional[str] = "NOAA",
    chunksize: int = 1000,
) -> None:
    """Real-time GDACS events from the last ``days_back`` days.

    Idempotent against storms.gdacs_exposure: re-running upserts on
    (eventid, episodeid, wind_speed_kt, admin_level, iso3,
    admin_name).
    """
    coloredlogs.install(
        logger=logger,
        fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    logger.info("Starting GDACS Current ETL pipeline...")

    now = datetime.now(timezone.utc)
    from_date = (now - pd.Timedelta(days=days_back)).strftime("%Y-%m-%d")
    to_date = now.strftime("%Y-%m-%d")
    _ingest_event_range(from_date, to_date, source, mode, chunksize)
    logger.info("Pipeline successfully finished!")


def run_gdacs_archive(
    from_date: str = "2010-01-01",
    to_date: Optional[str] = None,
    source: Optional[str] = "NOAA",
    mode: str = "dev",
    chunksize: int = 1000,
) -> None:
    """Historical GDACS backfill across a date range.

    Idempotent: re-running picks up events not already in
    storms.gdacs_exposure (upsert is constraint-based).
    """
    coloredlogs.install(
        logger=logger,
        fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    logger.info("Starting GDACS Archive ETL pipeline...")

    if to_date is None:
        to_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    _ingest_event_range(from_date, to_date, source, mode, chunksize)
    logger.info("Pipeline successfully finished!")
