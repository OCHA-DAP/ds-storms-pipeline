"""GDACS ETL pipeline.

✅ LIVE PRODUCTION (monitoring) — runs on the Databricks schedule as the
`gdacs` task of the gdacs_adam_pipeline job (every 3h), via
`run_pipeline.py gdacs` → `databricks/dispatch.py`. Writes
`storms.gdacs_exposure` and the `atcf_id` link in `storms.storm_id_lookup`.

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
from typing import Dict, Optional, Set, Tuple

import coloredlogs
import ocha_stratus as stratus
import pandas as pd
import requests
from dotenv import load_dotenv
from sqlalchemy import text

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


def _load_skip_info(
    engine,
) -> Tuple[Dict[int, pd.Timestamp], Set[int]]:
    """Pre-load DB state used to decide which events to skip.

    Returns
    -------
    db_max_vt : dict
        gdacs_eventid → MAX(valid_time) from storms.gdacs_exposure. An event
        is "already at this snapshot or fresher" when api_to_date <= the
        value here.
    matched_only_eventids : set
        gdacs_eventids that appear in storm_id_lookup (atcf resolved) but
        produced zero exposure rows — usually weak storms with no country
        in the wind buffer (LESLIE / JOYCE pattern). We don't have a
        valid_time for these to compare against, so we permanently skip
        them. Cheap correctness vs. re-fetching every run.
    """
    with engine.connect() as conn:
        db_max_vt_df = pd.read_sql(
            text(
                "SELECT gdacs_eventid, MAX(valid_time) AS max_vt "
                "FROM storms.gdacs_exposure "
                "GROUP BY gdacs_eventid"
            ),
            conn,
        )
        matched_df = pd.read_sql(
            text(
                "SELECT gdacs_eventid FROM storms.storm_id_lookup "
                "WHERE atcf_id IS NOT NULL"
            ),
            conn,
        )
    db_max_vt = {
        int(r.gdacs_eventid): pd.to_datetime(r.max_vt)
        for r in db_max_vt_df.itertuples()
    }
    matched_eventids = {int(x) for x in matched_df["gdacs_eventid"]}
    # Matched-only = matched but no exposure rows
    matched_only = matched_eventids - db_max_vt.keys()
    return db_max_vt, matched_only


def _load_existing_episode_pairs(engine) -> Set[Tuple[int, int]]:
    """``{(gdacs_eventid, gdacs_episodeid)}`` already in
    gdacs_exposure. Used by all-episodes mode to skip episodes
    already on file so re-runs only fetch new ones."""
    with engine.connect() as conn:
        df = pd.read_sql(
            text(
                "SELECT DISTINCT gdacs_eventid, gdacs_episodeid "
                "FROM storms.gdacs_exposure"
            ),
            conn,
        )
    return {
        (int(r.gdacs_eventid), int(r.gdacs_episodeid))
        for r in df.itertuples()
    }


def _ingest_event_range(
    from_date: str,
    to_date: str,
    source: Optional[str],
    mode: str,
    chunksize: int,
    all_episodes: bool = False,
) -> None:
    """Shared core: walk events in a date range, write exposure rows.
    Called by both run_gdacs_archive and run_gdacs_current.

    When ``all_episodes`` is True, fetch exposure for every episode
    of each event (using the timeline's ``actual=True`` rows for
    per-episode valid_times) rather than only the latest. Per-event
    cost rises from 2 to ~2×N_advisories HTTP calls; per-episode
    skip via existing-pairs lookup keeps re-runs cheap.
    """
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
    db_max_vt, matched_only_eventids = _load_skip_info(engine)
    existing_pairs = (
        _load_existing_episode_pairs(engine) if all_episodes else set()
    )
    logger.info(
        "Loaded %d NHC track rows; %d events already matched; "
        "%d events with exposure on file; %d zero-exposure-matched; "
        "%d (eventid,episodeid) pairs already in db (all-episodes mode)",
        len(nhc_tracks), len(already_matched),
        len(db_max_vt), len(matched_only_eventids), len(existing_pairs),
    )

    exposure_upsert = partial(
        stratus.postgres_upsert, constraint="gdacs_exposure_unique"
    )

    n_skipped_fresh = 0   # already have this snapshot or later
    n_skipped_zero = 0    # zero-exposure event we matched previously
    n_skipped_error = 0   # network / no-episodes during fetch
    n_processed = 0
    n_rows_written = 0
    n_matches = 0

    def _upsert_rows(rows):
        if not rows:
            return 0
        pd.DataFrame(rows).to_sql(
            "gdacs_exposure",
            engine,
            schema="storms",
            if_exists="append",
            index=False,
            method=exposure_upsert,
            chunksize=chunksize,
        )
        return len(rows)

    for i, ev in events.iterrows():
        eventid = int(ev["eventid"])
        api_to_date = pd.to_datetime(ev["to_date"])

        # Skip checks (cheap, no HTTP). The latest-only db_max_vt skip
        # doesn't apply in all-episodes mode — we want missing historical
        # episodes filled in even when the latest snapshot is on file.
        # matched_only still applies: those events have no exposure data
        # at any episode (weak-storm pattern), so re-fetching is wasteful.
        if not all_episodes:
            if eventid in db_max_vt and api_to_date <= db_max_vt[eventid]:
                n_skipped_fresh += 1
                continue
        if eventid in matched_only_eventids:
            n_skipped_zero += 1
            continue

        logger.info(
            "[%d/%d] eventid=%s name=%s",
            i + 1, len(events), eventid, ev["name"],
        )
        try:
            detail = gdacs_api.get_event_detail(eventid)
        except requests.exceptions.RequestException as e:
            logger.warning("  network error for %s: %s", eventid, e)
            n_skipped_error += 1
            continue

        if all_episodes:
            # Per-episode fetch. Timeline gives us {advisory_number:
            # advisory_datetime} on actual=True rows — advisory_number is
            # the episodeid and advisory_datetime is the snapshot
            # valid_time. Latest-only path uses ev.to_date because it's
            # implicitly the latest advisory's time; here we need the
            # actual per-snapshot value.
            try:
                timeline = gdacs_api.get_timeline(eventid, detail=detail)
            except gdacs_api.NoTimelineError:
                logger.info("  no timeline for %s, skipping", eventid)
                n_skipped_error += 1
                continue
            except requests.exceptions.RequestException as e:
                logger.warning("  timeline fetch failed for %s: %s", eventid, e)
                n_skipped_error += 1
                continue
            actuals = timeline[
                timeline["actual"].astype(str).str.lower() == "true"
            ]
            ev_rows_written = 0
            ev_episodes_fetched = 0
            ev_episodes_skipped = 0
            for _, ar in actuals.sort_values("advisory_number").iterrows():
                ep_id = int(ar["advisory_number"])
                if (eventid, ep_id) in existing_pairs:
                    ev_episodes_skipped += 1
                    continue
                valid_time = ar["advisory_datetime"]
                try:
                    adm0 = gdacs_api.get_exposure_adm0(
                        eventid, episodeid=ep_id,
                    )
                    adm1 = gdacs_api.get_exposure_adm1(
                        eventid, episodeid=ep_id,
                    )
                except requests.exceptions.RequestException as e:
                    # Individual episode fetch can 403/404 if GDACS
                    # pruned per-episode data; don't fail the event.
                    logger.warning(
                        "  episode %s/%s fetch failed: %s",
                        eventid, ep_id, e,
                    )
                    continue
                episode_rows = _emit_rows(
                    eventid, ep_id, valid_time, adm0, adm1,
                )
                ev_episodes_fetched += 1
                ev_rows_written += _upsert_rows(episode_rows)
                existing_pairs.add((eventid, ep_id))
            n_rows_written += ev_rows_written
            logger.info(
                "  episodes fetched=%d skipped=%d (+%d rows)",
                ev_episodes_fetched, ev_episodes_skipped, ev_rows_written,
            )
        else:
            try:
                episode_id = gdacs_api.latest_episode_id(detail)
                valid_time = pd.to_datetime(ev["to_date"])
                adm0 = gdacs_api.get_exposure_adm0(eventid, detail=detail)
                adm1 = gdacs_api.get_exposure_adm1(eventid, detail=detail)
            except gdacs_api.NoEpisodesError:
                logger.info("  no episodes yet for %s, skipping", eventid)
                n_skipped_error += 1
                continue
            except requests.exceptions.RequestException as e:
                logger.warning("  network error for %s: %s", eventid, e)
                n_skipped_error += 1
                continue
            rows = _emit_rows(eventid, episode_id, valid_time, adm0, adm1)
            logger.info("  +%d rows", len(rows))
            # Per-event upsert: durable as soon as the event is processed;
            # a crash mid-loop keeps prior events' work.
            n_rows_written += _upsert_rows(rows)

        if eventid not in already_matched:
            atcf_id = attempt_match(eventid, nhc_tracks, detail=detail)
            if atcf_id is not None:
                upsert_matches(
                    [{"gdacs_eventid": eventid, "atcf_id": atcf_id}], engine,
                )
                already_matched.add(eventid)
                n_matches += 1
                logger.info("  matched → atcf_id=%s", atcf_id)

        n_processed += 1

    logger.info(
        "Done: processed=%d, rows=%d, matches=%d; "
        "skipped fresh=%d, zero-exp=%d, errors=%d",
        n_processed, n_rows_written, n_matches,
        n_skipped_fresh, n_skipped_zero, n_skipped_error,
    )


def run_gdacs_current(
    mode: str = "dev",
    days_back: int = 7,
    source: Optional[str] = "NOAA",
    chunksize: int = 1000,
    all_episodes: bool = False,
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
    _ingest_event_range(
        from_date, to_date, source, mode, chunksize,
        all_episodes=all_episodes,
    )
    logger.info("Pipeline successfully finished!")


def run_gdacs_archive(
    from_date: str = "2010-01-01",
    to_date: Optional[str] = None,
    source: Optional[str] = "NOAA",
    mode: str = "dev",
    chunksize: int = 1000,
    all_episodes: bool = False,
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
    _ingest_event_range(
        from_date, to_date, source, mode, chunksize,
        all_episodes=all_episodes,
    )
    logger.info("Pipeline successfully finished!")
