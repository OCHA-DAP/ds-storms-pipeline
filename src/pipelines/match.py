"""GDACS → NHC ATCF matching.

Resolves the NHC atcf_id for GDACS events via geometric matching:
inner-join GDACS timeline ↔ NHC tracks on valid_time, group by
atcf_id, pick the one with smallest mean great-circle distance
(ocha_lens.datasources.gdacs.match_to_atcf).

Two entry points share the same per-event logic:

  - attempt_match(eventid, nhc_tracks) is called inline by the
    GDACS exposure pipeline, so timeline is fetched once per event
    lifetime alongside the exposure fetches.
  - run_match() is the standalone retry path for events already
    in gdacs_exposure but still missing an atcf_id (e.g., NHC
    tracks backfilled after GDACS, or a transient timeline-fetch
    failure during ingestion).

Both forms are idempotent — they only touch events whose
gdacs_eventid is not yet linked in storms.storm_id_lookup.
"""

import logging
from functools import partial
from typing import Optional, Set

import coloredlogs
import ocha_stratus as stratus
import pandas as pd
import requests
from dotenv import load_dotenv

from ocha_lens.datasources import gdacs as gdacs_api


load_dotenv()


logger = logging.getLogger(__name__)


def _load_unmatched_eventids(engine) -> pd.DataFrame:
    """GDACS events present in gdacs_exposure but not yet linked
    in storm_id_lookup. One row per gdacs_eventid."""
    with engine.connect() as conn:
        return pd.read_sql(
            """
            SELECT DISTINCT g.gdacs_eventid
            FROM storms.gdacs_exposure g
            LEFT JOIN storms.storm_id_lookup l
                ON g.gdacs_eventid = l.gdacs_eventid
            WHERE l.atcf_id IS NULL
            """,
            conn,
        )


def _load_matched_eventids(engine) -> Set[int]:
    """gdacs_eventids already linked to an atcf_id. Used by the
    inline pass in the GDACS pipeline to skip events that are
    already resolved."""
    with engine.connect() as conn:
        df = pd.read_sql(
            """
            SELECT gdacs_eventid
            FROM storms.storm_id_lookup
            WHERE atcf_id IS NOT NULL
            """,
            conn,
        )
    return {int(x) for x in df["gdacs_eventid"]}


def _load_freshest_nhc_tracks(engine) -> pd.DataFrame:
    """NHC tracks, one row per (atcf_id, valid_time) at freshest
    issuance. Required shape for gdacs.match_to_atcf — without
    dedup the mean distance gets dragged around by stale forecasts."""
    with engine.connect() as conn:
        return pd.read_sql(
            """
            SELECT DISTINCT ON (atcf_id, valid_time)
                   atcf_id, valid_time,
                   ST_Y(geometry) AS lat,
                   ST_X(geometry) AS lon
            FROM storms.nhc_tracks_geo
            ORDER BY atcf_id, valid_time, issued_time DESC
            """,
            conn,
        )


def attempt_match(
    eventid: int, nhc_tracks: pd.DataFrame
) -> Optional[str]:
    """Resolve a single GDACS eventid → atcf_id geometrically.

    Returns None when the event has no timeline, the timeline
    fetch fails, or no NHC track sits within the distance
    threshold. The caller treats None as "leave unmatched, retry
    later"; nothing here raises.
    """
    try:
        timeline = gdacs_api.get_timeline(eventid)
    except gdacs_api.NoTimelineError:
        logger.info(
            "  no timeline for eventid=%s — leaving unmatched", eventid,
        )
        return None
    except requests.exceptions.RequestException as e:
        logger.warning(
            "  timeline fetch failed for %s: %s — retry next cycle",
            eventid, e,
        )
        return None
    return gdacs_api.match_to_atcf(timeline, nhc_tracks)


def _upsert_matches(matches, engine) -> None:
    """matches: list of {'gdacs_eventid': int, 'atcf_id': str}.
    No-op when empty."""
    if not matches:
        return
    df = pd.DataFrame(matches)
    upsert = partial(
        stratus.postgres_upsert, constraint="storm_id_lookup_pkey"
    )
    logger.info("Upserting %d matches into storm_id_lookup", len(df))
    df.to_sql(
        "storm_id_lookup",
        engine,
        schema="storms",
        if_exists="append",
        index=False,
        method=upsert,
    )


def run_match(mode: str = "dev") -> None:
    """Standalone retry: attempt matching for GDACS events already
    ingested but still missing an atcf_id. Use after a late NHC
    backfill, or to retry transient timeline-fetch failures."""
    coloredlogs.install(
        logger=logger,
        fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    logger.info("Starting GDACS → ATCF matching pipeline...")

    engine = stratus.get_engine(mode, write=True)

    unmatched = _load_unmatched_eventids(engine)
    if len(unmatched) == 0:
        logger.info("No unmatched GDACS events. Done.")
        return
    logger.info("Found %d unmatched GDACS events", len(unmatched))

    nhc_tracks = _load_freshest_nhc_tracks(engine)
    logger.info(
        "Loaded %d NHC track rows (freshest per atcf×valid_time)",
        len(nhc_tracks),
    )

    matches = []
    for _, row in unmatched.iterrows():
        eventid = int(row["gdacs_eventid"])
        atcf_id = attempt_match(eventid, nhc_tracks)
        if atcf_id is not None:
            matches.append({"gdacs_eventid": eventid, "atcf_id": atcf_id})

    logger.info("Resolved %d/%d events", len(matches), len(unmatched))
    _upsert_matches(matches, engine)
    logger.info("Pipeline successfully finished!")
