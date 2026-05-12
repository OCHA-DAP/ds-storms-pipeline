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
from typing import Any, Dict, List, Optional, Set

import coloredlogs
import ocha_stratus as stratus
import pandas as pd
import requests
from dotenv import load_dotenv

from ocha_lens.datasources import gdacs as gdacs_api


load_dotenv()


logger = logging.getLogger(__name__)


# The two lookups below are deliberately asymmetric:
#   load_matched_eventids — *every* matched gdacs_eventid (used by the
#       inline path to skip events it just ingested but already linked).
#   _load_unmatched_eventids — events in gdacs_exposure not yet linked
#       (used by the standalone retry to bound work to events whose
#       exposure has already been ingested at least once).


def _load_unmatched_eventids(engine) -> List[int]:
    """GDACS events present in gdacs_exposure but not yet linked
    in storm_id_lookup."""
    with engine.connect() as conn:
        df = pd.read_sql(
            """
            SELECT DISTINCT g.gdacs_eventid
            FROM storms.gdacs_exposure g
            LEFT JOIN storms.storm_id_lookup l
                ON g.gdacs_eventid = l.gdacs_eventid
            WHERE l.atcf_id IS NULL
            """,
            conn,
        )
    return [int(x) for x in df["gdacs_eventid"]]


def load_matched_eventids(engine) -> Set[int]:
    """gdacs_eventids already linked to an atcf_id."""
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


def load_freshest_nhc_tracks(engine) -> pd.DataFrame:
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
    eventid: int,
    nhc_tracks: pd.DataFrame,
    detail: Optional[Dict[str, Any]] = None,
) -> Optional[str]:
    """Resolve a single GDACS eventid → atcf_id geometrically.

    Pass ``detail`` (a pre-fetched output of
    :func:`gdacs.get_event_detail`) to skip the get_timeline internal
    re-fetch when the caller already has it.

    Returns None when the event has no timeline, the timeline
    fetch fails, or no NHC track sits within the distance
    threshold. The caller treats None as "leave unmatched, retry
    later"; nothing here raises.
    """
    try:
        timeline = gdacs_api.get_timeline(eventid, detail=detail)
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


def upsert_matches(matches: List[dict], engine) -> None:
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
    if not unmatched:
        logger.info("No unmatched GDACS events. Done.")
        return
    logger.info("Found %d unmatched GDACS events", len(unmatched))

    nhc_tracks = load_freshest_nhc_tracks(engine)
    logger.info(
        "Loaded %d NHC track rows (freshest per atcf×valid_time)",
        len(nhc_tracks),
    )

    matches = []
    for eventid in unmatched:
        atcf_id = attempt_match(eventid, nhc_tracks)
        if atcf_id is not None:
            matches.append({"gdacs_eventid": eventid, "atcf_id": atcf_id})

    logger.info("Resolved %d/%d events", len(matches), len(unmatched))
    upsert_matches(matches, engine)
    logger.info("Pipeline successfully finished!")
