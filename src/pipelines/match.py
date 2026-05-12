"""GDACS → NHC ATCF matching pipeline.

Reads unmatched GDACS events from storms.gdacs_exposure, resolves
their atcf_id via a two-tier strategy, upserts storms.storm_id_lookup.

1. CHEAP — DB only. Look up distinct atcf_ids in NHC tracks at the
   same valid_time as the GDACS event's latest advisory issue time.
   If exactly one atcf_id is active, the match is unambiguous; no
   GDACS API call needed.

2. GEOMETRIC — only when tier 1 returns multiple candidates. Fetch
   the GDACS timeline live, run ocha_lens.datasources.gdacs.match_to_atcf
   (inner join on valid_time + groupby-mean-distance) to pick the
   right atcf_id among the candidates.

Idempotent. Re-runs only process events still missing an atcf_id.
"""

import logging
from functools import partial
from typing import List

import coloredlogs
import ocha_stratus as stratus
import pandas as pd
import requests
from dotenv import load_dotenv

from ocha_lens.datasources import gdacs as gdacs_api


load_dotenv()


logger = logging.getLogger(__name__)


def _load_unmatched_events(engine) -> pd.DataFrame:
    """GDACS events in gdacs_exposure but not yet linked in
    storm_id_lookup. Returns one row per gdacs_eventid with the
    latest valid_time (its latest advisory issue time)."""
    with engine.connect() as conn:
        return pd.read_sql(
            """
            SELECT g.gdacs_eventid,
                   MAX(g.valid_time) AS gdacs_issue_time
            FROM storms.gdacs_exposure g
            LEFT JOIN storms.storm_id_lookup l
                ON g.gdacs_eventid = l.gdacs_eventid
            WHERE l.atcf_id IS NULL
            GROUP BY g.gdacs_eventid
            """,
            conn,
        )


def _load_freshest_nhc_tracks(engine) -> pd.DataFrame:
    """NHC tracks, one row per (atcf_id, valid_time) at freshest
    issuance. Required shape for gdacs.match_to_atcf (geometric
    tier) — without dedup it averages across stale forecasts."""
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


def _candidates_at_time(
    nhc_tracks: pd.DataFrame, t: pd.Timestamp
) -> List[str]:
    return (
        nhc_tracks.loc[nhc_tracks["valid_time"] == t, "atcf_id"]
        .unique()
        .tolist()
    )


def run_match(mode: str = "dev") -> None:
    """Match all unmatched GDACS events to NHC atcf_ids."""
    coloredlogs.install(
        logger=logger,
        fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    logger.info("Starting GDACS → ATCF matching pipeline...")

    engine = stratus.get_engine(mode, write=True)

    unmatched = _load_unmatched_events(engine)
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
    n_cheap = 0
    n_geometric = 0
    n_no_match = 0
    n_failed = 0

    for _, row in unmatched.iterrows():
        eventid = int(row["gdacs_eventid"])
        t = row["gdacs_issue_time"]
        candidates = _candidates_at_time(nhc_tracks, t)

        if len(candidates) == 0:
            n_no_match += 1
            continue

        if len(candidates) == 1:
            matches.append({"gdacs_eventid": eventid, "atcf_id": candidates[0]})
            n_cheap += 1
            continue

        # Multiple candidates → fetch timeline + disambiguate geometrically
        try:
            timeline = gdacs_api.get_timeline(eventid)
        except gdacs_api.NoTimelineError:
            # Legitimate "event has no timeline" — skip, will stay
            # unmatched (downstream consumers can see this state).
            logger.info(
                "  no timeline for eventid=%s — left unmatched", eventid,
            )
            n_no_match += 1
            continue
        except requests.exceptions.RequestException as e:
            logger.warning(
                "Timeline fetch failed for %s: %s — retry next cycle",
                eventid, e,
            )
            n_failed += 1
            continue

        atcf_id = gdacs_api.match_to_atcf(timeline, nhc_tracks)
        if atcf_id is None:
            n_no_match += 1
            continue
        matches.append({"gdacs_eventid": eventid, "atcf_id": atcf_id})
        n_geometric += 1

    logger.info(
        "Match cycle: %d cheap, %d geometric, %d no NHC counterpart, "
        "%d timeline-fetch failures",
        n_cheap, n_geometric, n_no_match, n_failed,
    )

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
    logger.info("Pipeline successfully finished!")
