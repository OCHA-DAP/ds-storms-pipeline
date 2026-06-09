"""Historical GDACS + matching backfill orchestrator.

Runs the GDACS archive ingest (`run_gdacs_archive`) and the matching
step (`run_match`) in sequence, looping each phase until coverage
stops improving. Designed for the one-time historical population of
dev/prod from scratch — generous per-request timeouts and multiple
retry passes to tolerate GDACS API flakiness.

Both underlying pipelines are idempotent (upsert-keyed on natural
constraints), so re-running this script is always safe — it picks
up wherever the last run left off.

Usage
-----
Recommended — keeps the laptop from sleeping during the long run:

    caffeinate -dimsu uv run python scripts/backfill_gdacs_full.py --mode dev

Plain:

    uv run python scripts/backfill_gdacs_full.py --mode dev

Tunables (all optional, sensible defaults):

    --from-date 2010-01-01           # GDACS archive start
    --source NOAA                    # or JTWC
    --timeout 120                    # per-request HTTP timeout (seconds)
    --max-ingest-attempts 5          # retry passes through archive
    --max-match-attempts 3           # retry passes through matching
    --retry-delay 60                 # seconds between passes

Exits when each phase stabilizes (row count stops growing or
unmatched count stops shrinking) OR when max-attempts hit, whichever
comes first.
"""

import argparse
import logging
import sys
import time
from pathlib import Path

# Make `src.pipelines...` importable (script lives in ds-storms-pipeline/scripts/)
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import ocha_stratus as stratus
import requests
from dotenv import load_dotenv
from sqlalchemy import text

# Bump the GDACS HTTP timeout BEFORE any pipeline function runs. The
# library reads _TIMEOUT at call time, so this mutation takes effect
# for every subsequent request.
from ocha_lens.datasources import gdacs as gdacs_api  # noqa: E402

from src.pipelines.gdacs import run_gdacs_archive  # noqa: E402
from src.pipelines.match import run_match  # noqa: E402


load_dotenv()
logger = logging.getLogger(__name__)


def _count_exposure_rows(engine) -> int:
    with engine.connect() as conn:
        return conn.execute(
            text("SELECT COUNT(*) FROM storms.gdacs_exposure")
        ).scalar()


def _count_unmatched_events(engine) -> int:
    """gdacs_eventids in gdacs_exposure with no atcf_id in lookup."""
    with engine.connect() as conn:
        return conn.execute(
            text(
                """
                SELECT COUNT(DISTINCT g.gdacs_eventid)
                FROM storms.gdacs_exposure g
                LEFT JOIN storms.storm_id_lookup l
                    ON g.gdacs_eventid = l.gdacs_eventid
                WHERE l.atcf_id IS NULL
                """
            )
        ).scalar()


def _ingest_phase(
    engine, from_date: str, source: str, mode: str,
    max_attempts: int, retry_delay: int,
) -> None:
    last_count = 0
    for attempt in range(1, max_attempts + 1):
        logger.info(
            "%s\nINGEST PASS %d/%d\n%s",
            "=" * 60, attempt, max_attempts, "=" * 60,
        )
        try:
            run_gdacs_archive(
                from_date=from_date, source=source, mode=mode,
            )
        except requests.exceptions.RequestException as e:
            # Whole-pass network failure (e.g., paginated event-list
            # fetch timed out repeatedly). Log and let next pass try
            # again. Any other exception type (KeyError on missing
            # GDACS field, pandera validation error, DB constraint
            # violation) propagates — those are bugs or data issues,
            # not transient failures, and the user wants them loud.
            logger.error("Ingest pass %d network error: %s", attempt, e,
                         exc_info=True)

        count = _count_exposure_rows(engine)
        delta = count - last_count
        logger.info(
            "After pass %d: %d rows in storms.gdacs_exposure (+%d)",
            attempt, count, delta,
        )
        if attempt > 1 and delta == 0:
            logger.info("Row count stable — ingest done")
            return
        last_count = count
        if attempt < max_attempts:
            logger.info("Sleeping %ds before next pass...", retry_delay)
            time.sleep(retry_delay)
    logger.info("Hit max ingest attempts (%d) — moving on", max_attempts)


def _match_phase(
    engine, mode: str, max_attempts: int, retry_delay: int,
) -> None:
    last_unmatched = None
    for attempt in range(1, max_attempts + 1):
        logger.info(
            "%s\nMATCH PASS %d/%d\n%s",
            "=" * 60, attempt, max_attempts, "=" * 60,
        )
        try:
            run_match(mode=mode)
        except requests.exceptions.RequestException as e:
            logger.error("Match pass %d network error: %s", attempt, e,
                         exc_info=True)

        unmatched = _count_unmatched_events(engine)
        logger.info("After pass %d: %d unmatched events remain",
                    attempt, unmatched)
        if unmatched == 0:
            logger.info("All events matched — done")
            return
        if last_unmatched is not None and unmatched == last_unmatched:
            # Remaining unmatched are likely genuine non-NHC events
            # (JTWC basin) or persistent fetch failures. Re-running
            # won't help.
            logger.info("Unmatched count stable at %d — match done "
                        "(remaining are likely non-NHC or persistent "
                        "failures)", unmatched)
            return
        last_unmatched = unmatched
        if attempt < max_attempts:
            logger.info("Sleeping %ds before next pass...", retry_delay)
            time.sleep(retry_delay)
    logger.info("Hit max match attempts (%d)", max_attempts)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--mode", choices=["dev", "prod"], default="dev")
    p.add_argument("--from-date", default="2010-01-01")
    p.add_argument(
        "--source", choices=["NOAA", "JTWC"], default="NOAA",
    )
    p.add_argument(
        "--timeout", type=int, default=120,
        help="Per-request HTTP timeout (seconds, default 120)",
    )
    p.add_argument("--max-ingest-attempts", type=int, default=5)
    p.add_argument("--max-match-attempts", type=int, default=3)
    p.add_argument("--retry-delay", type=int, default=60)
    args = p.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    # Override ocha-lens HTTP timeout
    gdacs_api._TIMEOUT = args.timeout
    logger.info("Set GDACS HTTP timeout to %ds", args.timeout)

    engine = stratus.get_engine(args.mode, write=True)

    logger.info("Starting historical GDACS backfill (mode=%s, "
                "from_date=%s, source=%s)",
                args.mode, args.from_date, args.source)
    logger.info("Starting row count: %d", _count_exposure_rows(engine))

    _ingest_phase(
        engine,
        from_date=args.from_date,
        source=args.source,
        mode=args.mode,
        max_attempts=args.max_ingest_attempts,
        retry_delay=args.retry_delay,
    )

    _match_phase(
        engine,
        mode=args.mode,
        max_attempts=args.max_match_attempts,
        retry_delay=args.retry_delay,
    )

    logger.info("%s\nBACKFILL COMPLETE\n%s", "=" * 60, "=" * 60)
    logger.info("  total exposure rows:  %d", _count_exposure_rows(engine))
    logger.info("  unmatched events:     %d", _count_unmatched_events(engine))


if __name__ == "__main__":
    main()
