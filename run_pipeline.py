import argparse
import logging
from datetime import datetime, timedelta

import pandas as pd


def _parse_it(value):
    """Parse a CLI --issued-time argument into a Python datetime.

    Accepts formats like 'YYYY-MM-DDTHH' or 'YYYY-MM-DD HH:MM' that
    pandas understands. Returns None if value is None/empty.
    """
    if value is None or value == "":
        return None
    return pd.to_datetime(value).to_pydatetime()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
for _name in (
    "fsspec", "asyncio", "urllib3", "azure", "uamqp", "rasterio",
    "boto3", "botocore", "s3transfer",
):
    logging.getLogger(_name).setLevel(logging.WARNING)

# DBR auto-instruments pandas with a Spark usage logger that fails to
# attach on Python tasks (no JVM in the executor). The WARNING is
# emitted every time run_pipeline.py imports pandas, which floods the
# DBX run logs. Suppress it at the source.
logging.getLogger("pyspark.databricks.pandas").setLevel(logging.ERROR)
logging.getLogger("pyspark.databricks.pandas.usage_logger").setLevel(logging.ERROR)

from src.pipelines.ecmwf import run_ecmwf
from src.pipelines.ibtracs import (
    run_ibtracs,
    run_ibtracs_exp,
    run_ibtracs_realtime,
    run_wind_buffers,
)
from src.pipelines.nhc import (
    NHC_SAMPLE_JSON_URL,
    run_nhc_archive,
    run_nhc_current,
    run_nhc_realtime,
    run_nhc_scrub,
    run_nhc_tracks_fcast_buffers,
    run_nhc_tracks_fcast_exp,
    run_nhc_tracks_obsv_buffers,
    run_nhc_tracks_obsv_exp,
    run_nhc_tracks_fcastonly_buffers,
    run_nhc_tracks_fcastonly_exp,
    run_nhc_tracks_exp_realtime,
    run_nhc_wsp_exp,
    run_nhc_wsp_exp_realtime,
    run_nhc_wsp_fcastonly_polygons,
    run_nhc_wsp_fcastonly_exp,
    run_nhc_wsp_polygon_matched,
)
from src.pipelines.gdacs import run_gdacs_archive, run_gdacs_current
from src.pipelines.adam import run_adam_archive, run_adam_current
from src.pipelines.match import run_match


def main():
    parser = argparse.ArgumentParser(description="Run storm data pipelines")
    subparsers = parser.add_subparsers(dest="pipeline", required=True)

    # Common arguments as a parent parser
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument(
        "--mode",
        choices=["dev", "prod"],
        default="dev",
        help="Mode to run the pipeline in",
    )
    common.add_argument(
        "--chunksize",
        type=int,
        default=10000,
        help="Chunksize to use in SQL operations",
    )

    # Time-range filter, shared by every pipeline that operates on rows
    # keyed by issued_time / valid_time. Three usage patterns:
    #   single advisory: --issued-time YYYY-MM-DDTHH
    #   range:           --since YYYY-MM-DD --until YYYY-MM-DD
    #   full backfill:   (no time args) — only fills missing rows
    #   forced recompute: --overwrite (combine with any of the above)
    time_filter_common = argparse.ArgumentParser(add_help=False)
    time_filter_common.add_argument(
        "--since", metavar="YYYY-MM-DD",
        help="Inclusive lower bound on issued_time / valid_time",
    )
    time_filter_common.add_argument(
        "--until", metavar="YYYY-MM-DD",
        help="Exclusive upper bound on issued_time / valid_time",
    )
    time_filter_common.add_argument(
        "--issued-time", metavar="YYYY-MM-DDTHH",
        help=(
            "Process exactly this single issued_time (or valid_time for "
            "obsv pipelines). Mutually exclusive with --since/--until."
        ),
    )
    time_filter_common.add_argument(
        "--overwrite", action="store_true",
        help="Recompute and upsert even if results already exist",
    )

    # Storm-basin filter, used by every NHC pipeline (buffer, WSP, exposure).
    basin_common = argparse.ArgumentParser(add_help=False)
    basin_common.add_argument(
        "--basin", metavar="BASIN",
        help="Limit to a specific basin (e.g. NA, EP)",
    )

    # Exposure-specific filters (storm-keyed pipelines don't have these).
    exp_common = argparse.ArgumentParser(add_help=False)
    exp_common.add_argument(
        "--countries", nargs="+", metavar="ISO3",
        help="Limit to specific country ISO3 codes",
    )
    exp_common.add_argument(
        "--admin-level", type=int, choices=[0, 1], action="append",
        metavar="N", dest="admin_level",
        help=(
            "Admin level to compute exposure for (repeatable). "
            "Default: both 0 and 1."
        ),
    )

    # ------------------------------------------------------------------ #
    # IBTrACS ETL
    # ------------------------------------------------------------------ #
    ibtracs_parser = subparsers.add_parser(
        "ibtracs", parents=[common], help="Run IBTrACS ETL pipeline"
    )
    ibtracs_parser.add_argument("--save-to-blob", action="store_true")
    ibtracs_parser.add_argument(
        "--dataset-type",
        choices=["last3years", "ACTIVE", "ALL"],
        default="last3years",
    )
    ibtracs_parser.add_argument("--save-dir", default="/tmp")

    # ------------------------------------------------------------------ #
    # IBTrACS realtime (ETL + wind buffers + exposure)
    # ------------------------------------------------------------------ #
    ibtracs_rt_parser = subparsers.add_parser(
        "ibtracs-realtime",
        parents=[common],
        help="IBTrACS ETL then wind buffers then exposure (scoped to active storms)",
    )
    ibtracs_rt_parser.add_argument("--save-to-blob", action="store_true")
    ibtracs_rt_parser.add_argument("--save-dir", default="/tmp")

    # ------------------------------------------------------------------ #
    # IBTrACS wind buffers
    # ------------------------------------------------------------------ #
    wind_buffers_parser = subparsers.add_parser(
        "wind-buffers",
        parents=[common],
        help="Compute IBTrACS wind buffer polygons from tracks",
    )
    wind_buffers_parser.add_argument("--basin")
    wind_buffers_parser.add_argument("--start-year", type=int)
    wind_buffers_parser.add_argument("--overwrite", action="store_true")

    # ------------------------------------------------------------------ #
    # IBTrACS exposure
    # ------------------------------------------------------------------ #
    ibtracs_exp_parser = subparsers.add_parser(
        "ibtracs-track-exp",
        parents=[common, exp_common],
        help="Population exposure from IBTrACS wind buffers",
    )
    ibtracs_exp_parser.add_argument(
        "--since", type=int, metavar="YEAR",
        help="Only include storms from this season year onwards",
    )

    # ------------------------------------------------------------------ #
    # ECMWF
    # ------------------------------------------------------------------ #
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    ecmwf_parser = subparsers.add_parser(
        "ecmwf", parents=[common], help="Run ECMWF pipeline"
    )
    ecmwf_parser.add_argument("--start-date", default=yesterday)
    ecmwf_parser.add_argument("--end-date", default=yesterday)

    # ------------------------------------------------------------------ #
    # NHC ETL
    # ------------------------------------------------------------------ #
    nhc_parser = subparsers.add_parser(
        "nhc", parents=[common], help="Run NHC ETL pipeline"
    )
    nhc_parser.add_argument("--save-to-blob", action="store_true")
    nhc_parser.add_argument("--save-dir", default="/tmp")
    nhc_parser.add_argument(
        "--sample-json", metavar="URL",
        help=(
            "Test mode: fetch CurrentStorms.json from this URL instead of the "
            "live NHC endpoint. WSP polygons follow from the GIS URL inside "
            "the sample JSON, exactly like realtime. Recommended: "
            f"{NHC_SAMPLE_JSON_URL}"
        ),
    )
    nhc_parser.add_argument(
        "--start-year", type=int,
        help="Start year for archive mode. Omit for current active storms.",
    )
    nhc_parser.add_argument("--end-year", type=int)
    nhc_parser.add_argument(
        "--out-issued-times-json",
        metavar="PATH",
        help=(
            "After the 'current' mode finishes, write a small JSON file with"
            " {track_issued_time, wsp_issued_time} extracted from the scraped"
            " JSON. Downstream Databricks tasks can read this to set task"
            " values."
        ),
    )

    # ------------------------------------------------------------------ #
    # NHC realtime (ETL + wind buffers + track exp + WSP exp)
    # ------------------------------------------------------------------ #
    nhc_rt_parser = subparsers.add_parser(
        "nhc-realtime",
        parents=[common],
        help="NHC ETL then wind buffers then exposure (scoped to current issued_time)",
    )
    nhc_rt_parser.add_argument("--save-to-blob", action="store_true")
    nhc_rt_parser.add_argument("--save-dir", default="/tmp")

    # ------------------------------------------------------------------ #
    # NHC wind buffers
    # ------------------------------------------------------------------ #
    nhc_tracks_fcast_buffers_parser = subparsers.add_parser(
        "nhc-tracks-fcast-buffers",
        parents=[common, time_filter_common, basin_common],
        help="Compute NHC forecast wind buffer polygons from tracks",
    )

    # ------------------------------------------------------------------ #
    # NHC observational track buffers
    # ------------------------------------------------------------------ #
    nhc_tracks_obsv_buffers_parser = subparsers.add_parser(
        "nhc-tracks-obsv-buffers",
        parents=[common, time_filter_common, basin_common],
        help="Compute cumulative observational NHC wind buffer polygons from tracks",
    )

    # ------------------------------------------------------------------ #
    # NHC forecast-only track buffers
    # ------------------------------------------------------------------ #
    nhc_tracks_fcastonly_buffers_parser = subparsers.add_parser(
        "nhc-tracks-fcastonly-buffers",
        parents=[common, time_filter_common, basin_common],
        help="Compute NHC forecast-only wind buffer polygons (forecast minus observed swath)",
    )

    # ------------------------------------------------------------------ #
    # NHC track exposure
    # ------------------------------------------------------------------ #
    nhc_track_exp_parser = subparsers.add_parser(
        "nhc-track-exp",
        parents=[common, exp_common, time_filter_common, basin_common],
        help="Population exposure from NHC forecast wind buffers",
    )

    # ------------------------------------------------------------------ #
    # NHC observed track buffer exposure
    # ------------------------------------------------------------------ #
    nhc_obsv_exp_parser = subparsers.add_parser(
        "nhc-obsv-exp",
        parents=[common, exp_common, time_filter_common, basin_common],
        help="Population exposure from NHC cumulative observed track buffers",
    )
    nhc_obsv_exp_parser.add_argument(
        "--final-only", action="store_true",
        help=(
            "Keep only the final cumulative buffer per (atcf_id, wind_speed_kt) "
            "at max(valid_time). For historical backfills where intermediate "
            "advisories aren't needed."
        ),
    )

    # ------------------------------------------------------------------ #
    # NHC forecast-only track buffer exposure
    # ------------------------------------------------------------------ #
    nhc_fcastonly_exp_parser = subparsers.add_parser(
        "nhc-fcastonly-exp",
        parents=[common, exp_common, time_filter_common, basin_common],
        help="Population exposure from NHC forecast-only track buffers",
    )

    # ------------------------------------------------------------------ #
    # NHC WSP exposure
    # ------------------------------------------------------------------ #
    nhc_wsp_exp_parser = subparsers.add_parser(
        "nhc-wsp-exp",
        parents=[common, exp_common, time_filter_common, basin_common],
        help="Population exposure from NHC WSP polygons",
    )

    # ------------------------------------------------------------------ #
    # NHC WSP polygon matched (raw WSP + tracks -> per-storm MultiPolygons)
    # ------------------------------------------------------------------ #
    subparsers.add_parser(
        "nhc-wsp-polygon-matched",
        parents=[common, time_filter_common, basin_common],
        help="Build storms.nhc_wsp_polygon_matched from raw WSP + tracks",
    )

    # ------------------------------------------------------------------ #
    # NHC WSP forecast-only polygons (WSP minus observed track swath)
    # ------------------------------------------------------------------ #
    nhc_wsp_fcastonly_polygons_parser = subparsers.add_parser(
        "nhc-wsp-fcastonly-polygons",
        parents=[common, time_filter_common, basin_common],
        help="Compute WSP forecast-only polygons (WSP minus observed track swath)",
    )

    # ------------------------------------------------------------------ #
    # NHC WSP forecast-only exposure
    # ------------------------------------------------------------------ #
    nhc_wsp_fcastonly_exp_parser = subparsers.add_parser(
        "nhc-wsp-fcastonly-exp",
        parents=[common, exp_common, time_filter_common, basin_common],
        help="Population exposure from WSP forecast-only polygons",
    )

    # ------------------------------------------------------------------ #
    # NHC realtime exposure composites — one process per cascade, sharing
    # WorldPop + FieldMaps + engine across the inner pipelines. Used by
    # the DBX realtime-tracks-exposure / realtime-wsp-exposure tasks.
    # ------------------------------------------------------------------ #
    # Composite runners — accept the same time filters as their inner
    # pipelines so they can drive both realtime (one issuance) and
    # backfill (since/until range) flows with one DBX task.
    nhc_rt_tracks_exp_parser = subparsers.add_parser(
        "nhc-realtime-tracks-exp",
        parents=[common, exp_common, time_filter_common, basin_common],
        help="Composite tracks exposure cascade: fcast + obsv + fcastonly, shared setup",
    )

    nhc_rt_wsp_exp_parser = subparsers.add_parser(
        "nhc-realtime-wsp-exp",
        parents=[common, exp_common, time_filter_common, basin_common],
        help="Composite WSP exposure cascade: wsp + wsp-fcastonly, shared setup",
    )

    # ------------------------------------------------------------------ #
    # NHC scrub (cleanup test / sample rows from every NHC table)
    # ------------------------------------------------------------------ #
    nhc_scrub_parser = subparsers.add_parser(
        "nhc-scrub", parents=[common],
        help="Delete sample/test rows for given atcf_ids from all NHC tables",
    )
    nhc_scrub_parser.add_argument(
        "--sample", action="store_true",
        help=(
            "Auto-resolve atcf_ids and issued_times from the NHC sample JSON "
            f"({NHC_SAMPLE_JSON_URL}) and scrub those. Mutually exclusive "
            "with --atcf-id / --issued-time."
        ),
    )
    nhc_scrub_parser.add_argument(
        "--atcf-id", action="append", default=[], dest="atcf_id",
        help="atcf_id to scrub (repeatable). Ignored when --sample is set.",
    )
    nhc_scrub_parser.add_argument(
        "--issued-time", action="append", default=[], dest="issued_time",
        metavar="YYYY-MM-DDTHH",
        help=(
            "issued_time to scrub from nhc_wsp_polygon_raw (repeatable). "
            "Required if you want WSP raw rows gone. "
            "Ignored when --sample is set."
        ),
    )
    nhc_scrub_parser.add_argument(
        "--dry-run", action="store_true",
        help="Log counts only; don't actually delete anything.",
    )

    # GDACS subparser
    gdacs_parser = subparsers.add_parser(
        "gdacs", parents=[common], help="Run GDACS pipeline"
    )
    gdacs_parser.add_argument(
        "--from-date",
        help="Archive mode: start date (YYYY-MM-DD). If not provided, runs current mode (last `days_back` days).",
    )
    gdacs_parser.add_argument(
        "--to-date",
        help="Archive mode: end date (YYYY-MM-DD). Defaults to today.",
    )
    gdacs_parser.add_argument(
        "--days-back",
        type=int,
        default=7,
        help="Current mode: how many days back from today (default 7).",
    )
    gdacs_parser.add_argument(
        "--source",
        choices=["NOAA", "JTWC"],
        default="NOAA",
        help="GDACS source filter (default: NOAA)",
    )
    gdacs_parser.add_argument(
        "--all-episodes",
        action="store_true",
        help=(
            "Fetch exposure for every episode of each event (full GDACS "
            "history), not just the latest. ~Nx more HTTP calls per "
            "event; per-episode skip via existing rows keeps re-runs cheap."
        ),
    )

    # Match subparser (GDACS -> NHC atcf_id, writes storm_id_lookup)
    subparsers.add_parser(
        "match", parents=[common], help="Run GDACS->ATCF matching pipeline"
    )

    # ADAM subparser
    adam_parser = subparsers.add_parser(
        "adam", parents=[common], help="Run ADAM pipeline"
    )
    adam_parser.add_argument(
        "--from-date",
        help="Archive mode: start date (YYYY-MM-DD). If not provided, runs current mode (last `days_back` days).",
    )
    adam_parser.add_argument(
        "--to-date",
        help="Archive mode: end date (YYYY-MM-DD). Defaults to today.",
    )
    adam_parser.add_argument(
        "--days-back",
        type=int,
        default=14,
        help="Current mode: how many days back from today (default 14).",
    )
    adam_parser.add_argument(
        "--source",
        choices=["NOAA", "JTWC"],
        default="NOAA",
        help="ADAM source filter (default: NOAA)",
    )
    adam_parser.add_argument(
        "--all-episodes",
        action="store_true",
        help=(
            "Fetch exposure for every episode of each event (full ADAM "
            "history), not just the latest. ~Nx more CSV downloads per "
            "event; per-episode skip via existing rows keeps re-runs cheap."
        ),
    )

    # ------------------------------------------------------------------ #
    args = parser.parse_args()

    if args.pipeline == "ibtracs":
        run_ibtracs(
            mode=args.mode,
            dataset_type=args.dataset_type,
            save_to_blob=args.save_to_blob,
            save_dir=args.save_dir,
            chunksize=args.chunksize,
        )
    elif args.pipeline == "ibtracs-realtime":
        run_ibtracs_realtime(
            mode=args.mode,
            save_to_blob=args.save_to_blob,
            save_dir=args.save_dir,
            chunksize=args.chunksize,
        )
    elif args.pipeline == "wind-buffers":
        run_wind_buffers(
            write_mode=args.mode,
            chunksize=args.chunksize,
            basin=args.basin,
            start_year=args.start_year,
            overwrite=args.overwrite,
        )
    elif args.pipeline == "ibtracs-track-exp":
        countries = [c.upper() for c in args.countries] if args.countries else None
        run_ibtracs_exp(
            countries=countries,
            since=args.since,
            basin=args.basin,
            overwrite=args.overwrite,
            mode=args.mode,
        )
    elif args.pipeline == "ecmwf":
        run_ecmwf(
            mode=args.mode,
            start_date=datetime.strptime(args.start_date, "%Y-%m-%d"),
            end_date=datetime.strptime(args.end_date, "%Y-%m-%d"),
            chunksize=args.chunksize,
        )
    elif args.pipeline == "nhc":
        if args.start_year is not None:
            end_year = args.end_year if args.end_year is not None else args.start_year
            run_nhc_archive(
                start_year=args.start_year,
                end_year=end_year,
                mode=args.mode,
                save_to_blob=args.save_to_blob,
                save_dir=args.save_dir,
                chunksize=args.chunksize,
            )
        else:
            result = run_nhc_current(
                mode=args.mode,
                save_to_blob=args.save_to_blob,
                save_dir=args.save_dir,
                chunksize=args.chunksize,
                sample_json=args.sample_json,
            )
            if args.out_issued_times_json:
                import json
                payload = {
                    k: (None if v is None else pd.Timestamp(v).isoformat())
                    for k, v in (result or {}).items()
                }
                with open(args.out_issued_times_json, "w") as f:
                    json.dump(payload, f)
                logging.getLogger(__name__).info(
                    f"Wrote issued_times to {args.out_issued_times_json}: {payload}"
                )
    elif args.pipeline == "nhc-realtime":
        run_nhc_realtime(
            mode=args.mode,
            save_to_blob=args.save_to_blob,
            save_dir=args.save_dir,
            chunksize=args.chunksize,
        )
    elif args.pipeline == "nhc-tracks-fcast-buffers":
        run_nhc_tracks_fcast_buffers(
            write_mode=args.mode,
            chunksize=args.chunksize,
            basin=args.basin,
            since=args.since,
            until=args.until,
            overwrite=args.overwrite,
            issued_time=_parse_it(getattr(args, "issued_time", None)),
        )
    elif args.pipeline == "nhc-tracks-obsv-buffers":
        run_nhc_tracks_obsv_buffers(
            write_mode=args.mode,
            chunksize=args.chunksize,
            basin=args.basin,
            since=args.since,
            until=args.until,
            overwrite=args.overwrite,
            issued_time=_parse_it(getattr(args, "issued_time", None)),
        )
    elif args.pipeline == "nhc-tracks-fcastonly-buffers":
        run_nhc_tracks_fcastonly_buffers(
            write_mode=args.mode,
            chunksize=args.chunksize,
            basin=args.basin,
            since=args.since,
            until=args.until,
            overwrite=args.overwrite,
            issued_time=_parse_it(getattr(args, "issued_time", None)),
        )
    elif args.pipeline == "nhc-track-exp":
        countries = [c.upper() for c in args.countries] if args.countries else None
        run_nhc_tracks_fcast_exp(
            countries=countries,
            since=args.since,
            until=args.until,
            basin=args.basin,
            overwrite=args.overwrite,
            mode=args.mode,
            issued_time=_parse_it(getattr(args, "issued_time", None)),
            admin_levels=getattr(args, "admin_level", None),
        )
    elif args.pipeline == "nhc-obsv-exp":
        countries = [c.upper() for c in args.countries] if args.countries else None
        # obsv-exp's underlying function uses ``valid_time`` (the obsv
        # buffer key); the realtime orchestrator passes track_issued_time
        # here, which equals the latest obsv valid_time.
        run_nhc_tracks_obsv_exp(
            countries=countries,
            since=args.since,
            until=args.until,
            basin=args.basin,
            overwrite=args.overwrite,
            mode=args.mode,
            valid_time=_parse_it(getattr(args, "issued_time", None)),
            admin_levels=getattr(args, "admin_level", None),
            final_only=getattr(args, "final_only", False),
        )
    elif args.pipeline == "nhc-fcastonly-exp":
        countries = [c.upper() for c in args.countries] if args.countries else None
        run_nhc_tracks_fcastonly_exp(
            countries=countries,
            since=args.since,
            until=args.until,
            basin=args.basin,
            overwrite=args.overwrite,
            mode=args.mode,
            issued_time=_parse_it(getattr(args, "issued_time", None)),
            admin_levels=getattr(args, "admin_level", None),
        )
    elif args.pipeline == "nhc-wsp-exp":
        countries = [c.upper() for c in args.countries] if args.countries else None
        run_nhc_wsp_exp(
            countries=countries,
            since=args.since,
            until=args.until,
            basin=args.basin,
            overwrite=args.overwrite,
            mode=args.mode,
            issued_time=_parse_it(getattr(args, "issued_time", None)),
            admin_levels=getattr(args, "admin_level", None),
        )
    elif args.pipeline == "nhc-wsp-polygon-matched":
        run_nhc_wsp_polygon_matched(
            mode=args.mode,
            since=args.since,
            until=args.until,
            basin=args.basin,
            issued_time=_parse_it(getattr(args, "issued_time", None)),
            overwrite=args.overwrite,
        )
    elif args.pipeline == "nhc-wsp-fcastonly-polygons":
        run_nhc_wsp_fcastonly_polygons(
            mode=args.mode,
            since=args.since,
            until=args.until,
            basin=args.basin,
            issued_time=_parse_it(getattr(args, "issued_time", None)),
            overwrite=args.overwrite,
        )
    elif args.pipeline == "nhc-wsp-fcastonly-exp":
        countries = [c.upper() for c in args.countries] if args.countries else None
        run_nhc_wsp_fcastonly_exp(
            countries=countries,
            since=args.since,
            until=args.until,
            basin=args.basin,
            overwrite=args.overwrite,
            mode=args.mode,
            issued_time=_parse_it(getattr(args, "issued_time", None)),
            admin_levels=getattr(args, "admin_level", None),
        )
    elif args.pipeline == "nhc-realtime-tracks-exp":
        countries = [c.upper() for c in args.countries] if args.countries else None
        run_nhc_tracks_exp_realtime(
            mode=args.mode,
            issued_time=_parse_it(getattr(args, "issued_time", None)),
            countries=countries,
            since=args.since,
            until=args.until,
            basin=args.basin,
            overwrite=args.overwrite,
            admin_levels=getattr(args, "admin_level", None),
        )
    elif args.pipeline == "nhc-realtime-wsp-exp":
        countries = [c.upper() for c in args.countries] if args.countries else None
        run_nhc_wsp_exp_realtime(
            mode=args.mode,
            issued_time=_parse_it(getattr(args, "issued_time", None)),
            countries=countries,
            since=args.since,
            until=args.until,
            basin=args.basin,
            overwrite=args.overwrite,
            admin_levels=getattr(args, "admin_level", None),
        )
    elif args.pipeline == "nhc-scrub":
        if args.sample:
            import requests
            data = requests.get(NHC_SAMPLE_JSON_URL, timeout=10).json()
            storms = data.get("activeStorms", [])
            atcf_ids = sorted({
                s["id"][:2].upper() + s["id"][2:] for s in storms
            })
            issued_times: list[pd.Timestamp] = []
            track_times = [pd.Timestamp(s["lastUpdate"]) for s in storms]
            if track_times:
                issued_times.append(max(track_times))
            wsp_times = [
                pd.Timestamp(s["windSpeedProbabilitiesGIS"]["issuance"])
                for s in storms
                if s.get("windSpeedProbabilitiesGIS")
            ]
            if wsp_times:
                issued_times.append(max(wsp_times))
            issued_times = sorted(set(issued_times))
        else:
            if not args.atcf_id:
                nhc_scrub_parser.error(
                    "either --sample or at least one --atcf-id is required"
                )
            atcf_ids = args.atcf_id
            issued_times = [
                pd.Timestamp(t) for t in args.issued_time
            ]
        run_nhc_scrub(
            atcf_ids=atcf_ids,
            issued_times=issued_times,
            mode=args.mode,
            dry_run=args.dry_run,
        )
    elif args.pipeline == "gdacs":
        if args.from_date is not None:
            # Archive mode: date range provided
            run_gdacs_archive(
                from_date=args.from_date,
                to_date=args.to_date,
                source=args.source,
                mode=args.mode,
                chunksize=args.chunksize,
                all_episodes=args.all_episodes,
            )
        else:
            # Current mode: rolling window from today
            run_gdacs_current(
                mode=args.mode,
                days_back=args.days_back,
                source=args.source,
                chunksize=args.chunksize,
                all_episodes=args.all_episodes,
            )
    elif args.pipeline == "match":
        run_match(mode=args.mode)
    elif args.pipeline == "adam":
        if args.from_date is not None:
            run_adam_archive(
                from_date=args.from_date,
                to_date=args.to_date,
                source=args.source,
                mode=args.mode,
                chunksize=args.chunksize,
                all_episodes=args.all_episodes,
            )
        else:
            run_adam_current(
                mode=args.mode,
                days_back=args.days_back,
                source=args.source,
                chunksize=args.chunksize,
                all_episodes=args.all_episodes,
            )


if __name__ == "__main__":
    main()
