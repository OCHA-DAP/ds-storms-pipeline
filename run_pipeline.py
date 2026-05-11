import argparse
import logging
from datetime import datetime, timedelta

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
for _name in ("fsspec", "asyncio", "urllib3", "azure", "uamqp", "rasterio"):
    logging.getLogger(_name).setLevel(logging.WARNING)

from src.pipelines.ecmwf import run_ecmwf
from src.pipelines.ibtracs import (
    run_ibtracs,
    run_ibtracs_exp,
    run_ibtracs_realtime,
    run_wind_buffers,
)
from src.pipelines.nhc import (
    run_nhc_archive,
    run_nhc_current,
    run_nhc_realtime,
    run_nhc_tracks_fcast_buffers,
    run_nhc_tracks_fcast_exp,
    run_nhc_tracks_obsv_buffers,
    run_nhc_tracks_obsv_exp,
    run_nhc_tracks_fcastonly_buffers,
    run_nhc_tracks_fcastonly_exp,
    run_nhc_wsp_exp,
    run_nhc_wsp_fcastonly_polygons,
    run_nhc_wsp_fcastonly_exp,
)


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

    # Exposure common args (countries, since, basin, overwrite)
    exp_common = argparse.ArgumentParser(add_help=False)
    exp_common.add_argument(
        "--countries", nargs="+", metavar="ISO3",
        help="Limit to specific country ISO3 codes",
    )
    exp_common.add_argument(
        "--basin", metavar="BASIN",
        help="Limit to a specific basin (e.g. NA, EP)",
    )
    exp_common.add_argument(
        "--overwrite", action="store_true",
        help="Recalculate even if results already exist",
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
        "--start-year", type=int,
        help="Start year for archive mode. Omit for current active storms.",
    )
    nhc_parser.add_argument("--end-year", type=int)

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
        parents=[common],
        help="Compute NHC forecast wind buffer polygons from tracks",
    )
    nhc_tracks_fcast_buffers_parser.add_argument("--basin")
    nhc_tracks_fcast_buffers_parser.add_argument("--start-year", type=int)
    nhc_tracks_fcast_buffers_parser.add_argument("--overwrite", action="store_true")

    # ------------------------------------------------------------------ #
    # NHC observational track buffers
    # ------------------------------------------------------------------ #
    nhc_tracks_obsv_buffers_parser = subparsers.add_parser(
        "nhc-tracks-obsv-buffers",
        parents=[common],
        help="Compute cumulative observational NHC wind buffer polygons from tracks",
    )
    nhc_tracks_obsv_buffers_parser.add_argument("--basin")
    nhc_tracks_obsv_buffers_parser.add_argument("--start-year", type=int)
    nhc_tracks_obsv_buffers_parser.add_argument("--overwrite", action="store_true")

    # ------------------------------------------------------------------ #
    # NHC forecast-only track buffers
    # ------------------------------------------------------------------ #
    nhc_tracks_fcastonly_buffers_parser = subparsers.add_parser(
        "nhc-tracks-fcastonly-buffers",
        parents=[common],
        help="Compute NHC forecast-only wind buffer polygons (forecast minus observed swath)",
    )
    nhc_tracks_fcastonly_buffers_parser.add_argument("--basin")
    nhc_tracks_fcastonly_buffers_parser.add_argument("--start-year", type=int)
    nhc_tracks_fcastonly_buffers_parser.add_argument("--overwrite", action="store_true")

    # ------------------------------------------------------------------ #
    # NHC track exposure
    # ------------------------------------------------------------------ #
    nhc_track_exp_parser = subparsers.add_parser(
        "nhc-track-exp",
        parents=[common, exp_common],
        help="Population exposure from NHC forecast wind buffers",
    )
    nhc_track_exp_parser.add_argument(
        "--since", metavar="YYYY-MM-DD",
        help="Only include buffers with issued_time on or after this date",
    )

    # ------------------------------------------------------------------ #
    # NHC observed track buffer exposure
    # ------------------------------------------------------------------ #
    nhc_obsv_exp_parser = subparsers.add_parser(
        "nhc-obsv-exp",
        parents=[common, exp_common],
        help="Population exposure from NHC cumulative observed track buffers",
    )
    nhc_obsv_exp_parser.add_argument(
        "--since", metavar="YYYY-MM-DD",
        help="Only include buffers with valid_time on or after this date",
    )

    # ------------------------------------------------------------------ #
    # NHC forecast-only track buffer exposure
    # ------------------------------------------------------------------ #
    nhc_fcastonly_exp_parser = subparsers.add_parser(
        "nhc-fcastonly-exp",
        parents=[common, exp_common],
        help="Population exposure from NHC forecast-only track buffers",
    )
    nhc_fcastonly_exp_parser.add_argument(
        "--since", metavar="YYYY-MM-DD",
        help="Only include buffers with issued_time on or after this date",
    )

    # ------------------------------------------------------------------ #
    # NHC WSP exposure
    # ------------------------------------------------------------------ #
    nhc_wsp_exp_parser = subparsers.add_parser(
        "nhc-wsp-exp",
        parents=[common, exp_common],
        help="Population exposure from NHC WSP polygons",
    )
    nhc_wsp_exp_parser.add_argument(
        "--since", metavar="YYYY-MM-DD",
        help="Only include WSP polygons with issued_time on or after this date",
    )

    # ------------------------------------------------------------------ #
    # NHC WSP forecast-only polygons (WSP minus observed track swath)
    # ------------------------------------------------------------------ #
    nhc_wsp_fcastonly_polygons_parser = subparsers.add_parser(
        "nhc-wsp-fcastonly-polygons",
        parents=[common],
        help="Compute WSP forecast-only polygons (WSP minus observed track swath)",
    )
    nhc_wsp_fcastonly_polygons_parser.add_argument(
        "--since", metavar="YYYY-MM-DD",
        help="Only process WSP polygons with issued_time on or after this date",
    )
    nhc_wsp_fcastonly_polygons_parser.add_argument("--basin", metavar="BASIN")
    nhc_wsp_fcastonly_polygons_parser.add_argument(
        "--issued-time", metavar="YYYY-MM-DDTHH",
        help="Process only this specific issued_time",
    )
    nhc_wsp_fcastonly_polygons_parser.add_argument("--overwrite", action="store_true")

    # ------------------------------------------------------------------ #
    # NHC WSP forecast-only exposure
    # ------------------------------------------------------------------ #
    nhc_wsp_fcastonly_exp_parser = subparsers.add_parser(
        "nhc-wsp-fcastonly-exp",
        parents=[common, exp_common],
        help="Population exposure from WSP forecast-only polygons",
    )
    nhc_wsp_fcastonly_exp_parser.add_argument(
        "--since", metavar="YYYY-MM-DD",
        help="Only include polygons with issued_time on or after this date",
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
            run_nhc_current(
                mode=args.mode,
                save_to_blob=args.save_to_blob,
                save_dir=args.save_dir,
                chunksize=args.chunksize,
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
            start_year=args.start_year,
            overwrite=args.overwrite,
        )
    elif args.pipeline == "nhc-tracks-obsv-buffers":
        run_nhc_tracks_obsv_buffers(
            write_mode=args.mode,
            chunksize=args.chunksize,
            basin=args.basin,
            start_year=args.start_year,
            overwrite=args.overwrite,
        )
    elif args.pipeline == "nhc-tracks-fcastonly-buffers":
        run_nhc_tracks_fcastonly_buffers(
            write_mode=args.mode,
            chunksize=args.chunksize,
            basin=args.basin,
            start_year=args.start_year,
            overwrite=args.overwrite,
        )
    elif args.pipeline == "nhc-track-exp":
        countries = [c.upper() for c in args.countries] if args.countries else None
        run_nhc_tracks_fcast_exp(
            countries=countries,
            since=args.since,
            basin=args.basin,
            overwrite=args.overwrite,
            mode=args.mode,
        )
    elif args.pipeline == "nhc-obsv-exp":
        countries = [c.upper() for c in args.countries] if args.countries else None
        run_nhc_tracks_obsv_exp(
            countries=countries,
            since=args.since,
            basin=args.basin,
            overwrite=args.overwrite,
            mode=args.mode,
        )
    elif args.pipeline == "nhc-fcastonly-exp":
        countries = [c.upper() for c in args.countries] if args.countries else None
        run_nhc_tracks_fcastonly_exp(
            countries=countries,
            since=args.since,
            basin=args.basin,
            overwrite=args.overwrite,
            mode=args.mode,
        )
    elif args.pipeline == "nhc-wsp-exp":
        countries = [c.upper() for c in args.countries] if args.countries else None
        run_nhc_wsp_exp(
            countries=countries,
            since=args.since,
            basin=args.basin,
            overwrite=args.overwrite,
            mode=args.mode,
        )
    elif args.pipeline == "nhc-wsp-fcastonly-polygons":
        run_nhc_wsp_fcastonly_polygons(
            mode=args.mode,
            since=args.since,
            basin=args.basin,
            issued_time=getattr(args, "issued_time", None),
            overwrite=args.overwrite,
        )
    elif args.pipeline == "nhc-wsp-fcastonly-exp":
        countries = [c.upper() for c in args.countries] if args.countries else None
        run_nhc_wsp_fcastonly_exp(
            countries=countries,
            since=args.since,
            basin=args.basin,
            overwrite=args.overwrite,
            mode=args.mode,
        )


if __name__ == "__main__":
    main()
