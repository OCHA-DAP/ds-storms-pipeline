import argparse
from datetime import datetime, timedelta

from src.pipelines.ecmwf import run_ecmwf
from src.pipelines.ibtracs import run_ibtracs, run_wind_buffers
from src.pipelines.nhc import run_nhc_current, run_nhc_archive, run_nhc_wind_buffers


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

    # IBTrACS subparser
    ibtracs_parser = subparsers.add_parser(
        "ibtracs", parents=[common], help="Run IBTrACS pipeline"
    )
    ibtracs_parser.add_argument(
        "--save-to-blob",
        action="store_true",
        help="Save netcdf file in blob",
    )
    ibtracs_parser.add_argument(
        "--dataset-type",
        choices=["last3years", "ACTIVE", "ALL"],
        default="last3years",
        help="Which dataset type to use",
    )
    ibtracs_parser.add_argument(
        "--save-dir",
        default="/tmp",
        help="Where to save downloaded file",
    )

    # ECMWF subparser
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    ecmwf_parser = subparsers.add_parser(
        "ecmwf", parents=[common], help="Run ECMWF pipeline"
    )
    ecmwf_parser.add_argument(
        "--start-date",
        default=yesterday,
        help="Start date (YYYY-MM-DD) (default: yesterday)",
    )
    ecmwf_parser.add_argument(
        "--end-date",
        default=yesterday,
        help="End date (YYYY-MM-DD) (default: yesterday)",
    )

    # NHC subparser
    nhc_parser = subparsers.add_parser(
        "nhc", parents=[common], help="Run NHC pipeline"
    )
    nhc_parser.add_argument(
        "--save-to-blob",
        action="store_true",
        help="Save files to blob storage",
    )
    nhc_parser.add_argument(
        "--save-dir",
        default="/tmp",
        help="Where to save downloaded files",
    )
    nhc_parser.add_argument(
        "--start-year",
        type=int,
        help="Start year for archive processing (e.g., 2020). If not provided, processes current active storms.",
    )
    nhc_parser.add_argument(
        "--end-year",
        type=int,
        help="End year for archive processing (e.g., 2024). If not provided, only processes start-year.",
    )

    # Wind buffers subparser
    wind_buffers_parser = subparsers.add_parser(
        "wind-buffers",
        parents=[common],
        help="Run IBTrACS wind buffers pipeline (reads PROD tracks, writes DEV buffers)",
    )
    wind_buffers_parser.add_argument(
        "--basin",
        help="Filter to a single basin (e.g. NA, WP, EP)",
    )
    wind_buffers_parser.add_argument(
        "--start-year",
        type=int,
        help="Only process storms with track points from this year onwards",
    )
    wind_buffers_parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Recalculate buffers even for storms already in the database",
    )

    # NHC wind buffers subparser
    nhc_wind_buffers_parser = subparsers.add_parser(
        "nhc-wind-buffers",
        parents=[common],
        help="Run NHC forecast wind buffers pipeline (reads PROD tracks, writes DEV buffers)",
    )
    nhc_wind_buffers_parser.add_argument(
        "--basin",
        help="Filter to a single basin (e.g. NA, EP)",
    )
    nhc_wind_buffers_parser.add_argument(
        "--start-year",
        type=int,
        help="Only process issuances from this year onwards",
    )
    nhc_wind_buffers_parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Recalculate buffers even for issuances already in the database",
    )

    args = parser.parse_args()

    if args.pipeline == "ibtracs":
        run_ibtracs(
            mode=args.mode,
            dataset_type=args.dataset_type,
            save_to_blob=args.save_to_blob,
            save_dir=args.save_dir,
            chunksize=args.chunksize,
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
            # Archive mode: year(s) provided
            end_year = (
                args.end_year if args.end_year is not None else args.start_year
            )
            run_nhc_archive(
                start_year=args.start_year,
                end_year=end_year,
                mode=args.mode,
                save_to_blob=args.save_to_blob,
                save_dir=args.save_dir,
                chunksize=args.chunksize,
            )
        else:
            # Current mode: no year provided
            run_nhc_current(
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
    elif args.pipeline == "nhc-wind-buffers":
        run_nhc_wind_buffers(
            write_mode=args.mode,
            chunksize=args.chunksize,
            basin=args.basin,
            start_year=args.start_year,
            overwrite=args.overwrite,
        )


if __name__ == "__main__":
    main()
