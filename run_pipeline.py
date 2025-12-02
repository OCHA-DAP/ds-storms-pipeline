import argparse
import sys

from src.pipelines.ecmwf import run_ecmwf
from src.pipelines.ibtracs import run_ibtracs


def main():
    main_parser = argparse.ArgumentParser()
    main_parser.add_argument(
        "pipeline",
        choices=["ibtracs", "ecmwf"],
        help="Pipeline to run",
    )
    main_parser.add_argument(
        "--mode",
        choices=["dev", "prod"],
        default="dev",
        help="Mode to run the pipeline in",
    )
    main_parser.add_argument(
        "--save-to-blob",
        choices=[True, False],
        default=False,
        nargs="?",
        help="Save netcdf file in blob",
    )
    main_parser.add_argument(
        "--dataset-type",
        choices=["last3years", "ACTIVE", "ALL"],
        default="last3years",
        nargs="?",
        help="Which dataset type to use",
    )
    main_parser.add_argument(
        "--start-date",
        default="2019-01-01",
        nargs="?",
        help="Which date to start",
    )
    main_parser.add_argument(
        "--end-date",
        default="2019-03-31",
        nargs="?",
        help="Which date to end",
    )
    main_parser.add_argument(
        "--save-dir",
        default="/tmp",
        nargs="?",
        help="Where to save downloaded file",
    )
    main_parser.add_argument(
        "--chunksize",
        default=10000,
        nargs="?",
        help="Which chunksize to use in sql",
    )

    args, remaining_args = main_parser.parse_known_args()
    sys.argv = [sys.argv[0]] + remaining_args

    if args.pipeline == "ibtracs":
        run_ibtracs(
            args.mode,
            args.save_to_blob,
            args.save_dir,
            args.chunksize,
        )
    elif args.pipeline == "ecmwf":
        from datetime import datetime

        run_ecmwf(
            mode=args.mode,
            start_date=datetime.strptime(args.start_date, "%Y-%m-%d"),
            end_date=datetime.strptime(args.end_date, "%Y-%m-%d"),
            save_to_blob=args.save_to_blob,
            save_dir=args.save_dir,
            chunksize=args.chunksize,
        )
    else:
        raise ValueError(f"Unknown pipeline: {args.pipeline}")


if __name__ == "__main__":
    main()
