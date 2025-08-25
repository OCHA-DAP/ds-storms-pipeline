import argparse
import sys

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
        "--chunksize",
        choices=["last3years", "ACTIVE", "ALL"],
        default=10000,
        nargs="?",
        help="Which chunksize to us in sql",
    )

    args, remaining_args = main_parser.parse_known_args()
    sys.argv = [sys.argv[0]] + remaining_args

    if args.pipeline == "ibtracs":
        run_ibtracs(
            args.mode, args.dataset_type, args.save_to_blob, args.chunksize
        )
    elif args.pipeline == "ecmwf":
        # TODO
        raise NotImplementedError()
    else:
        raise ValueError(f"Unknown pipeline: {args.pipeline}")


if __name__ == "__main__":
    main()
