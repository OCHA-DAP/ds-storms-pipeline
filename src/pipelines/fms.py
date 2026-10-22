#!/usr/bin/env python3
"""
FMS (Fiji Meteorological Service / RSMC Nadi) forecast-track ETL.

RSMC Nadi emails each forecast track as a CSV export of its cyclone forecast
software. ``parse_fms_csv`` turns one file into rows for storms.fms_storms,
storms.fms_forecasts and storms.fms_tracks_geo (schema:
src/schemas/sql/fms_tables.sql); ``ingest_fms_csv`` archives the raw file to
blob and writes that one issuance.

The pa-aa-fji-storms Databricks job runs this module on its own cluster
(``python -m src.pipelines.fms ingest ...``), which has Python 3.11 and none
of this repo's other dependencies. Keep the imports here to pandas,
sqlalchemy, python-dotenv and ocha-stratus (tests/test_fms.py checks). That
job runs a tagged version of this repo (``fms-v*``): after changing this
module, tag main and bump ``storms_pipeline_ref`` in its databricks.yml.
"""

import argparse
import hashlib
import io
import json
import logging
import math
import re
import uuid
from dataclasses import dataclass
from datetime import datetime

import ocha_stratus as stratus
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import text

load_dotenv()

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

PROVIDER = "fms"
BASIN = "SP"
RAW_CONTAINER = "storm"

TIME_COL = "Time[fmt=yyyy-MM-dd'T'HH:mm:ss'Z']"

# FMS's word for each wind threshold (knots) in its radius column headings,
# e.g. GaleRadius / NEGaleRadius. 48 kt ("strong gale") has no NHC column.
RADIUS_WORDS = {34: "Gale", 48: "StrongGale", 50: "Storm", 64: "Hurricane"}
# Same order as nhc_tracks_geo's quadrant_radius_* arrays.
QUADS = ("NE", "SE", "SW", "NW")

SCALAR_COLS = {
    "MeanWind": "wind_speed",
    "WindGust": "gust_speed",
    "Pressure": "pressure",
    "RadiusMaxWinds": "max_wind_radius",
    "RadiusOCI": "last_closed_isobar_radius",
    "PressureOCI": "last_closed_isobar_pressure",
    "Category": "category",
    "Uncertainty": "uncertainty",
}

# The columns the pa-aa-fji-storms trigger reads: position, wind, category,
# uncertainty and the 34/50/64 kt radii. An unreadable value in one of these
# fails the file, so the Fiji monitor falls back to the emailed CSV instead
# of running the trigger on a NULL.
TRIGGER_COLS = {
    "Latitude",
    "Longitude",
    "MeanWind",
    "Category",
    "Uncertainty",
    *(
        f"{quad}{RADIUS_WORDS[speed]}Radius"
        for speed in (34, 50, 64)
        for quad in ("", *QUADS)
    ),
}

# Archived folders also hold hand-edited copies made for simulation
# exercises and tests; never backfill those as real forecasts.
BACKFILL_EXCLUDE = (
    "MODIFIED_",
    "inject_forecast",
    "_edited",
    " copy",
    "/test/",
)

_DISTURBANCE_RE = re.compile(r"^(\d{2})(\d{2})-(\d{2}[A-Z])$")
_PRONUNCIATION_RE = re.compile(r"[\[(][^\])]*[\])]")
_UNNAMED = {"", "NONAME", "NO NAME", "UNNAMED"}

TRACK_COLUMNS = [
    "disturbance_id",
    "provider",
    "basin",
    "track_type",
    "issued_time",
    "valid_time",
    "leadtime",
    *SCALAR_COLS.values(),
    *(f"radius_{speed}" for speed in RADIUS_WORDS),
    *(f"quadrant_radius_{speed}" for speed in RADIUS_WORDS),
    "raw_fields",
    "storm_id",
    "point_id",
    "geometry",
]


class FmsParseError(ValueError):
    """The file is not a usable FMS track export."""


class NotAForecastTrack(FmsParseError):
    """A valid FMS export, but not a forecast (e.g. an Analysis Track)."""


@dataclass(frozen=True)
class FmsForecast:
    storm: dict
    forecast: dict
    tracks: pd.DataFrame


def season_from_disturbance_id(disturbance_id: str) -> int:
    """Season end year from a DisturbanceId: 2526-11F -> 2026."""
    match = _DISTURBANCE_RE.match(disturbance_id)
    if not match:
        raise FmsParseError(f"Unexpected DisturbanceId {disturbance_id!r}")
    end = int(match.group(2))
    return (1900 if end >= 70 else 2000) + end


def southern_season(ts: datetime) -> int:
    """Season of a time: July onwards belongs to the next year's season.

    Same rule as pa-aa-fji-storms ``datetime_to_season``.
    """
    return ts.year + 1 if ts.month >= 7 else ts.year


def clean_name(name_as_issued: str | None) -> str | None:
    """Storm name without FMS's pronunciation guide: 'RAE [RAY]' -> 'RAE'."""
    if name_as_issued is None:
        return None
    name = _PRONUNCIATION_RE.sub("", name_as_issued).strip().upper()
    return None if name in _UNNAMED else name


def make_storm_id(name: str | None, season: int) -> str | None:
    return f"{name}_{BASIN}_{season}".lower() if name else None


def _decode(data: bytes) -> str:
    try:
        return data.decode("utf-8-sig")
    except UnicodeDecodeError:
        return data.decode("cp1252")


def _header_and_rows(content: str) -> tuple[dict, list[str]]:
    """Split a file into its ``# key=value`` header fields and CSV lines.

    Line 1 can hold several fields (``trackType=...,dataSource=Fiji,
    baseTime=...``) and hand-edited files carry trailing commas, so every
    comma-separated ``key=value`` part of every comment line is read.
    """
    lines = content.splitlines()
    header = {}
    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith("Time["):
            rows = [
                row
                for row in lines[i + 1 :]
                if row.strip() and not row.strip().startswith("#")
            ]
            return header, [line, *rows]
        if stripped.startswith("#"):
            for part in stripped.lstrip("#").split(","):
                key, sep, value = part.partition("=")
                if sep:
                    header[key.strip()] = value.strip()
    raise FmsParseError("No column heading line starting with 'Time['")


def _num(value) -> float | None:
    if value is None or isinstance(value, str):
        return None
    try:
        value = float(value)
    except (TypeError, ValueError):
        return None
    return value if math.isfinite(value) else None


def _numeric_columns() -> set[str]:
    cols = {"Latitude", "Longitude", *SCALAR_COLS}
    for word in RADIUS_WORDS.values():
        cols.add(f"{word}Radius")
        cols.update(f"{quad}{word}Radius" for quad in QUADS)
    return cols


def _read_numbers(df: pd.DataFrame, label: str) -> None:
    """Make the numeric columns numeric, cell by cell, in place.

    One unreadable cell makes ``read_csv`` type its whole column as text,
    which would otherwise lose every value in it. Blank cells are missing
    values. Any other unreadable value fails the file if the Fiji trigger
    reads that column (TRIGGER_COLS); elsewhere it is logged and stored as
    NULL.
    """
    for col in sorted(_numeric_columns() & set(df.columns)):
        if pd.api.types.is_numeric_dtype(df[col]):
            continue
        values = df[col].map(lambda v: v.strip() if isinstance(v, str) else v)
        blank = values.isna() | values.eq("")
        numbers = pd.to_numeric(values.where(~blank), errors="coerce")
        bad = sorted(set(values[~blank & numbers.isna()]))
        if bad:
            message = f"{label}: unreadable {col} values {bad}"
            if col in TRIGGER_COLS:
                raise FmsParseError(message)
            logger.warning(f"{message}, stored as NULL")
        df[col] = numbers


def _raw_value(value) -> float | str | None:
    """A cell for ``raw_fields``: a number where it reads as one."""
    if not isinstance(value, str):
        return _num(value)
    value = value.strip()
    if not value:
        return None
    try:
        number = float(value)
    except ValueError:
        return value
    return number if math.isfinite(number) else value


def _wrap_longitude(lon: float) -> float:
    """Into [-180, 180). FMS writes east of the dateline as 180-360; values
    already in range are returned untouched (no float round-trip noise)."""
    while lon >= 180:
        lon -= 360
    while lon < -180:
        lon += 360
    return lon


def _quadrants(row: pd.Series, word: str) -> str | None:
    values = [_num(row.get(f"{quad}{word}Radius")) for quad in QUADS]
    if all(v is None for v in values):
        return None
    return json.dumps(values)


def _consumed_columns() -> set[str]:
    return {TIME_COL, "valid_time", *_numeric_columns()}


def _raw_fields(row: pd.Series, consumed: set[str]) -> str | None:
    fields = {}
    for col, value in row.items():
        if col in consumed:
            continue
        value = _raw_value(value)
        if value is not None:
            fields[col] = value
    return json.dumps(fields) if fields else None


def parse_fms_csv(data: bytes, filename: str | None = None) -> FmsForecast:
    """Parse one FMS track export into database rows.

    Raises NotAForecastTrack for analysis/best tracks and FmsParseError for
    anything that is not a usable export.
    """
    header, csv_lines = _header_and_rows(_decode(data))

    track_type = header.get("trackType")
    if not track_type:
        raise FmsParseError("Missing trackType header")
    if "Forecast Track" not in track_type:
        raise NotAForecastTrack(f"trackType {track_type!r} is not a forecast")
    for key in ("baseTime", "DisturbanceId"):
        if not header.get(key):
            raise FmsParseError(f"Missing {key} header")

    disturbance_id = header["DisturbanceId"]
    season = season_from_disturbance_id(disturbance_id)
    issued = pd.Timestamp(header["baseTime"])
    if issued.tzinfo is not None:
        issued = issued.tz_convert(None)
    issued_time = issued.to_pydatetime()
    if southern_season(issued_time) != season:
        logger.warning(
            f"{disturbance_id}: baseTime {issued_time} falls in season "
            f"{southern_season(issued_time)}, DisturbanceId says {season}"
        )
    name_as_issued = header.get("CycloneName")
    name = clean_name(name_as_issued)
    storm_id = make_storm_id(name, season)

    label = f"{disturbance_id} {issued_time}"
    df = pd.read_csv(io.StringIO("\n".join(csv_lines)))
    for col in (TIME_COL, "Latitude", "Longitude"):
        if col not in df.columns:
            raise FmsParseError(f"Missing column {col!r}")
    # An empty file must not replace a stored issuance's points with none.
    if df.empty:
        raise FmsParseError(f"{label}: no track points")
    _read_numbers(df, label)
    df["valid_time"] = pd.to_datetime(
        df[TIME_COL], utc=True, errors="coerce"
    ).dt.tz_localize(None)
    if df["valid_time"].isna().any():
        raise FmsParseError(f"{label}: track point without a readable time")
    if df[["Latitude", "Longitude"]].isna().any().any():
        raise FmsParseError(f"{label}: track point without a position")
    duplicated = df["valid_time"].duplicated()
    if duplicated.any():
        logger.warning(
            f"{disturbance_id} {issued_time}: dropping {duplicated.sum()} "
            "points with a repeated valid time"
        )
        df = df[~duplicated]
    leadtime = (df["valid_time"] - issued_time) / pd.Timedelta(hours=1)
    if (leadtime < 0).any():
        raise FmsParseError("Track point before baseTime")

    consumed = _consumed_columns()
    rows = []
    for (_, row), lead in zip(df.iterrows(), leadtime):
        lon = _wrap_longitude(float(row["Longitude"]))
        lat = float(row["Latitude"])
        rows.append(
            {
                "disturbance_id": disturbance_id,
                "provider": PROVIDER,
                "basin": BASIN,
                "track_type": track_type,
                "issued_time": issued_time,
                "valid_time": row["valid_time"].to_pydatetime(),
                "leadtime": int(lead),
                **{
                    out: _num(row.get(col)) for col, out in SCALAR_COLS.items()
                },
                **{
                    f"radius_{speed}": _num(row.get(f"{word}Radius"))
                    for speed, word in RADIUS_WORDS.items()
                },
                **{
                    f"quadrant_radius_{speed}": _quadrants(row, word)
                    for speed, word in RADIUS_WORDS.items()
                },
                "raw_fields": _raw_fields(row, consumed),
                "storm_id": storm_id,
                "point_id": str(uuid.uuid4()),
                "geometry": f"POINT ({lon!r} {lat!r})",
            }
        )
    tracks = pd.DataFrame(rows, columns=TRACK_COLUMNS)

    storm = {
        "disturbance_id": disturbance_id,
        "name": name,
        "number": disturbance_id.split("-", 1)[1],
        "season": season,
        "genesis_basin": BASIN,
        "provider": PROVIDER,
        "storm_id": storm_id,
    }
    forecast = {
        "disturbance_id": disturbance_id,
        "track_type": track_type,
        "issued_time": issued_time,
        "name_as_issued": name_as_issued,
        "data_source": header.get("dataSource"),
        "source_filename": filename,
        "content_sha256": hashlib.sha256(data).hexdigest(),
        "n_points": len(tracks),
    }
    return FmsForecast(storm=storm, forecast=forecast, tracks=tracks)


def raw_blob_name(forecast: dict) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", forecast["track_type"].lower()).strip(
        "-"
    )
    return (
        f"fms/raw/{forecast['disturbance_id']}/"
        f"{forecast['issued_time']:%Y%m%dT%H%MZ}_{slug}_"
        f"{forecast['content_sha256'][:12]}.csv"
    )


_INSERT_STORM = text(
    """
    INSERT INTO storms.fms_storms
        (disturbance_id, name, number, season, genesis_basin, provider,
         storm_id)
    VALUES
        (:disturbance_id, :name, :number, :season, :genesis_basin,
         :provider, :storm_id)
    ON CONFLICT (disturbance_id) DO NOTHING
    """
)

_DELETE_FORECAST = text(
    """
    DELETE FROM storms.fms_forecasts
    WHERE disturbance_id = :disturbance_id
      AND track_type = :track_type
      AND issued_time = :issued_time
    """
)

_INSERT_FORECAST = text(
    """
    INSERT INTO storms.fms_forecasts
        (disturbance_id, track_type, issued_time, name_as_issued,
         data_source, source_filename, content_sha256, raw_blob, n_points)
    VALUES
        (:disturbance_id, :track_type, :issued_time, :name_as_issued,
         :data_source, :source_filename, :content_sha256, :raw_blob,
         :n_points)
    """
)


def _track_value_sql(col: str) -> str:
    if col == "raw_fields":
        return "CAST(:raw_fields AS jsonb)"
    if col == "geometry":
        return "ST_SetSRID(ST_GeomFromText(:geometry), 4326)"
    return f":{col}"


_INSERT_TRACK = text(
    "INSERT INTO storms.fms_tracks_geo ("
    + ", ".join(TRACK_COLUMNS)
    + ") VALUES ("
    + ", ".join(map(_track_value_sql, TRACK_COLUMNS))
    + ")"
)

_NAMES_AS_ISSUED = text(
    """
    SELECT name_as_issued FROM storms.fms_forecasts
    WHERE disturbance_id = :disturbance_id
    ORDER BY issued_time DESC
    """
)

_UPDATE_STORM_NAME = text(
    """
    UPDATE storms.fms_storms SET name = :name, storm_id = :storm_id
    WHERE disturbance_id = :disturbance_id
    """
)


def write_forecast(conn, fc: FmsForecast, raw_blob: str | None) -> None:
    """Replace one issuance (forecast row + its track points) on ``conn``.

    The caller owns the transaction. The storm's name is then taken from
    its latest named forecast, so loading an older file never renames a
    storm back to an earlier name.
    """
    key = {
        k: fc.forecast[k]
        for k in ("disturbance_id", "track_type", "issued_time")
    }
    conn.execute(_INSERT_STORM, fc.storm)
    conn.execute(_DELETE_FORECAST, key)
    conn.execute(_INSERT_FORECAST, {**fc.forecast, "raw_blob": raw_blob})
    if len(fc.tracks):
        records = fc.tracks.astype(object).where(fc.tracks.notna(), None)
        conn.execute(_INSERT_TRACK, records.to_dict(orient="records"))

    names = conn.execute(
        _NAMES_AS_ISSUED, {"disturbance_id": fc.storm["disturbance_id"]}
    ).scalars()
    name = next((n for n in map(clean_name, names) if n), None)
    conn.execute(
        _UPDATE_STORM_NAME,
        {
            "disturbance_id": fc.storm["disturbance_id"],
            "name": name,
            "storm_id": make_storm_id(name, fc.storm["season"]),
        },
    )


def summary(fc: FmsForecast, mode: str, raw_blob: str | None) -> dict:
    return {
        "status": "ingested",
        "mode": mode,
        "disturbance_id": fc.forecast["disturbance_id"],
        "track_type": fc.forecast["track_type"],
        "issued_time": fc.forecast["issued_time"].isoformat(),
        "name_as_issued": fc.forecast["name_as_issued"],
        "content_sha256": fc.forecast["content_sha256"],
        "n_points": fc.forecast["n_points"],
        "raw_blob": raw_blob,
    }


def ingest_fms_csv(
    data: bytes,
    filename: str | None,
    mode: str,
    archive: bool = True,
    engine=None,
) -> dict:
    """Archive one FMS CSV to blob, then write it to the ``mode`` database.

    Returns a summary dict; ``status`` is ``skipped`` (with a ``reason``)
    for a valid export that is not a forecast track.
    """
    try:
        fc = parse_fms_csv(data, filename)
    except NotAForecastTrack as exc:
        logger.info(f"Skipping {filename}: {exc}")
        return {"status": "skipped", "reason": str(exc), "mode": mode}

    raw_blob = raw_blob_name(fc.forecast) if archive else None
    if archive:
        stratus.upload_blob_data(
            data,
            raw_blob,
            stage=mode,
            container_name=RAW_CONTAINER,
            content_type="text/csv",
        )
        logger.info(f"Archived raw CSV to {RAW_CONTAINER}/{raw_blob}")

    if engine is None:
        engine = stratus.get_engine(stage=mode, write=True)
    with engine.begin() as conn:
        write_forecast(conn, fc, raw_blob)
    logger.info(
        f"Wrote {fc.forecast['disturbance_id']} "
        f"{fc.forecast['track_type']} {fc.forecast['issued_time']}: "
        f"{fc.forecast['n_points']} points ({mode})"
    )
    return summary(fc, mode, raw_blob)


def backfill_fms(
    prefixes: list[str],
    mode: str,
    blob_stage: str = "dev",
    container: str = "projects",
    dry_run: bool = False,
    exclude: tuple[str, ...] = BACKFILL_EXCLUDE,
) -> dict:
    """Ingest every forecast-track CSV under ``prefixes``, oldest first.

    Blob names containing any of ``exclude`` are left out and reported.

    Prefixes are in priority order: when several files hold the same
    issuance, the copy under the earliest prefix is written and copies with
    different content are reported. (Some archived copies were re-exported
    with rounded positions; the emailed originals should win.)
    """
    names, excluded = [], []
    for prefix in prefixes:
        for name in sorted(
            stratus.list_container_blobs(
                name_starts_with=prefix,
                stage=blob_stage,
                container_name=container,
            )
        ):
            if not name.lower().endswith(".csv") or name in names:
                continue
            if any(pattern in name for pattern in exclude):
                excluded.append(name)
                continue
            names.append(name)
    logger.info(f"{len(names)} CSV files under {prefixes}")

    by_key, skipped, failed = {}, [], []
    for name in names:
        data = stratus.load_blob_data(
            name, stage=blob_stage, container_name=container
        )
        filename = name.rsplit("/", 1)[-1]
        try:
            fc = parse_fms_csv(data, filename)
        except NotAForecastTrack as exc:
            skipped.append({"blob": name, "reason": str(exc)})
            continue
        except Exception as exc:  # noqa: BLE001 — report and carry on
            failed.append({"blob": name, "error": repr(exc)})
            continue
        key = tuple(
            fc.forecast[k]
            for k in ("disturbance_id", "track_type", "issued_time")
        )
        by_key.setdefault(key, []).append((name, data, fc))

    duplicates = []
    chosen = []
    for key in sorted(by_key):
        candidates = by_key[key]
        chosen.append(candidates[0])
        first_sha = candidates[0][2].forecast["content_sha256"]
        differing = [
            c[0]
            for c in candidates[1:]
            if c[2].forecast["content_sha256"] != first_sha
        ]
        if differing:
            duplicates.append(
                {
                    "key": [str(k) for k in key],
                    "written": candidates[0][0],
                    "not_written": differing,
                }
            )

    results = []
    if not dry_run:
        engine = stratus.get_engine(stage=mode, write=True)
        for name, data, _ in chosen:
            results.append(
                ingest_fms_csv(
                    data, name.rsplit("/", 1)[-1], mode, engine=engine
                )
            )
    report = {
        "mode": mode,
        "dry_run": dry_run,
        "files": len(names),
        "issuances": len(chosen),
        "written": len(results),
        "excluded": excluded,
        "skipped": skipped,
        "failed": failed,
        "differing_duplicates": duplicates,
    }
    logger.info(
        f"Backfill {mode}: {report['issuances']} issuances from "
        f"{report['files']} files, {len(results)} written, "
        f"{len(skipped)} skipped, {len(excluded)} excluded, "
        f"{len(failed)} failed, "
        f"{len(duplicates)} issuances with differing copies"
    )
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="FMS forecast-track ETL")
    sub = parser.add_subparsers(dest="command", required=True)

    ingest = sub.add_parser("ingest", help="Ingest one CSV")
    source = ingest.add_mutually_exclusive_group(required=True)
    source.add_argument("--csv-blob", help="Blob name of the CSV")
    source.add_argument("--csv-path", help="Local path of the CSV")
    ingest.add_argument(
        "--csv-blob-stage", choices=["dev", "prod"], default="dev"
    )
    ingest.add_argument("--csv-container", default="projects")
    ingest.add_argument(
        "--filename", help="Original attachment name (default: basename)"
    )
    ingest.add_argument("--no-archive", action="store_true")

    backfill = sub.add_parser(
        "backfill", help="Ingest every CSV under prefixes"
    )
    backfill.add_argument("--blob-prefix", action="append", required=True)
    backfill.add_argument(
        "--blob-stage", choices=["dev", "prod"], default="dev"
    )
    backfill.add_argument("--container", default="projects")
    backfill.add_argument("--dry-run", action="store_true")

    for p in (ingest, backfill):
        p.add_argument("--mode", choices=["dev", "prod"], default="dev")
        p.add_argument("--out-json", help="Write the result as JSON here")
    args = parser.parse_args(argv)

    if args.command == "ingest":
        if args.csv_blob:
            data = stratus.load_blob_data(
                args.csv_blob,
                stage=args.csv_blob_stage,
                container_name=args.csv_container,
            )
            default_name = args.csv_blob.rsplit("/", 1)[-1]
        else:
            with open(args.csv_path, "rb") as f:
                data = f.read()
            default_name = args.csv_path.rsplit("/", 1)[-1]
        result = ingest_fms_csv(
            data,
            args.filename or default_name,
            args.mode,
            archive=not args.no_archive,
        )
    else:
        result = backfill_fms(
            args.blob_prefix,
            args.mode,
            blob_stage=args.blob_stage,
            container=args.container,
            dry_run=args.dry_run,
        )

    print(json.dumps(result, indent=2, default=str))
    if args.out_json:
        with open(args.out_json, "w") as f:
            json.dump(result, f, default=str)
    return 1 if result.get("failed") else 0


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    # the Azure SDK logs every HTTP request and response at INFO
    logging.getLogger("azure").setLevel(logging.WARNING)
    raise SystemExit(main())
