#!/usr/bin/env python3
"""
MFR (Météo-France La Réunion / RSMC La Réunion) forecast-track ETL.

Météo-France La Réunion publishes one JSON file per system per forecast
(``CMRSTRACK_SWI$NN_SSSSSSSS_<NAME>_<YYYY_MM_DD_HH>Z.json``), for every
system in its area of responsibility, plus a track-only file when a system
ends (and revised track-only files later). ``parse_mfr_json`` turns one
file into rows for storms.mfr_storms, storms.mfr_forecasts and
storms.mfr_tracks_geo (schema: src/schemas/sql/mfr_tables.sql);
``ingest_mfr_bytes`` archives the raw file to blob, writes it, and records
it in storms.mfr_files.

Where the files come from:
  * ``sync`` (the scheduled "MFR Ingest" Databricks job): the dev blob
    container ``meteofr``, which the Azure Data Factory pipeline
    CopyMeteoFRToBlob fills from Météo-France's SFTP every 6 hours. Reading
    the copy means this job never logs in to Météo-France itself.
  * ``poll``: Météo-France's FTP site directly (list + download only). Not
    scheduled: the login is the Mozambique cyclone monitor's
    (ds-aa-moz-cyclones-monitoring), which must not be disturbed.
  * ``ingest`` (local files) and ``backfill`` (any blob prefix), which
    also read the older CXML product (cxml.1.1, e.g. the 2010/11-2023/24
    forecast archive): ``parse_mfr_cxml`` maps it onto the same rows.

Things the file format needs care with (seen in the 2022/23 to 2026/27
files):
  * The hour in the filename is the upload hour, not the forecast's
    reference time (Chido's ``..._02Z.json`` is the 00Z forecast). Files
    are keyed on ``reference_time`` inside the file.
  * A file without forecast points is a track-only file (``kind``
    'track'): the system's final track, sent when it ends and revised up
    to months later, sometimes with points after its reference time. It is
    stored apart from the forecasts; a system keeps one, its latest
    revision (revisions can move the reference time).
  * Météo-France re-publishes some forecast files long after they were
    issued. A forecast file produced more than LATE_REISSUE after its
    reference time is stored (``late_reissue``) only when no operational
    version exists, and never replaces one.
  * Wind contours are 28, 34, 48 and 64 kt in the JSON files (27/34/50/64
    in the older CXML product), 10-minute winds; a contour can list only
    some sectors.

Keep the imports to pandas, sqlalchemy, python-dotenv, ocha-stratus and the
standard library (tests/test_mfr.py checks), so the job needs no more than
those on the cluster.
"""

import argparse
import ftplib
import hashlib
import io
import json
import logging
import math
import os
import re
import uuid
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timedelta

import ocha_stratus as stratus
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import text

load_dotenv()

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

PROVIDER = "mfr"
BASIN = "SI"
RAW_CONTAINER = "storm"
FILE_PREFIX = "CMRSTRACK_SWI"
FTP_TIMEOUT_SECONDS = 60

# The Data Factory copy of Météo-France's SFTP (dev storage account only).
SYNC_STAGE = "dev"
SYNC_CONTAINER = "meteofr"
SYNC_PREFIX = "RSMC_LaReunion/"

# Wind contour speeds (knots) with a column. The JSON files give 28/34/48/64;
# the older CXML product gives 27/34/50/64.
SPEEDS = (27, 28, 34, 48, 50, 64)
# File sectors in the [NE, SE, SW, NW] order of nhc/fms_tracks_geo.
SECTORS = ("NEQ", "SEQ", "SWQ", "NWQ")

LATE_REISSUE = timedelta(hours=24)

_CYCLONE_ID_RE = re.compile(r"^SWI\$(\d{1,2})/(\d{4})(\d{4})$")
# Unnamed systems: '10-20252026' in the JSON product; '0120102011', or the
# season alone, in CXML.
_UNNAMED_RE = re.compile(r"^(\d{1,2}-?)?\d{8}$")
_UNNAMED = {"", "NONAME", "NO NAME", "UNNAMED", "INVEST"}
# 'EX-GUAMBE': the same storm after it weakened.
_EX_PREFIX_RE = re.compile(r"^EX[-\s]+")

# Point properties read into columns, as paths in the file. Everything else
# with a value goes to raw_fields.
SCALAR_PATHS = {
    "cyclone_data.development": "development",
    "cyclone_data.maximum_wind.wind_speed_kt": "wind_speed",
    "cyclone_data.maximum_wind.wind_speed_gust_kt": "gust_speed",
    "cyclone_data.minimum_pressure": "pressure",
    "cyclone_data.maximum_wind.radius": "max_wind_radius",
    "cyclone_data.last_closed_isobar.pressure": "last_closed_isobar_pressure",
    "cyclone_data.last_closed_isobar.radius": "last_closed_isobar_radius",
    "position_accuracy": "position_accuracy",
    "cyclone_data.storm_motion.speed_kt": "motion_speed",
    "cyclone_data.storm_motion.direction_toward": "motion_direction",
    "cyclone_data.Dvorak.final_T_number": "dvorak_final_t",
    "cyclone_data.Dvorak.current_intensity": "dvorak_ci",
}
_TEXT_COLS = {"development"}
_CONSUMED = {
    "data_type",
    "time",
    "hour",
    "cyclone_data.wind_contours",
    *SCALAR_PATHS,
}

TRACK_COLUMNS = [
    "disturbance_id",
    "provider",
    "basin",
    "issued_time",
    "kind",
    "data_type",
    "valid_time",
    "leadtime",
    *SCALAR_PATHS.values(),
    *(f"quadrant_radius_{speed}" for speed in SPEEDS),
    "raw_fields",
    "storm_id",
    "point_id",
    "geometry",
]


class MfrParseError(ValueError):
    """The file is not a usable Météo-France track file."""


@dataclass(frozen=True)
class MfrForecast:
    storm: dict
    forecast: dict
    tracks: pd.DataFrame
    uncertainty_cone: str | None  # GeoJSON geometry


def parse_cyclone_id(cyclone_id: str) -> tuple[int, str, int]:
    """'SWI$10/20252026' -> (10, '20252026', 2026)."""
    match = _CYCLONE_ID_RE.match(cyclone_id or "")
    if not match:
        raise MfrParseError(f"Unexpected cyclone_id {cyclone_id!r}")
    number = int(match.group(1))
    start, end = int(match.group(2)), int(match.group(3))
    if end != start + 1:
        raise MfrParseError(f"Unexpected season in cyclone_id {cyclone_id!r}")
    return number, f"{start}{end}", end


def southern_season(ts: datetime) -> int:
    """Season of a time: July onwards belongs to the next year's season."""
    return ts.year + 1 if ts.month >= 7 else ts.year


def clean_name(name_as_issued: str | None) -> str | None:
    """Storm name, or None for an unnamed system ('10-20252026')."""
    if name_as_issued is None:
        return None
    name = _EX_PREFIX_RE.sub("", str(name_as_issued).strip().upper())
    if name in _UNNAMED or _UNNAMED_RE.match(name):
        return None
    return name


def make_storm_id(name: str | None, season: int) -> str | None:
    """Same form as storms.ibtracs_storms.storm_id, e.g. gezani_si_2026."""
    return f"{name}_{BASIN}_{season}".lower() if name else None


def _num(value) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        value = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return value if math.isfinite(value) else None


def _utc(value, what: str) -> datetime:
    try:
        ts = pd.Timestamp(value)
    except (TypeError, ValueError) as exc:
        raise MfrParseError(f"Unreadable {what} {value!r}") from exc
    if pd.isna(ts):
        raise MfrParseError(f"Missing {what}")
    if ts.tzinfo is not None:
        ts = ts.tz_convert(None)
    return ts.to_pydatetime()


def _flatten(obj: dict, prefix: str = "") -> dict:
    """Nested dicts to {'a.b': value}; lists are kept whole."""
    out = {}
    for key, value in obj.items():
        path = f"{prefix}{key}"
        if isinstance(value, dict):
            out.update(_flatten(value, f"{path}."))
        else:
            out[path] = value
    return out


def _json_safe(value):
    """For raw_fields (jsonb): non-finite numbers become strings and NUL
    characters are dropped, so one odd value can't fail the insert."""
    if isinstance(value, float) and not math.isfinite(value):
        return str(value)
    if isinstance(value, str):
        return value.replace("\x00", "")
    if isinstance(value, dict):
        return {
            str(k).replace("\x00", ""): _json_safe(v) for k, v in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    return value


def _text(value) -> str | None:
    """A file-level string for a VARCHAR column: None stays None, anything
    else becomes text without NUL characters."""
    if value is None:
        return None
    return str(value).replace("\x00", "")


def _wrap_longitude(lon: float) -> float:
    while lon >= 180:
        lon -= 360
    while lon < -180:
        lon += 360
    return lon


def _contours(contours, label: str) -> tuple[dict, list]:
    """{speed: JSON [NE, SE, SW, NW]} for the known speeds, plus the
    contours at any other speed (kept in raw_fields)."""
    radii, other = {}, []
    for contour in contours or []:
        speed = _num(contour.get("wind_speed_kt"))
        if speed is None or speed != int(speed) or int(speed) not in SPEEDS:
            logger.warning(f"{label}: wind contour at {speed} kt kept raw")
            other.append(contour)
            continue
        by_sector = {}
        for entry in contour.get("radius") or []:
            sector = entry.get("sector")
            if sector not in SECTORS:
                logger.warning(f"{label}: unknown sector {sector!r} ignored")
                continue
            by_sector[sector] = _num(entry.get("value"))
        values = [by_sector.get(sector) for sector in SECTORS]
        speed = int(speed)
        if speed in radii:
            logger.warning(f"{label}: repeated {speed} kt contour, first kept")
            continue
        radii[speed] = (
            None if all(v is None for v in values) else json.dumps(values)
        )
    return radii, other


def parse_mfr_json(
    data: bytes, filename: str | None = None, data_source: str = "local"
) -> MfrForecast:
    """Parse one Météo-France JSON track file into database rows."""
    try:
        doc = json.loads(data)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MfrParseError(f"Not JSON: {exc}") from exc
    return _parse_doc(doc, data, filename, data_source)


def parse_mfr_file(
    data: bytes, filename: str | None = None, data_source: str = "local"
) -> MfrForecast:
    """JSON or CXML, by content. Any structure the parsers don't expect
    (a list where an object should be, ...) is an MfrParseError, so the file
    is archived and recorded as failed like any other unreadable file."""
    try:
        if data.lstrip()[:1] == b"<":
            return parse_mfr_cxml(data, filename, data_source)
        return parse_mfr_json(data, filename, data_source)
    except MfrParseError:
        raise
    except (
        AttributeError,
        TypeError,
        KeyError,
        ValueError,
        OverflowError,
    ) as exc:
        raise MfrParseError(f"Malformed file: {exc!r}") from exc


# CXML (cxml.1.1, the older Météo-France product): element paths under
# cycloneData, mapped onto the JSON product's property names so both go
# through _parse_doc.
_CXML_SCALARS = {
    "development": "development",
    "minimumPressure/pressure": "minimum_pressure",
    "maximumWind/speed": "maximum_wind.wind_speed_kt",
    "maximumWind/gusts": "maximum_wind.wind_speed_gust_kt",
    "maximumWind/radius": "maximum_wind.radius",
    "lastClosedIsobar/pressure": "last_closed_isobar.pressure",
    "lastClosedIsobar/radius": "last_closed_isobar.radius",
    "stormMotion/speed": "storm_motion.speed_kt",
    "stormMotion/directionToward": "storm_motion.direction_toward",
    "Dvorak/finalTnumber": "Dvorak.final_T_number",
    "Dvorak/currentIntensity": "Dvorak.current_intensity",
}
_CXML_FILENAME_RE = re.compile(r"F(\d{8})_(\d{1,2})_")
# Children of <fix> read above; any others go to raw_fields.
_CXML_FIX_READ = {
    "validTime",
    "latitude",
    "longitude",
    "accuracy",
    "navAccuracy",
    "cycloneData",
}
# The units each element must carry (when it carries any): a file in other
# units fails rather than storing numbers in the wrong unit.
_CXML_UNITS = {
    "speed": "kt",
    "gusts": "kt",
    "radius": "nm",
    "accuracy": "nm",
    "navAccuracy": "nm",
    "pressure": "hPa",
    "directionToward": "deg",
    "windSpeed": "kt",
}


def _nest(target: dict, dotted: str, value) -> None:
    *parents, leaf = dotted.split(".")
    for key in parents:
        target = target.setdefault(key, {})
    target[leaf] = value


def _cxml_number(el, label: str, what: str) -> float:
    value = _num(el.text if el is not None else None)
    if value is None:
        raise MfrParseError(f"{label}: no readable {what}")
    return value


def _check_cxml_units(fix, label: str) -> None:
    for el in fix.iter():
        expected = _CXML_UNITS.get(el.tag)
        units = el.get("units")
        if expected and units is not None and units.strip() != expected:
            raise MfrParseError(
                f"{label}: <{el.tag}> in {units!r}, expected {expected!r}"
            )


def _check_cxml_tag(tag: str, label: str) -> None:
    """Element names become dotted paths in raw_fields: refuse a dot."""
    if not isinstance(tag, str) or "." in tag:
        raise MfrParseError(f"{label}: unexpected element {tag!r}")


def _cxml_coordinate(el, label: str, what: str, signs: dict) -> float:
    """Degrees with the sign from the units ('deg S' -> negative); the value
    itself must be positive and the units one of ``signs``."""
    value = _cxml_number(el, label, what)
    units = (el.get("units") or "").strip()
    if units not in signs or value < 0:
        raise MfrParseError(f"{label}: {what} {value} in {units!r}")
    return signs[units] * value


def parse_mfr_cxml(
    data: bytes, filename: str | None = None, data_source: str = "local"
) -> MfrForecast:
    """Parse one Météo-France CXML forecast (cxml.1.1) into database rows.

    Each file is one forecast: fix hour 0 is the analysis at the reference
    time (no earlier track), then forecast fixes. Elements are mapped onto
    the JSON product's names; anything else in cycloneData goes to
    raw_fields. Only ``<data type="forecast">`` is read.
    """
    try:
        root = ET.fromstring(data)
    except ET.ParseError as exc:
        raise MfrParseError(f"Not XML: {exc}") from exc
    datas = [d for d in root.findall("data") if d.get("type") == "forecast"]
    if len(datas) != 1:
        raise MfrParseError(f"{len(datas)} <data type='forecast'> elements")
    disturbances = datas[0].findall("disturbance")
    if len(disturbances) != 1:
        raise MfrParseError(f"{len(disturbances)} <disturbance> elements")
    dist = disturbances[0]
    label = filename or "cxml"

    cyclone_id = (dist.findtext("localID") or "").strip()
    if not cyclone_id:
        match = _CXML_FILENAME_RE.search(filename or "")
        number = dist.findtext("cycloneNumber")
        if not match or not (number or "").strip().isdigit():
            raise MfrParseError(f"{label}: no localID")
        cyclone_id = f"SWI${int(number):02d}/{match.group(1)}"
    fixes = dist.findall("fix")
    if not fixes:
        raise MfrParseError(f"{label}: no fixes")

    features, reference_time = [], None
    for fix in fixes:
        hour = _num(fix.get("hour"))
        valid = fix.findtext("validTime")
        if hour is None or not valid:
            raise MfrParseError(f"{label}: fix without hour or validTime")
        _check_cxml_units(fix, label)
        lat = _cxml_coordinate(
            fix.find("latitude"), label, "latitude", {"deg S": -1, "deg N": 1}
        )
        lon = _cxml_coordinate(
            fix.find("longitude"),
            label,
            "longitude",
            {"deg E": 1, "deg W": -1},
        )
        if hour == 0:
            reference_time = valid
        props = {"data_type": "forecast", "time": valid, "hour": hour}
        # 2010/11 files call it navAccuracy; later ones, accuracy.
        accuracy = fix.find("accuracy")
        if accuracy is None:
            accuracy = fix.find("navAccuracy")
        if accuracy is not None:
            props["position_accuracy"] = accuracy.text
        # Other elements of the fix (e.g. onGoingChange) are kept raw,
        # under their own prefix so they can't land in a column.
        for child in fix:
            if child.tag not in _CXML_FIX_READ:
                _check_cxml_tag(child.tag, label)
                props.setdefault("fix_extra", {})[child.tag] = (
                    child.text or ""
                ).strip() or None
        cyclone = {}
        data_el = fix.find("cycloneData")
        # Mapped elements count as read even when empty or partial (e.g. a
        # <Dvorak> with only pastChange); their other children go to _extra.
        consumed = {path.split("/")[0] for path in _CXML_SCALARS}
        consumed.add("windContours")
        if data_el is not None:
            for path, name in _CXML_SCALARS.items():
                el = data_el.find(path)
                if el is not None:
                    _nest(cyclone, name, (el.text or "").strip() or None)
            contours = []
            for wind in data_el.findall("windContours/windSpeed"):
                contours.append(
                    {
                        "wind_speed_kt": (wind.text or "").strip(),
                        "radius": [
                            {"sector": r.get("sector"), "value": r.text}
                            for r in wind.findall("radius")
                        ],
                    }
                )
            if contours:
                cyclone["wind_contours"] = contours
            # Anything else (e.g. systemDepth, Dvorak/pastChange) is kept.
            for child in data_el:
                if child.tag not in consumed:
                    _check_cxml_tag(child.tag, label)
                    key = f"cyclone_data.{child.tag}"
                    if any(
                        c == key or c.startswith(f"{key}.") for c in _CONSUMED
                    ):
                        raise MfrParseError(
                            f"{label}: <{child.tag}> would overwrite a "
                            "column"
                        )
                    cyclone[child.tag] = (child.text or "").strip() or None
                for grandchild in child:
                    path = f"{child.tag}/{grandchild.tag}"
                    if child.tag != "windContours" and path not in (
                        _CXML_SCALARS
                    ):
                        _nest(
                            cyclone,
                            f"{child.tag}_extra.{grandchild.tag}",
                            (grandchild.text or "").strip() or None,
                        )
        props["cyclone_data"] = cyclone
        features.append(
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [lon, lat]},
                "properties": props,
            }
        )
    if reference_time is None:
        raise MfrParseError(f"{label}: no hour-0 fix")
    doc = {
        "product": root.findtext("header/product"),
        "update_time": root.findtext("header/creationTime"),
        "production_center": root.findtext("header/productionCenter"),
        "cyclone_name": dist.findtext("cycloneName"),
        "cyclone_id": cyclone_id,
        "reference_time": reference_time,
        "features": features,
    }
    return _parse_doc(doc, data, filename, data_source)


def _parse_doc(
    doc, data: bytes, filename: str | None, data_source: str
) -> MfrForecast:
    """Rows from a parsed document (JSON product layout)."""
    if not isinstance(doc, dict) or not isinstance(doc.get("features"), list):
        raise MfrParseError("No 'features' list")

    cyclone_id = doc.get("cyclone_id")
    number, season_span, season = parse_cyclone_id(cyclone_id)
    if str(doc.get("season", season_span)) != season_span:
        logger.warning(
            f"{cyclone_id}: season {doc.get('season')!r} differs from "
            f"the cyclone_id"
        )
    disturbance_id = f"{number:02d}-{season_span}"
    issued_time = _utc(doc.get("reference_time"), "reference_time")
    if southern_season(issued_time) != season:
        logger.warning(
            f"{disturbance_id}: reference_time {issued_time} falls in season "
            f"{southern_season(issued_time)}"
        )
    update_time = (
        _utc(doc["update_time"], "update_time")
        if doc.get("update_time")
        else None
    )
    name_as_issued = _text(doc.get("cyclone_name"))
    name = clean_name(name_as_issued)
    storm_id = make_storm_id(name, season)
    label = f"{disturbance_id} {issued_time:%Y-%m-%dT%H:%MZ}"

    rows, cone = [], None
    for feature in doc["features"]:
        props = (feature or {}).get("properties") or {}
        geometry = (feature or {}).get("geometry") or {}
        data_type = props.get("data_type")
        if data_type == "uncertainty_cone":
            if cone is not None:
                logger.warning(f"{label}: second uncertainty cone ignored")
            elif geometry.get("type") in ("Polygon", "MultiPolygon"):
                cone = json.dumps(geometry)
            continue
        if data_type not in ("analysis", "forecast"):
            logger.warning(f"{label}: feature type {data_type!r} ignored")
            continue
        coords = geometry.get("coordinates")
        if geometry.get("type") != "Point" or not coords or len(coords) < 2:
            raise MfrParseError(
                f"{label}: {data_type} point without a position"
            )
        lon, lat = _num(coords[0]), _num(coords[1])
        if lon is None or lat is None:
            raise MfrParseError(
                f"{label}: {data_type} point without a position"
            )
        valid_time = _utc(props.get("time"), f"{data_type} time")
        hours = (valid_time - issued_time) / timedelta(hours=1)
        if data_type == "forecast" and hours < 0:
            raise MfrParseError(
                f"{label}: forecast point before reference_time"
            )
        if hours != int(hours):
            logger.warning(f"{label}: leadtime {hours} h rounded")
        if "hour" in props and _num(props["hour"]) != round(hours):
            logger.warning(
                f"{label}: 'hour' {props['hour']!r} differs from the "
                f"leadtime {round(hours)} h from the times"
            )

        flat = _flatten(props)
        radii, other_contours = _contours(
            flat.get("cyclone_data.wind_contours"), label
        )
        raw = {
            key: value
            for key, value in flat.items()
            if key not in _CONSUMED and value is not None
        }
        if other_contours:
            raw["cyclone_data.wind_contours"] = other_contours
        record = {
            "disturbance_id": disturbance_id,
            "provider": PROVIDER,
            "basin": BASIN,
            "issued_time": issued_time,
            "data_type": data_type,
            "valid_time": valid_time,
            "leadtime": int(round(hours)),
        }
        for path, col in SCALAR_PATHS.items():
            value = flat.get(path)
            if col in _TEXT_COLS:
                record[col] = (
                    None if value is None else str(value).replace("\x00", "")
                )
            else:
                record[col] = _num(value)
                if value is not None and record[col] is None:
                    raw[path] = value
                    logger.warning(f"{label}: unreadable {path} {value!r}")
        for speed in SPEEDS:
            record[f"quadrant_radius_{speed}"] = radii.get(speed)
        lon = _wrap_longitude(lon)
        record.update(
            {
                "raw_fields": (
                    json.dumps(
                        _json_safe(raw), sort_keys=True, allow_nan=False
                    )
                    if raw
                    else None
                ),
                "storm_id": storm_id,
                "point_id": str(uuid.uuid4()),
                "geometry": f"POINT ({lon!r} {lat!r})",
            }
        )
        rows.append(record)

    if not rows:
        raise MfrParseError(f"{label}: no track points")
    tracks = pd.DataFrame(rows, columns=TRACK_COLUMNS)
    duplicated = tracks.duplicated(["data_type", "valid_time"])
    if duplicated.any():
        logger.warning(
            f"{label}: dropping {duplicated.sum()} points with a repeated "
            "valid time"
        )
        tracks = tracks[~duplicated].reset_index(drop=True)
    is_fc = tracks["data_type"].eq("forecast")
    kind = "forecast" if is_fc.any() else "track"
    tracks["kind"] = kind
    if kind == "forecast":
        after = tracks["data_type"].eq("analysis") & tracks["leadtime"].gt(0)
        if after.any():
            logger.warning(
                f"{label}: {after.sum()} analysis points after reference_time"
            )
    late_reissue = bool(
        kind == "forecast"
        and update_time is not None
        and update_time - issued_time > LATE_REISSUE
    )

    storm = {
        "disturbance_id": disturbance_id,
        "cyclone_id": cyclone_id,
        "name": name,
        "number": number,
        "season": season,
        "genesis_basin": BASIN,
        "provider": PROVIDER,
        "storm_id": storm_id,
    }
    forecast = {
        "disturbance_id": disturbance_id,
        "issued_time": issued_time,
        "kind": kind,
        "update_time": update_time,
        "name_as_issued": name_as_issued,
        "product": _text(doc.get("product")),
        "production_center": _text(doc.get("production_center")),
        "data_source": data_source,
        "source_filename": filename,
        "content_sha256": hashlib.sha256(data).hexdigest(),
        "n_analysis": int((~is_fc).sum()),
        "n_forecast": int(is_fc.sum()),
        "max_leadtime": (
            int(tracks.loc[is_fc, "leadtime"].max()) if is_fc.any() else None
        ),
        "late_reissue": late_reissue,
    }
    return MfrForecast(
        storm=storm, forecast=forecast, tracks=tracks, uncertainty_cone=cone
    )


def _status_counts(results: list[dict]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for r in results:
        key = r.get("status", "parsed")
        counts[key] = counts.get(key, 0) + 1
    return counts


def _safe(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", name).strip("_") or "file"


def raw_blob_name(
    filename: str | None, sha256: str, fc: MfrForecast | None
) -> str:
    """Where the raw file is archived in the storm container.

    Parsed files go under their season and system; files that could not be
    parsed under unparsed/, so nothing read is ever lost.
    """
    stem, dot, ext = _safe(filename or "file").rpartition(".")
    if not dot:
        stem, ext = ext, "bin"
    base = f"{stem}_{sha256[:12]}.{ext}"
    if fc is None:
        return f"mfr/raw/unparsed/{base}"
    return f"mfr/raw/{fc.storm['season']}/{fc.storm['disturbance_id']}/{base}"


# ---------------------------------------------------------------------------
# Writing
# ---------------------------------------------------------------------------

_KEY = ("disturbance_id", "issued_time", "kind")

_INSERT_STORM = text(
    """
    INSERT INTO storms.mfr_storms
        (disturbance_id, cyclone_id, name, number, season, genesis_basin,
         provider, storm_id)
    VALUES
        (:disturbance_id, :cyclone_id, :name, :number, :season,
         :genesis_basin, :provider, :storm_id)
    ON CONFLICT (disturbance_id) DO NOTHING
    """
)

_EXISTING_FORECAST = text(
    """
    SELECT content_sha256, update_time, late_reissue, name_as_issued
    FROM storms.mfr_forecasts
    WHERE disturbance_id = :disturbance_id AND issued_time = :issued_time
      AND kind = :kind
    FOR UPDATE
    """
)

_DELETE_FORECAST = text(
    """
    DELETE FROM storms.mfr_forecasts
    WHERE disturbance_id = :disturbance_id AND issued_time = :issued_time
      AND kind = :kind
    """
)

# A system keeps one final track: revisions can move its reference time
# (GEZANI 2026: 12Z, then 18Z), so tracks are compared per system.
_EXISTING_TRACK = text(
    """
    SELECT content_sha256, update_time, late_reissue, name_as_issued
    FROM storms.mfr_forecasts
    WHERE disturbance_id = :disturbance_id AND kind = 'track'
    ORDER BY update_time DESC NULLS LAST, content_sha256 DESC
    LIMIT 1
    FOR UPDATE
    """
)

_DELETE_TRACKS = text(
    """
    DELETE FROM storms.mfr_forecasts
    WHERE disturbance_id = :disturbance_id AND kind = 'track'
    """
)

_FORECAST_COLUMNS = [
    "disturbance_id",
    "issued_time",
    "kind",
    "update_time",
    "name_as_issued",
    "product",
    "production_center",
    "data_source",
    "source_filename",
    "content_sha256",
    "raw_blob",
    "n_analysis",
    "n_forecast",
    "max_leadtime",
    "late_reissue",
]

_INSERT_FORECAST = text(
    "INSERT INTO storms.mfr_forecasts ("
    + ", ".join(_FORECAST_COLUMNS)
    + ") VALUES ("
    + ", ".join(f":{c}" for c in _FORECAST_COLUMNS)
    + ")"
)

_SET_CONE = text(
    """
    UPDATE storms.mfr_forecasts
    SET uncertainty_cone = ST_MakeValid(
        ST_SetSRID(ST_GeomFromGeoJSON(CAST(:cone AS text)), 4326))
    WHERE disturbance_id = :disturbance_id AND issued_time = :issued_time
      AND kind = :kind
    """
)


def _track_value_sql(col: str) -> str:
    if col == "raw_fields":
        return "CAST(:raw_fields AS jsonb)"
    if col == "geometry":
        return "ST_SetSRID(ST_GeomFromText(:geometry), 4326)"
    return f":{col}"


_INSERT_TRACK = text(
    "INSERT INTO storms.mfr_tracks_geo ("
    + ", ".join(TRACK_COLUMNS)
    + ") VALUES ("
    + ", ".join(map(_track_value_sql, TRACK_COLUMNS))
    + ")"
)

_NAMES_AS_ISSUED = text(
    """
    SELECT name_as_issued FROM storms.mfr_forecasts
    WHERE disturbance_id = :disturbance_id
    ORDER BY issued_time DESC, update_time DESC NULLS LAST
    """
)

_UPDATE_STORM_NAME = text(
    """
    UPDATE storms.mfr_storms SET name = :name, storm_id = :storm_id
    WHERE disturbance_id = :disturbance_id
    """
)

# Every point of the system gets the system's storm_id, including those of
# forecasts issued before it was named (the name as issued stays in
# mfr_forecasts.name_as_issued).
_UPDATE_TRACK_STORM_IDS = text(
    """
    UPDATE storms.mfr_tracks_geo SET storm_id = :storm_id
    WHERE disturbance_id = :disturbance_id
      AND storm_id IS DISTINCT FROM :storm_id
    """
)

_RECORD_FILE = text(
    """
    INSERT INTO storms.mfr_files
        (source, source_path, content_sha256, source_filename, size_bytes,
         source_modified, raw_blob, status, message, disturbance_id,
         issued_time, kind)
    VALUES
        (:source, :source_path, :content_sha256, :source_filename,
         :size_bytes, :source_modified, :raw_blob, :status, :message,
         :disturbance_id, :issued_time, :kind)
    ON CONFLICT (source, source_path, content_sha256) DO UPDATE SET
        last_seen = (now() AT TIME ZONE 'utc'),
        size_bytes = COALESCE(EXCLUDED.size_bytes, storms.mfr_files.size_bytes),
        source_modified = COALESCE(
            EXCLUDED.source_modified, storms.mfr_files.source_modified),
        raw_blob = CASE WHEN storms.mfr_files.status = 'failed'
                        THEN EXCLUDED.raw_blob
                        ELSE COALESCE(storms.mfr_files.raw_blob,
                                      EXCLUDED.raw_blob) END,
        status = CASE WHEN storms.mfr_files.status = 'failed'
                      THEN EXCLUDED.status ELSE storms.mfr_files.status END,
        message = CASE WHEN storms.mfr_files.status = 'failed'
                       THEN EXCLUDED.message ELSE storms.mfr_files.message END,
        disturbance_id = COALESCE(
            storms.mfr_files.disturbance_id, EXCLUDED.disturbance_id),
        issued_time = COALESCE(
            storms.mfr_files.issued_time, EXCLUDED.issued_time),
        kind = COALESCE(storms.mfr_files.kind, EXCLUDED.kind)
    """
)


def should_replace(
    kind: str,
    old_update: datetime | None,
    old_late: bool,
    new_update: datetime | None,
    new_late: bool,
    old_sha: str = "",
    new_sha: str = "",
    old_named: bool = False,
    new_named: bool = False,
) -> tuple[bool, str]:
    """Does a new version of a stored forecast (or track) replace it?

    For forecasts an operational file always wins over a late re-issue,
    whatever their order of arrival. Otherwise, and always for track-only
    files, the file produced last wins (a correction or a revision). Equal
    update times (or none on both) go to the version that names the
    storm (CXML twins differ only in <cycloneName>: '0120172018' or AVA),
    then to the content hash, so the result never depends on the order
    files are read in.
    """
    if kind == "forecast":
        if new_late and not old_late:
            return False, "late re-issue; the operational forecast is kept"
        if old_late and not new_late:
            return True, "operational forecast replaces a late re-issue"
    if old_update == new_update:
        if old_named != new_named:
            return new_named, "same update_time; the named version wins"
        if new_sha > old_sha:
            return True, "same update_time; tie broken by content hash"
        return False, "same update_time; tie broken by content hash"
    if new_update is not None and (
        old_update is None or new_update > old_update
    ):
        return True, "newer version"
    return False, "not newer than the stored version"


def write_forecast(
    conn, fc: MfrForecast, raw_blob: str | None
) -> tuple[str, str | None]:
    """Write one file's forecast (or track) on ``conn`` unless a better
    version is stored.

    Returns (status, message), status 'written', 'unchanged' or
    'kept_existing'. The caller owns the transaction.
    """
    key = {k: fc.forecast[k] for k in _KEY}
    is_track = fc.forecast["kind"] == "track"
    conn.execute(_INSERT_STORM, fc.storm)
    existing = conn.execute(
        _EXISTING_TRACK if is_track else _EXISTING_FORECAST, key
    ).first()
    message = None
    if existing is not None:
        old_sha, old_update, old_late, old_name = existing
        if old_sha == fc.forecast["content_sha256"]:
            return "unchanged", None
        replace, message = should_replace(
            fc.forecast["kind"],
            old_update,
            bool(old_late),
            fc.forecast["update_time"],
            fc.forecast["late_reissue"],
            old_sha,
            fc.forecast["content_sha256"],
            old_named=clean_name(old_name) is not None,
            new_named=clean_name(fc.forecast["name_as_issued"]) is not None,
        )
        if not replace:
            return "kept_existing", message
    if is_track:
        conn.execute(_DELETE_TRACKS, key)
    elif existing is not None:
        conn.execute(_DELETE_FORECAST, key)

    conn.execute(_INSERT_FORECAST, {**fc.forecast, "raw_blob": raw_blob})
    if fc.uncertainty_cone is not None:
        # A cone PostGIS can't read must not cost the forecast.
        nested = conn.begin_nested()
        try:
            conn.execute(_SET_CONE, {**key, "cone": fc.uncertainty_cone})
            nested.commit()
        except Exception as exc:  # noqa: BLE001
            nested.rollback()
            logger.warning(f"{key}: uncertainty cone not stored: {exc}")
    if len(fc.tracks):
        records = fc.tracks.astype(object).where(fc.tracks.notna(), None)
        conn.execute(_INSERT_TRACK, records.to_dict(orient="records"))

    names = conn.execute(
        _NAMES_AS_ISSUED, {"disturbance_id": fc.storm["disturbance_id"]}
    ).scalars()
    name = next((n for n in map(clean_name, names) if n), None)
    naming = {
        "disturbance_id": fc.storm["disturbance_id"],
        "name": name,
        "storm_id": make_storm_id(name, fc.storm["season"]),
    }
    conn.execute(_UPDATE_STORM_NAME, naming)
    conn.execute(
        _UPDATE_TRACK_STORM_IDS,
        {k: naming[k] for k in ("disturbance_id", "storm_id")},
    )
    return "written", message


def ingest_mfr_bytes(
    data: bytes,
    filename: str,
    mode: str,
    source: str,
    source_path: str | None = None,
    engine=None,
    archive: bool = True,
    size_bytes: int | None = None,
    source_modified: datetime | None = None,
) -> dict:
    """Archive one file to blob, write it, and record it in mfr_files.

    The raw file is archived before anything else, parsed or not.
    """
    sha256 = hashlib.sha256(data).hexdigest()
    fc, error = None, None
    try:
        fc = parse_mfr_file(data, filename, data_source=source)
    except MfrParseError as exc:
        error = str(exc)
        logger.error(f"{filename}: {error}")

    raw_blob = raw_blob_name(filename, sha256, fc) if archive else None
    if archive:
        stratus.upload_blob_data(
            data,
            raw_blob,
            stage=mode,
            container_name=RAW_CONTAINER,
            content_type=(
                "application/xml"
                if data.lstrip()[:1] == b"<"
                else "application/json"
            ),
        )
        logger.info(f"Archived {filename} to {RAW_CONTAINER}/{raw_blob}")

    if engine is None:
        engine = stratus.get_engine(stage=mode, write=True)
    with engine.begin() as conn:
        if fc is None:
            status, message = "failed", error
        else:
            status, message = write_forecast(conn, fc, raw_blob)
        conn.execute(
            _RECORD_FILE,
            {
                "source": source,
                "source_path": source_path or filename,
                "content_sha256": sha256,
                "source_filename": filename,
                "size_bytes": (
                    size_bytes if size_bytes is not None else len(data)
                ),
                "source_modified": source_modified,
                "raw_blob": raw_blob,
                "status": status,
                "message": message,
                "disturbance_id": fc.forecast["disturbance_id"]
                if fc
                else None,
                "issued_time": fc.forecast["issued_time"] if fc else None,
                "kind": fc.forecast["kind"] if fc else None,
            },
        )
    result = {
        "filename": filename,
        "status": status,
        "message": message,
        "raw_blob": raw_blob,
    }
    if fc is not None:
        result.update(describe(fc))
    logger.info(f"{filename}: {status}" + (f" ({message})" if message else ""))
    return result


def describe(fc: MfrForecast) -> dict:
    """What a dry run reports for a parsed file."""
    f = fc.forecast
    return {
        "disturbance_id": f["disturbance_id"],
        "kind": f["kind"],
        "name_as_issued": f["name_as_issued"],
        "issued_time": f["issued_time"].isoformat(),
        "update_time": (
            f["update_time"].isoformat() if f["update_time"] else None
        ),
        "late_reissue": f["late_reissue"],
        "n_analysis": f["n_analysis"],
        "n_forecast": f["n_forecast"],
        "max_leadtime": f["max_leadtime"],
        "has_cone": fc.uncertainty_cone is not None,
    }


# ---------------------------------------------------------------------------
# Fetching new files (blob copy or FTP)
# ---------------------------------------------------------------------------

_KNOWN_FILES = text(
    """
    SELECT source_path, size_bytes, source_modified, content_sha256, status
    FROM storms.mfr_files WHERE source = :source
    """
)

_SEEN_AGAIN = text(
    """
    UPDATE storms.mfr_files
    SET last_seen = (now() AT TIME ZONE 'utc'),
        size_bytes = COALESCE(:size_bytes, size_bytes),
        source_modified = COALESCE(:source_modified, source_modified)
    WHERE source = :source AND source_path = :source_path
      AND content_sha256 = :content_sha256
    """
)

_TOUCH_FILES = text(
    """
    UPDATE storms.mfr_files SET last_seen = (now() AT TIME ZONE 'utc')
    WHERE source = :source AND source_path = ANY(:paths)
    """
)


def _is_track_file(path: str) -> bool:
    name = path.rsplit("/", 1)[-1]
    return name.startswith(FILE_PREFIX) and name.endswith(".json")


def _is_backfill_file(path: str) -> bool:
    """A JSON track file or a CXML forecast (not the other products
    Météo-France sends with them: wind/probability polygons, ensembles)."""
    return _is_track_file(path) or path.lower().endswith((".cxml", ".xml"))


def files_to_fetch(
    listing: dict[str, dict], known: dict[str, dict]
) -> list[str]:
    """Track files to download, oldest first.

    ``listing`` maps a path to {'size', 'modified', 'type'}; ``known`` maps
    a path already read to {'versions': {(size, modified), ...}, 'shas':
    {...}}. A known path is downloaded again whenever the listing shows a
    size or modification time not recorded for it, so a file changed in
    place is never missed even at the same size. (The blob copy rewrites
    files for about two weeks after they appear; a rewrite with the same
    content costs a download and is then recognised by its hash.) Without
    a size in the listing (FTP servers without MLSD) a known name is not
    downloaded again.
    """
    todo = []
    for path, entry in listing.items():
        if entry.get("type", "file") != "file" or not _is_track_file(path):
            continue
        seen = known.get(path)
        if seen is None:
            todo.append(path)
            continue
        if entry.get("size") is None:
            continue
        if (entry["size"], entry.get("modified")) not in seen["versions"]:
            todo.append(path)
    return sorted(
        todo, key=lambda p: (listing[p].get("modified") or datetime.min, p)
    )


def _known(engine, source: str, retry_failed: bool = False) -> dict[str, dict]:
    """Paths already read from ``source``. With ``retry_failed``, files that
    failed to parse are left out, so they are read and parsed again (after
    a parser fix)."""
    known: dict[str, dict] = {}
    if engine is None:
        return known
    with engine.connect() as conn:
        for path, size, modified, sha, status in conn.execute(
            _KNOWN_FILES, {"source": source}
        ):
            if retry_failed and status == "failed":
                continue
            entry = known.setdefault(path, {"versions": set(), "shas": set()})
            entry["versions"].add((size, modified))
            entry["shas"].add(sha)
    return known


def _fetch(item) -> bytes:
    """Bytes, a zero-argument download function, or the exception an earlier
    download raised."""
    if isinstance(item, BaseException):
        raise item
    return item() if callable(item) else item


def _ingest_downloaded(
    downloaded: dict,
    listing: dict[str, dict],
    known: dict[str, set],
    mode: str,
    source: str,
    engine,
    dry_run: bool,
    archive: bool,
) -> dict:
    """``downloaded``: path -> bytes, or a function returning them (called
    here, one file at a time), or the exception its download raised. A file
    that can't be downloaded fails alone; it isn't recorded, so the next run
    tries it again."""
    results, failed = [], []
    for path, item in downloaded.items():
        entry = listing[path]
        filename = path.rsplit("/", 1)[-1]
        try:
            data = _fetch(item)
        except Exception as exc:  # noqa: BLE001 — report, carry on
            logger.exception(f"{path}: download failed")
            failed.append({"filename": filename, "error": f"download failed: {exc!r}"})
            continue
        sha256 = hashlib.sha256(data).hexdigest()
        if sha256 in known.get(path, {}).get("shas", ()):
            # Same content as already read from this path (a rewrite).
            if not dry_run:
                with engine.begin() as conn:
                    conn.execute(
                        _SEEN_AGAIN,
                        {
                            "source": source,
                            "source_path": path,
                            "content_sha256": sha256,
                            "size_bytes": entry.get("size"),
                            "source_modified": entry.get("modified"),
                        },
                    )
            results.append({"filename": filename, "status": "seen_before"})
            continue
        if dry_run:
            try:
                fc = parse_mfr_file(data, filename, data_source=source)
                results.append({"filename": filename, **describe(fc)})
            except MfrParseError as exc:
                failed.append({"filename": filename, "error": str(exc)})
            continue
        try:
            result = ingest_mfr_bytes(
                data,
                filename,
                mode,
                source=source,
                source_path=path,
                engine=engine,
                archive=archive,
                size_bytes=entry.get("size"),
                source_modified=entry.get("modified"),
            )
        except Exception as exc:  # noqa: BLE001 — report, carry on
            logger.exception(f"{path}: ingest failed")
            failed.append({"filename": filename, "error": repr(exc)})
            continue
        results.append(result)
        if result["status"] == "failed":
            failed.append({"filename": filename, "error": result["message"]})

    seen_paths = [
        p
        for p in listing
        if p in known and p not in downloaded and _is_track_file(p)
    ]
    if engine is not None and seen_paths and not dry_run:
        with engine.begin() as conn:
            conn.execute(_TOUCH_FILES, {"source": source, "paths": seen_paths})

    counts = _status_counts(results)
    logger.info(
        f"{source} {mode}: listed {len(listing)}, fetched {len(downloaded)}, "
        f"{counts}, {len(failed)} failed"
    )
    return {
        "mode": mode,
        "source": source,
        "dry_run": dry_run,
        "listed": len(listing),
        "fetched": len(downloaded),
        "counts": counts,
        "results": results,
        "failed": failed,
    }


def _naive_utc(ts) -> datetime | None:
    if ts is None:
        return None
    ts = pd.Timestamp(ts)
    if ts.tzinfo is not None:
        ts = ts.tz_convert(None)
    return ts.to_pydatetime()


def sync_blob(
    mode: str,
    dry_run: bool = False,
    archive: bool = True,
    engine=None,
    container_client=None,
    stage: str = SYNC_STAGE,
    container: str = SYNC_CONTAINER,
    prefix: str = SYNC_PREFIX,
    retry_failed: bool = False,
) -> dict:
    """Ingest every track file in the Data Factory copy not read before.

    Files sit both in per-forecast folders and at the prefix root; each
    path is read (the same content under two paths is 'unchanged' the
    second time). A dry run has no database, so it reads every file.
    """
    if engine is None and not dry_run:
        engine = stratus.get_engine(stage=mode, write=True)
    if container_client is None:
        container_client = stratus.get_container_client(
            container_name=container, stage=stage
        )
    listing = {}
    for blob in container_client.list_blobs(name_starts_with=prefix):
        if _is_track_file(blob.name):
            listing[blob.name] = {
                "size": blob.size,
                "modified": _naive_utc(blob.last_modified),
                "type": "file",
            }
    logger.info(f"{len(listing)} track files under {container}/{prefix}")
    known = _known(engine, "meteofr_blob", retry_failed=retry_failed)
    todo = files_to_fetch(listing, known)
    logger.info(f"{len(todo)} new or changed track files to read")
    downloaded = {
        path: (lambda p=path: container_client.get_blob_client(p).download_blob().readall())
        for path in todo
    }
    return _ingest_downloaded(
        downloaded,
        listing,
        known,
        mode,
        "meteofr_blob",
        engine,
        dry_run,
        archive,
    )


def _parse_mlsd_time(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.strptime(value[:14], "%Y%m%d%H%M%S")
    except ValueError:
        return None


def list_ftp_directory(ftp) -> dict[str, dict]:
    """{name: {'size', 'modified', 'type'}} for the current directory.

    MLSD gives sizes and times in one command; servers without it fall back
    to NLST (names only).
    """
    try:
        entries = {}
        for name, facts in ftp.mlsd():
            if name in (".", ".."):
                continue
            size = facts.get("size")
            entries[name] = {
                "size": int(size) if size and size.isdigit() else None,
                "modified": _parse_mlsd_time(facts.get("modify")),
                "type": facts.get("type", "file"),
            }
        return entries
    except ftplib.error_perm as exc:
        logger.info(f"MLSD not available ({exc}); listing names only")
        return {
            name: {"size": None, "modified": None, "type": "file"}
            for name in ftp.nlst()
        }


def _ftp_config() -> dict:
    keys = {
        "host": "FTP_HOST",
        "user": "FTP_USER",
        "password": "FTP_PASS",
        "directory": "FORECAST_DIR",
    }
    config = {k: os.getenv(v) for k, v in keys.items()}
    missing = [keys[k] for k, v in config.items() if not v]
    if missing:
        raise RuntimeError(f"Missing FTP settings: {', '.join(missing)}")
    return config


def poll_ftp(
    mode: str,
    dry_run: bool = False,
    archive: bool = True,
    engine=None,
    ftp_factory=ftplib.FTP,
    retry_failed: bool = False,
) -> dict:
    """List the FTP directory and ingest every track file not read before.

    Not scheduled (see the module docstring). All new files are downloaded
    first and the session closed before anything is written.
    """
    config = _ftp_config()
    if engine is None and not dry_run:
        engine = stratus.get_engine(stage=mode, write=True)
    known = _known(engine, "ftp", retry_failed=retry_failed)

    ftp = ftp_factory(config["host"], timeout=FTP_TIMEOUT_SECONDS)
    downloaded = {}
    try:
        ftp.login(config["user"], config["password"])
        ftp.cwd(config["directory"])
        listing = list_ftp_directory(ftp)
        others = sorted(n for n in listing if not _is_track_file(n))
        logger.info(
            f"FTP listing: {len(listing)} entries; others: {others[:20]}"
            + (" ..." if len(others) > 20 else "")
        )
        for name in files_to_fetch(listing, known):
            buffer = io.BytesIO()
            try:
                ftp.retrbinary(f"RETR {name}", buffer.write)
                downloaded[name] = buffer.getvalue()
            except ftplib.error_perm as exc:
                # One file the server refuses fails alone; a lost
                # connection still ends the session.
                downloaded[name] = exc
    finally:
        try:
            ftp.quit()
        except Exception:  # noqa: BLE001 — closing anyway
            ftp.close()
    return _ingest_downloaded(
        downloaded, listing, known, mode, "ftp", engine, dry_run, archive
    )


def backfill_blobs(
    prefixes: list[str],
    mode: str,
    blob_stage: str = "dev",
    container: str = "projects",
    dry_run: bool = False,
    archive: bool = True,
) -> dict:
    """Ingest every track file under ``prefixes`` (blob), in any order.

    Order doesn't matter: which version of a forecast is kept depends on
    the files (should_replace), not on when they are read.
    """
    names = []
    for prefix in prefixes:
        for name in sorted(
            stratus.list_container_blobs(
                name_starts_with=prefix,
                stage=blob_stage,
                container_name=container,
            )
        ):
            if _is_backfill_file(name) and name not in names:
                names.append(name)
    logger.info(f"{len(names)} JSON/CXML files under {prefixes}")
    listing = {name: {"type": "file"} for name in names}
    downloaded = {
        name: (
            lambda n=name: stratus.load_blob_data(
                n, stage=blob_stage, container_name=container
            )
        )
        for name in names
    }
    engine = None if dry_run else stratus.get_engine(stage=mode, write=True)
    return _ingest_downloaded(
        downloaded, listing, {}, mode, "backfill", engine, dry_run, archive
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Météo-France La Réunion forecast-track ETL"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser(
        "sync", help="Ingest new files from the Data Factory blob copy"
    )
    sub.add_parser("poll", help="Ingest new files from the FTP site")

    ingest = sub.add_parser("ingest", help="Ingest local files")
    ingest.add_argument("paths", nargs="+")

    backfill = sub.add_parser("backfill", help="Ingest files held in blob")
    backfill.add_argument("--blob-prefix", action="append", required=True)
    backfill.add_argument(
        "--blob-stage", choices=["dev", "prod"], default="dev"
    )
    backfill.add_argument("--container", default="projects")

    for name in ("sync", "poll"):
        sub.choices[name].add_argument(
            "--retry-failed",
            action="store_true",
            help="Read again the files that failed to parse (after a fix)",
        )
    for p in sub.choices.values():
        p.add_argument("--mode", choices=["dev", "prod"], default="dev")
        p.add_argument(
            "--dry-run",
            action="store_true",
            help="Read and parse only; no blob or database writes",
        )
        p.add_argument("--no-archive", action="store_true")
        p.add_argument("--out-json", help="Write the result as JSON here")
    args = parser.parse_args(argv)
    archive = not args.no_archive

    if args.command == "sync":
        result = sync_blob(
            args.mode,
            dry_run=args.dry_run,
            archive=archive,
            retry_failed=args.retry_failed,
        )
    elif args.command == "poll":
        result = poll_ftp(
            args.mode,
            dry_run=args.dry_run,
            archive=archive,
            retry_failed=args.retry_failed,
        )
    elif args.command == "ingest":
        listing, downloaded = {}, {}
        for path in args.paths:
            with open(path, "rb") as f:
                name = os.path.basename(path)
                downloaded[name] = f.read()
                listing[name] = {"type": "file"}
        engine = (
            None
            if args.dry_run
            else stratus.get_engine(stage=args.mode, write=True)
        )
        result = _ingest_downloaded(
            downloaded,
            listing,
            {},
            args.mode,
            "local",
            engine,
            args.dry_run,
            archive,
        )
    else:
        result = backfill_blobs(
            args.blob_prefix,
            args.mode,
            blob_stage=args.blob_stage,
            container=args.container,
            dry_run=args.dry_run,
            archive=archive,
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
