"""Tests for the FMS (RSMC Nadi) forecast-track parser and writer.

The CSVs are synthetic: same layout as the FMS export (header comments,
57-column heading, ``# Start of data``), made-up values. No DB needed; the
writer is checked against a fake connection that records each statement.
"""

import ast
import json
import logging
import re
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest

from src.pipelines import fms

HEADINGS = (
    "Time[fmt=yyyy-MM-dd'T'HH:mm:ss'Z'],Latitude,Longitude,Symbol,Category,"
    "Pressure,PressureOCI,RadiusOCI,Radius1000hPa,RadiusMaxWinds,MeanWind,"
    "WindGust,VerticalExtent,Uncertainty,FinalT,CurrentIntensity,GaleRadius,"
    "StrongGaleRadius,StormRadius,HurricaneRadius,NEGaleRadius,SEGaleRadius,"
    "SWGaleRadius,NWGaleRadius,NEStrongGaleRadius,SEStrongGaleRadius,"
    "SWStrongGaleRadius,NWStrongGaleRadius,NEStormRadius,SEStormRadius,"
    "SWStormRadius,NWStormRadius,NEHurricaneRadius,SEHurricaneRadius,"
    "SWHurricaneRadius,NWHurricaneRadius,P5Wind,DataSource,Land/Water,"
    "CycloneStatus,HowLocation,DataTNoDvorak,ModelTNoDvorak,"
    "PatternTNoDvorak,UncPressure,HowPressure,P/W_Method,HowMaxWindRadius,"
    "HowGaleRadius,HowStormRadius,HowHurricaneRadius,UncMaxWindSpeed,"
    "HowMaxWindSpeed,HowGust,EyeRadius,UncEyeRadius,HowEyeRadius"
).split(",")

ROWS = [
    {
        HEADINGS[0]: "2026-04-07T00:00:00Z",
        "Latitude": -18.1,
        "Longitude": 174.8,
        "Category": 3.0,
        "MeanWind": 85.0,
        "WindGust": 120.0,
        "Uncertainty": 50.0,
        "NEGaleRadius": 110.0,
        "SEGaleRadius": 110.0,
        "SWGaleRadius": 70.0,
        "NWGaleRadius": 70.0,
        "StormRadius": 40.0,
        "HurricaneRadius": 25.0,
        "P5Wind": 105.0,
    },
    {
        HEADINGS[0]: "2026-04-07T06:00:00Z",
        "Latitude": -19.5,
        "Longitude": 181.5,
        "Category": 3.0,
        "MeanWind": 80.0,
        "Uncertainty": 65.0,
        "NEGaleRadius": 100.0,
        "SWGaleRadius": 60.0,
        "HurricaneRadius": 20.0,
    },
    {
        HEADINGS[0]: "2026-04-07T12:00:00Z",
        "Latitude": -21.0,
        "Longitude": 199.5,
        "Uncertainty": 80.0,
    },
]


def make_csv(
    rows=ROWS,
    line1="# trackType=Official Forecast Track,baseTime=2026-04-07T00:00:00Z",
    disturbance_id="2526-11F",
    name="VAIANU",
    newline="\n",
) -> bytes:
    lines = [line1]
    if disturbance_id is not None:
        lines.append(f"# DisturbanceId={disturbance_id}")
    lines += [
        f"# CycloneName={name}",
        "# MathType of Data",
        "(Time->(Latitude,Longitude,...))",
        "# Column Headings",
        ",".join(HEADINGS),
        "# Start of data",
    ]
    for row in rows:
        lines.append(",".join(str(row.get(h, "NaN")) for h in HEADINGS))
    return (newline.join(lines) + newline).encode()


def test_parse_official_forecast():
    fc = fms.parse_fms_csv(make_csv(), "vaianu.csv")

    assert fc.storm == {
        "disturbance_id": "2526-11F",
        "name": "VAIANU",
        "number": "11F",
        "season": 2026,
        "genesis_basin": "SP",
        "provider": "fms",
        "storm_id": "vaianu_sp_2026",
    }
    assert fc.forecast["track_type"] == "Official Forecast Track"
    assert fc.forecast["issued_time"] == datetime(2026, 4, 7)
    assert fc.forecast["name_as_issued"] == "VAIANU"
    assert fc.forecast["source_filename"] == "vaianu.csv"
    assert fc.forecast["n_points"] == 3
    assert len(fc.forecast["content_sha256"]) == 64

    t = fc.tracks
    assert list(t.columns) == fms.TRACK_COLUMNS
    assert t["leadtime"].tolist() == [0, 6, 12]
    assert t["valid_time"].iloc[1] == datetime(2026, 4, 7, 6)
    assert t["wind_speed"].iloc[0] == 85.0
    assert t["gust_speed"].iloc[0] == 120.0
    assert t["category"].iloc[0] == 3.0
    assert t["uncertainty"].tolist() == [50.0, 65.0, 80.0]


def test_radii_keep_both_styles():
    t = fms.parse_fms_csv(make_csv()).tracks
    first = t.iloc[0]
    # Quadrant gale radii, symmetric storm/hurricane radii, in one row.
    assert json.loads(first["quadrant_radius_34"]) == [110, 110, 70, 70]
    assert pd.isna(first["quadrant_radius_50"])
    assert first["radius_50"] == 40.0
    assert first["radius_64"] == 25.0
    # A missing quadrant is null, not 0.
    second = t.iloc[1]
    assert json.loads(second["quadrant_radius_34"]) == [100, None, 60, None]
    assert pd.isna(t["quadrant_radius_34"].iloc[2])


def test_longitude_normalised_and_wkt():
    t = fms.parse_fms_csv(make_csv()).tracks
    assert t["geometry"].tolist() == [
        "POINT (174.8 -18.1)",
        "POINT (-178.5 -19.5)",
        "POINT (-160.5 -21.0)",
    ]


def test_raw_fields_hold_the_other_columns():
    t = fms.parse_fms_csv(make_csv()).tracks
    assert json.loads(t["raw_fields"].iloc[0]) == {"P5Wind": 105.0}
    assert pd.isna(t["raw_fields"].iloc[1])


def with_cell(index, col, value):
    rows = [dict(r) for r in ROWS]
    rows[index][col] = value
    return make_csv(rows=rows)


def test_blank_cell_is_missing_and_keeps_the_column():
    t = fms.parse_fms_csv(with_cell(2, "HurricaneRadius", " ")).tracks
    assert t["radius_64"].iloc[:2].tolist() == [25.0, 20.0]
    assert pd.isna(t["radius_64"].iloc[2])


@pytest.mark.parametrize(
    "col, value",
    [
        ("NEHurricaneRadius", "?"),
        ("MeanWind", "-"),
        ("Latitude", "?"),
        # read_csv reads inf as a number; it must not reach the trigger,
        # and an infinite longitude would never wrap into range.
        ("MeanWind", "inf"),
        ("Longitude", "inf"),
        ("Longitude", "-inf"),
    ],
)
def test_unreadable_trigger_value_fails(col, value):
    with pytest.raises(fms.FmsParseError, match=f"unreadable {col}"):
        fms.parse_fms_csv(with_cell(2, col, value))


@pytest.mark.parametrize("value", ["-", "inf"])
def test_unreadable_other_value_is_logged_and_null(caplog, value):
    with caplog.at_level(logging.WARNING, logger=fms.logger.name):
        t = fms.parse_fms_csv(with_cell(2, "WindGust", value)).tracks
    assert t["gust_speed"].iloc[0] == 120.0
    assert t["gust_speed"].iloc[1:].isna().all()
    assert f"unreadable WindGust values ['{value}']" in caplog.text


def test_missing_trigger_column_fails():
    data = make_csv().replace(b",MeanWind,", b",MeanWindX,")
    with pytest.raises(
        fms.FmsParseError, match=r"missing columns \['MeanWind'\]"
    ):
        fms.parse_fms_csv(data)


def test_unreadable_base_time_fails():
    data = make_csv(line1="# trackType=Official Forecast Track,baseTime=soon")
    with pytest.raises(fms.FmsParseError, match="baseTime"):
        fms.parse_fms_csv(data)


def test_raw_fields_keep_numbers_beside_an_unreadable_cell():
    rows = [dict(r) for r in ROWS]
    rows[1]["P5Wind"] = 90.0
    rows[2]["P5Wind"] = "-"
    t = fms.parse_fms_csv(make_csv(rows=rows)).tracks
    assert [json.loads(r) for r in t["raw_fields"]] == [
        {"P5Wind": 105.0},
        {"P5Wind": 90.0},
        {"P5Wind": "-"},
    ]


@pytest.mark.parametrize("time", ["", "not a time"])
def test_unreadable_time_fails(time):
    with pytest.raises(fms.FmsParseError, match="readable time"):
        fms.parse_fms_csv(with_cell(1, HEADINGS[0], time))


def test_file_without_points_fails():
    with pytest.raises(fms.FmsParseError, match="no track points"):
        fms.parse_fms_csv(make_csv(rows=[]))


def test_imports_stay_light():
    # pa-aa-fji-storms runs fms.py on a cluster with only these installed.
    tree = ast.parse(Path(fms.__file__).read_text())
    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            imported.add("." if node.level else node.module.split(".")[0])
    allowed = {"dotenv", "ocha_stratus", "pandas", "sqlalchemy"}
    assert imported - set(sys.stdlib_module_names) <= allowed


def test_header_variants():
    # Analysis-style line 1 with an extra field, trailing commas, CRLF.
    data = make_csv(
        line1=(
            "# trackType=Official Forecast Track,dataSource=Fiji,"
            "baseTime=2026-04-07T00:00:00Z,,,,"
        ),
        name="RAE [RAY]",
        newline="\r\n",
    )
    fc = fms.parse_fms_csv(data)
    assert fc.forecast["data_source"] == "Fiji"
    assert fc.forecast["issued_time"] == datetime(2026, 4, 7)
    assert fc.forecast["name_as_issued"] == "RAE [RAY]"
    assert fc.storm["name"] == "RAE"
    assert fc.tracks["leadtime"].tolist() == [0, 6, 12]


def test_unnamed_storm():
    fc = fms.parse_fms_csv(make_csv(name="noname"))
    assert fc.storm["name"] is None
    assert fc.storm["storm_id"] is None
    assert fc.tracks["storm_id"].isna().all()


def test_analysis_track_is_skipped():
    data = make_csv(
        line1="# trackType=Analysis Track,baseTime=2026-04-07T00:00:00Z"
    )
    with pytest.raises(fms.NotAForecastTrack):
        fms.parse_fms_csv(data)


def test_missing_disturbance_id_fails():
    with pytest.raises(fms.FmsParseError, match="DisturbanceId"):
        fms.parse_fms_csv(make_csv(disturbance_id=None))


def test_point_before_base_time_fails():
    rows = [{**ROWS[0], HEADINGS[0]: "2026-04-06T18:00:00Z"}, *ROWS]
    with pytest.raises(fms.FmsParseError, match="before baseTime"):
        fms.parse_fms_csv(make_csv(rows=rows))


def test_repeated_valid_time_keeps_first():
    t = fms.parse_fms_csv(make_csv(rows=[*ROWS, ROWS[2]])).tracks
    assert len(t) == 3


@pytest.mark.parametrize(
    "disturbance_id, season",
    [
        ("2526-11F", 2026),
        ("1213-04F", 2013),
        ("9900-01F", 2000),
        ("9798-03F", 1998),
    ],
)
def test_season_from_disturbance_id(disturbance_id, season):
    assert fms.season_from_disturbance_id(disturbance_id) == season


def test_bad_disturbance_id():
    with pytest.raises(fms.FmsParseError):
        fms.season_from_disturbance_id("11F")


@pytest.mark.parametrize(
    "raw, clean",
    [
        ("VAIANU", "VAIANU"),
        ("Lola", "LOLA"),
        ("RAE [RAY]", "RAE"),
        ("Seru (seh-roo)", "SERU"),
        ("noname", None),
        ("", None),
        (None, None),
    ],
)
def test_clean_name(raw, clean):
    assert fms.clean_name(raw) == clean


def test_raw_blob_name():
    fc = fms.parse_fms_csv(make_csv())
    name = fms.raw_blob_name(fc.forecast)
    assert re.fullmatch(
        r"fms/raw/2526-11F/20260407T0000Z_official-forecast-track_"
        r"[0-9a-f]{12}\.csv",
        name,
    )


class FakeResult:
    def __init__(self, scalars=()):
        self._scalars = list(scalars)

    def scalars(self):
        return iter(self._scalars)


class FakeConn:
    """Records (sql, params); answers the latest-name SELECT."""

    def __init__(self, names_as_issued):
        self.calls = []
        self.names_as_issued = names_as_issued

    def execute(self, stmt, params=None):
        sql = " ".join(str(stmt).split())
        self.calls.append((sql, params))
        if sql.startswith("SELECT name_as_issued"):
            return FakeResult(self.names_as_issued)
        return FakeResult()


def test_write_forecast_replaces_issuance_and_renames():
    fc = fms.parse_fms_csv(make_csv(name="noname"))
    # Newest first, as the SELECT orders them: this unnamed forecast, then
    # an older named one. The storm keeps its name.
    conn = FakeConn(["noname", "VAIANU"])
    fms.write_forecast(conn, fc, "fms/raw/x.csv")

    sqls = [sql for sql, _ in conn.calls]
    assert sqls[0].startswith("INSERT INTO storms.fms_storms")
    assert "ON CONFLICT (disturbance_id) DO NOTHING" in sqls[0]
    assert sqls[1].startswith("DELETE FROM storms.fms_forecasts")
    assert sqls[2].startswith("INSERT INTO storms.fms_forecasts")
    assert sqls[3].startswith("INSERT INTO storms.fms_tracks_geo")
    assert sqls[4].startswith("SELECT name_as_issued")
    assert sqls[5].startswith("UPDATE storms.fms_storms")

    forecast_params = conn.calls[2][1]
    assert forecast_params["raw_blob"] == "fms/raw/x.csv"
    track_params = conn.calls[3][1]
    assert len(track_params) == 3
    assert track_params[0]["leadtime"] == 0
    assert isinstance(track_params[0]["leadtime"], int)
    # NaN goes in as NULL.
    assert track_params[2]["wind_speed"] is None
    assert conn.calls[5][1] == {
        "disturbance_id": "2526-11F",
        "name": "VAIANU",
        "storm_id": "vaianu_sp_2026",
    }


def test_track_insert_binds_every_column():
    sql = str(fms._INSERT_TRACK)
    for col in fms.TRACK_COLUMNS:
        assert f" {col}" in sql or f"({col}" in sql
    assert "CAST(:raw_fields AS jsonb)" in sql
    assert "ST_SetSRID(ST_GeomFromText(:geometry), 4326)" in sql


def test_schema_has_every_track_column():
    sql = (
        Path(__file__).parent.parent / "src/schemas/sql/fms_tables.sql"
    ).read_text()
    block = sql.split("CREATE TABLE IF NOT EXISTS storms.fms_tracks_geo")[1]
    block = block.split("TABLESPACE")[0]
    declared = {
        line.split()[0]
        for line in block.splitlines()
        if line.strip()
        and not line.strip().startswith(
            ("(", ")", "CONSTRAINT", "REFERENCES", "ON ")
        )
    }
    assert set(fms.TRACK_COLUMNS) <= declared


def test_backfill_excludes_test_files_and_prefers_earlier_prefix(monkeypatch):
    blobs = {
        "a/onedrive/f1.csv": make_csv(),
        "a/onedrive/MODIFIED_f1.csv": make_csv(name="FAKE"),
        "b/archive/f1.csv": make_csv(rows=ROWS[:2]),
        "b/archive/test/f2.csv": make_csv(disturbance_id="2526-12F"),
        "b/archive/notes.txt": b"x",
    }
    monkeypatch.setattr(
        fms.stratus,
        "list_container_blobs",
        lambda name_starts_with, stage, container_name: [
            b for b in blobs if b.startswith(name_starts_with)
        ],
    )
    monkeypatch.setattr(
        fms.stratus,
        "load_blob_data",
        lambda name, stage, container_name: blobs[name],
    )
    report = fms.backfill_fms(["a/", "b/"], "dev", dry_run=True)
    assert sorted(report["excluded"]) == [
        "a/onedrive/MODIFIED_f1.csv",
        "b/archive/test/f2.csv",
    ]
    assert report["issuances"] == 1
    assert report["differing_duplicates"] == [
        {
            "key": [
                "2526-11F",
                "Official Forecast Track",
                "2026-04-07 00:00:00",
            ],
            "written": "a/onedrive/f1.csv",
            "not_written": ["b/archive/f1.csv"],
        }
    ]
