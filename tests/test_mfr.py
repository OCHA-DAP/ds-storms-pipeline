"""Tests for the Météo-France La Réunion (MFR) track parser, writer and poller.

The files are synthetic: same layout as Météo-France's CMRSTRACK_SWI JSON
(analysis + forecast points, wind contours by sector, uncertainty cone),
made-up values. No DB, blob or FTP needed: the writer runs against a fake
connection that records each statement, the poller against a fake FTP.
"""

import ast
import copy
import hashlib
import json
import re
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest

from src.pipelines import mfr


def _contour(speed, ne=None, se=None, sw=None, nw=None, sectors=None):
    values = {"NEQ": ne, "SEQ": se, "SWQ": sw, "NWQ": nw}
    keep = sectors or ("NEQ", "SEQ", "SWQ", "NWQ")
    return {
        "wind_speed_kt": speed,
        "radius": [{"sector": s, "value": values[s]} for s in keep],
    }


def _point(data_type, time, lon, lat, wind, contours=None, **extra):
    props = {
        "data_type": data_type,
        "time": time,
        "position_accuracy": extra.pop("position_accuracy", 30),
        "cyclone_data": {
            "development": extra.pop("development", "tropical cyclone"),
            "minimum_pressure": 970,
            "maximum_wind": {"wind_speed_kt": wind, "wind_speed_gust_kt": 90},
            **({"wind_contours": contours} if contours else {}),
        },
        **extra,
    }
    return {
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [lon, lat]},
        "properties": props,
    }


DOC = {
    "product": "Cyclone trajectory",
    "update_time": "2026-02-09T18:23:53Z",
    "production_center": "METEO-FRANCE",
    "cyclone_name": "TESTA",
    "cyclone_number": 10,
    "cyclone_id": "SWI$10/20252026",
    "basin": "SWI",
    "season": 20252026,
    "reference_time": "2026-02-09T18:00:00Z",
    "type": "FeatureCollection",
    "features": [
        _point("analysis", "2026-02-09T06:00:00Z", 60.0, -14.0, 35),
        _point(
            "analysis",
            "2026-02-09T18:00:00Z",
            58.0,
            -15.0,
            60,
            [_contour(34, 40, 50, 70, 60)],
        ),
        _point(
            "forecast",
            "2026-02-09T18:00:00Z",
            58.0,
            -15.0,
            60,
            [
                _contour(28, 60, 75, 95, 85),
                _contour(34, 40, 50, 70, 60),
                _contour(48, 30, 30, 45, 50),
            ],
            position_accuracy=30,
        ),
        _point(
            "forecast",
            "2026-02-10T06:00:00Z",
            55.0,
            -16.0,
            80,
            [
                _contour(34, se=180, sw=100, sectors=("SEQ", "SWQ")),
                _contour(64, 20, 20, 25, 20),
            ],
            position_accuracy=43,
            hour=12,
        ),
        {
            "type": "Feature",
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [[58, -14], [54, -15], [54, -17], [58, -16], [58, -14]]
                ],
            },
            "properties": {"data_type": "uncertainty_cone"},
        },
    ],
}

FILENAME = "CMRSTRACK_SWI$10_20252026_TESTA_2026_02_09_18Z.json"


def _bytes(doc=DOC) -> bytes:
    return json.dumps(doc).encode()


def _parse(doc=DOC, filename=FILENAME):
    return mfr.parse_mfr_json(_bytes(doc), filename)


def test_parse_ids_times_and_counts():
    fc = _parse()
    assert fc.storm == {
        "disturbance_id": "10-20252026",
        "cyclone_id": "SWI$10/20252026",
        "name": "TESTA",
        "number": 10,
        "season": 2026,
        "genesis_basin": "SI",
        "provider": "mfr",
        "storm_id": "testa_si_2026",
    }
    f = fc.forecast
    assert f["issued_time"] == datetime(2026, 2, 9, 18)
    assert f["update_time"] == datetime(2026, 2, 9, 18, 23, 53)
    assert f["late_reissue"] is False
    assert (f["n_analysis"], f["n_forecast"], f["max_leadtime"]) == (2, 2, 12)
    assert f["source_filename"] == FILENAME
    assert json.loads(fc.uncertainty_cone)["type"] == "Polygon"


def test_reference_time_not_filename_hour():
    # Uploaded at 02Z, the file is the 00Z forecast.
    doc = copy.deepcopy(DOC)
    doc["reference_time"] = "2026-02-10T00:00:00Z"
    doc["update_time"] = "2026-02-10T02:34:00Z"
    for feat in doc["features"][:3]:
        feat["properties"]["time"] = "2026-02-10T00:00:00Z"
    doc["features"][0]["properties"]["time"] = "2026-02-09T12:00:00Z"
    doc["features"][3]["properties"]["time"] = "2026-02-10T12:00:00Z"
    fc = _parse(doc, "CMRSTRACK_SWI$10_20252026_TESTA_2026_02_10_02Z.json")
    assert fc.forecast["issued_time"] == datetime(2026, 2, 10, 0)


def test_leadtimes_and_both_copies_of_the_reference_analysis():
    t = _parse().tracks
    assert list(zip(t.data_type, t.leadtime)) == [
        ("analysis", -12),
        ("analysis", 0),
        ("forecast", 0),
        ("forecast", 12),
    ]


def test_radii_in_ne_se_sw_nw_order_and_missing_sectors_null():
    t = _parse().tracks
    lt0 = t[(t.data_type == "forecast") & (t.leadtime == 0)].iloc[0]
    assert json.loads(lt0.quadrant_radius_28) == [60, 75, 95, 85]
    assert json.loads(lt0.quadrant_radius_48) == [30, 30, 45, 50]
    assert pd.isna(lt0.quadrant_radius_64)
    lt12 = t[t.leadtime == 12].iloc[0]
    assert json.loads(lt12.quadrant_radius_34) == [None, 180, 100, None]
    assert json.loads(lt12.quadrant_radius_64) == [20, 20, 25, 20]


def test_scalars_and_geometry():
    t = _parse().tracks
    row = t[t.leadtime == 12].iloc[0]
    assert row.wind_speed == 80 and row.gust_speed == 90
    assert row.pressure == 970 and row.position_accuracy == 43
    assert row.development == "tropical cyclone"
    assert row.geometry == "POINT (55.0 -16.0)"
    assert pd.isna(row.raw_fields)  # "hour" is read, not kept raw


def test_unknown_contour_speed_kept_raw(caplog):
    doc = copy.deepcopy(DOC)
    contours = doc["features"][2]["properties"]["cyclone_data"]
    contours["wind_contours"].append(_contour(40, 35, 35, 40, 45))
    t = _parse(doc).tracks
    lt0 = t[(t.data_type == "forecast") & (t.leadtime == 0)].iloc[0]
    raw = json.loads(lt0.raw_fields)
    assert raw["cyclone_data.wind_contours"][0]["wind_speed_kt"] == 40
    assert "40" in caplog.text


def test_cxml_speeds_have_columns():
    doc = copy.deepcopy(DOC)
    contours = doc["features"][2]["properties"]["cyclone_data"]
    contours["wind_contours"] += [
        _contour(27, 70, 80, 90, 85),
        _contour(50, 25, 25, 30, 30),
    ]
    t = _parse(doc).tracks
    lt0 = t[(t.data_type == "forecast") & (t.leadtime == 0)].iloc[0]
    assert json.loads(lt0.quadrant_radius_27) == [70, 80, 90, 85]
    assert json.loads(lt0.quadrant_radius_50) == [25, 25, 30, 30]
    assert pd.isna(lt0.raw_fields)


def test_unnamed_system():
    doc = copy.deepcopy(DOC)
    doc["cyclone_name"] = "10-20252026"
    fc = _parse(doc)
    assert fc.storm["name"] is None and fc.storm["storm_id"] is None
    assert fc.tracks.storm_id.isna().all()
    assert fc.forecast["name_as_issued"] == "10-20252026"


def test_late_reissue_flag():
    doc = copy.deepcopy(DOC)
    doc["update_time"] = "2026-06-27T13:00:00Z"
    fc = _parse(doc)
    assert fc.forecast["late_reissue"] is True
    assert fc.forecast["kind"] == "forecast"
    assert (fc.tracks.kind == "forecast").all()


def _track_only(update_time="2026-02-12T09:00:00Z"):
    """A final-track file: analysis points only, one after reference_time."""
    doc = copy.deepcopy(DOC)
    doc["update_time"] = update_time
    doc["features"] = [
        f
        for f in doc["features"]
        if f["properties"]["data_type"] == "analysis"
    ]
    doc["features"].append(
        _point("analysis", "2026-02-09T22:00:00Z", 57.0, -15.3, 55)
    )
    return doc


def test_track_only_file():
    fc = _parse(_track_only())
    f = fc.forecast
    assert f["kind"] == "track"
    assert (f["n_analysis"], f["n_forecast"], f["max_leadtime"]) == (
        3,
        0,
        None,
    )
    assert f["late_reissue"] is False  # revisions of a final track are normal
    assert list(fc.tracks.leadtime) == [-12, 0, 4]
    assert (fc.tracks.kind == "track").all()


def test_analysis_after_reference_in_a_forecast_warns(caplog):
    doc = copy.deepcopy(DOC)
    doc["features"].insert(
        2, _point("analysis", "2026-02-09T21:00:00Z", 57.5, -15.2, 60)
    )
    fc = _parse(doc)
    assert fc.forecast["n_analysis"] == 3
    assert "after reference_time" in caplog.text


@pytest.mark.parametrize(
    "cyclone_id",
    ["SWI$10/2025", "SWI$10/20252027", "SIO$10/20252026", None],
)
def test_bad_cyclone_id(cyclone_id):
    doc = copy.deepcopy(DOC)
    doc["cyclone_id"] = cyclone_id
    with pytest.raises(mfr.MfrParseError):
        _parse(doc)


def test_not_json_fails():
    with pytest.raises(mfr.MfrParseError):
        mfr.parse_mfr_json(b"<html>", "x.json")


def test_missing_reference_time_fails():
    doc = copy.deepcopy(DOC)
    del doc["reference_time"]
    with pytest.raises(mfr.MfrParseError):
        _parse(doc)


def test_point_without_position_fails():
    doc = copy.deepcopy(DOC)
    doc["features"][3]["geometry"]["coordinates"] = []
    with pytest.raises(mfr.MfrParseError):
        _parse(doc)


def test_forecast_before_reference_time_fails():
    doc = copy.deepcopy(DOC)
    doc["features"][3]["properties"]["time"] = "2026-02-09T12:00:00Z"
    with pytest.raises(mfr.MfrParseError):
        _parse(doc)


def test_file_without_points_fails():
    doc = copy.deepcopy(DOC)
    doc["features"] = doc["features"][4:]
    with pytest.raises(mfr.MfrParseError):
        _parse(doc)


def test_repeated_point_keeps_first(caplog):
    doc = copy.deepcopy(DOC)
    doc["features"].insert(4, copy.deepcopy(doc["features"][3]))
    doc["features"][4]["properties"]["cyclone_data"]["maximum_wind"][
        "wind_speed_kt"
    ] = 99
    t = _parse(doc).tracks
    assert (t.leadtime == 12).sum() == 1
    assert t[t.leadtime == 12].wind_speed.iloc[0] == 80
    assert "repeated" in caplog.text


@pytest.mark.parametrize(
    "raw, clean",
    [
        ("GEZANI", "GEZANI"),
        (" gezani ", "GEZANI"),
        ("05-20242025", None),
        ("0120102011", None),
        ("20182019", None),
        ("INVEST", None),
        ("EX-GUAMBE", "GUAMBE"),
        ("", None),
        (None, None),
    ],
)
def test_clean_name(raw, clean):
    assert mfr.clean_name(raw) == clean


def test_raw_blob_name():
    fc = _parse()
    sha = fc.forecast["content_sha256"]
    assert mfr.raw_blob_name(FILENAME, sha, fc) == (
        "mfr/raw/2026/10-20252026/"
        f"CMRSTRACK_SWI_10_20252026_TESTA_2026_02_09_18Z_{sha[:12]}.json"
    )
    assert mfr.raw_blob_name("odd name.txt", sha, None) == (
        f"mfr/raw/unparsed/odd_name_{sha[:12]}.txt"
    )


@pytest.mark.parametrize(
    "kind, old_late, new_late, old_up, new_up, replace",
    [
        ("forecast", False, False, 1, 2, True),  # a correction
        ("forecast", False, False, 2, 1, False),  # older version arrives later
        ("forecast", False, True, 1, 9, False),  # late re-issue never replaces
        ("forecast", True, False, 9, 1, True),  # operational replaces late
        ("forecast", True, True, 8, 9, True),
        ("forecast", False, False, None, 1, True),
        ("forecast", False, False, 1, None, False),
        ("track", False, False, 1, 9, True),  # latest revision wins
        ("track", False, False, 9, 1, False),
    ],
)
def test_should_replace(kind, old_late, new_late, old_up, new_up, replace):
    def t(h):
        return None if h is None else datetime(2026, 2, 9, h)

    result = mfr.should_replace(kind, t(old_up), old_late, t(new_up), new_late)
    assert result[0] is replace


def test_imports_stay_light():
    tree = ast.parse(Path(mfr.__file__).read_text())
    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            imported.add("." if node.level else node.module.split(".")[0])
    allowed = {"dotenv", "ocha_stratus", "pandas", "sqlalchemy"}
    assert imported - set(sys.stdlib_module_names) <= allowed


# ---------------------------------------------------------------------------
# Writer
# ---------------------------------------------------------------------------


class FakeResult:
    def __init__(self, first=None, scalars=()):
        self._first = first
        self._scalars = list(scalars)

    def first(self):
        return self._first

    def scalars(self):
        return iter(self._scalars)


class FakeNested:
    def commit(self):
        pass

    def rollback(self):
        pass


class FakeConn:
    """Records (sql, params); answers the existing-forecast and name SELECTs."""

    def __init__(self, existing=None, names=("TESTA",)):
        self.calls = []
        self.existing = existing
        self.names = names

    def execute(self, stmt, params=None):
        sql = " ".join(str(stmt).split())
        self.calls.append((sql, params))
        if sql.startswith("SELECT content_sha256"):
            return FakeResult(first=self.existing)
        if sql.startswith("SELECT name_as_issued"):
            return FakeResult(scalars=self.names)
        return FakeResult()

    def begin_nested(self):
        return FakeNested()

    def statements(self):
        return [sql.split(" (")[0].split(" WHERE")[0] for sql, _ in self.calls]


def test_write_new_forecast():
    conn = FakeConn()
    status, _ = mfr.write_forecast(conn, _parse(), "mfr/raw/x.json")
    assert status == "written"
    stmts = conn.statements()
    assert stmts[0].startswith("INSERT INTO storms.mfr_storms")
    assert not any(s.startswith("DELETE") for s in stmts)
    assert any("SET uncertainty_cone" in s for s, _ in conn.calls)
    track_insert = [
        p
        for s, p in conn.calls
        if s.startswith("INSERT INTO storms.mfr_tracks_geo")
    ][0]
    assert len(track_insert) == 4
    assert set(track_insert[0]) == set(mfr.TRACK_COLUMNS)
    rename = [
        p for s, p in conn.calls if s.startswith("UPDATE storms.mfr_storms")
    ]
    assert rename == [
        {
            "disturbance_id": "10-20252026",
            "name": "TESTA",
            "storm_id": "testa_si_2026",
        }
    ]
    # every point of the system, including earlier unnamed forecasts
    retag = [
        p for s, p in conn.calls if s.startswith("UPDATE storms.mfr_tracks_geo SET storm_id")
    ]
    assert retag == [{"disturbance_id": "10-20252026", "storm_id": "testa_si_2026"}]


def test_same_content_is_unchanged():
    fc = _parse()
    conn = FakeConn(
        existing=(fc.forecast["content_sha256"], None, False, "TESTA")
    )
    assert mfr.write_forecast(conn, fc, None)[0] == "unchanged"
    assert not any(
        s.startswith("INSERT INTO storms.mfr_forecasts") for s, _ in conn.calls
    )


def test_late_reissue_does_not_replace():
    doc = copy.deepcopy(DOC)
    doc["update_time"] = "2026-06-27T13:00:00Z"
    conn = FakeConn(
        existing=("0" * 64, datetime(2026, 2, 9, 18, 20), False, "TESTA")
    )
    status, message = mfr.write_forecast(conn, _parse(doc), None)
    assert status == "kept_existing" and "late" in message
    assert not any(s.startswith("DELETE") for s, _ in conn.calls)


def test_track_is_compared_and_replaced_per_system():
    # A track revision with another reference time (GEZANI 2026: 12Z then
    # 18Z) must replace the stored track, not sit beside it.
    conn = FakeConn(existing=("0" * 64, datetime(2026, 2, 10), False, "TESTA"))
    status, _ = mfr.write_forecast(conn, _parse(_track_only()), None)
    assert status == "written"
    lookup = [
        s for s, _ in conn.calls if s.startswith("SELECT content_sha256")
    ]
    assert "kind = 'track'" in lookup[0] and "issued_time" not in lookup[0]
    deletes = [s for s, _ in conn.calls if s.startswith("DELETE")]
    assert deletes and "issued_time" not in deletes[0]


def test_first_track_of_a_system_clears_nothing_else():
    conn = FakeConn()
    assert mfr.write_forecast(conn, _parse(_track_only()), None)[0] == (
        "written"
    )


def test_older_track_revision_is_kept_out():
    conn = FakeConn(existing=("0" * 64, datetime(2026, 3, 1), False, "TESTA"))
    status, _ = mfr.write_forecast(conn, _parse(_track_only()), None)
    assert status == "kept_existing"
    assert not any(s.startswith("DELETE") for s, _ in conn.calls)


def test_correction_replaces():
    conn = FakeConn(
        existing=("0" * 64, datetime(2026, 2, 9, 18, 10), False, "TESTA")
    )
    assert mfr.write_forecast(conn, _parse(), None)[0] == "written"
    assert any(
        s.startswith("DELETE FROM storms.mfr_forecasts") for s, _ in conn.calls
    )


def test_schema_has_every_column():
    sql = (
        Path(mfr.__file__).parents[1] / "schemas" / "sql" / "mfr_tables.sql"
    ).read_text()
    tracks = sql.split("CREATE TABLE IF NOT EXISTS storms.mfr_tracks_geo")[1]
    tracks = tracks.split(")\nTABLESPACE")[0]
    for col in mfr.TRACK_COLUMNS:
        assert f"\n    {col} " in tracks, col
    forecasts = sql.split("CREATE TABLE IF NOT EXISTS storms.mfr_forecasts")[1]
    forecasts = forecasts.split(")\nTABLESPACE")[0]
    for col in mfr._FORECAST_COLUMNS + ["uncertainty_cone"]:
        assert f"\n    {col} " in forecasts, col
    files = sql.split("CREATE TABLE IF NOT EXISTS storms.mfr_files")[1]
    files = files.split(")\nTABLESPACE")[0]
    insert = str(mfr._RECORD_FILE).split("VALUES")[0]
    for col in re.findall(r"\w+", insert.split("(", 1)[1]):
        assert f"\n    {col} " in files, col


# ---------------------------------------------------------------------------
# Blob sync and FTP poll
# ---------------------------------------------------------------------------


class FakeBlob:
    def __init__(self, name, data, modified):
        self.name, self.size, self.last_modified = name, len(data), modified


class FakeContainer:
    """The few ContainerClient calls sync_blob makes; records downloads."""

    def __init__(self, blobs):
        self.blobs = blobs  # name -> bytes
        self.downloaded = []

    def list_blobs(self, name_starts_with=None):
        when = datetime(2026, 11, 16, 13, 14)
        return [
            FakeBlob(n, d, when)
            for n, d in self.blobs.items()
            if n.startswith(name_starts_with or "")
        ]

    def get_blob_client(self, name):
        container = self

        class _Client:
            def download_blob(self):
                container.downloaded.append(name)

                class _Stream:
                    def readall(self):
                        return container.blobs[name]

                return _Stream()

        return _Client()


class FakeEngine:
    """Answers the known-files query with ``known`` rows (source_path,
    size, modified, sha, status); records other statements."""

    def __init__(self, known_rows=()):
        self.known_rows = list(known_rows)
        self.calls = []

    def connect(self):
        engine = self

        class _Conn:
            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

            def execute(self, stmt, params=None):
                engine.calls.append((" ".join(str(stmt).split()), params))
                return iter(engine.known_rows)

        return _Conn()

    begin = connect


SHA = hashlib.sha256(_bytes()).hexdigest()


def test_sync_reads_only_new_track_files():
    prefix = mfr.SYNC_PREFIX
    folder = f"{prefix}20252026_10_TESTA_2026_02_09_18Z/"
    container = FakeContainer(
        {
            f"{prefix}{FILENAME}": _bytes(),
            f"{folder}{FILENAME}": _bytes(),
            f"{folder}34ktCMRS_SWI$10_20252026_TESTA.json": b"{}",
            f"{folder}SPICYTRACKS_SWI$10_20252026_TESTA.txt": b"x",
            "elsewhere/" + FILENAME: _bytes(),
        }
    )
    known = [
        (
            f"{prefix}{FILENAME}",
            len(_bytes()),
            datetime(2026, 11, 16, 13, 14),
            SHA,
            "written",
        )
    ]
    report = mfr.sync_blob(
        "dev",
        dry_run=True,
        engine=FakeEngine(known),
        container_client=container,
    )
    assert container.downloaded == [f"{folder}{FILENAME}"]
    assert report["listed"] == 2
    assert report["results"][0]["disturbance_id"] == "10-20252026"


@pytest.mark.parametrize(
    "size, modified",
    [(5, datetime(2026, 11, 16, 13, 14)), (None, datetime(2026, 1, 1))],
    ids=["size changed", "rewritten, same size"],
)
def test_sync_rereads_a_path_whose_listing_changed(size, modified):
    path = mfr.SYNC_PREFIX + FILENAME
    size = len(_bytes()) if size is None else size
    container = FakeContainer({path: _bytes()})
    known = [(path, size, modified, "f" * 64, "written")]
    report = mfr.sync_blob(
        "dev",
        dry_run=True,
        engine=FakeEngine(known),
        container_client=container,
    )
    assert container.downloaded == [path]
    assert report["results"][0]["disturbance_id"] == "10-20252026"


def test_sync_same_content_rewritten_is_only_touched(monkeypatch):
    path = mfr.SYNC_PREFIX + FILENAME
    container = FakeContainer({path: _bytes()})
    engine = FakeEngine(
        [(path, len(_bytes()), datetime(2026, 1, 1), SHA, "written")]
    )
    monkeypatch.setattr(
        mfr, "ingest_mfr_bytes", lambda *a, **k: pytest.fail("re-ingested")
    )
    report = mfr.sync_blob("dev", engine=engine, container_client=container)
    assert container.downloaded == [path]
    assert report["counts"] == {"seen_before": 1}
    touched = [
        p
        for s, p in engine.calls
        if s.startswith(
            "UPDATE storms.mfr_files SET last_seen = (now() AT TIME ZONE 'utc'), size_bytes"
        )
    ]
    assert touched[0]["source_modified"] == datetime(2026, 11, 16, 13, 14)


def test_failed_files_are_skipped_unless_retried():
    path = mfr.SYNC_PREFIX + FILENAME
    rows = [
        (path, len(_bytes()), datetime(2026, 11, 16, 13, 14), SHA, "failed")
    ]
    assert path in mfr._known(FakeEngine(rows), "meteofr_blob")
    assert path not in mfr._known(
        FakeEngine(rows), "meteofr_blob", retry_failed=True
    )


def test_backfill_reads_only_track_and_cxml_files():
    assert mfr._is_backfill_file("x/CMRSTRACK_SWI$01_20242025_A_2024.json")
    assert mfr._is_backfill_file("x/F20232024_07_2024022306.1.TESTB.cxml")
    assert not mfr._is_backfill_file("x/34ktCMRS_SWI$01_20242025_A.json")
    assert not mfr._is_backfill_file("x/SPICYTRACKS_SWI$01_20242025_A.json")


class FakeFTP:
    """Only the read commands the poller may use; anything else fails."""

    ALLOWED = {"login", "cwd", "mlsd", "nlst", "retrbinary", "quit", "close"}

    def __init__(self, files, mlsd=True):
        self.files = files  # name -> (bytes, modify)
        self.mlsd_ok = mlsd
        self.commands = []

    def __call__(self, host, timeout=None):
        self.commands.append(("connect", host, timeout))
        return self

    def __getattr__(self, name):
        raise AssertionError(f"poller called FTP.{name}")

    def login(self, user, password):
        self.commands.append(("login",))

    def cwd(self, path):
        self.commands.append(("cwd",))

    def mlsd(self):
        self.commands.append(("mlsd",))
        if not self.mlsd_ok:
            import ftplib

            raise ftplib.error_perm("500 MLSD not understood")
        yield ".", {"type": "cdir"}
        yield "sub", {"type": "dir"}
        for name, (data, modify) in self.files.items():
            yield (
                name,
                {"type": "file", "size": str(len(data)), "modify": modify},
            )

    def nlst(self):
        self.commands.append(("nlst",))
        return list(self.files)

    def retrbinary(self, cmd, callback):
        name = cmd.split(" ", 1)[1]
        self.commands.append(("retr", name))
        callback(self.files[name][0])

    def quit(self):
        self.commands.append(("quit",))

    def close(self):
        self.commands.append(("close",))


@pytest.fixture
def ftp_env(monkeypatch):
    for key, value in {
        "FTP_HOST": "ftp.example",
        "FTP_USER": "u",
        "FTP_PASS": "p",
        "FORECAST_DIR": "/d",
    }.items():
        monkeypatch.setenv(key, value)


def test_files_to_fetch_skips_known_and_refetches_changed():
    listing = {
        "CMRSTRACK_SWI$01_new.json": {
            "size": 10,
            "modified": datetime(2026, 1, 2),
        },
        "CMRSTRACK_SWI$01_old.json": {
            "size": 10,
            "modified": datetime(2026, 1, 1),
        },
        "CMRSTRACK_SWI$01_changed.json": {
            "size": 12,
            "modified": datetime(2026, 1, 1),
        },
        "README.txt": {"size": 1, "modified": None},
        "CMRSTRACK_SWI_dir": {"type": "dir"},
    }
    known = {
        "CMRSTRACK_SWI$01_old.json": {
            "versions": {(10, datetime(2026, 1, 1))},
            "shas": {"a"},
        },
        "CMRSTRACK_SWI$01_changed.json": {
            "versions": {(10, datetime(2026, 1, 1))},
            "shas": {"b"},
        },
    }
    assert mfr.files_to_fetch(listing, known) == [
        "CMRSTRACK_SWI$01_changed.json",
        "CMRSTRACK_SWI$01_new.json",
    ]
    # A new modification time alone (same size) is read again too.
    known["CMRSTRACK_SWI$01_old.json"]["versions"] = {
        (10, datetime(2025, 1, 1))
    }
    assert "CMRSTRACK_SWI$01_old.json" in mfr.files_to_fetch(listing, known)


def test_poll_dry_run_reads_only(ftp_env):
    ftp = FakeFTP(
        {
            FILENAME: (_bytes(), "20260209182400"),
            "CMRSTRACK_SWI$10_bad.json": (b"not json", "20260209182500"),
            "other.txt": (b"x", "20260209182500"),
        }
    )
    report = mfr.poll_ftp("dev", dry_run=True, ftp_factory=ftp)
    retrieved = [c[1] for c in ftp.commands if c[0] == "retr"]
    assert retrieved == [FILENAME, "CMRSTRACK_SWI$10_bad.json"]
    assert ftp.commands[-1] == ("quit",)
    assert report["fetched"] == 2
    assert report["results"][0]["disturbance_id"] == "10-20252026"
    assert report["failed"][0]["filename"] == "CMRSTRACK_SWI$10_bad.json"


def test_poll_falls_back_to_nlst(ftp_env):
    ftp = FakeFTP({FILENAME: (_bytes(), "20260209182400")}, mlsd=False)
    report = mfr.poll_ftp("dev", dry_run=True, ftp_factory=ftp)
    assert ("nlst",) in ftp.commands
    assert report["fetched"] == 1


def test_poll_needs_ftp_settings(monkeypatch):
    for key in ("FTP_HOST", "FTP_USER", "FTP_PASS", "FORECAST_DIR"):
        monkeypatch.delenv(key, raising=False)
    with pytest.raises(RuntimeError, match="FTP_HOST"):
        mfr.poll_ftp("dev", dry_run=True, ftp_factory=FakeFTP({}))


# ---------------------------------------------------------------------------
# CXML (older product)
# ---------------------------------------------------------------------------

CXML = b"""<?xml version="1.0" encoding="UTF-8"?>
<cxml>
 <header>
  <product>Cyclone Forecast</product>
  <productionCenter>METEO-FRANCE</productionCenter>
  <creationTime>2024-02-23T05:49:04Z</creationTime>
 </header>
 <data origin="Officielle" type="forecast">
  <disturbance>
   <cycloneName>TESTB</cycloneName>
   <cycloneNumber>7</cycloneNumber>
   <localID>SWI$07/20232024</localID>
   <basin>SWI</basin>
   <fix hour="0">
    <validTime>2024-02-23T06:00:00Z</validTime>
    <latitude units="deg S">23.5</latitude>
    <longitude units="deg E">58.1</longitude>
    <accuracy units="nm">20</accuracy>
    <cycloneData>
     <development>severe tropical storm</development>
     <systemDepth>deep</systemDepth>
     <minimumPressure><pressure units="hPa">988</pressure></minimumPressure>
     <maximumWind>
      <speed units="kt">50</speed><gusts units="kt">70</gusts>
      <radius units="nm">25</radius>
     </maximumWind>
     <Dvorak><finalTnumber>3.5</finalTnumber><pastChange>W0.5</pastChange></Dvorak>
     <windContours>
      <windSpeed units="kt">50<radius sector="NWQ" units="nm">0</radius><radius sector="NEQ" units="nm">30</radius><radius sector="SEQ" units="nm">35</radius><radius sector="SWQ" units="nm">35</radius></windSpeed>
      <windSpeed units="kt">27<radius sector="NWQ" units="nm">65</radius><radius sector="NEQ" units="nm">110</radius><radius sector="SEQ" units="nm">220</radius><radius sector="SWQ" units="nm">200</radius></windSpeed>
     </windContours>
    </cycloneData>
   </fix>
   <fix hour="12">
    <validTime>2024-02-23T18:00:00Z</validTime>
    <latitude units="deg S">24.1</latitude>
    <longitude units="deg E">57.7</longitude>
    <accuracy units="nm">21</accuracy>
    <cycloneData>
     <maximumWind><speed units="kt">40</speed></maximumWind>
    </cycloneData>
   </fix>
  </disturbance>
 </data>
</cxml>
"""


def test_cxml_parses_into_the_same_rows():
    fc = mfr.parse_mfr_file(CXML, "F20232024_07_2024022306.x.TESTB.cxml")
    assert fc.storm["disturbance_id"] == "07-20232024"
    assert fc.storm["storm_id"] == "testb_si_2024"
    f = fc.forecast
    assert f["issued_time"] == datetime(2024, 2, 23, 6)
    assert f["update_time"] == datetime(2024, 2, 23, 5, 49, 4)
    assert (f["kind"], f["n_analysis"], f["n_forecast"]) == ("forecast", 0, 2)
    t = fc.tracks
    lt0 = t[t.leadtime == 0].iloc[0]
    assert lt0.geometry == "POINT (58.1 -23.5)"
    assert (lt0.wind_speed, lt0.gust_speed, lt0.pressure) == (50, 70, 988)
    assert lt0.max_wind_radius == 25 and lt0.position_accuracy == 20
    assert lt0.dvorak_final_t == 3.5
    assert json.loads(lt0.quadrant_radius_50) == [30, 35, 35, 0]
    assert json.loads(lt0.quadrant_radius_27) == [110, 220, 200, 65]
    raw = json.loads(lt0.raw_fields)
    assert raw["cyclone_data.systemDepth"] == "deep"
    assert raw["cyclone_data.Dvorak_extra.pastChange"] == "W0.5"
    assert t[t.leadtime == 12].iloc[0].wind_speed == 40


def test_cxml_2010_layout_nav_accuracy_and_extra_fix_elements():
    # 2010/11 files: navAccuracy instead of accuracy; an unnamed system is
    # called e.g. 0720232024; stray fix elements are kept raw.
    data = (
        CXML.replace(b"<accuracy units", b"<navAccuracy units")
        .replace(b"</accuracy>", b"</navAccuracy>")
        .replace(b"TESTB", b"0720232024")
        .replace(
            b"<cycloneData>\n     <development>",
            b"<onGoingChange>W</onGoingChange>"
            b"<cycloneData>\n     <development>",
        )
    )
    fc = mfr.parse_mfr_file(data, "F20232024_07_2024022306.x.0720232024.cxml")
    assert fc.storm["name"] is None and fc.storm["storm_id"] is None
    t = fc.tracks
    assert t.position_accuracy.tolist() == [20, 21]
    assert json.loads(t.iloc[0].raw_fields)["fix_extra.onGoingChange"] == "W"
    with pytest.raises(mfr.MfrParseError):
        mfr.parse_mfr_file(
            data.replace(b'navAccuracy units="nm"', b'navAccuracy units="km"'),
            "x.cxml",
        )


@pytest.mark.parametrize(
    "extra",
    [
        b"<cyclone_data.minimum_pressure>29</cyclone_data.minimum_pressure>",
        b"<data_type>analysis</data_type>",
        b"<time>2031-01-01T00:00:00Z</time>",
        b"<position_accuracy>5</position_accuracy>",
    ],
)
def test_cxml_fix_extras_cannot_reach_columns(extra):
    data = CXML.replace(
        b"<cycloneData>\n     <development>",
        extra + b"<cycloneData>\n     <development>",
    )
    assert data != CXML
    try:
        fc = mfr.parse_mfr_file(data, "F20232024_07_2024022306.x.TESTB.cxml")
    except mfr.MfrParseError:
        return
    lt0 = fc.tracks[fc.tracks.leadtime == 0].iloc[0]
    assert fc.forecast["kind"] == "forecast"
    assert lt0.data_type == "forecast"
    assert lt0.valid_time == datetime(2024, 2, 23, 6)
    assert (lt0.pressure, lt0.position_accuracy) == (988, 20)


@pytest.mark.parametrize(
    "extra",
    [
        b"<minimum_pressure>29</minimum_pressure>",
        b"<maximum_wind>99</maximum_wind>",
        b"<wind_contours>zz</wind_contours>",
        b"<a.b>1</a.b>",
    ],
)
def test_cxml_cyclone_data_extra_cannot_reach_a_column(extra):
    data = CXML.replace(
        b"<systemDepth>deep</systemDepth>",
        b"<systemDepth>deep</systemDepth>" + extra,
    )
    with pytest.raises(mfr.MfrParseError):
        mfr.parse_mfr_file(data, "F20232024_07_2024022306.x.TESTB.cxml")


@pytest.mark.parametrize(
    "old_named, new_named", [(False, True), (True, False)]
)
def test_same_time_twins_keep_the_named_version(old_named, new_named):
    up = datetime(2018, 1, 1, 6)
    for a, b in (("a" * 64, "b" * 64), ("b" * 64, "a" * 64)):
        replace, _ = mfr.should_replace(
            "forecast", up, False, up, False, a, b, old_named, new_named
        )
        assert replace is new_named


def test_cxml_partial_mapped_element_is_kept_raw():
    data = CXML.replace(
        b"<Dvorak><finalTnumber>3.5</finalTnumber><pastChange>",
        b"<Dvorak><pastChange>",
    )
    assert data != CXML
    fc = mfr.parse_mfr_file(data, "F20232024_07_2024022306.x.TESTB.cxml")
    lt0 = fc.tracks[fc.tracks.leadtime == 0].iloc[0]
    assert pd.isna(lt0.dvorak_final_t)
    assert (
        json.loads(lt0.raw_fields)["cyclone_data.Dvorak_extra.pastChange"]
        == "W0.5"
    )


def test_cxml_without_hour_0_fails():
    with pytest.raises(mfr.MfrParseError):
        mfr.parse_mfr_cxml(CXML.replace(b'hour="0"', b'hour="6"'), "x.cxml")


def test_cxml_id_from_filename_when_no_local_id():
    data = CXML.replace(b"<localID>SWI$07/20232024</localID>", b"")
    fc = mfr.parse_mfr_cxml(data, "F20232024_07_2024022306.x.TESTB.cxml")
    assert fc.storm["cyclone_id"] == "SWI$07/20232024"


@pytest.mark.parametrize("old_up", [datetime(2026, 2, 9, 18), None])
def test_ties_are_broken_by_content_hash_in_either_order(old_up):
    a, b = "a" * 64, "b" * 64
    first = mfr.should_replace("forecast", old_up, False, old_up, False, a, b)
    second = mfr.should_replace("forecast", old_up, False, old_up, False, b, a)
    assert first[0] is True and second[0] is False


def test_non_finite_and_nul_values_are_json_safe():
    doc = copy.deepcopy(DOC)
    props = doc["features"][3]["properties"]
    props["odd"] = float("nan")
    props["note"] = "a\x00b"
    props["cyclone_data"]["development"] = "trop\x00ical"
    t = _parse(doc).tracks
    row = t[t.leadtime == 12].iloc[0]
    raw = json.loads(row.raw_fields)
    assert raw["odd"] == "nan" and raw["note"] == "ab"
    assert row.development == "tropical"


@pytest.mark.parametrize(
    "mutate",
    [
        lambda d: d["features"][2]["properties"]["cyclone_data"].update(
            wind_contours="oops"
        ),
        lambda d: d["features"][3].update(properties=["not", "a", "dict"]),
    ],
    ids=["contours as text", "properties as list"],
)
def test_malformed_structure_is_a_parse_error(mutate):
    doc = copy.deepcopy(DOC)
    mutate(doc)
    with pytest.raises(mfr.MfrParseError):
        mfr.parse_mfr_file(_bytes(doc), FILENAME)


@pytest.mark.parametrize(
    "old, new",
    [
        (b'<speed units="kt">50', b'<speed units="m/s">50'),
        (b'<radius units="nm">25', b'<radius units="km">25'),
        (b'units="deg S">23.5', b'units="deg South">23.5'),
        (b'units="deg S">23.5', b'units="deg S">-23.5'),
        (b"<cycloneNumber>7<", b"<cycloneNumber>seven<"),
    ],
    ids=[
        "speed m/s",
        "radius km",
        "deg South",
        "negative with deg S",
        "number",
    ],
)
def test_cxml_refuses_unexpected_units_and_signs(old, new):
    data = CXML.replace(old, new)
    if b"seven" in new:
        data = data.replace(b"<localID>SWI$07/20232024</localID>", b"")
    assert data != CXML
    with pytest.raises(mfr.MfrParseError):
        mfr.parse_mfr_file(data, "F20232024_07_2024022306.x.TESTB.cxml")


def test_odd_file_level_values_are_cleaned_or_fail_cleanly():
    doc = copy.deepcopy(DOC)
    doc["cyclone_name"] = "TES\x00TA"
    doc["product"] = {"odd": True}
    doc["features"][3]["properties"]["k\x00ey"] = 1
    fc = _parse(doc)
    assert fc.forecast["name_as_issued"] == "TESTA"
    assert fc.forecast["product"] == "{'odd': True}"
    assert "key" in json.loads(fc.tracks.raw_fields.dropna().iloc[0])
    doc = copy.deepcopy(DOC)
    doc["features"][3]["properties"]["cyclone_data"]["maximum_wind"][
        "wind_speed_kt"
    ] = 10**400
    fc = mfr.parse_mfr_file(_bytes(doc), FILENAME)  # stored as NULL + raw
    assert fc.tracks[fc.tracks.leadtime == 12].wind_speed.isna().all()


def test_sync_download_failure_fails_only_that_file():
    prefix = mfr.SYNC_PREFIX
    good, bad = f"{prefix}{FILENAME}", f"{prefix}a/{FILENAME}"
    container = FakeContainer({good: _bytes(), bad: _bytes()})
    real = container.get_blob_client

    def flaky(name):
        if name == bad:
            raise OSError("blob vanished")
        return real(name)

    container.get_blob_client = flaky
    report = mfr.sync_blob(
        "dev", dry_run=True, engine=FakeEngine(), container_client=container
    )
    assert [r["disturbance_id"] for r in report["results"]] == ["10-20252026"]
    assert len(report["failed"]) == 1
    assert report["failed"][0]["error"].startswith("download failed")


def test_backfill_download_failure_fails_only_that_file(monkeypatch):
    names = ["p/" + FILENAME, "p/b/" + FILENAME]
    monkeypatch.setattr(mfr.stratus, "list_container_blobs", lambda **k: names)

    def load(name, **kwargs):
        if name == names[1]:
            raise OSError("blob vanished")
        return _bytes()

    monkeypatch.setattr(mfr.stratus, "load_blob_data", load)
    report = mfr.backfill_blobs(["p/"], "dev", dry_run=True)
    assert len(report["results"]) == 1 and len(report["failed"]) == 1


def test_ftp_refused_file_fails_alone(monkeypatch):
    monkeypatch.setattr(
        mfr, "_ftp_config",
        lambda: {"host": "h", "user": "u", "password": "p", "directory": "d"},
    )
    other = FILENAME.replace("TESTA", "TESTB")

    class FTP:
        def __init__(self, *a, **k):
            pass

        def login(self, *a):
            pass

        def cwd(self, *a):
            pass

        def mlsd(self, *a, **k):
            for n in (FILENAME, other):
                yield n, {"type": "file", "size": "1", "modify": "20261116131400"}

        def nlst(self, *a):
            return [FILENAME, other]

        def retrbinary(self, cmd, cb):
            if cmd.endswith(other):
                raise mfr.ftplib.error_perm("550 gone")
            cb(_bytes())

        def quit(self):
            pass

    monkeypatch.setattr(mfr, "list_ftp_directory", lambda ftp: {
        FILENAME: {"type": "file", "size": 1, "modified": None},
        other: {"type": "file", "size": 1, "modified": None},
    })
    report = mfr.poll_ftp("dev", dry_run=True, engine=FakeEngine(), ftp_factory=FTP)
    assert len(report["results"]) == 1 and len(report["failed"]) == 1
