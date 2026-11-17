"""Tests for the SWIO swath / exposure pipeline (synthetic data only: the repo
is public and Météo-France's files are not)."""

import json

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest

from src.pipelines import swio
from src.pipelines.swio_radii import QUADS, SPEEDS, mean_radius, modelled_mean, quadrant_radii

HEADER = (
    'Name "Depr Num" Day Month Year UTC Lat Lon "Accu (mn)" "Max wind (kt)" "Gust (kt)" '
    '"RMW (mn)" "MSLP (hPa)" FT Ci '
    + " ".join(f"{k}kt_{q}_mn" for k in (28, 34, 48, 64) for q in ("NE", "SE", "SW", "NW"))
)


def _bt_line(name, num, day, month, year, hour, lat, lon, wind, mslp, r34=None, r48=None):
    radii = ["null"] * 16
    for k, vals in ((34, r34), (48, r48)):
        if vals is not None:
            i = (1 if k == 34 else 2) * 4
            radii[i:i + 4] = [str(v) for v in vals]
    return " ".join(map(str, [
        name, num, day, month, year, hour, lat, lon, "null", wind, "null", "null", mslp, "null", "null", *radii,
    ]))


def _besttrack_text():
    lines = [
        HEADER,
        # A system starting in late June belongs to the season before.
        _bt_line("AAA", 12, 28, 6, 2011, 0, "15,50", 60.0, 30, 1000),
        _bt_line("AAA", 12, 28, 6, 2011, 6, 16.0, 59.0, 40, 995, r34=(50, 60, 40, 30)),
        # Season 2011/12, number 1, unnamed (number + season as name).
        _bt_line("0120112012", 1, 30, 12, 2011, 18, "20,25", 45.5, 35, 1000),
        _bt_line("0120112012", 1, 31, 12, 2011, 0, 21.0, 45.0, "null", "null"),
        _bt_line("EX-BBB", 2, 2, 1, 2012, 0, 22.0, 40.0, 50, 985, r48=(20, 25, 20, 15)),
    ]
    return ("\n".join(lines) + "\n").encode()


def test_read_besttrack():
    df = swio.read_besttrack(_besttrack_text())
    assert set(df.disturbance_id) == {"12-20102011", "01-20112012", "02-20112012"}
    a = df[df.disturbance_id == "12-20102011"]
    assert (a.season == 2010).all() and (a.storm_id == "aaa_si_2011").all()
    assert a.lat.iloc[0] == pytest.approx(-15.5)  # degrees south, decimal comma
    assert a.r34_se.iloc[1] == 60 and np.isnan(a.r34_se.iloc[0])
    u = df[df.disturbance_id == "01-20112012"]
    assert u.storm_id.isna().all() and np.isnan(u.wind.iloc[1])
    b = df[df.disturbance_id == "02-20112012"]
    assert (b.storm_id == "bbb_si_2012").all()  # EX- prefix dropped
    assert b.r48_ne.iloc[0] == 20


def test_read_besttrack_rejects_malformed_line():
    bad = _besttrack_text() + b"CCC 3 1 1 2012 0 20.0\n"
    with pytest.raises(ValueError, match="malformed"):
        swio.read_besttrack(bad)


def test_season_of_disturbance():
    assert swio.season_of_disturbance("10-20252026") == 2025
    with pytest.raises(ValueError):
        swio.season_of_disturbance("10-20252027")
    with pytest.raises(ValueError):
        swio.season_of_disturbance("2025056S16055")


def test_radii_columns_json_and_postgres_arrays():
    df = pd.DataFrame({
        "a": [json.dumps([50, None, 30, None]), None, json.dumps([None] * 4), "{75.0,NaN,60.0,NaN}"],
    })
    out = swio.radii_columns(df, {34: "a"})
    # some sectors given: the omitted ones are 0
    assert out.loc[0, [f"r34_{q}" for q in QUADS]].tolist() == [50, 0, 30, 0]
    # nothing given: NaN everywhere (no radius at that speed)
    assert out.loc[1, [f"r34_{q}" for q in QUADS]].isna().all()
    assert out.loc[2, [f"r34_{q}" for q in QUADS]].isna().all()
    assert out.loc[3, [f"r34_{q}" for q in QUADS]].tolist() == [75, 0, 60, 0]


def test_mfr_radii_reads_cxml_50_as_48():
    df = pd.DataFrame({
        "quadrant_radius_34": [json.dumps([90, 90, 90, 90])] * 2,
        "quadrant_radius_48": [json.dumps([40, 40, 40, 40]), None],
        "quadrant_radius_50": [None, json.dumps([35, 35, 35, 35])],
        "quadrant_radius_64": [None, None],
    })
    out = swio._mfr_radii(df)
    assert out.r48_ne.tolist() == [40, 35]
    assert out.r64_ne.isna().all()


def test_latest_per_time():
    t0, t6 = pd.Timestamp("2026-01-01 00:00"), pd.Timestamp("2026-01-01 06:00")
    df = pd.DataFrame({
        "valid_time": [t0, t0, t0, t6],
        "issued_time": [t0, t6, t6, t6],
        "data_type": ["forecast", "analysis", "forecast", "forecast"],
        "wind": [40, 45, 99, 50],
    })
    out = swio.latest_per_time(df)
    # t0: the later forecast's analysis wins over its own leadtime-0 point
    assert out.wind.tolist() == [45, 50]


def _pts(**cols):
    n = len(next(iter(cols.values())))
    base = {"wind": [60.0] * n, "lat": [-18.0] * n, "lon": [55.0] * n, "mslp": [975.0] * n}
    base.update(cols)
    df = pd.DataFrame(base)
    for k in SPEEDS:
        for q in QUADS:
            if f"r{k}_{q}" not in df:
                df[f"r{k}_{q}"] = np.nan
    return df


def test_quadrant_radii_gate_keep_model_and_nest():
    df = _pts(wind=[30.0, 40.0, 60.0, 70.0])
    for q, v in zip(QUADS, (80, 100, 90, 60)):
        df.loc[2, f"r34_{q}"] = v  # Réunion's 34 kt radii at point 2 only
    q, m = quadrant_radii(df)
    # below 34 kt: nothing
    assert (q.loc[0] == 0).all() and not m.loc[0].any()
    # point 1 (40 kt): 34 kt modelled, 48/64 zero
    assert m.loc[1, "modelled_34"] and q.loc[1, "q_34_ne"] > 0
    assert q.loc[1, [f"q_48_{x}" for x in QUADS]].eq(0).all()
    # point 2: Réunion's 34 kt kept exactly, 48 modelled inside it
    assert q.loc[2, [f"q_34_{x}" for x in QUADS]].tolist() == [80, 100, 90, 60]
    assert not m.loc[2, "modelled_34"] and m.loc[2, "modelled_48"]
    for x in QUADS:
        assert q.loc[2, f"q_48_{x}"] <= q.loc[2, f"q_34_{x}"]
    # point 3: 64 kt modelled, nested everywhere
    for x in QUADS:
        assert q.loc[3, f"q_34_{x}"] >= q.loc[3, f"q_48_{x}"] >= q.loc[3, f"q_64_{x}"] > 0


def test_quadrant_radii_unknown_wind_is_zero():
    q, m = quadrant_radii(_pts(wind=[np.nan]))
    assert (q.loc[0] == 0).all() and not m.loc[0].any()


def test_reunion_radii_kept_whatever_its_wind():
    df = _pts(wind=[30.0, np.nan])
    df["r34_se"] = 40.0
    q, m = quadrant_radii(df)
    assert q.q_34_se.tolist() == [40.0, 40.0] and not m.modelled_34.any()
    assert (q[[f"q_48_{x}" for x in QUADS]] == 0).all().all()


def test_reunion_zero_radii_stay_zero():
    df = _pts(wind=[50.0])
    for q in QUADS:
        df[f"r34_{q}"] = 0.0
    q, m = quadrant_radii(df)
    assert not m.loc[0, "modelled_34"]
    assert q.loc[0, [f"q_34_{x}" for x in QUADS]].eq(0).all()


def test_modelled_radius_grows_with_wind_and_uses_observed_48():
    d = _pts(wind=[40.0, 80.0], mslp=[995.0, 960.0])
    m34 = modelled_mean(d, 34)
    assert 10 < m34[0] < m34[1] < 300
    # the 48 kt anchor: a bigger observed 48 kt radius gives a bigger 34 kt one
    small = modelled_mean(d.iloc[[1]], 34, r48=np.array([20.0]))
    big = modelled_mean(d.iloc[[1]], 34, r48=np.array([80.0]))
    assert small[0] < big[0]
    # no pressure: the fallback model
    assert np.isfinite(modelled_mean(_pts(mslp=[np.nan]), 48)).all()


def test_regional_terms_shrink_radii_near_madagascar():
    open_ocean = modelled_mean(_pts(lon=[60.0], lat=[-18.0]), 34)
    madagascar = modelled_mean(_pts(lon=[47.0], lat=[-18.0]), 34)
    assert madagascar[0] < open_ocean[0]


def test_mean_radius_counts_missing_quadrants_as_zero():
    df = _pts(wind=[50.0])
    df["r34_ne"], df["r34_se"] = 40.0, 80.0
    assert mean_radius(df, 34)[0] == pytest.approx(30.0)
    assert np.isnan(mean_radius(_pts(wind=[50.0]), 34)[0])


def _track(radius_nm=60.0):
    times = pd.date_range("2026-01-01", periods=3, freq="6h")
    df = pd.DataFrame({"valid_time": times, "wind": [70.0] * 3})
    for k in SPEEDS:
        for q in QUADS:
            df[f"q_{k}_{q}"] = radius_nm * {34: 1.0, 48: 0.6, 64: 0.3}[k]
    return gpd.GeoDataFrame(df, geometry=gpd.points_from_xy([55.0, 56.0, 57.0], [-20.0] * 3), crs=4326)


def test_swaths_are_geodesic_and_nested():
    sw = swio.swaths(_track())
    assert sw.wind_speed_kt.tolist() == SPEEDS
    g = dict(zip(sw.wind_speed_kt, sw.geometry))
    assert g[34].contains(g[48]) and g[48].contains(g[64])
    # 60 nm = 111 km north and south of the track: about 2 degrees of latitude
    _, miny, _, maxy = g[34].bounds
    assert maxy - miny == pytest.approx(2 * 111.12 / 110.6, rel=0.03)


def test_swaths_reject_a_runaway_geometry(monkeypatch):
    monkeypatch.setattr(swio, "MAX_SWATH_LON_SPAN", 1.0)
    with pytest.raises(ValueError, match="swath spans"):
        swio.swaths(_track())


def test_shift_points_moves_by_position_accuracy():
    p = _track()
    p["position_accuracy"] = [60.0, 0.0, np.nan]
    s = swio.shift_points(p, 0.0)  # north
    assert s.geometry.y.iloc[0] == pytest.approx(-20.0 + 111.12 / 110.6, rel=0.01)
    assert s.geometry.y.iloc[1] == pytest.approx(-20.0)
    assert s.geometry.y.iloc[2] == pytest.approx(-20.0)


# --- review fixes -----------------------------------------------------------


def test_model_e_only_at_48_kt_or_more():
    weak = _pts(wind=[35.0])
    # a 48 kt radius on a 35 kt point (inconsistent input) must not switch to E
    assert modelled_mean(weak, 34, r48=np.array([20.0]))[0] == pytest.approx(modelled_mean(weak, 34)[0])
    strong = _pts(wind=[70.0])
    assert modelled_mean(strong, 34, r48=np.array([20.0]))[0] != pytest.approx(modelled_mean(strong, 34)[0])


def test_older_pressure_is_moved_to_current_practice():
    now = _pts(wind=[70.0], mslp=[960.0])
    old = now.assign(season=[1995])
    current = now.assign(season=[2020])
    assert modelled_mean(current, 34)[0] == pytest.approx(modelled_mean(now, 34)[0])
    # the same pressure meant a weaker storm-size signal before 2010/11
    assert modelled_mean(old, 34)[0] < modelled_mean(now, 34)[0]


def test_latest_per_time_takes_radii_from_an_earlier_point():
    t0 = pd.Timestamp("2026-01-01 00:00")
    df = pd.DataFrame({
        "valid_time": [t0, t0],
        "issued_time": [t0, t0 + pd.Timedelta(hours=6)],
        "data_type": ["analysis", "analysis"],
        "wind": [50.0, 55.0],
        "quadrant_radius_34": [json.dumps([60, 60, 60, 60]), None],
        "quadrant_radius_48": [None, None],
        "quadrant_radius_50": [None, None],
        "quadrant_radius_64": [None, None],
    })
    out = swio.latest_per_time(df)
    assert len(out) == 1 and out.wind.iloc[0] == 55.0  # the latest analysis
    assert json.loads(out.quadrant_radius_34.iloc[0]) == [60, 60, 60, 60]


def _hist(times, winds, lon=55.0):
    df = pd.DataFrame({"valid_time": pd.to_datetime(times), "wind": winds, "lon": lon, "lat": -20.0,
                       "mslp": 990.0, "season": 2024})
    for k in SPEEDS:
        for q in QUADS:
            df[f"r{k}_{q}"] = np.nan
    return df


def test_combine_with_ibtracs_tops_up_a_truncated_track():
    mf = _hist(["2025-02-16 00:00", "2025-02-16 06:00"], [60, 55])
    ib = _hist(["2025-02-16 00:00", "2025-02-16 06:00", "2025-02-17 00:00", "2025-02-18 00:00"],
               [60, 55, 50, 40])
    pts, src = swio.combine_with_ibtracs(mf, ib)
    assert src == "mfr_forecasts_ibtracs"
    assert pts.valid_time.tolist() == list(pd.to_datetime(
        ["2025-02-16 00:00", "2025-02-16 06:00", "2025-02-17 00:00", "2025-02-18 00:00"]))


def test_combine_with_ibtracs_keeps_a_complete_track_and_replaces_a_weak_one():
    mf = _hist(["2025-02-16 00:00", "2025-02-16 06:00"], [60, 55])
    ib = _hist(["2025-02-16 00:00", "2025-02-16 12:00"], [60, 40])
    assert swio.combine_with_ibtracs(mf, ib)[1] == "mfr_forecasts"
    weak = _hist(["2025-02-16 00:00"], [30])
    assert swio.combine_with_ibtracs(weak, ib)[1] == "ibtracs"


def test_same_system_by_place_and_time():
    a = _hist(["2025-02-16 00:00"], [40], lon=55.0)
    near = _hist(["2025-02-16 00:00"], [40], lon=56.0)
    far = _hist(["2025-02-16 00:00"], [40], lon=70.0)
    later = _hist(["2025-02-20 00:00"], [40], lon=55.0)
    assert swio.same_system(a, [far, near]) and not swio.same_system(a, [far, later])


def test_read_besttrack_refuses_merged_systems():
    text = (HEADER + "\n"
            + _bt_line("AAA", 3, 1, 1, 2012, 0, 20.0, 50.0, 40, 990) + "\n"
            + _bt_line("AAA", 3, 10, 1, 2012, 0, 21.0, 51.0, 40, 990) + "\n").encode()
    with pytest.raises(ValueError, match="gaps"):
        swio.read_besttrack(text)


def test_cli_timestamps_are_utc():
    assert swio._ts("2026-02-01T22:00+04:00") == pd.Timestamp("2026-02-01 18:00")
    assert swio._ts("2026-02-01T22") == pd.Timestamp("2026-02-01 22:00")
    assert swio._ts(None) is None


def test_realtime_one_failure_does_not_stop_the_rest(monkeypatch):
    todo = pd.DataFrame({"disturbance_id": ["01-20262027", "02-20262027"],
                         "issued_time": pd.to_datetime(["2026-11-01", "2026-11-02"])})
    seen = []

    def process(engine, ctx, did, it, scenarios=True):
        seen.append(did)
        if did == "01-20262027":
            raise ValueError("bad forecast")

    monkeypatch.setattr(swio.stratus, "get_engine", lambda **k: object())
    monkeypatch.setattr(swio, "pending_forecasts", lambda *a, **k: todo)
    monkeypatch.setattr(swio, "ExposureContext", lambda *a, **k: None)
    monkeypatch.setattr(swio, "process_mfr_forecast", process)
    with pytest.raises(RuntimeError, match="1 of 2 forecasts failed"):
        swio.run_swio_realtime("dev")
    assert seen == ["01-20262027", "02-20262027"]


def test_pending_forecasts_also_takes_late_arrivals():
    class Eng:
        def connect(self):
            return self

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    captured = {}

    def fake_read(engine, sql, params):
        captured["sql"], captured["params"] = " ".join(sql.split()), params
        return pd.DataFrame(columns=["disturbance_id", "issued_time"])

    import src.pipelines.swio as m
    orig = m._read
    m._read = fake_read
    try:
        t = pd.Timestamp("2026-11-10")
        swio.pending_forecasts(Eng(), since=t, ingested_since=t, oldest=t - pd.Timedelta(days=25))
    finally:
        m._read = orig
    assert "f.ingested_at >= :ingested_since AND f.issued_time >= :oldest" in captured["sql"]
