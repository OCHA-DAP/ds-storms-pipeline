"""Tests for the South Pacific buffers/exposure helpers (src/pipelines/sp.py).

Pure functions only: no database, no blob. Geometry checks are geodesic
(pyproj), since the point of the module is true ground distances.
"""

import json

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
from pyproj import Geod
from shapely.geometry import Point, box

from src.pipelines import sp

GEOD = Geod(ellps="WGS84")


def _fms_rows(**cols):
    n = len(next(iter(cols.values())))
    base = {
        "wind_speed": [80.0] * n,
        "uncertainty": [50.0] * n,
        **{f"radius_{k}": [np.nan] * n for k in sp.SPEEDS},
        **{f"quadrant_radius_{k}": [None] * n for k in sp.SPEEDS},
    }
    base.update(cols)
    return gpd.GeoDataFrame(base, geometry=[Point(178.0, -18.0)] * n, crs=4326)


def test_fms_quadrant_radii_win_over_symmetric():
    df = _fms_rows(quadrant_radius_34=[json.dumps([100, 90, 80, 70])], radius_34=[60.0])
    out = sp.fms_point_radii(df)
    assert [out[f"q_34_{q}"].iloc[0] for q in sp.QUADS] == [100, 90, 80, 70]


def test_fms_missing_quadrant_falls_back_to_symmetric_then_zero():
    df = _fms_rows(quadrant_radius_34=[json.dumps([100, None, None, None])], radius_34=[60.0])
    out = sp.fms_point_radii(df)
    assert [out[f"q_34_{q}"].iloc[0] for q in sp.QUADS] == [100, 60, 60, 60]
    df = _fms_rows(quadrant_radius_50=[json.dumps([40, None, None, None])])
    out = sp.fms_point_radii(df)
    assert [out[f"q_50_{q}"].iloc[0] for q in sp.QUADS] == [40, 0, 0, 0]


def test_fms_no_radius_at_all_uses_regression_only_if_wind_reaches_it():
    df = _fms_rows(wind_speed=[80.0, 40.0])
    out = sp.fms_point_radii(df)
    # 80 kt reaches 64 kt: regression radius; 40 kt does not: zero.
    assert out["q_64_ne"].iloc[0] > 0
    assert out["q_64_ne"].iloc[1] == 0
    assert out["q_34_ne"].iloc[1] > 0


def _ibtracs(n=4, r34=100.0, r64=30.0, usa_wind=90.0, wmo_wind=70.0, agency="nadi"):
    df = pd.DataFrame({
        "sid": ["S1"] * n,
        "valid_time": pd.date_range("2020-01-01", periods=n, freq="6h"),
        "usa_lat": np.linspace(-15, -18, n), "usa_lon": np.linspace(175, 179, n),
        "lat": np.linspace(-15, -18, n), "lon": np.linspace(175, 179, n),
        "usa_wind": [usa_wind] * n, "wmo_wind": [wmo_wind] * n, "wmo_agency": [agency] * n,
    })
    for k, r in ((34, r34), (50, (r34 + r64) / 2), (64, r64)):
        for q in sp.QUADS:
            df[f"usa_r{k}_{q}"] = r
    return df


def test_history_gate_drops_thresholds_the_official_wind_does_not_reach():
    pts, src = sp.hist_track_points(_ibtracs(wmo_wind=55.0))
    assert src == "jtwc"
    assert (pts["q_34_ne"] == 100).all()
    assert (pts["q_50_ne"] == 65).all()
    assert (pts["q_64_ne"] == 0).all()  # JTWC has 64 kt radii, Nadi says 55 kt


def test_history_gate_falls_back_to_scaled_jtwc_wind():
    # No official wind: 0.90 x 70 kt = 63 kt < 64.
    pts, _ = sp.hist_track_points(_ibtracs(wmo_wind=np.nan, usa_wind=70.0))
    assert (pts["q_64_ne"] == 0).all()
    pts, _ = sp.hist_track_points(_ibtracs(wmo_wind=np.nan, usa_wind=75.0))
    assert (pts["q_64_ne"] == 30).all()


def test_history_without_jtwc_radii_uses_regression():
    df = _ibtracs(r34=np.nan, r64=np.nan, wmo_wind=70.0)
    pts, src = sp.hist_track_points(df)
    assert src == "fji_regression"
    assert (pts["q_64_ne"] > 0).all()
    assert (pts["q_64_ne"] == pts["q_64_sw"]).all()


def test_history_weak_storm_without_radii_is_skipped():
    pts, src = sp.hist_track_points(_ibtracs(r34=np.nan, r64=np.nan, wmo_wind=30.0, usa_wind=30.0))
    assert pts is None and src is None


def test_shift_points_moves_by_the_uncertainty_on_the_ground():
    df = _fms_rows(uncertainty=[100.0])
    moved = sp.shift_points(df, 90)
    _, _, d = GEOD.inv(178.0, -18.0, moved.geometry.x.iloc[0], moved.geometry.y.iloc[0])
    assert d == pytest.approx(100 * 1852, rel=1e-6)
    assert moved.geometry.x.iloc[0] > 178.0  # east


def test_shift_points_across_the_dateline_stays_in_range():
    df = gpd.GeoDataFrame({"uncertainty": [200.0]}, geometry=[Point(179.5, -18.0)], crs=4326)
    moved = sp.shift_points(df, 90)
    assert -180 <= moved.geometry.x.iloc[0] < -170


def test_fcastonly_difference():
    a, b = box(0, 0, 2, 2), box(1, 0, 3, 2)
    assert sp._fcastonly(a, None).equals(a)
    assert sp._fcastonly(a, b).area == pytest.approx(2.0)
    assert sp._fcastonly(a, box(-1, -1, 5, 5)) is None
    assert sp._fcastonly(None, b) is None


def _units(polys, iso3="FJI"):
    return gpd.GeoDataFrame(
        {"iso3": iso3, "pcode": [f"P{i}" for i in range(len(polys))]},
        geometry=polys, crs=4326,
    )


def test_sea_expansion_adds_sea_only_and_keeps_units_apart():
    # Two adjacent land units on a coast at 18°S.
    u = _units([box(178.0, -18.1, 178.1, -18.0), box(178.1, -18.1, 178.2, -18.0)])
    out = sp.expand_units_into_sea(u, metres=500)
    g0, g1 = out.geometry
    assert g0.contains(u.geometry.iloc[0].buffer(-1e-6))
    assert g0.intersection(g1).area < 1e-12  # no overlap
    # The extension reaches ~500 m into the sea, measured on the ground.
    _, _, d = GEOD.inv(178.05, -18.0, 178.05, g0.bounds[3])
    assert d == pytest.approx(500, rel=0.05)


def test_sea_expansion_of_a_dateline_country_is_split_and_complete():
    u = _units([box(179.9, -16.2, 180.0, -16.1), box(-180.0, -16.2, -179.9, -16.1)])
    out = sp.expand_units_into_sea(u, metres=500)
    for g in out.geometry:
        for p in getattr(g, "geoms", [g]):
            minx, _, maxx, _ = p.bounds
            assert -180 <= minx and maxx <= 180 and maxx - minx < 180
    # Each unit gains a sea margin on its outer side.
    assert out.geometry.iloc[0].bounds[0] < 179.9
    assert out.geometry.iloc[1].bounds[2] > -179.9


def test_regression_radius_zero_below_threshold():
    r = sp.regression_radius(64, [50.0, 100.0], [-18.0, -18.0])
    assert r[0] == 0 and r[1] > 0


def test_fms_malformed_quadrant_json_is_treated_as_absent():
    df = _fms_rows(quadrant_radius_34=["null"], radius_34=[60.0])
    out = sp.fms_point_radii(df)
    assert [out[f"q_34_{q}"].iloc[0] for q in sp.QUADS] == [60, 60, 60, 60]
