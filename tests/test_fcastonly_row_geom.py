"""Regression test for NULL-geometry handling in the fcastonly buffers step.

The bug this guards against: ``nhc_tracks_fcast_buffers`` stores empty
buffers as NULL geometry (``_to_multipolygon`` returns None for a wind band
with no radii). When such a row also had an observed buffer at the same key,
``process_nhc_tracks_fcastonly_buffers`` called
``fcast_geom.difference(obsv_geom)`` with ``fcast_geom = None``
(``shapely.wkt.loads(None)`` passes None through) and crashed with
AttributeError — killing the tracks_processing task, and with it exposure
and WSP matching, on every run from 2026-08-24 (Lala/CP012026) onward.
"""

import math

import pytest
from shapely import wkt

from src.pipelines.nhc import _fcastonly_row_geom

FCAST = "POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0))"
OBSV = "POLYGON ((0 0, 2 0, 2 4, 0 4, 0 0))"


def test_null_fcast_with_obsv_returns_none():
    # The crashing case: no forecast geometry, observed present.
    assert _fcastonly_row_geom(None, OBSV) is None


@pytest.mark.parametrize("null_fcast", [None, float("nan")])
def test_null_fcast_without_obsv_returns_none(null_fcast):
    assert _fcastonly_row_geom(null_fcast, None) is None


def test_null_obsv_passes_full_forecast_through():
    result = _fcastonly_row_geom(FCAST, None)
    assert result.equals(wkt.loads(FCAST))


def test_difference_of_valid_geoms():
    result = _fcastonly_row_geom(FCAST, OBSV)
    assert math.isclose(result.area, 8.0)
    assert result.equals(wkt.loads("POLYGON ((2 0, 4 0, 4 4, 2 4, 2 0))"))


def test_empty_difference_returns_none():
    # Observed fully covers the forecast — empty diff is stored as NULL.
    assert _fcastonly_row_geom(OBSV, FCAST) is None
