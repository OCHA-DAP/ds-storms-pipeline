"""Modelled wind radii for Réunion track points without radii.

Réunion's best track has no radii before 2003/04, its 48 kt radius from
2003/04 and its 34 and 64 kt radii from 2010/11; later tracks and forecasts
have gaps (mostly weak, far-south or overland points). Where Réunion's
10-minute wind reaches k kt but it gives no k-kt radius, the mean of the
four quadrant radii (nm) is modelled:

  C  log R = b0 + b1 log V + b2 log(V - k + 2) + b3 log|lat|
             + b4 madagascar + b5 west + b6 log(1010 - MSLP)
  B  the same without MSLP (where it is missing)
  E  C + b7 log R48, for 34 and 64 kt at points of 48 kt or more where
     Réunion's own 48 kt radius exists (2003/04 to 2009/10, IBTrACS-only
     storms); E was fitted on such points only

The pressure term uses Réunion's pressure as it reports it now: its pressure
at a given wind was lower before 2010/11 (1010 - MSLP about 15% larger at
the same wind and latitude), so each point's log(1010 - MSLP) is first moved
by its era's offset (DP_ERA_SHIFT, fitted on the best track). Without that,
the pressure term made pre-2010 radii about 15-20% larger than the same
model without pressure.

``madagascar``: 43-51°E, 11.5-26°S; ``west``: west of 43°E (Mozambique
Channel and the mainland). Without them the model made radii about 25%
too big near Madagascar and 17% in the Channel (and slightly too small in
the open ocean), which is where the exposure is.

The mean is spread over the quadrants with each speed's median quadrant /
mean ratio (the south-east quadrant is the largest). Then radii are nested
(34 >= 48 >= 64 in every quadrant, moving only modelled values).

Fitted by OLS on Réunion's best track 1985/86-2022/23 (points at or above k
kt with a k-kt radius), with indicators for 2003/04-2009/10 and
2010/11-2015/16 that are dropped when predicting (see _ols_without_era);
refit with scripts/fit_swio_radii.py. Checked
season by season (each season predicted by a model fitted without it) on
exposure, 2010/11-2022/23: see the PR and README for the figures.
"""

import numpy as np
import pandas as pd

QUADS = ["ne", "se", "sw", "nw"]
SPEEDS = [34, 48, 64]
FIRST_RADII_SEASON = {34: 2010, 48: 2003, 64: 2010}
# Reference era (current practice) starts here; earlier eras are offsets.
ERAS = ((1900, 2002), (2003, 2009), (2010, 2015))
REFERENCE_FIRST_SEASON = 2016

# Era offsets of log(1010 - MSLP) against current practice, for ERAS.
DP_ERA_SHIFT = np.array([0.178731, 0.111909, 0.105597])
# {(speed, model): coefficients in _design order}, from
# scripts/fit_swio_radii.py on the best track 1985/86-2022/23, at today's
# practice (see _ols_without_era and _log_dp)
# (34 kt: 2,585 points 2010/11-2022/23; 48 kt: 2,108 points 2003/04-2022/23;
# 64 kt: 785 points 2010/11-2022/23).
COEF = {
    (34, "B"): np.array([3.931754, -1.008973, 0.686262, 0.816491, -0.228144, -0.260643]),
    (34, "C"): np.array([7.060258, -2.372934, 0.602417, 0.551576, -0.296443, -0.219370, 1.023428]),
    (34, "E"): np.array([-2.320709, 1.575562, -1.604389, 0.251334, -0.107786, -0.102172, 0.762488, 0.635847]),
    (48, "B"): np.array([3.099385, -0.832194, 0.545220, 0.863156, -0.192676, -0.295143]),
    (48, "C"): np.array([5.590283, -1.948241, 0.541277, 0.728308, -0.165238, -0.255162, 0.726343]),
    (64, "B"): np.array([4.361818, -0.942638, 0.416323, 0.691346, -0.154135, -0.235379]),
    (64, "C"): np.array([6.475586, -1.928262, 0.399629, 0.519119, -0.173464, -0.239580, 0.713398]),
    (64, "E"): np.array([0.153217, 0.016156, 0.282664, -0.001096, 0.042359, -0.025569, -0.481908, 1.103078]),
}
# Median quadrant / mean radius (NE, SE, SW, NW), normalised to mean 1.
SHARES = {
    34: np.array([0.9606, 1.1874, 1.0702, 0.7818]),
    48: np.array([1.0007, 1.0586, 1.0189, 0.9217]),
    64: np.array([1.0000, 1.0000, 1.0000, 1.0000]),
}


def _era_columns(season: np.ndarray) -> np.ndarray:
    return np.column_stack([((season >= a) & (season <= b)).astype(float) for a, b in ERAS])


def _log_dp(d: pd.DataFrame, dp_shift=None) -> np.ndarray:
    """log(1010 - MSLP), moved to current practice by each point's era offset
    (points without a season, e.g. live forecasts, are current)."""
    shift = DP_ERA_SHIFT if dp_shift is None else dp_shift
    out = np.log(np.clip(1010 - d["mslp"].to_numpy(float), 2, None))
    if "season" in d.columns:
        season = d["season"].to_numpy(float)
        out = out - _era_columns(season) @ np.asarray(shift, float)
    return out


def _design(d: pd.DataFrame, k: int, model: str, r48=None, dp_shift=None) -> np.ndarray:
    V = d["wind"].to_numpy(float)
    lon, lat = d["lon"].to_numpy(float), d["lat"].to_numpy(float)
    madagascar = (lon >= 43) & (lon <= 51) & (lat <= -11.5) & (lat >= -26)
    west = lon < 43
    cols = [
        np.ones(len(d)), np.log(V), np.log(np.clip(V - k + 2, 1, None)), np.log(np.abs(lat)),
        madagascar.astype(float), west.astype(float),
    ]
    if model in ("C", "E"):
        cols.append(_log_dp(d, dp_shift))
    if model == "E":
        cols.append(np.log(np.clip(np.asarray(r48, float), 5, None)))
    return np.column_stack(cols)


def mean_radius(df: pd.DataFrame, k: int) -> np.ndarray:
    """Mean of the four quadrant radii r{k}_{quad} (missing quadrants 0);
    NaN where none is given."""
    r = df[[f"r{k}_{q}" for q in QUADS]].to_numpy(float)
    has = np.isfinite(r).any(axis=1)
    with np.errstate(invalid="ignore"):
        return np.where(has, np.nansum(np.where(np.isfinite(r), r, 0.0), axis=1) / 4, np.nan)


def _ols_without_era(X: np.ndarray, y: np.ndarray, season: np.ndarray) -> np.ndarray:
    """OLS with era indicators (ERAS), returned without them.

    At the same wind, Réunion drew its 48 kt radii smaller in 2003/04-2009/10
    and its 48/64 kt radii about 20% larger in 2010/11-2015/16 than since
    (no clear difference at 34 kt); 2016/17-2022/23 and the 2023-2026 final
    tracks agree. Predicting with the indicators at 0 gives radii as Réunion
    draws them now, which is what the live forecasts are compared with. An
    era with no training points (34/64 kt start in 2010/11) is left out.
    """
    eras = _era_columns(season)
    eras = eras[:, eras.any(axis=0)]
    b = np.linalg.lstsq(np.column_stack([X, eras]), y, rcond=None)[0]
    return b[:X.shape[1]]


def fit_dp_shift(df: pd.DataFrame) -> np.ndarray:
    """Era offsets of log(1010 - MSLP) at the same wind and latitude, against
    current practice (points of 34 kt or more with a pressure)."""
    ok = (df["wind"].to_numpy(float) >= 34) & df["mslp"].notna().to_numpy()
    d = df[ok]
    V = d["wind"].to_numpy(float)
    X = np.column_stack([np.ones(len(d)), np.log(V), np.log(np.abs(d["lat"].to_numpy(float))),
                         _era_columns(d["season"].to_numpy(float))])
    y = np.log(np.clip(1010 - d["mslp"].to_numpy(float), 2, None))
    return np.linalg.lstsq(X, y, rcond=None)[0][3:]


def fit(df: pd.DataFrame) -> tuple[dict, dict, np.ndarray]:
    """(COEF, SHARES, DP_ERA_SHIFT) from best-track points: wind, lat, lon,
    mslp, season, r{k}_{quad}."""
    coef, shares = {}, {}
    dp_shift = fit_dp_shift(df)
    for k in SPEEDS:
        R = mean_radius(df, k)
        ok = (df["season"].to_numpy() >= FIRST_RADII_SEASON[k]) & (df["wind"].to_numpy(float) >= k) & (R > 0)
        d, y = df[ok], np.log(R[ok])
        season = d["season"].to_numpy()
        hasdp = d["mslp"].notna().to_numpy()
        for model, rows in (("C", hasdp), ("B", np.ones(len(d), bool))):
            coef[(k, model)] = _ols_without_era(
                _design(d[rows], k, model, dp_shift=dp_shift), y[rows], season[rows])
        if k != 48:
            R48 = mean_radius(d, 48)
            rows = hasdp & (R48 > 0) & (d["wind"].to_numpy(float) >= 48)
            coef[(k, "E")] = _ols_without_era(
                _design(d[rows], k, "E", r48=R48[rows], dp_shift=dp_shift), y[rows], season[rows])
        q = d[[f"r{k}_{x}" for x in QUADS]].fillna(0).to_numpy(float) / R[ok][:, None]
        s = np.median(q, axis=0)
        shares[k] = s / s.mean()
    return coef, shares, dp_shift


def modelled_mean(d: pd.DataFrame, k: int, r48=None, coef=None, dp_shift=None) -> np.ndarray:
    """Modelled mean radius (nm): E where ``r48`` > 0 and the wind is 48 kt or
    more (34/64 kt; E was fitted only there), else C, else B."""
    coef = COEF if coef is None else coef
    out = np.full(len(d), np.nan)
    hasdp = d["mslp"].notna().to_numpy()
    use_e = np.zeros(len(d), bool)
    if r48 is not None and k != 48:
        use_e = ((np.nan_to_num(np.asarray(r48, float)) > 0) & hasdp
                 & (d["wind"].to_numpy(float) >= 48))
    for model, rows in (("E", use_e), ("C", hasdp & ~use_e), ("B", ~hasdp)):
        if rows.any():
            X = _design(d[rows], k, model, r48=np.asarray(r48, float)[rows] if model == "E" else None,
                        dp_shift=dp_shift)
            out[rows] = np.exp(X @ coef[(k, model)])
    return out


def quadrant_radii(df: pd.DataFrame, coef=None, shares=None, dp_shift=None) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Quadrant radii q_{k}_{quad} (nm) and modelled_{k} flags for track points.

    ``df``: wind (10-minute, kt), lat, lon, mslp and Réunion's radii
    r{k}_{quad} (NaN at every quadrant of a speed it gives no radius for).
    Réunion's radii are kept wherever it gives any at a speed (a missing
    quadrant is 0), whatever its wind (0.3% of forecast points in the
    2010-2024 archive give a 34 kt radius with a weaker maximum wind; none
    in the current product). Points at or above k kt without them are
    modelled; elsewhere the radius is 0.
    """
    shares = SHARES if shares is None else shares
    wind = df["wind"].to_numpy(float)
    out, modelled = pd.DataFrame(index=df.index), pd.DataFrame(index=df.index)
    r48_obs = mean_radius(df, 48)
    r48_obs = np.where(r48_obs > 0, r48_obs, np.nan)
    for k in SPEEDS:
        obs = df[[f"r{k}_{q}" for q in QUADS]].to_numpy(float)
        has_obs = np.isfinite(obs).any(axis=1)
        need = np.isfinite(wind) & (wind >= k) & ~has_obs
        vals = np.nan_to_num(obs, nan=0.0)
        if need.any():
            m = modelled_mean(df[need], k, r48=r48_obs[need] if k != 48 else None, coef=coef,
                              dp_shift=dp_shift)
            vals[need] = m[:, None] * np.asarray(shares[k])[None, :]
        for i, q in enumerate(QUADS):
            out[f"q_{k}_{q}"] = vals[:, i]
        modelled[f"modelled_{k}"] = need
    # Nesting, 34 >= 48 >= 64 in every quadrant: only modelled values move.
    m34, m48, m64 = (modelled[f"modelled_{k}"].to_numpy() for k in SPEEDS)
    for q in QUADS:
        r34, r48, r64 = (out[f"q_{k}_{q}"].to_numpy() for k in SPEEDS)
        r48 = np.where(m48 & ~m34, np.minimum(r48, r34), r48)
        r48 = np.where(m48 & ~m64, np.maximum(r48, r64), r48)
        r34 = np.where(m34, np.maximum(r34, r48), r34)
        r64 = np.where(m64, np.minimum(r64, r48), r64)
        out[f"q_34_{q}"], out[f"q_48_{q}"], out[f"q_64_{q}"] = r34, r48, r64
    return out, modelled
