"""Rebuild realtime-equivalent NHC track rows from NHC's text-product archive.

The realtime ETL (``run_nhc_current``) builds ``storms.nhc_tracks_geo`` from
CurrentStorms.json plus the linked Forecast/Advisory text, via ocha-lens's
``_process_nhc_to_df``. When a realtime run is missed, CurrentStorms.json no
longer carries that advisory, but NHC keeps every text product at

    https://www.nhc.noaa.gov/archive/{year}/{bb}{nn}/{atcf}.{kind}.{NNN}.shtml

- ``fstadv`` — full Forecast/Advisory (03/09/15/21Z): current centre + the
  forecast points + quadrant wind radii. Exactly the text realtime parses.
- ``public_a`` — intermediate public advisory (00/06/12/18Z, only while
  watches/warnings are up): current centre, intensity and pressure only.

Rows are rebuilt by feeding ocha-lens's OWN realtime parser a synthetic
CurrentStorms entry, so parsing, leadtimes and radii are identical to what
realtime would have written:

- full advisory ``N``: entry with ``lastUpdate`` = advisory time and
  ``forecastAdvisory.url`` = the archived fstadv ``N``;
- intermediate ``NA``: entry with ``lastUpdate`` = intermediate time, centre /
  intensity / pressure from public_a ``N``, and ``forecastAdvisory`` = fstadv
  ``N`` (the advisory realtime would have been linking to at that time). Only
  the leadtime-0 row is kept — realtime's re-upsert of advisory ``N``'s
  forecast rows is a no-op.

Differences from realtime that cannot be recovered are documented in the
backfill script (none for the rows themselves; see ``check_equivalence``).
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import timedelta

import pandas as pd
import requests
from ocha_lens.datasources import nhc as lens_nhc

logger = logging.getLogger(__name__)

ARCHIVE_DIR_URL = "https://www.nhc.noaa.gov/archive/{year}/{basin}{number}/"

_MONTHS = {
    m: i
    for i, m in enumerate(
        "JAN FEB MAR APR MAY JUN JUL AUG SEP OCT NOV DEC".split(), 1
    )
}
# Local time zones NHC uses in public advisory headers (UTC offset, hours).
_TZ_OFFSETS = {
    "EDT": -4, "EST": -5, "CDT": -5, "CST": -6, "MDT": -6, "MST": -7,
    "PDT": -7, "PST": -8, "AST": -4, "HDT": -9, "HST": -10, "AKDT": -8,
    "UTC": 0, "GMT": 0,
}


@dataclass(frozen=True)
class Product:
    atcf_id: str
    kind: str  # "fstadv" | "public_a"
    number: str  # e.g. "014"
    url: str


def archive_dir(atcf_id: str) -> str:
    return ARCHIVE_DIR_URL.format(
        year=atcf_id[4:], basin=atcf_id[:2].lower(), number=atcf_id[2:4]
    )


def list_products(atcf_id: str, timeout: int = 30) -> list[Product]:
    """All archived fstadv + public_a products for one storm."""
    base = archive_dir(atcf_id)
    html = requests.get(base, timeout=timeout).text
    pat = rf"{atcf_id.lower()}\.(fstadv|public_a)\.(\d+[A-Za-z]?)\.shtml"
    found = sorted(set(re.findall(pat, html)))
    return [
        Product(atcf_id, kind, num, f"{base}{atcf_id.lower()}.{kind}.{num}.shtml")
        for kind, num in found
    ]


def fetch_text(url: str) -> str:
    txt = lens_nhc._fetch_forecast_advisory(url)
    if not txt:
        raise RuntimeError(f"Could not fetch archived product {url}")
    return txt


def _latlon(lat, ns, lon, ew) -> tuple[float, float]:
    la = float(lat) * (1 if ns.upper() == "N" else -1)
    lo = float(lon) * (1 if ew.upper() == "E" else -1)
    return la, lo


def parse_fstadv(txt: str) -> dict:
    """Header of a Forecast/Advisory: issuance (UTC), centre, intensity (kt),
    pressure (mb)."""
    m = re.search(
        r"^(\d{3,4}) UTC \w{3} (\w{3}) (\d{1,2}) (\d{4})", txt, re.M | re.I
    )
    if not m:
        raise ValueError("fstadv: no 'HHMM UTC DAY MON DD YYYY' header")
    hhmm = m[1].zfill(4)
    issued = pd.Timestamp(
        int(m[4]), _MONTHS[m[2].upper()], int(m[3]),
        int(hhmm[:2]), int(hhmm[2:]), tz="UTC",
    )
    c = re.search(
        r"CENTER LOCATED NEAR\s+([\d.]+)([NS])\s+([\d.]+)([EW])", txt, re.I
    )
    if not c:
        raise ValueError("fstadv: no 'CENTER LOCATED NEAR' line")
    lat, lon = _latlon(*c.groups())
    w = re.search(r"MAX SUSTAINED WINDS\s+(\d+) KT", txt, re.I)
    p = re.search(r"MINIMUM CENTRAL PRESSURE\s+(\d+) MB", txt, re.I)
    return {
        "issued": issued, "lat": lat, "lon": lon,
        "wind_kt": int(w[1]) if w else None,
        "pressure_mb": int(p[1]) if p else None,
        "name": parse_name(txt),
    }


_NAME_RE = re.compile(
    r"^\s*(?:[A-Z-]+\s+)*?(?:HURRICANE|TYPHOON|STORM|DEPRESSION|CYCLONE|"
    r"DISTURBANCE|REMNANTS OF)\s+([A-Z][A-Z-]*)\s+(?:SPECIAL\s+)?"
    r"(?:FORECAST/ADVISORY|INTERMEDIATE ADVISORY)",
    re.I | re.M,
)


def parse_name(txt: str) -> str:
    """Storm name as NHC used it in this product ("Fifteen-E" before naming,
    "Nolo" after) — what CurrentStorms.json carried at the time, and what
    ocha-lens derives ``storm_id`` from."""
    m = _NAME_RE.search(txt)
    if not m:
        raise ValueError("could not find the storm name in the product header")
    return m[1].title()


def _naive_utc(ts) -> pd.Timestamp:
    ts = pd.Timestamp(ts)
    return ts.tz_convert("UTC").tz_localize(None) if ts.tzinfo else ts


def _mph_to_kt(mph: int) -> int:
    """NHC publishes intensity in kt (multiples of 5) and derives the public
    advisory's mph as round(kt * 1.15078 / 5) * 5. Invert that exactly."""
    candidates = range(0, 250, 5)
    return min(candidates, key=lambda k: abs(round(k * 1.15078 / 5) * 5 - mph))


def parse_public_a(txt: str) -> dict:
    """Intermediate public advisory: issuance (UTC), centre, intensity (kt,
    converted back from mph), pressure (mb)."""
    h = re.search(
        r"^(\d{3,4}) (AM|PM) ([A-Z]{2,4}) \w{3} (\w{3}) (\d{1,2}) (\d{4})",
        txt, re.M | re.I,
    )
    if not h:
        raise ValueError("public_a: no local-time header line")
    hhmm = h[1].zfill(4)
    hour = int(hhmm[:2]) % 12 + (12 if h[2].upper() == "PM" else 0)
    tz = h[3].upper()
    if tz not in _TZ_OFFSETS:
        raise ValueError(f"public_a: unknown time zone {tz}")
    local = pd.Timestamp(
        int(h[6]), _MONTHS[h[4].upper()], int(h[5]), hour, int(hhmm[2:])
    )
    issued = (local - timedelta(hours=_TZ_OFFSETS[tz])).tz_localize("UTC")
    # Cross-check against the "...SUMMARY OF ... ...HHMM UTC...INFORMATION" line.
    s = re.search(r"SUMMARY OF .*?(\d{3,4}) UTC", txt, re.I)
    if s and int(s[1].zfill(4)[:2]) != issued.hour:
        raise ValueError(
            f"public_a: header {issued} disagrees with summary {s[1]} UTC"
        )
    c = re.search(r"LOCATION\.+\s*([\d.]+)([NS])\s+([\d.]+)([EW])", txt, re.I)
    if not c:
        raise ValueError("public_a: no LOCATION line")
    lat, lon = _latlon(*c.groups())
    w = re.search(r"MAXIMUM SUSTAINED WINDS\.+\s*(\d+) MPH", txt, re.I)
    p = re.search(r"MINIMUM CENTRAL PRESSURE\.+\s*(\d+) MB", txt, re.I)
    return {
        "issued": issued, "lat": lat, "lon": lon,
        "wind_kt": _mph_to_kt(int(w[1])) if w else None,
        "pressure_mb": int(p[1]) if p else None,
        "name": parse_name(txt),
    }


def _synthetic_entry(atcf_id: str, obs: dict, fstadv_url: str,
                     fstadv_issued: pd.Timestamp) -> dict:
    """A CurrentStorms.json 'activeStorms' entry, shaped like NHC's."""
    return {
        "id": atcf_id.lower(),
        "name": obs["name"],
        "lastUpdate": obs["issued"].isoformat(),
        "intensity": str(obs["wind_kt"]) if obs["wind_kt"] is not None else "0",
        "pressure": str(obs["pressure_mb"]) if obs["pressure_mb"] else None,
        "latitudeNumeric": obs["lat"],
        "longitudeNumeric": obs["lon"],
        "forecastAdvisory": {
            "url": fstadv_url,
            "issuance": fstadv_issued.isoformat(),
        },
    }


def rebuild_product(product: Product,
                    fstadv_by_number: dict[str, Product]) -> pd.DataFrame:
    """Raw rows (ocha-lens ``_process_nhc_to_df`` output) realtime would have
    produced for this product. The storm name comes from the product's own
    header, as CurrentStorms.json carried it at the time (ocha-lens derives
    ``storm_id`` from it, so a pre-naming advisory keeps e.g. fifteen-e)."""
    if product.kind == "fstadv":
        obs = parse_fstadv(fetch_text(product.url))
        entry = _synthetic_entry(product.atcf_id, obs, product.url,
                                 obs["issued"])
        return lens_nhc._process_nhc_to_df({"activeStorms": [entry]})

    # Intermediate "NA": linked Forecast/Advisory is full advisory N.
    base_num = re.match(r"\d+", product.number)[0]
    parent = fstadv_by_number.get(base_num)
    if parent is None:
        raise ValueError(
            f"{product.url}: no archived fstadv {base_num} to take radii from"
        )
    obs = parse_public_a(fetch_text(product.url))
    parent_issued = parse_fstadv(fetch_text(parent.url))["issued"]
    entry = _synthetic_entry(product.atcf_id, obs, parent.url, parent_issued)
    df = lens_nhc._process_nhc_to_df({"activeStorms": [entry]})
    # Keep only the intermediate's own leadtime-0 row (realtime's re-upsert of
    # the parent advisory's forecast rows is a no-op).
    it = df["issued_time"].map(_naive_utc)
    return df[(it == _naive_utc(obs["issued"])) & (df["leadtime"] == 0)]


def product_issued_time(product: Product) -> pd.Timestamp:
    txt = fetch_text(product.url)
    parsed = parse_fstadv(txt) if product.kind == "fstadv" else parse_public_a(txt)
    return _naive_utc(parsed["issued"])
