"""Backfill NHC advisories a realtime outage missed, from NHC's archives.

Written for the 2026-09-24 prod outage (the ``dsci`` DB host secret pointed at
the wrong server from ~03:30 to 18:30 UTC, so every NHC Pipeline run failed).
Lost: all stages for the 03/09/15Z full advisories of AL062026, EP152026,
EP162026, EP172026 and the 00/06/12/18Z intermediate advisories of EP152026 and
EP172026 (20 products), plus WSP raw polygons for 2026092400/06 and the
downstream WSP stages for 2026092412. Later issuances of those storms were
computed with a 24 h hole in the observed track (bridged by interpolation), so
their cumulative observed swath — and everything derived from it — is stale.

Sources (both confirmed for 2026-09-24):
- tracks: NHC text archive, rebuilt through ocha-lens's own realtime parser
  (src/pipelines/nhc_archive_advisories.py);
- WSP: https://www.nhc.noaa.gov/gis/forecast/archive/{YYYYMMDDHH}_wsp_120hr5km.zip
  via ``lens.nhc.get_wsp(issued_time=...)``.

Modes
-----
``--plan`` (default, READ-ONLY; works with read creds from a laptop):
  lists the products to restore, runs both self-checks, and prints exactly
  which rows would be backed up / deleted / recomputed.
``--execute`` (Databricks only — prod write creds):
  0. refuses to start if the live NHC Pipeline job has an active run;
  1. self-checks again (abort on any mismatch);
  2. restores the missing track rows (tracks only — ``nhc_storms`` untouched);
  3. snapshots T_CUT = the latest leadtime-0 issuance of the storms NOW and
     computes the scope. Every live run after step 2 builds its cumulative
     swath from the repaired tracks, so only issuances <= T_CUT need
     recomputing (no live pause);
  4. additive work BEFORE any delete, so network/blob I/O never sits in the
     post-delete window: exposure session (WorldPop + FieldMaps), WSP archive
     fetch for the lost synoptic times, their matching + WSP exposure, fcast
     buffers + exposure at the restored issuances (full AND intermediate —
     realtime builds fcast buffers from leadtime-0-only rows too);
  5. backs up every row in the affected tables at the affected times (all
     storms) into ``storms._bak_<date>_<table>`` — the deletes are reversible —
     and persists (t0, T_CUT, scope) in ``storms._bak_<date>_meta`` in the same
     transaction, so a rerun reuses exactly the same times and never deletes
     rows that were not backed up;
  6. deletes the 4 storms' derived rows at the affected times (the pipeline's
     ``overwrite`` only upserts, so stale pcodes would otherwise survive), and
     all WSP matched rows at existing issuances whose match window now holds a
     restored track row (2026092418: live matching used 21Z points because the
     18Z intermediates were missing);
  7. recomputes (DB-only), one advisory at a time: obsv buffers -> fcastonly
     buffers -> fcastonly / obsv exposure -> WSP re-matching + exposure ->
     WSP fcastonly polygons + exposure;
  8. verifies coverage and uniqueness, completion markers for every stage
     (fcast exposure at the restored issuances included), that every
     (table, time) that had rows in the backup has rows again (completion
     markers from the original live runs survive the deletes, so they cannot
     prove a refill), and prints each storm's final observed exposure (the
     return-period input) before vs after.

Rerunnable: restores skip present products, backups are taken once, the
persisted scope is reused, deletes stay inside it, recomputes use overwrite.

    python scripts/backfill_nhc_from_archive.py --plan
    python scripts/backfill_nhc_from_archive.py --execute
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time

try:
    _HERE = os.path.dirname(os.path.abspath(__file__))
except NameError:  # DBX spark_python_task exec context has no __file__
    _HERE = os.path.dirname(os.path.abspath(sys.argv[0]))
sys.path.insert(0, os.path.abspath(os.path.join(_HERE, "..")))

import ocha_lens as lens  # noqa: E402
import ocha_stratus as stratus  # noqa: E402
import pandas as pd  # noqa: E402
from shapely import wkt as shapely_wkt  # noqa: E402
from sqlalchemy import text  # noqa: E402

from src.pipelines import nhc as P  # noqa: E402
from src.pipelines import nhc_archive_advisories as A  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("backfill")

STORMS = ["AL062026", "EP152026", "EP162026", "EP172026"]
# Exclusive bounds: the last issuance prod captured before the outage and the
# first one after it (both present for all four storms).
GAP_AFTER = pd.Timestamp("2026-09-23 21:00")
GAP_BEFORE = pd.Timestamp("2026-09-24 21:00")
LIVE_NHC_JOB_ID = 959161297191654
BACKUP_TAG = "20260929"
CHUNK = 1000
WSP_OFFSET = pd.Timedelta(hours=3)  # WSP synoptic time -> its full advisory


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------
def q(engine, sql, **params) -> pd.DataFrame:
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params)


def storm_names(engine) -> dict:
    df = q(engine, "select atcf_id, name from storms.nhc_storms where atcf_id = any(:a)",
           a=STORMS)
    names = dict(df.values)
    missing = [s for s in STORMS if not names.get(s)]
    if missing:
        raise SystemExit(f"nhc_storms has no name for {missing}")
    return names


def archive_inventory() -> pd.DataFrame:
    """Every archived fstadv/public_a product of the storms, with its time."""
    rows = []
    for atcf in STORMS:
        for p in A.list_products(atcf):
            rows.append({"atcf_id": atcf, "product": p,
                         "kind": p.kind, "number": p.number,
                         "issued_time": A.product_issued_time(p)})
            time.sleep(0.1)
    return pd.DataFrame(rows)


def prod_issuances(engine, lo, hi) -> set:
    df = q(engine, "select distinct atcf_id, issued_time from storms.nhc_tracks_geo "
                   "where atcf_id = any(:a) and issued_time between :lo and :hi",
           a=STORMS, lo=lo, hi=hi)
    return set(zip(df.atcf_id, df.issued_time))


def missing_products(engine, inv) -> pd.DataFrame:
    have = prod_issuances(engine, GAP_AFTER, GAP_BEFORE)
    win = inv[(inv.issued_time > GAP_AFTER) & (inv.issued_time < GAP_BEFORE)]
    keep = [(a, t) not in have for a, t in zip(win.atcf_id, win.issued_time)]
    return win[keep].sort_values(["issued_time", "atcf_id"])


def rebuild(inv, row) -> pd.DataFrame:
    fst = {p.number: p for p in inv[inv.atcf_id == row.atcf_id]["product"]
           if p.kind == "fstadv"}
    return A.rebuild_product(row["product"], fst)


def to_track_rows(raw: pd.DataFrame) -> pd.DataFrame:
    """What process_tracks would write, as comparable columns."""
    g = lens.nhc.get_tracks(raw)
    out = pd.DataFrame({
        "atcf_id": g.atcf_id, "issued_time": pd.to_datetime(g.issued_time).dt.tz_localize(None)
        if pd.to_datetime(g.issued_time).dt.tz is not None else pd.to_datetime(g.issued_time),
        "leadtime": g.leadtime.astype(int),
        "wind_speed": g.wind_speed.astype(float),
        "pressure": pd.to_numeric(g.pressure, errors="coerce"),
        "lat": g.geometry.y.round(4), "lon": g.geometry.x.round(4),
        "storm_id": g.storm_id,
    })
    for c in ["quadrant_radius_34", "quadrant_radius_50", "quadrant_radius_64"]:
        out[c] = [json.dumps(x) if isinstance(x, list) else None for x in g[c]]
    return out


def prod_track_rows(engine, atcf_id, issued_time) -> pd.DataFrame:
    df = q(engine, """
        select atcf_id, issued_time, leadtime, wind_speed::float wind_speed, pressure,
               round(st_y(geometry)::numeric, 4)::float lat, round(st_x(geometry)::numeric, 4)::float lon,
               storm_id, quadrant_radius_34, quadrant_radius_50, quadrant_radius_64
        from storms.nhc_tracks_geo where atcf_id = :a and issued_time = :t""",
           a=atcf_id, t=issued_time)
    for c in ["quadrant_radius_34", "quadrant_radius_50", "quadrant_radius_64"]:
        df[c] = [json.dumps(json.loads(x)) if isinstance(x, str) else None for x in df[c]]
    return df


# ---------------------------------------------------------------------------
# self-checks
# ---------------------------------------------------------------------------
def check_equivalence(engine, inv) -> list[str]:
    """Rebuild the archived products prod DID capture live on either side of
    the gap (both kinds) and diff them field by field against prod."""
    problems = []
    near = inv[(inv.issued_time >= GAP_AFTER - pd.Timedelta(hours=9))
               & (inv.issued_time <= GAP_BEFORE + pd.Timedelta(hours=9))]
    have = prod_issuances(engine, GAP_AFTER - pd.Timedelta(hours=9),
                          GAP_BEFORE + pd.Timedelta(hours=9))
    checked = 0
    for _, row in near.iterrows():
        if (row.atcf_id, row.issued_time) not in have:
            continue
        mine = to_track_rows(rebuild(inv, row))
        mine = mine[mine.issued_time == row.issued_time]
        theirs = prod_track_rows(engine, row.atcf_id, row.issued_time)
        m = mine.merge(theirs, on=["atcf_id", "issued_time", "leadtime"],
                       how="outer", suffixes=("_arch", "_prod"), indicator=True)
        tag = f"{row.atcf_id} {row.kind} {row.number} @ {row.issued_time}"
        if (m._merge != "both").any():
            problems.append(f"{tag}: leadtimes differ "
                            f"{sorted(mine.leadtime)} vs {sorted(theirs.leadtime)}")
        for c in ["wind_speed", "pressure", "lat", "lon", "storm_id",
                  "quadrant_radius_34", "quadrant_radius_50", "quadrant_radius_64"]:
            a, b = m[f"{c}_arch"], m[f"{c}_prod"]
            same = (a == b) | (a.isna() & b.isna())
            if c in ("lat", "lon", "wind_speed", "pressure"):
                same = same | ((pd.to_numeric(a) - pd.to_numeric(b)).abs() < 1e-6)
            if not same[m._merge == "both"].all():
                bad = m[(m._merge == "both") & ~same][["leadtime", f"{c}_arch", f"{c}_prod"]]
                problems.append(f"{tag}: {c} differs\n{bad.to_string(index=False)}")
        checked += 1
    log.info(f"equivalence: {checked} live-captured products rebuilt from the archive, "
             f"{len(problems)} problem(s)")
    if checked < 8:
        problems.append(f"equivalence: only {checked} products compared (expected >= 8)")
    return problems


def _geom_close(a, b, tol=1e-5) -> bool:
    """Relative symmetric-difference area. Recomputing on a different GEOS
    build than the one that wrote prod differs by <= ~4e-6 on these storms."""
    if a is None or b is None:
        return a is None and b is None
    union = a.union(b).area
    return union == 0 or a.symmetric_difference(b).area / union < tol


def check_determinism(engine) -> list[str]:
    """Recompute, in memory, obsv + fcastonly buffers at issuances the gap
    cannot affect (each storm's last pre-gap advisory) and diff against the
    stored geometry. If these don't reproduce, a recompute would change values
    outside the gap too."""
    problems, checked = [], 0
    for atcf in STORMS:
        t = GAP_AFTER
        g = P._load_nhc_tracks_obsv_buffer_tracks(engine, issued_time=t)
        g = g[g.atcf_id == atcf]
        # Realtime semantics: a buffer at t exists only when the storm's own
        # leadtime-0 row AT t carries radii (process_nhc_tracks_obsv_buffers
        # iterates the radii-bearing times only). Check that absence holds.
        if g.empty or t not in set(g.issued_time):
            n = q(engine, "select count(*) n from storms.nhc_tracks_obsv_buffers "
                          "where atcf_id=:a and valid_time=:t", a=atcf, t=t).n.iloc[0]
            if n:
                problems.append(f"determinism: {atcf} {t} has stored obsv buffers "
                                "but no radii-bearing leadtime-0 row at t")
            checked += 1
            continue
        for speed in P.BUFFER_SPEEDS:
            g = P.expand_quad_col(g, f"quadrant_radius_{speed}")
        buf = P.calculate_wind_buffers_gdf(
            g[g.issued_time <= t], quad_cols_format="quadrant_radius_{speed}_{quad}")
        buf["geometry"] = buf.geometry.apply(P._fix_antimeridian)
        stored = q(engine, "select wind_speed_kt, st_astext(geometry) g from "
                           "storms.nhc_tracks_obsv_buffers where atcf_id=:a and valid_time=:t",
                   a=atcf, t=t)
        stored = {int(k): (shapely_wkt.loads(v) if isinstance(v, str) else None)
                  for k, v in stored.values}
        fco = q(engine, "select f.wind_speed_kt, st_astext(f.geometry) fg, "
                        "st_astext(c.geometry) cg from storms.nhc_tracks_fcast_buffers f "
                        "join storms.nhc_tracks_fcastonly_buffers c using (atcf_id, issued_time, wind_speed_kt) "
                        "where f.atcf_id=:a and f.issued_time=:t", a=atcf, t=t)
        for _, r in buf.iterrows():
            kt = int(r["wind_speed_kt"])
            mine = r.geometry if (r.geometry is not None and not r.geometry.is_empty) else None
            if not _geom_close(mine, stored.get(kt)):
                problems.append(f"determinism: obsv buffer {atcf} {t} {kt}kt differs")
            f = fco[fco.wind_speed_kt == kt]
            if not f.empty:
                fg = f.fg.iloc[0] if isinstance(f.fg.iloc[0], str) else None
                recomputed = P._fcastonly_row_geom(fg, mine.wkt if mine is not None else None)
                cg = f.cg.iloc[0]
                storedc = shapely_wkt.loads(cg) if isinstance(cg, str) else None
                if not _geom_close(recomputed, storedc):
                    problems.append(f"determinism: fcastonly buffer {atcf} {t} {kt}kt differs")
            checked += 1
    log.info(f"determinism: {checked} buffers recomputed in memory, {len(problems)} problem(s)")
    if checked == 0:
        problems.append("determinism: nothing compared")
    return problems


# ---------------------------------------------------------------------------
# scope of the recompute
# ---------------------------------------------------------------------------
META = f"_bak_{BACKUP_TAG}_meta"
_RADII = ("(quadrant_radius_34 is not null or quadrant_radius_50 is not null "
          "or quadrant_radius_64 is not null)")


def gap_products(inv) -> pd.DataFrame:
    """Every archived product inside the gap — the lost set, whether or not it
    has been restored yet (so a rerun sees the same scope)."""
    return inv[(inv.issued_time > GAP_AFTER) & (inv.issued_time < GAP_BEFORE)]


def wsp_new_its(gap) -> list:
    """Synoptic WSP issuances whose full advisory was lost: raw may be missing
    and matched / exposure / fcastonly never ran."""
    full = gap[gap.kind == "fstadv"].issued_time.unique()
    return sorted({pd.Timestamp(t) - WSP_OFFSET for t in full})


def affected(engine, t0, t_cut, gap_times, new_its) -> dict:
    """Times per stage to recompute for the 4 storms in [t0, t_cut].

    - obsv_times: leadtime-0 issuances (cumulative swath changes).
    - fcast_times: every issuance with a radii-bearing row, mirroring
      _load_nhc_tracks_fcast_buffer_tracks — realtime builds fcast/fcastonly
      buffers at intermediate (leadtime-0-only) issuances too.
    - restored_times: the gap products' times (fcast buffers + exposure are
      new there; elsewhere fcast is unaffected by the observed hole).
    - new_its: rebuilt WSP issuances (raw may be absent until fetched).
    - matched_redo_its: existing WSP issuances whose [it, it+3h] match window
      now contains a restored track row that the live match could not see.
    - wsp_its: WSP fcastonly (uses the obsv buffer at it+3h, else it).
    """
    lt0 = q(engine, "select distinct issued_time from storms.nhc_tracks_geo "
                    "where atcf_id = any(:a) and leadtime = 0 and issued_time between :lo and :hi",
            a=STORMS, lo=t0, hi=t_cut)
    fc = q(engine, f"select distinct issued_time from storms.nhc_tracks_geo "
                   f"where atcf_id = any(:a) and {_RADII} and issued_time between :lo and :hi",
           a=STORMS, lo=t0, hi=t_cut)
    raw = q(engine, "select distinct issued_time from storms.nhc_wsp_polygon_raw "
                    "where issued_time between :lo and :hi", lo=t0 - WSP_OFFSET, hi=t_cut)
    wsp_its = {t for t in raw.issued_time if t + WSP_OFFSET >= t0}
    wsp_its |= {t for t in new_its if t0 - WSP_OFFSET <= t <= t_cut}
    matched_redo = sorted(
        it for it in raw.issued_time
        if it not in set(new_its)
        and any(it <= g <= it + WSP_OFFSET for g in gap_times)
    )
    return {
        "obsv_times": sorted(lt0.issued_time.unique()),
        "fcast_times": sorted(fc.issued_time.unique()),
        "restored_times": sorted(pd.Timestamp(t) for t in gap_times),
        "new_its": sorted(new_its),
        "matched_redo_its": matched_redo,
        "wsp_its": sorted(wsp_its),
    }


# (table, time column, scope key, 4-storms-only?)
DELETE_SCOPE = [
    ("nhc_tracks_obsv_buffers", "valid_time", "obsv_times", True),
    ("nhc_tracks_obsv_exposure", "valid_time", "obsv_times", True),
    ("nhc_tracks_fcastonly_buffers", "issued_time", "fcast_times", True),
    ("nhc_tracks_fcastonly_exposure", "issued_time", "fcast_times", True),
    ("nhc_wsp_fcastonly_exposure", "issued_time", "wsp_its", True),
    ("nhc_wsp_exposure", "issued_time", "matched_redo_its", True),
    # All storms incl. NULL atcf_id: the matcher upserts, so a polygon that
    # the restored rows now assign to a storm would leave a NULL orphan.
    ("nhc_wsp_polygon_matched", "issued_time", "matched_redo_its", False),
]
# Backed up for ALL storms at the affected times (overwrite passes rewrite
# other storms' rows at the same times; the WSP fcastonly overwrite path
# DELETEs every storm's rows at an issued_time itself).
BACKUP_SCOPE = DELETE_SCOPE + [("nhc_wsp_fcastonly_polygon", "issued_time", "wsp_its", False)]


def scope_counts(engine, scope) -> pd.DataFrame:
    rows = []
    for table, col, key, four in BACKUP_SCOPE:
        times = list(scope[key])
        n4 = q(engine, f"select count(*) n from storms.{table} where atcf_id = any(:a) "
                       f"and {col} = any(:t)", a=STORMS, t=times).n.iloc[0]
        nall = q(engine, f"select count(*) n from storms.{table} where {col} = any(:t)",
                 t=times).n.iloc[0]
        deleted = (n4 if four else nall) if (table, col, key, four) in DELETE_SCOPE else 0
        rows.append({"table": table, "times": len(times),
                     "rows deleted before recompute": deleted,
                     "rows backed up (all storms)": nall})
    return pd.DataFrame(rows)


def final_obsv_snapshot(engine) -> pd.DataFrame:
    """Each storm's latest observed exposure per country and threshold — the
    return-period input used by ds-storms-alerts."""
    return q(engine, """
        select distinct on (atcf_id, iso3, wind_speed_kt)
               atcf_id, iso3, wind_speed_kt, valid_time, pop_exposed
        from storms.nhc_tracks_obsv_exposure
        where atcf_id = any(:a) and admin_level = 0
        order by atcf_id, iso3, wind_speed_kt, valid_time desc""", a=STORMS)


def load_meta(engine):
    """(t0, t_cut, scope) persisted by the first execute run, or None."""
    exists = q(engine, "select to_regclass(:t) is not null e", t=f"storms.{META}").e.iloc[0]
    if not exists:
        return None
    m = q(engine, f"select t0, t_cut, scope_json from storms.{META}")
    if not len(m):
        return None
    scope = {k: [pd.Timestamp(x) for x in v] for k, v in json.loads(m.scope_json.iloc[0]).items()}
    return m.t0.iloc[0], m.t_cut.iloc[0], scope


def fetch_wsp_archive(it) -> "pd.DataFrame":
    """Read-only fetch of one archived WSP issuance. Absolute cache dir: the
    relative default ("storm") is not writable from a job's working dir."""
    import tempfile
    return lens.nhc.get_wsp(issued_time=it.strftime("%Y%m%d%H"),
                            cache_dir=tempfile.mkdtemp(prefix="wsp_"), use_cache=False)


# ---------------------------------------------------------------------------
# execute
# ---------------------------------------------------------------------------
def live_job_active() -> bool:
    from databricks.sdk import WorkspaceClient
    runs = list(WorkspaceClient().jobs.list_runs(job_id=LIVE_NHC_JOB_ID, active_only=True))
    return len(runs) > 0


def execute(read_eng, write_eng, inv, missing):
    if live_job_active():
        raise SystemExit("The live NHC Pipeline job has an active run — start this "
                         "right after a cycle finishes (e.g. hh:45 for hh in 00,03,...).")
    gap = gap_products(inv)
    gap_times = sorted(set(gap.issued_time))
    new_its = wsp_new_its(gap)

    # 2. restore tracks (only what is still absent — a rerun restores nothing)
    if len(missing):
        raw = pd.concat([rebuild(inv, row) for _, row in missing.iterrows()],
                        ignore_index=True)
        log.info(f"restoring {len(missing)} products -> {len(raw)} raw rows")
        P.process_tracks(raw, write_eng, CHUNK)
    dup = q(read_eng, """select atcf_id, issued_time, leadtime, count(*) n from storms.nhc_tracks_geo
        where atcf_id = any(:a) and issued_time > :lo and issued_time < :hi
        group by 1,2,3 having count(*) > 1""", a=STORMS, lo=GAP_AFTER, hi=GAP_BEFORE)
    if len(dup):
        raise SystemExit(f"duplicate track rows after restore:\n{dup}")
    still = missing_products(read_eng, inv)
    if len(still):
        raise SystemExit(f"products still missing after restore:\n{still}")

    # 3. window + scope: computed AFTER the restore commit on the first run and
    # persisted (with the backups) so a rerun reuses exactly the same times.
    meta = load_meta(read_eng)
    first_run = meta is None
    if first_run:
        t0 = min(gap_times)
        t_cut = q(read_eng, "select max(issued_time) t from storms.nhc_tracks_geo "
                            "where atcf_id = any(:a) and leadtime = 0", a=STORMS).t.iloc[0]
        scope = affected(read_eng, t0, t_cut, gap_times, new_its)
    else:
        t0, t_cut, scope = meta
        log.info(f"RERUN: reusing persisted window [{t0}, {t_cut}] and scope")
    log.info(f"window [{t0}, {t_cut}]: " + ", ".join(f"{k}={len(v)}" for k, v in scope.items()))
    before = final_obsv_snapshot(read_eng)

    # 4. additive work BEFORE any delete (keeps network/blob I/O out of the
    # post-delete window): exposure session, WSP archive fetch, new-issuance
    # matching + WSP exposure, fcast buffers + exposure at restored times.
    session = P.build_exposure_session(mode="prod")
    for it in scope["new_its"]:
        n_raw = q(read_eng, "select count(*) n from storms.nhc_wsp_polygon_raw where issued_time=:t",
                  t=it).n.iloc[0]
        if n_raw == 0:
            gdf = fetch_wsp_archive(it)
            if gdf is None or gdf.empty:
                raise SystemExit(f"WSP archive returned nothing for {it}")
            P.process_wsp_polygons(gdf, write_eng, CHUNK)
            log.info(f"WSP raw {it}: {len(gdf)} polygons fetched from the archive")
        n_m = q(read_eng, "select count(*) n from storms.nhc_wsp_polygon_matched where issued_time=:t",
                t=it).n.iloc[0]
        if n_m and first_run:
            raise SystemExit(f"nhc_wsp_polygon_matched already has {n_m} rows at {it}; "
                             "expected none for a lost advisory — investigate")
        if not n_m:
            P.process_nhc_wsp_polygon_matched(write_eng, issued_time=it)
        P.run_nhc_wsp_exp(mode="prod", issued_time=it, session=session)
    for t in scope["restored_times"]:
        P.process_nhc_tracks_fcast_buffers(read_eng, write_eng, CHUNK, issued_time=t)
        P.run_nhc_tracks_fcast_exp(mode="prod", issued_time=t, session=session)

    # 5. backups + meta, one transaction, first run only
    if first_run:
        with write_eng.begin() as conn:
            for table, col, key, _ in BACKUP_SCOPE:
                bak = f"_bak_{BACKUP_TAG}_{table}"
                if conn.execute(text("select to_regclass(:t) is not null"),
                                {"t": f"storms.{bak}"}).scalar():
                    raise SystemExit(f"storms.{bak} exists but storms.{META} does not — "
                                     "inconsistent state, investigate before running")
                conn.execute(text(f"create table storms.{bak} as select * from storms.{table} "
                                  f"where {col} = any(:t)"), {"t": list(scope[key])})
                n = conn.execute(text(f"select count(*) from storms.{bak}")).scalar()
                log.info(f"backup storms.{bak}: {n} rows")
            conn.execute(text(f"create table storms.{META} (t0 timestamp, t_cut timestamp, "
                              "scope_json text, created_at timestamp default now())"))
            conn.execute(text(f"insert into storms.{META} (t0, t_cut, scope_json) "
                              "values (:a, :b, :s)"),
                         {"a": t0, "b": t_cut,
                          "s": json.dumps({k: [str(x) for x in v] for k, v in scope.items()})})

    # 6. scoped deletes — only times inside the persisted, backed-up scope
    with write_eng.begin() as conn:
        for table, col, key, four in DELETE_SCOPE:
            where = f"{col} = any(:t)" + (" and atcf_id = any(:a)" if four else "")
            r = conn.execute(text(f"delete from storms.{table} where {where}"),
                             {"a": STORMS, "t": list(scope[key])})
            log.info(f"deleted {r.rowcount} rows from storms.{table}")

    # 7. recompute (DB-only from here; the session is already loaded)
    for t in scope["obsv_times"]:
        P.process_nhc_tracks_obsv_buffers(read_eng, write_eng, CHUNK, overwrite=True, issued_time=t)
    for t in scope["fcast_times"]:
        P.process_nhc_tracks_fcastonly_buffers(read_eng, write_eng, CHUNK, overwrite=True, issued_time=t)
    for t in scope["fcast_times"]:
        P.run_nhc_tracks_fcastonly_exp(mode="prod", issued_time=t, overwrite=True, session=session)
    for t in scope["obsv_times"]:
        P.run_nhc_tracks_obsv_exp(mode="prod", valid_time=t, overwrite=True, session=session)
    for it in scope["matched_redo_its"]:
        P.process_nhc_wsp_polygon_matched(write_eng, issued_time=it, overwrite=True)
        P.run_nhc_wsp_exp(mode="prod", issued_time=it, overwrite=True, session=session)
    for it in scope["wsp_its"]:
        P.process_nhc_wsp_fcastonly_polygons(write_eng, issued_time=it, overwrite=True)
        P.run_nhc_wsp_fcastonly_exp(mode="prod", issued_time=it, overwrite=True, session=session)

    # 8. verify
    problems = verify(read_eng, scope)
    after = final_obsv_snapshot(read_eng)
    cmp = before.merge(after, on=["atcf_id", "iso3", "wind_speed_kt"], how="outer",
                       suffixes=("_before", "_after"))
    log.info("final observed exposure (admin0) before vs after:\n" + cmp.to_string(index=False))
    if problems:
        raise SystemExit("VERIFY FAILED:\n" + "\n".join(problems))
    log.info(f"DONE. Backups: storms._bak_{BACKUP_TAG}_* — drop once reviewed.")


def verify(engine, scope) -> list[str]:
    problems = []
    dup = q(engine, """select atcf_id, issued_time, leadtime, count(*) n from storms.nhc_tracks_geo
        where atcf_id = any(:a) and issued_time >= :lo group by 1,2,3 having count(*) > 1""",
            a=STORMS, lo=GAP_AFTER)
    if len(dup):
        problems.append(f"duplicate track rows:\n{dup}")
    lt0 = q(engine, f"""select atcf_id, issued_time from storms.nhc_tracks_geo g
        where atcf_id = any(:a) and leadtime = 0 and issued_time = any(:t) and {_RADII}
          and not exists (select 1 from storms.nhc_tracks_obsv_buffers b
                          where b.atcf_id = g.atcf_id and b.valid_time = g.issued_time)""",
            a=STORMS, t=list(scope["obsv_times"]))
    if len(lt0):
        problems.append(f"leadtime-0 rows without an obsv buffer:\n{lt0}")
    nofb = q(engine, f"""select distinct atcf_id, issued_time from storms.nhc_tracks_geo g
        where atcf_id = any(:a) and issued_time = any(:t) and {_RADII}
          and not exists (select 1 from storms.nhc_tracks_fcast_buffers f
                          where f.atcf_id = g.atcf_id and f.issued_time = g.issued_time)""",
             a=STORMS, t=list(scope["restored_times"]))
    if len(nofb):
        problems.append(f"restored issuances without fcast buffers:\n{nofb}")
    fc = q(engine, """select distinct atcf_id, issued_time from storms.nhc_tracks_fcast_buffers f
        where atcf_id = any(:a) and issued_time = any(:t)
          and not exists (select 1 from storms.nhc_tracks_fcastonly_buffers c
                          where c.atcf_id = f.atcf_id and c.issued_time = f.issued_time)""",
           a=STORMS, t=list(scope["fcast_times"]))
    if len(fc):
        problems.append(f"fcast buffers without fcastonly buffers:\n{fc}")
    restored_radii = q(engine, f"select distinct issued_time from storms.nhc_tracks_geo "
                               f"where atcf_id = any(:a) and issued_time = any(:t) and {_RADII}",
                       a=STORMS, t=list(scope["restored_times"])).issued_time
    for table, times in [("nhc_tracks_obsv_exposure", scope["obsv_times"]),
                         ("nhc_tracks_fcastonly_exposure", scope["fcast_times"]),
                         ("nhc_tracks_fcast_exposure", list(restored_radii)),
                         ("nhc_wsp_fcastonly_exposure", scope["wsp_its"]),
                         ("nhc_wsp_exposure", scope["new_its"] + scope["matched_redo_its"])]:
        done = q(engine, "select key_val, count(*) n from storms.exposure_completion "
                         "where out_table = :o and key_val = any(:t) group by 1",
                 o=table, t=list(times))
        lacking = set(times) - set(done[done.n >= 2].key_val)
        if lacking:
            problems.append(f"{table}: no completion markers for {sorted(lacking)}")
    for table, its in [("nhc_wsp_polygon_matched", scope["new_its"] + scope["matched_redo_its"]),
                       ("nhc_wsp_fcastonly_polygon", scope["new_its"])]:
        have = set(q(engine, f"select distinct issued_time from storms.{table} "
                             "where issued_time = any(:t)", t=list(its)).issued_time)
        if set(its) - have:
            problems.append(f"{table}: no rows for {sorted(set(its) - have)}")
    # Refill check: completion markers from the original live runs survive the
    # deletes, so they cannot prove a deleted row was rebuilt. Compare, per
    # (table, time), the backed-up count with what is there now.
    for table, col, key, four in BACKUP_SCOPE:
        f4 = " and atcf_id = any(:a)" if four else ""
        bak = q(engine, f"select {col} t, count(*) n from storms._bak_{BACKUP_TAG}_{table} "
                        f"where true{f4} group by 1", a=STORMS)
        now = q(engine, f"select {col} t, count(*) n from storms.{table} "
                        f"where {col} = any(:tt){f4} group by 1", tt=list(scope[key]), a=STORMS)
        m = bak.merge(now, on="t", how="left", suffixes=("_bak", "_now")).fillna({"n_now": 0})
        lost = m[(m.n_bak > 0) & (m.n_now == 0)]
        if len(lost):
            problems.append(f"{table}: rows deleted but not rebuilt at {sorted(lost.t)}")
        log.info(f"refill {table}: backup {int(m.n_bak.sum())} rows -> now "
                 f"{int(m.n_now.sum())} at the backed-up times")
    return problems


# ---------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--plan", action="store_true", help="read-only (default)")
    g.add_argument("--execute", action="store_true", help="write to prod (Databricks)")
    args = ap.parse_args()

    read_eng = stratus.get_engine("prod")
    log.info(f"storms: {storm_names(read_eng)}")
    inv = archive_inventory()
    missing = missing_products(read_eng, inv)
    log.info(f"{len(missing)} archived products absent from prod in "
             f"({GAP_AFTER}, {GAP_BEFORE}):\n"
             + missing[["atcf_id", "kind", "number", "issued_time"]].to_string(index=False))

    problems = []
    for check in (lambda: check_equivalence(read_eng, inv),
                  lambda: check_determinism(read_eng)):
        found = check()
        for p in found:
            log.error(p)
        problems += found

    if not args.execute:
        gap = gap_products(inv)
        gap_times = sorted(set(gap.issued_time))
        new_its = wsp_new_its(gap)
        meta = load_meta(read_eng)
        if meta is not None:
            log.info(f"PLAN note: a previous execute run persisted window {meta[:2]}")
        t0 = min(gap_times)
        t_cut = q(read_eng, "select max(issued_time) t from storms.nhc_tracks_geo "
                            "where atcf_id = any(:a) and leadtime = 0", a=STORMS).t.iloc[0]
        scope = affected(read_eng, t0, t_cut, gap_times, new_its)
        log.info(f"PLAN window [{t0}, {t_cut}] (T_CUT as of now; snapshotted after the "
                 "restore and persisted at execute time): "
                 + ", ".join(f"{k}={len(v)}" for k, v in scope.items())
                 + f"; matched_redo_its={[str(t) for t in scope['matched_redo_its']]}")
        log.info("PLAN backup/delete scope:\n" + scope_counts(read_eng, scope).to_string(index=False))
        have_raw = set(q(read_eng, "select distinct issued_time from storms.nhc_wsp_polygon_raw "
                                   "where issued_time = any(:t)", t=new_its).issued_time)
        for it in new_its:
            n_m = q(read_eng, "select count(*) n from storms.nhc_wsp_polygon_matched "
                              "where issued_time=:t", t=it).n.iloc[0]
            if n_m and meta is None:
                problems.append(f"nhc_wsp_polygon_matched already has {n_m} rows at {it}")
            if it in have_raw:
                log.info(f"PLAN WSP {it}: raw present in prod; matched rows now {n_m}")
                continue
            gdf = fetch_wsp_archive(it)  # read-only: nothing is written
            if gdf is None or gdf.empty:
                problems.append(f"WSP archive returned nothing for {it}")
            else:
                log.info(f"PLAN WSP {it}: raw absent in prod; archive fetch OK, "
                         f"{len(gdf)} polygons")
        try:
            log.info(f"PLAN live NHC job active right now: {live_job_active()}")
        except Exception as exc:  # noqa: BLE001 — informative only in plan mode
            log.warning(f"PLAN could not query the live job ({exc.__class__.__name__}: "
                        f"{exc}); on Databricks this must work or --execute will refuse")
        log.info("PLAN final observed exposure now (admin0):\n"
                 + final_obsv_snapshot(read_eng).to_string(index=False))
        log.info(f"PLAN self-checks: {'PASS' if not problems else 'FAIL'}")
        # Databricks treats ANY SystemExit (even code 0) as a task failure, so
        # only raise on problems and let success return normally.
        if problems:
            raise SystemExit(1)
        return

    if problems:
        raise SystemExit("self-checks failed — not executing")
    write_eng = stratus.get_engine("prod", write=True)
    execute(read_eng, write_eng, inv, missing)


if __name__ == "__main__":
    main()
