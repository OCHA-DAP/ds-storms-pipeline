"""Copy the storms history from the DEV DB into the PROD DB (one-off).

Context: the storms jobs cut over to prod on 2026-09-22 with an empty
history (dev was unreachable then). Dev is reachable again from Databricks
over its private endpoint, so the history is copied rather than recomputed.

Rules (each is enforced below, not just documented):
- PROD WINS. For a table with a time column, only dev rows strictly older
  than prod's earliest row in that table are copied; prod has been the only
  real writer since the cutover, and dev kept receiving rows afterwards from
  other (dev-target) jobs that must not be mixed in. Tables without a time
  column are merged with ON CONFLICT DO NOTHING, so existing prod rows are
  never modified.
- INSERT ONLY. Nothing in prod is updated or deleted. Re-running is safe:
  every insert is ON CONFLICT DO NOTHING on the table's own unique
  constraints.
- Serial `id` columns are not copied (prod assigns new ids; nothing
  references them).
- Streaming: dev COPY TO a local temp file per chunk (a year of the time
  column, or the whole table), prod COPY into a temp staging table, then
  INSERT ... SELECT ... ON CONFLICT DO NOTHING, one transaction per chunk.

Run as a Databricks job (prod write creds exist only there):

    python scripts/copy_history_dev_to_prod.py --phase 1 --dry-run
    python scripts/copy_history_dev_to_prod.py --phase 1
    python scripts/copy_history_dev_to_prod.py --phase 2
"""

import argparse
import os
import sys
import tempfile
import time

import ocha_stratus as stratus
from sqlalchemy import text

try:
    _HERE = os.path.dirname(os.path.abspath(__file__))
except NameError:  # DBX spark_python_task exec context has no __file__
    _HERE = os.path.dirname(os.path.abspath(sys.argv[0]))
SQL_DIR = os.path.join(_HERE, "..", "src", "schemas", "sql")

SCHEMA = "storms"
# Hard ceiling: prod became the storms writer at the 2026-09-22 12Z issuance.
# No dev row at or after this instant is ever copied, even for a table where
# prod happens to have no rows yet for that period (dev kept being written
# after the cutover by dev-target jobs).
CUTOVER = "2026-09-22 12:00:00"

# (table, time column or None). Phase 1 = what the alerts / monitors / RPs
# need (small-to-medium); phase 2 = the geometry-heavy intermediates.
PHASE_1 = [
    ("nhc_storms", None),
    ("storm_id_lookup", None),
    ("gdacs_fm_lookup", None),
    ("adam_fm_lookup", None),
    ("exposure_completion", "key_val"),
    ("gdacs_exposure", "valid_time"),
    ("adam_exposure", "valid_time"),
    ("nhc_tracks_obsv_exposure", "valid_time"),
    ("nhc_tracks_fcast_exposure", "issued_time"),
    ("nhc_tracks_fcastonly_exposure", "issued_time"),
    ("nhc_wsp_exposure", "issued_time"),
    ("nhc_wsp_fcastonly_exposure", "issued_time"),
    ("ibtracs_wind_exposure", None),
]
PHASE_2 = [
    ("ibtracs_wind_buffers", None),
    ("nhc_tracks_obsv_buffers", "valid_time"),
    ("nhc_tracks_fcast_buffers", "issued_time"),
    ("nhc_tracks_fcastonly_buffers", "issued_time"),
    ("nhc_wsp_polygon_raw", "issued_time"),
    ("nhc_wsp_polygon_matched", "issued_time"),
    ("nhc_wsp_fcastonly_polygon", "issued_time"),
]
# Created in prod from the repo DDL if absent (they never existed there).
ENSURE_DDL = {
    "ibtracs_wind_buffers": "ibtracs_wind_buffers.sql",
    "ibtracs_wind_exposure": "ibtracs_wind_exp.sql",
}


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def table_exists(conn, table):
    return conn.execute(
        text("select to_regclass(:t) is not null"), {"t": f"{SCHEMA}.{table}"}
    ).scalar()


def copy_columns(dev_conn, prod_conn, table):
    """Columns present in BOTH dbs with the same type, minus serial ids."""
    q = text(
        "select column_name, udt_name, coalesce(column_default,'') d "
        "from information_schema.columns where table_schema=:s and table_name=:t "
        "order by ordinal_position"
    )
    dev = {r[0]: r[1] for r in dev_conn.execute(q, {"s": SCHEMA, "t": table})}
    prod = list(prod_conn.execute(q, {"s": SCHEMA, "t": table}))
    cols, skipped = [], []
    for name, udt, default in prod:
        if "nextval(" in default:
            skipped.append(f"{name} (serial)")
        elif name not in dev:
            skipped.append(f"{name} (not in dev)")
        elif dev[name] != udt:
            raise SystemExit(f"{table}.{name}: type mismatch dev={dev[name]} prod={udt}")
        else:
            cols.append(name)
    extra = sorted(set(dev) - {p[0] for p in prod})
    if extra:
        raise SystemExit(f"{table}: dev has columns prod lacks: {extra}")
    return cols, skipped


def chunks(dev_conn, table, tcol, cutoff):
    """(lo, hi) bounds per calendar year below the cutoff; [(None, None)] if
    the table has no time column."""
    if tcol is None:
        return [(None, None)]
    where = f"where {tcol} < :cutoff" if cutoff is not None else ""
    lo, hi = dev_conn.execute(
        text(f"select min({tcol}), max({tcol}) from {SCHEMA}.{table} {where}"),
        {"cutoff": cutoff},
    ).one()
    if lo is None:
        return []
    return [(f"{y}-01-01", f"{y + 1}-01-01") for y in range(lo.year, hi.year + 1)]


def where_clause(tcol, lo, hi, cutoff):
    parts = []
    if lo is not None:
        parts += [f"{tcol} >= '{lo}'", f"{tcol} < '{hi}'"]
    if tcol is not None and cutoff is not None:
        parts.append(f"{tcol} < '{cutoff}'")
    return ("where " + " and ".join(parts)) if parts else ""


def run(tables, dry_run):
    dev_eng = stratus.get_engine("dev")
    # Dry runs only read, so they work with read creds (e.g. from a laptop).
    prod_eng = stratus.get_engine("prod", write=not dry_run)
    summary = []
    with dev_eng.connect() as dconn, prod_eng.connect() as pconn:
        for table, _ in tables:
            if table in ENSURE_DDL and not table_exists(pconn, table):
                if dry_run:
                    log(f"{table}: would CREATE from {ENSURE_DDL[table]}")
                    continue
                sql = open(os.path.join(SQL_DIR, ENSURE_DDL[table])).read()
                sql = sql.replace("{owner}", "dbwriter")
                pconn.execute(text(sql))
                pconn.commit()
                log(f"{table}: created in prod from {ENSURE_DDL[table]}")

        for table, tcol in tables:
            if not table_exists(dconn, table):
                log(f"{table}: not in dev — skip")
                continue
            if not table_exists(pconn, table):
                log(f"{table}: not in prod (dry run) — would be created; counting dev only")
                n_dev = dconn.execute(text(f"select count(*) from {SCHEMA}.{table}")).scalar()
                summary.append((table, None, n_dev, 0, None))
                continue
            cols, skipped = copy_columns(dconn, pconn, table)
            cutoff = None
            if tcol is not None:
                prod_min = pconn.execute(
                    text(f"select min({tcol}) from {SCHEMA}.{table}")
                ).scalar()
                cutoff = min(str(prod_min), CUTOVER) if prod_min is not None else CUTOVER
            n_prod_before = pconn.execute(text(f"select count(*) from {SCHEMA}.{table}")).scalar()
            n_cand = dconn.execute(
                text(f"select count(*) from {SCHEMA}.{table} {where_clause(tcol, None, None, cutoff)}")
            ).scalar()
            log(f"{table}: cutoff={cutoff} dev_candidates={n_cand} prod_before={n_prod_before} "
                f"cols={len(cols)} skipped={skipped}")
            if dry_run:
                summary.append((table, cutoff, n_cand, n_prod_before, None))
                continue

            collist = ", ".join(cols)
            inserted = 0
            for lo, hi in chunks(dconn, table, tcol, cutoff):
                # End SQLAlchemy's implicit transaction so each chunk is its
                # own explicit transaction on the raw connection.
                pconn.commit()
                w = where_clause(tcol, lo, hi, cutoff)
                with tempfile.TemporaryFile() as buf:
                    draw = dconn.connection.dbapi_connection.cursor()
                    draw.copy_expert(
                        f"COPY (select {collist} from {SCHEMA}.{table} {w}) TO STDOUT", buf
                    )
                    draw.close()
                    size = buf.tell()
                    buf.seek(0)
                    if size == 0:
                        continue
                    praw = pconn.connection.dbapi_connection
                    cur = praw.cursor()
                    cur.execute("SET LOCAL synchronous_commit = off")
                    cur.execute(
                        f"CREATE TEMP TABLE _stage ON COMMIT DROP AS "
                        f"SELECT {collist} FROM {SCHEMA}.{table} WITH NO DATA"
                    )
                    cur.copy_expert(f"COPY _stage ({collist}) FROM STDIN", buf)
                    staged = cur.rowcount
                    cur.execute(
                        f"INSERT INTO {SCHEMA}.{table} ({collist}) "
                        f"SELECT {collist} FROM _stage ON CONFLICT DO NOTHING"
                    )
                    ins = cur.rowcount
                    praw.commit()
                    cur.close()
                    inserted += ins
                    log(f"  {table} [{lo or 'all'}..{hi or ''}] staged={staged} "
                        f"inserted={ins} ({size / 1e6:.0f} MB)")
                time.sleep(0.5)  # be gentle with the shared prod server
            n_prod_after = pconn.execute(text(f"select count(*) from {SCHEMA}.{table}")).scalar()
            if n_prod_after != n_prod_before + inserted:
                raise SystemExit(
                    f"{table}: count check failed before={n_prod_before} "
                    f"inserted={inserted} after={n_prod_after}"
                )
            pconn.execute(text(f"ANALYZE {SCHEMA}.{table}"))
            pconn.commit()
            summary.append((table, cutoff, n_cand, n_prod_before, inserted))
            log(f"{table}: done inserted={inserted} conflicts_skipped={n_cand - inserted} "
                f"prod {n_prod_before} -> {n_prod_after}")

    log("SUMMARY table | cutoff | dev_candidates | prod_before | inserted")
    for row in summary:
        log("  " + " | ".join(str(x) for x in row))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--phase", choices=["1", "2"], required=True)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--tables", nargs="*", help="restrict to these tables (within the phase)")
    a = ap.parse_args()
    tabs = PHASE_1 if a.phase == "1" else PHASE_2
    if a.tables:
        tabs = [t for t in tabs if t[0] in a.tables]
    log(f"phase={a.phase} dry_run={a.dry_run} tables={[t for t, _ in tabs]}")
    run(tabs, a.dry_run)
