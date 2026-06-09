"""Column-scoped upsert for ``storms.storm_id_lookup``.

``storm_id_lookup`` is written by several pipelines, each supplying only
the column(s) it owns:

  - ``adam.py``           → ``adam_eventid``
  - ``match.py`` / ``gdacs.py`` (inline) → ``atcf_id``

We deliberately do NOT use :func:`ocha_stratus.postgres_upsert` here. That
helper builds its ``ON CONFLICT DO UPDATE SET`` clause from
``insert_statement.excluded``, which SQLAlchemy populates with *every*
column of the target table — not just the columns being inserted. A
partial-column DataFrame would therefore NULL the columns it did not
supply (an ADAM write would wipe a previously-resolved ``atcf_id``, and a
``match`` write would wipe ``adam_eventid``), so the gdacs→adam→match
cascade would destroy its own cross-source links.

This helper scopes the SET clause to only the DataFrame's own columns, so
each writer touches just the column(s) it owns and leaves the rest intact;
``last_updated`` is bumped on every update. Full-row writers
(``gdacs_exposure``, ``adam_exposure``) are unaffected and keep using
``stratus.postgres_upsert`` — the clobber only bites partial-column writes.
"""

from typing import List

import pandas as pd
from sqlalchemy import MetaData, Table, text
from sqlalchemy.dialects.postgresql import Insert, insert

CONFLICT_COL = "gdacs_eventid"


def _build_upsert_stmt(
    table: Table,
    records: List[dict],
    conflict_col: str = CONFLICT_COL,
) -> Insert:
    """Build a column-scoped ``ON CONFLICT DO UPDATE`` statement.

    The SET clause references only the columns present in ``records``
    (minus the conflict key), plus ``last_updated`` when the table has it.
    Separated from execution so it can be unit-tested without a database.
    """
    stmt = insert(table).values(records)
    update_cols = [c for c in records[0].keys() if c != conflict_col]
    set_ = {c: stmt.excluded[c] for c in update_cols}
    if "last_updated" in table.c:
        set_["last_updated"] = text("CURRENT_TIMESTAMP")
    return stmt.on_conflict_do_update(
        index_elements=[conflict_col],
        set_=set_,
    )


def upsert_storm_id_lookup(
    df: pd.DataFrame,
    engine,
    schema: str = "storms",
    table_name: str = "storm_id_lookup",
    conflict_col: str = CONFLICT_COL,
) -> None:
    """Upsert ``df`` into ``storms.storm_id_lookup``, writing only the
    columns ``df`` carries (siblings are preserved). No-op on empty df."""
    if df is None or df.empty:
        return
    tbl = Table(table_name, MetaData(), schema=schema, autoload_with=engine)
    stmt = _build_upsert_stmt(tbl, df.to_dict("records"), conflict_col)
    with engine.begin() as conn:
        conn.execute(stmt)
