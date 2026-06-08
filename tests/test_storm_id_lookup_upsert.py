"""Regression test for the storm_id_lookup column-scoped upsert.

The bug this guards against: routing partial-column writes through
``ocha_stratus.postgres_upsert`` produced an ``ON CONFLICT DO UPDATE SET``
clause spanning *every* table column, so an ADAM write (adam_eventid only)
NULLed atcf_id/sid, and a match write (atcf_id only) NULLed adam_eventid —
the gdacs→adam→match cascade destroyed its own cross-source links.

We assert on the *generated SQL* (no DB needed): each writer's SET clause
must touch only its own column (plus last_updated), never its siblings.
"""

from sqlalchemy import (
    TIMESTAMP,
    BigInteger,
    Column,
    Integer,
    MetaData,
    String,
    Table,
)
from sqlalchemy.dialects import postgresql

from src.pipelines._upsert import _build_upsert_stmt


def _storm_id_lookup_table() -> Table:
    """Minimal stand-in for storms.storm_id_lookup — no DB connection."""
    return Table(
        "storm_id_lookup",
        MetaData(),
        Column("gdacs_eventid", Integer, primary_key=True),
        Column("atcf_id", String),
        Column("sid", String),
        Column("adam_eventid", BigInteger),
        Column("last_updated", TIMESTAMP),
        schema="storms",
    )


def _set_clause(records) -> str:
    stmt = _build_upsert_stmt(_storm_id_lookup_table(), records)
    sql = str(stmt.compile(dialect=postgresql.dialect())).lower()
    # everything after "do update set" is the SET clause
    return sql.split("do update set", 1)[1]


def test_adam_writer_sets_only_adam_eventid():
    set_clause = _set_clause([{"gdacs_eventid": 1, "adam_eventid": 1}])
    assert "adam_eventid = excluded.adam_eventid" in set_clause
    assert "last_updated = current_timestamp" in set_clause
    # siblings must NOT be clobbered
    assert "atcf_id" not in set_clause
    assert "sid" not in set_clause


def test_match_writer_sets_only_atcf_id():
    set_clause = _set_clause([{"gdacs_eventid": 1, "atcf_id": "AL142024"}])
    assert "atcf_id = excluded.atcf_id" in set_clause
    assert "last_updated = current_timestamp" in set_clause
    # siblings must NOT be clobbered
    assert "adam_eventid" not in set_clause
    assert "sid" not in set_clause


def test_conflict_target_is_the_primary_key():
    stmt = _build_upsert_stmt(
        _storm_id_lookup_table(), [{"gdacs_eventid": 1, "atcf_id": "AL"}]
    )
    sql = str(stmt.compile(dialect=postgresql.dialect())).lower()
    assert "on conflict (gdacs_eventid) do update" in sql
