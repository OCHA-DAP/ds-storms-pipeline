"""Regression tests for the exposure completion-marker short-circuit.

The bug this guards against: `_exposure_already_done` used to check for
row PRESENCE per admin_level in the output table. An OOM-killed run that
had already written some rows for every admin_level therefore looked
"done", and the partial issuance froze as-is (wsp_exposure, 2026-08-28 —
Dolly's 06:00 issuance sat at 79/98 rows until manually recomputed).
The check now reads storms.exposure_completion markers, which the runners
write strictly AFTER a single-issuance pass finishes end-to-end.

No DB needed: a stub engine/connection captures the SQL and plays back
canned marker rows.
"""

from unittest.mock import MagicMock

from sqlalchemy.exc import ProgrammingError

from src.pipelines.nhc import (
    _exposure_already_done,
    _mark_exposure_complete,
)


def _session_with_rows(rows):
    """A fake _ExposureSession whose connection returns `rows` for the
    marker SELECT."""
    conn = MagicMock()
    conn.execute.return_value = [(r,) for r in rows]
    engine = MagicMock()
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    session = MagicMock()
    session.engine = engine
    return session, conn


def test_done_only_when_every_level_marked():
    session, _ = _session_with_rows([0, 1])
    assert _exposure_already_done(
        out_table="nhc_wsp_fcastonly_exposure", key_col="issued_time",
        key_val="2026-08-28T06:00:00", admin_levels=[0, 1],
        mode="dev", session=session,
    )


def test_partial_markers_do_not_short_circuit():
    # The OOM scenario: adm0 marked (hypothetically), adm1 not.
    session, _ = _session_with_rows([0])
    assert not _exposure_already_done(
        out_table="nhc_wsp_fcastonly_exposure", key_col="issued_time",
        key_val="2026-08-28T06:00:00", admin_levels=[0, 1],
        mode="dev", session=session,
    )


def test_rows_in_output_table_alone_do_not_count():
    # Zero markers => recompute, no matter what the output table holds.
    session, conn = _session_with_rows([])
    assert not _exposure_already_done(
        out_table="nhc_wsp_fcastonly_exposure", key_col="issued_time",
        key_val="2026-08-28T06:00:00", admin_levels=[0, 1],
        mode="dev", session=session,
    )
    sql = str(conn.execute.call_args_list[0].args[0])
    assert "exposure_completion" in sql
    assert "nhc_wsp_fcastonly_exposure" not in sql  # table name is a bind param


def test_missing_marker_table_means_not_done():
    # First run after this change (or a read-only engine that can't create
    # the table): the SELECT raises, the check must say "recompute".
    session, conn = _session_with_rows([])
    conn.execute.side_effect = ProgrammingError("stmt", {}, Exception("no table"))
    assert not _exposure_already_done(
        out_table="nhc_wsp_exposure", key_col="issued_time",
        key_val="2026-08-28T06:00:00", admin_levels=[0, 1],
        mode="dev", session=session,
    )


def test_mark_writes_one_upsert_per_level():
    conn = MagicMock()
    engine = MagicMock()
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)

    _mark_exposure_complete(
        engine, "nhc_wsp_exposure", "2026-08-28T06:00:00", [0, 1]
    )

    sqls = [str(c.args[0]) for c in conn.execute.call_args_list]
    assert any("CREATE TABLE IF NOT EXISTS" in s for s in sqls)
    upserts = [s for s in sqls if "ON CONFLICT" in s]
    assert len(upserts) == 2
    levels = [
        c.args[1]["al"] for c in conn.execute.call_args_list
        if len(c.args) > 1 and "al" in c.args[1]
    ]
    assert levels == [0, 1]
