---
status: "accepted"
date: 2026-06-05
decision-makers: Zack Arno
consulted: Claude (pairing session)
informed: ds-storms-pipeline contributors
---

# Keep storm→ATCF matching as an independent, idempotent component

## Context and Problem Statement

Matching resolves a storm's NHC `atcf_id` from its `gdacs_eventid` (via the
GDACS timeline vs. the NHC forecast cone) and records it in
`storms.storm_id_lookup`. Today three writers populate that table: GDACS
matches **inline per event** (reusing the `detail` it already fetched), ADAM
writes only the identity link (`adam_eventid`, leaving `atcf_id` NULL), and a
standalone `match` step (`run_pipeline.py match`) is a mop-up/retry that
resolves any row still missing an `atcf_id`.

Should matching stay a separate component, or be folded into the end of the
GDACS and ADAM pipelines so each owns its own resolution? This matters because
the pipelines may later be **split apart and orchestrated independently**
(e.g. after merging with the NHC bundle: NHC → ADAM, with GDACS or a schedule
kicking off MATCH).

## Decision Drivers

* The pipelines may be decoupled later and triggered by arbitrary upstream
  processes/schedules — each unit should be independently runnable.
* Matching must "match whatever is present" — in particular a GDACS event with
  no ADAM counterpart (the common case; the reverse can't happen, see below).
* Avoid duplicate work (re-fetching the same GDACS timeline, loading NHC tracks
  twice).
* Idempotency and eventual consistency, so order-of-arrival (e.g. a late NHC
  cone) self-heals on the next run.
* `gdacs_eventid` is the identity source of truth: it is the PK of
  `storm_id_lookup`, so every row originates from a GDACS event.

## Considered Options

* **A — Keep `match` as a standalone, idempotent component** (status quo:
  GDACS inline match + standalone mop-up/retry).
* **B — Independent match steps appended to *both* GDACS and ADAM**, drop the
  standalone component.
* **C — Fold the single mop-up into ADAM's final step** (since ADAM runs last
  in the current cascade).

## Decision Outcome

Chosen option: **A — keep `match` as a standalone, idempotent component**,
because it is the only option that can be kicked off by *any* process in *any*
order, already "matches whatever is present," and survives a future split of
the pipelines with **orchestration-only (YAML) changes and no code rework**.
Folding matching into a sibling pipeline (B/C) would couple resolution to that
pipeline's run and fragment the cross-run retry logic.

### Consequences

* Good: `match` is a self-contained entry point (`run_pipeline.py match`) that
  reads only the DB, takes only `--mode`, and is safely re-runnable — it can be
  scheduled on its own and/or triggered by GDACS/ADAM/NHC completion.
* Good: a GDACS-only storm is resolved with no ADAM dependency; an ADAM-only
  registration (e.g. exposure CSV 403'd) is still resolvable because
  `attempt_match` fetches the timeline **by id**, not from exposure rows.
* Good: a future split becomes a bundle reconfiguration (drop `depends_on`,
  optionally add Databricks "Run Job" trigger tasks) — the Python is untouched.
* Bad: ADAM run in isolation leaves `atcf_id` NULL until `match` fires; the
  orchestration must guarantee `match` eventually runs.
* Bad: a standalone step that is usually a no-op ("No unmatched GDACS events")
  is a small amount of extra scheduling surface.

### Confirmation

* `match`'s `_load_unmatched_eventids` selects unmatched events from
  `gdacs_exposure` (ADAM-independent) UNION any `storm_id_lookup` row with
  `atcf_id IS NULL`, so it provably matches whatever is present.
* Each writer's upsert is **column-scoped** (`src/pipelines/_upsert.py`):
  ADAM writes only `adam_eventid`, `match`/GDACS write only `atcf_id`, and
  `last_updated` is bumped — so cross-source links accumulate on the row
  instead of clobbering each other, and re-running is idempotent. (An
  earlier implementation routed these partial writes through
  `stratus.postgres_upsert`, whose all-columns `SET` silently NULLed the
  siblings; the column-scoped helper fixes that.)
* Guarded by `tests/test_storm_id_lookup_upsert.py`, which asserts each
  writer's generated `SET` clause touches only its own column.

## Pros and Cons of the Options

### A — Standalone `match`

* Good, because it is kickable by any process, any order, idempotent.
* Good, because one `nhc_tracks` load handles all residue (GDACS retries +
  ADAM-discovered events) in a single pass.
* Good, because it survives a pipeline split with no code changes.
* Neutral, because it is often a no-op on a clean cycle.
* Bad, because ADAM-alone defers `atcf_id` until `match` runs.

### B — Match appended to both GDACS and ADAM

* Good, because each pipeline is self-sufficient if run truly alone.
* Bad, because it loads `nhc_tracks` twice and re-attempts events the other
  side already matched unless the skip logic is duplicated.
* Bad, because the *cross-run* retry (late NHC cone) still needs an
  "unmatched-rescan," so `run_match`'s logic is relocated, not eliminated.

### C — Fold the mop-up into ADAM's end

* Good, because it removes a separate task while keeping one efficient pass
  (ADAM runs last).
* Bad, because it couples matching to the ADAM run — it can no longer be kicked
  off independently, defeating the primary driver.
* Bad, because GDACS-timeline-based matching living in `adam.py` is misleading.

## More Information

* Schema enforces the identity model: `storm_id_lookup.gdacs_eventid` is the
  PRIMARY KEY, with `atcf_id`/`sid`/`adam_eventid` nullable. ADAM writes its
  row keyed by `event_id` (== `gdacs_eventid`), so an "ADAM event with no GDACS
  event" is impossible by construction. See `src/schemas/sql/storm_id_lookup.sql`.
* Ordering caveat: `match` resolves against `nhc_tracks_geo`, so for *timely*
  results the sensible chain is **NHC → (GDACS/ADAM) → MATCH**, plus `match` on
  its own periodic schedule as a safety net for late-arriving cones. Because it
  is idempotent, a miss this cycle is corrected next cycle.
* Revisit if `storm_id_lookup` moves off the `gdacs_eventid`-PK MVP shape (e.g.
  an NHC- or IBTrACS-side registration pipeline lands), per the migration note
  in `storm_id_lookup.sql`.
