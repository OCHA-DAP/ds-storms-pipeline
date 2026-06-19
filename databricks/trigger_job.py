"""DBX task: fire-and-forget trigger for the Cuba Forecast Monitor.

Added as a leaf task after the NHC ``etl`` stage. Its only job is to
*start* a separate Databricks job (the Cuba Hurricane Forecast Monitor)
and return immediately — without waiting for it, and without ever
failing the NHC run.

Isolation is the whole point: the monitor is meant to be independent of
the storms pipeline, so a problem triggering it (or in the monitor
itself) must not turn the NHC run red. Two things guarantee that:

  * we call ``jobs.run_now``, which returns as soon as the run is
    *queued* — it does not block until the monitor finishes, and we
    never call ``.result()``; and
  * we catch every exception and let the script end normally. We never
    ``raise`` and never call ``sys.exit`` — DBX's exec context maps both
    onto task failure (it treats even ``SystemExit(0)`` as failure; see
    the note in dispatch_gdacs_adam.py). The only safe success signal is
    to fall off the end of ``main``.

Positional contract (mirrors the task ``parameters:`` in databricks.yml):
    sys.argv[1] = job_id   # numeric Databricks job id, or "" to no-op

An empty job_id is a deliberate no-op (logged, exits clean). That lets a
target with no monitor wired up — e.g. prod, where no prod Cuba monitor
exists yet — carry the task harmlessly until an id is filled in.
"""

import sys


def _arg(i, default=""):
    return sys.argv[i] if len(sys.argv) > i else default


JOB_ID = _arg(1).strip()


def main() -> None:
    if not JOB_ID:
        print(
            "trigger_job.py: no job_id provided — nothing to trigger.",
            flush=True,
        )
        return
    try:
        from databricks.sdk import WorkspaceClient

        run = WorkspaceClient().jobs.run_now(job_id=int(JOB_ID))
        # run_now returns once the run is queued; we intentionally do NOT
        # call .result() — firing and forgetting is the contract.
        run_id = getattr(run, "run_id", None)
        print(
            f"trigger_job.py: queued run for job_id={JOB_ID} "
            f"(run_id={run_id}).",
            flush=True,
        )
    except Exception as exc:  # noqa: BLE001 — never fail the NHC run
        print(
            f"trigger_job.py: could not trigger job_id={JOB_ID}: "
            f"{exc!r}. Ignoring (isolated by design).",
            flush=True,
        )


if __name__ == "__main__":
    main()
