#!/usr/bin/env bash
#
# Deploy the ds-storms-pipeline bundle with the correct git_branch wiring,
# encoding the safe path so the easy command is the correct one.
#
#   ./scripts/deploy.sh dev    Deploy to YOUR dev (jobs auto-prefixed
#                              "[dev <you>]" under your own workspace). The
#                              cluster's runtime code (run_pipeline.py,
#                              dispatch.py, nhc.py, ...) tracks your CURRENT
#                              branch — not main. Dev job schedules are PAUSED
#                              after deploy: dev is for manual "Run now" tests;
#                              prod is the scheduled runner. This keeps two
#                              devs' dev jobs from both cron-writing the shared
#                              DEV DB. Unpause by hand if you really want cron.
#
#   ./scripts/deploy.sh prod   Deploy the shared prod job(s). Refuses unless
#                              you're on a clean, up-to-date main, so prod's
#                              bundle definition AND runtime code both come
#                              from reviewed main.
#
# Why this exists: the bundle pulls runtime Python from GitHub at run time via
# `git_source.git_branch` (default: main). `databricks bundle deploy` alone
# updates only the job *definition* from your local databricks.yml; it does
# NOT change which branch the cluster executes. This script sets git_branch so
# dev follows your branch and prod follows main — and blocks the silent
# mistakes (deploying prod from a feature branch; forgetting to push).
#
# git_source reads from the GitHub remote, so dev runs the last *pushed* commit
# of your branch — uncommitted/unpushed work won't run until you push it.

set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

TARGET="${1:-}"

case "$TARGET" in
  dev)
    BRANCH="$(git rev-parse --abbrev-ref HEAD)"

    # git_source pulls from origin, so the branch has to exist there.
    if ! git ls-remote --exit-code --heads origin "$BRANCH" >/dev/null 2>&1; then
      echo "ERROR: branch '$BRANCH' is not on origin." >&2
      echo "       Push it first:  git push -u origin $BRANCH" >&2
      exit 1
    fi

    # Warn (don't block) when local != pushed: dev runs the pushed commit.
    git fetch origin "$BRANCH" --quiet
    if [ -n "$(git status --porcelain)" ]; then
      echo "WARNING: uncommitted changes present — dev will run the last PUSHED commit of '$BRANCH', not your working tree." >&2
    fi
    if [ "$(git rev-parse HEAD)" != "$(git rev-parse "origin/$BRANCH")" ]; then
      echo "WARNING: HEAD differs from origin/$BRANCH — push so dev runs your latest commit." >&2
    fi

    echo "Deploying -t dev  (git_branch=$BRANCH) ..."
    databricks bundle deploy -t dev --var="git_branch=$BRANCH"

    # Dev is manual-test only: pause every scheduled dev job so two devs'
    # dev deployments don't both cron-write the shared DEV DB. Best-effort —
    # a failure here never fails the deploy.
    echo "Pausing dev schedules (dev = manual 'Run now'; prod owns the cron) ..."
    python3 - <<'PY' || echo "WARNING: auto-pause step hit an error; check schedules by hand." >&2
import json, subprocess, sys

def db(args):
    return subprocess.run(["databricks", *args], capture_output=True, text=True)

summ = db(["bundle", "summary", "-t", "dev", "-o", "json"])
if summ.returncode != 0:
    print("  could not read bundle summary; skipping auto-pause.", file=sys.stderr)
    sys.exit(0)

jobs = json.loads(summ.stdout).get("resources", {}).get("jobs", {})
for name, j in jobs.items():
    jid = j.get("id")
    if not jid:
        continue
    got = db(["jobs", "get", str(jid), "-o", "json"])
    if got.returncode != 0:
        print(f"  WARN: jobs get {jid} failed; skip.", file=sys.stderr)
        continue
    sched = json.loads(got.stdout).get("settings", {}).get("schedule")
    if not sched:
        continue
    if sched.get("pause_status") == "PAUSED":
        print(f"  {name} ({jid}): already paused")
        continue
    sched["pause_status"] = "PAUSED"
    body = json.dumps({"job_id": int(jid), "new_settings": {"schedule": sched}})
    up = db(["jobs", "update", "--json", body])
    if up.returncode == 0:
        print(f"  {name} ({jid}): schedule PAUSED")
    else:
        print(f"  WARN: pausing {name} ({jid}) failed: {up.stderr.strip()}", file=sys.stderr)
PY
    ;;

  prod)
    BRANCH="$(git rev-parse --abbrev-ref HEAD)"
    if [ "$BRANCH" != "main" ]; then
      echo "ERROR: prod must be deployed from 'main' (you're on '$BRANCH')." >&2
      echo "       Run:  git checkout main && git pull" >&2
      exit 1
    fi
    if [ -n "$(git status --porcelain)" ]; then
      echo "ERROR: working tree is dirty — prod must deploy a pristine main." >&2
      echo "       Commit, stash, or 'git restore' your changes first." >&2
      git status --short >&2
      exit 1
    fi
    git fetch origin main --quiet
    if [ "$(git rev-parse HEAD)" != "$(git rev-parse origin/main)" ]; then
      echo "ERROR: local main is out of sync with origin/main." >&2
      echo "       Run:  git pull" >&2
      exit 1
    fi

    echo "Deploying -t prod  (git_branch=main, from clean main) ..."
    databricks bundle deploy -t prod
    ;;

  *)
    echo "Usage: $0 {dev|prod}" >&2
    exit 1
    ;;
esac
