# `scripts/_archive/` — superseded scripts (kept for reference)

These are **archived, not maintained, and not meant to run as-is** (their
relative-path assumptions are for the old `scripts/` location). They are kept
for historical reference only.

| Archived | Superseded by | Why |
|---|---|---|
| `build_adam_fm_lookup.py` (v1) | `scripts/build_adam_fm_lookup.py` (was `_v2`) | v1 drove the lookup from spatial + TOML alone and wrote with `if_exists="replace"`. The current builder projects the **human-reviewed crosswalk** into the typed/constrained table. |
| `build_canonical_lookup.py` (v1 GDACS) | `scripts/build_gdacs_fm_lookup.py` (was `_v2`) | Same as above, GDACS side. |

The current builders read `data/review/*_humanreview.csv` (the source of
truth) and write via TRUNCATE+append into the pre-created DDL
(`scripts/init_db_{adam,gdacs}_lookup.py`). See PR #19.
