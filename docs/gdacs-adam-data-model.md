# GDACS / ADAM data model & pipeline wiring

How the GDACS and ADAM pipelines (`run_pipeline.py gdacs` / `adam`) fill the
`storms.*` tables, and how a single storm gets linked across sources. Each
pipeline does two distinct jobs, so each gets two diagrams:

1. **ETL** — fetch exposure and fill the exposure table.
2. **Storm ID lookup** — record the storm's cross-source ids in
   `storms.storm_id_lookup`.

**Reading the diagrams**

| symbol | meaning |
|---|---|
| `[[ ... ]]` subroutine, purple | `ocha_lens.datasources.*` library call |
| `[[ ... ]]` subroutine, amber | pipeline / `match.py` helper function |
| `[( ... )]` cylinder, green | Postgres table (`storms.*`) |
| stadium | CLI entry point |
| hexagon | per-event loop |
| dotted edge | **read** from a table |
| thick edge | **write** (upsert) to a table |

Steps are numbered in execution order. The same exposure/lookup table is often
**read early** (skip cache) and **written late** (upsert) — watch the step
numbers on its two edges.

---

## 1. GDACS & ADAM ETL

How the exposure tables get filled. Same skeleton on both sides — fetch events,
load a skip cache, loop, fetch per-event exposure, upsert — but ADAM fetches a
single population CSV per event (which WFP often 403s) where GDACS fetches the
adm0/adm1 exposure off the event detail.

### GDACS → `storms.gdacs_exposure`

```mermaid
flowchart TD
    START(["1 · run_pipeline gdacs<br/>--mode (dev | prod)"]):::entry
    GETEV[["2 · ocha_lens.datasources.gdacs<br/>get_events"]]:::lens
    SKIP[["3 · _load_skip_info"]]:::local
    LOOP{{"4 · for each event in window"}}:::loop
    DETAIL[["5 · ocha_lens.datasources.gdacs<br/>get_event_detail"]]:::lens
    EPID[["6 · ocha_lens.datasources.gdacs<br/>latest_episode_id"]]:::lens
    EXP[["7 · ocha_lens.datasources.gdacs<br/>get_exposure_adm0 / adm1"]]:::lens
    EMIT[["8 · _emit_rows<br/>buffer to kt, to_iso3, wide to long"]]:::local
    GE[("storms.gdacs_exposure")]:::tbl

    START --> GETEV --> SKIP --> LOOP
    GE -. "read: latest stored<br/>snapshot per event" .-> SKIP
    SKIP -. "skip if already fresh" .-> LOOP
    LOOP --> DETAIL --> EPID --> EXP --> EMIT
    EMIT == "9 · write: upsert<br/>(gdacs_exposure_unique)" ==> GE

    classDef entry fill:#e7efff,stroke:#3768b0,color:#10203a,stroke-width:2px
    classDef lens fill:#efe3ff,stroke:#8a4fd0,color:#2e1457
    classDef local fill:#fff0d6,stroke:#d6920a,color:#5a3d00
    classDef loop fill:#eeeeee,stroke:#888888,color:#222222
    classDef tbl fill:#d8f6e6,stroke:#1f9d63,color:#0a3a26,stroke-width:2px
```

### ADAM → `storms.adam_exposure`

```mermaid
flowchart TD
    START(["1 · run_pipeline adam<br/>--mode (dev | prod)"]):::entry
    GETEV[["2 · ocha_lens.datasources.adam<br/>get_events"]]:::lens
    SKIP[["3 · _load_ingested_episodes"]]:::local
    LOOP{{"4 · for each event"}}:::loop
    GETEXP[["5 · ocha_lens.datasources.adam<br/>get_exposure<br/>(event_id, population_csv_url)"]]:::lens
    EMIT[["6 · _emit_rows<br/>long-format exposure rows"]]:::local
    NOCSV(["WFP 403 or no CSV<br/>skip exposure for this event"]):::loop
    AE[("storms.adam_exposure")]:::tbl

    START --> GETEV --> SKIP --> LOOP
    AE -. "read: already-ingested<br/>(event_id, episode_id) pairs" .-> SKIP
    SKIP -. "skip if episode<br/>already on file" .-> LOOP
    LOOP --> GETEXP
    GETEXP -- "CSV ok" --> EMIT
    GETEXP -- "403 / fetch error" --> NOCSV
    EMIT == "7 · write: upsert all rows<br/>(adam_exposure_unique)" ==> AE

    classDef entry fill:#e7efff,stroke:#3768b0,color:#10203a,stroke-width:2px
    classDef lens fill:#efe3ff,stroke:#8a4fd0,color:#2e1457
    classDef local fill:#fff0d6,stroke:#d6920a,color:#5a3d00
    classDef loop fill:#eeeeee,stroke:#888888,color:#222222
    classDef tbl fill:#d8f6e6,stroke:#1f9d63,color:#0a3a26,stroke-width:2px
```

---

## 2. Storm ID lookup

How `storms.storm_id_lookup` gets each storm's cross-source ids. The two sides
are fundamentally different: GDACS resolves its NHC `atcf_id` by **spatial
matching** (the forecast cone against `nhc_tracks_geo`), while ADAM's link is a
pure **identity** — the ADAM API's `event_id` already equals `gdacs_eventid`
(shared id space), so no matching is needed.

### GDACS → `atcf_id` (spatial match)

```mermaid
flowchart TD
    START(["1 · gdacs inline match<br/>/ run_pipeline match"]):::entry
    LFNT[["2 · load_freshest_nhc_tracks"]]:::local
    LME[["3 · load_matched_eventids"]]:::local
    LOOP{{"4 · for each unmatched event"}}:::loop
    AM[["5 · attempt_match"]]:::local
    TL[["6 · ocha_lens.datasources.gdacs<br/>get_timeline"]]:::lens
    M2A[["7 · ocha_lens.datasources.gdacs<br/>match_to_atcf<br/>forecast-cone vote, genesis fallback"]]:::lens
    UM[["8 · upsert_matches"]]:::local
    SKIPN(["leave unmatched, retry later"]):::loop
    NT[("storms.nhc_tracks_geo")]:::tbl
    SL[("storms.storm_id_lookup")]:::tbl

    START --> LFNT --> LME --> LOOP
    NT -. "read: freshest per<br/>atcf, valid_time" .-> LFNT
    SL -. "read: already matched<br/>(atcf_id not null)" .-> LME
    LOOP --> AM --> TL --> M2A
    M2A -- "atcf_id" --> UM
    M2A -- "None" --> SKIPN
    UM == "9 · write: upsert atcf_id<br/>(storm_id_lookup_pkey)" ==> SL

    classDef entry fill:#e7efff,stroke:#3768b0,color:#10203a,stroke-width:2px
    classDef lens fill:#efe3ff,stroke:#8a4fd0,color:#2e1457
    classDef local fill:#fff0d6,stroke:#d6920a,color:#5a3d00
    classDef loop fill:#eeeeee,stroke:#888888,color:#222222
    classDef tbl fill:#d8f6e6,stroke:#1f9d63,color:#0a3a26,stroke-width:2px
```

### ADAM → `adam_eventid` (identity)

```mermaid
flowchart TD
    START(["1 · run_pipeline adam<br/>--mode (dev | prod)"]):::entry
    GETEV[["2 · ocha_lens.datasources.adam<br/>get_events"]]:::lens
    LOOP{{"3 · for each event"}}:::loop
    LINK[["4 · record link<br/>(every event, even if the CSV 403s)<br/>adam_eventid = event_id from get_events"]]:::local
    DEDUP[["5 · drop_duplicates<br/>on gdacs_eventid"]]:::local
    SL[("storms.storm_id_lookup")]:::tbl
    NOTE["the ADAM API event_id already equals<br/>gdacs_eventid (ADAM ingests GDACS upstream,<br/>shared id space) so no spatial match is needed"]:::note

    START --> GETEV --> LOOP --> LINK --> DEDUP
    DEDUP == "6 · write: upsert<br/>set adam_eventid, leave atcf_id untouched<br/>(storm_id_lookup_pkey)" ==> SL
    GETEV -. "why no match step" .-> NOTE

    classDef entry fill:#e7efff,stroke:#3768b0,color:#10203a,stroke-width:2px
    classDef lens fill:#efe3ff,stroke:#8a4fd0,color:#2e1457
    classDef local fill:#fff0d6,stroke:#d6920a,color:#5a3d00
    classDef loop fill:#eeeeee,stroke:#888888,color:#222222
    classDef tbl fill:#d8f6e6,stroke:#1f9d63,color:#0a3a26,stroke-width:2px
    classDef note fill:#fffbe6,stroke:#caa300,color:#5b4a00,stroke-dasharray:4 3
```

---

The standalone diagram sources live alongside this doc as
`gdacs-1-exposure-fill.mmd`, `adam-1-exposure-fill.mmd`,
`gdacs-2-atcf-match.mmd`, `adam-2-eventid-link.mmd`. Table DDL is in
`src/schemas/sql/` (`gdacs_exposure.sql`, `adam_exposure.sql`,
`storm_id_lookup.sql`, `nhc_tables.sql`).
