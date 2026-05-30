# FM ↔ ADAM lookup — reviewer checklist

Pair this with `exploration/review_report_adam.html`. Countries are
listed alphabetically here, same order as the qmd, so you can scroll
both side-by-side.

## Legend

- **Per-row Status** (you set):
  - `⬜ pending` — haven't reviewed yet (default)
  - `✅ apply`   — agree, apply my recommendation
  - `💬 comment` — see your comment field; needs discussion
  - `❌ skip`    — disagree, do nothing
- **Top-of-country `🚀 Accept all`** — fast track. If checked, every
  `⬜ pending` finding in that country gets treated as `✅ apply`
  when I sweep. Per-row `❌ skip` and `💬 comment` still override.
- **Checkbox `[ ]` / `[x]`** — me marking "applied this in commit X".
  You don't need to touch these.

## Workflow

1. Scroll qmd panel for a country → scroll matching section here
2. For each finding: read **Current** + **Why flagged** + **My recommendation**
3. If you agree → tick `🚀 Accept all` at the country top, move on
4. If you want individual control → set per-row `Status` icons
5. Add `💬 comment` with your text where needed; add new findings
   under the country's "Reviewer findings" subsection
6. Tell me "go" → I sweep, apply `✅`, reply to `💬`, leave `❌` alone,
   tick the `[ ]` checkboxes in the same commit with hash links

---

## Historical (already applied)

Audit trail — these landed in earlier commits, kept for reference.

- [x] BHS — 3 remaps (Central Abaco, Central Andros, Freeport) + 1 caveat (Nassau) → `6daba6e`
- [x] USA — drop 902134 (Under National Administration → fed lands) → `6daba6e`
- [x] VGB — drop 39505 (Other Islands catch-all) → `6daba6e`
- [x] BHS — 3 shared_source caveats (Central Abaco↔Hope Town, San Salvador↔Rum Cay, South Andros↔Mangrove Cay) → `9a02e3f`
- [x] DOM — caveat for 901058 (national-lands polygon in SW DR) → `cb2e0bc`
- [x] Cleaned 12 redundant `[[per_row_notes]]` (CUB/NIC/ISL/GRL) → `cb2e0bc`

---

## ADAM — active review

### AIA — 10 candidates, all expected

**Country default**: 🚀 `[ ]` Accept all

- ⬜ **Aggregate policy expectation** — AIA uses `aggregate_adam_to_fm`.
  ADAM has 10 small villages all rolling up to FM's 1 island polygon.
  All 10 IoUs are low (0.003–0.25) because each village is tiny
  relative to the whole island. This is correct behavior for an
  aggregate policy.
  - **Status**: ⬜ pending
  - **Your comment**:

---

### ATG — 1 candidate, no action needed

**Country default**: 🚀 `[ ]` Accept all

- ⬜ **Redonda** (ATG-20191128-2) ← ADAM "Redonda" (iou 0.12)
  - **Why flagged**: low IoU (<0.3)
  - **My recommendation**: NO ACTION. Redonda is a tiny uninhabited
    rocky islet ~50 km SW of Antigua. Low IoU is because the polygon
    is small in absolute terms, not because the mapping is wrong —
    name match is exact (ge "Redonda" → FM "Redonda").
  - **Status**: ⬜ pending
  - **Your comment**:

---

### BHS — 9 candidates worth eyeballing

**Country default**: 🚀 `[ ]` Accept all

#### 7 FM units with no ADAM source (offshore cays / small islands)

These FM units have no ge_adm1 polygon representing them. ADAM
doesn't report data at this granularity. The `no_adam_at_adm1` caveat
already on each row is the right outcome.

- ⬜ **Berry Islands** (BHS-20201113-02), no ADAM source
  - **Why flagged**: NULL source
  - **My recommendation**: NO ACTION. ge_adm1 has no Berry Islands
    polygon; ADAM doesn't report exposure at this admin level. The
    `no_adam_at_adm1` caveat already explains this.
  - **Status**: ⬜ pending
  - **Your comment**:

- ⬜ **Black Point** (BHS-20201113-04), no ADAM source
  - Same as Berry Islands — small cay, no ge polygon, no action.
  - **Status**: ⬜ pending
  - **Your comment**:

- ⬜ **Grand Cay** (BHS-20201113-13), no ADAM source
  - Same — small cay, no action.
  - **Status**: ⬜ pending
  - **Your comment**:

- ⬜ **Harbour Island** (BHS-20201113-14), no ADAM source
  - Same — small cay in N. Eleuthera area, no action.
  - **Status**: ⬜ pending
  - **Your comment**:

- ⬜ **Moore's Island** (BHS-20201113-20), no ADAM source
  - Same — small Abaco cay, no action.
  - **Status**: ⬜ pending
  - **Your comment**:

- ⬜ **Ragged Island** (BHS-20201113-25), no ADAM source
  - Same — small SE Bahamas cay, no action.
  - **Status**: ⬜ pending
  - **Your comment**:

- ⬜ **Spanish Wells** (BHS-20201113-31), no ADAM source
  - Same — small N. Eleuthera cay, no action.
  - **Status**: ⬜ pending
  - **Your comment**:

#### 2 low-IoU rows worth a closer look

- ⬜ **North Andros** (BHS-20201113-23) ← ADAM "North Andros" (iou 0.26)
  - **Why flagged**: low IoU
  - **My recommendation**: NO ACTION. Name match is exact. Low IoU
    reflects that FM and ge draw the boundary between N/Central
    Andros differently — but ge "North Andros" → FM "North Andros"
    is the correct semantic mapping; that's why we explicitly
    remapped 901502 (ge Central Andros) to FM Central Andros earlier.
  - **Status**: ⬜ pending
  - **Your comment**:

- ⬜ **West Grand Bahama** (BHS-20201113-32) ← ADAM "West Grand Bahama" (iou 0.12)
  - **Why flagged**: low IoU
  - **My recommendation**: NO ACTION. Name match is exact. Same
    story as North Andros — ge and FM draw the West Grand Bahama
    boundary differently (in particular, ge "Freeport" overlaps part
    of it, which is why we remapped 901507 to FM City of Freeport
    earlier). The IoU here is low but the row is the right one.
  - **Status**: ⬜ pending
  - **Your comment**:

#### Reviewer findings

<!-- Add 🆕 findings here as `🆕 [ ] **...** ...` -->

---

### CYM — 6 NULL-source rows, by-design

**Country default**: 🚀 `[ ]` Accept all

- ⬜ **Policy expectation** — CYM is `fm_adm1_only` because ADAM's CYM
  polygon set doesn't break down to FM's 6 districts (Bodden Town,
  East End, George Town, North Side, Sister Islands, West Bay). All
  6 NULL-source rows are by-design output of the fm_adm1_only policy;
  ADAM exposure attaches at adm0 only.
  - **Status**: ⬜ pending
  - **Your comment**:

#### Reviewer findings

<!-- -->

---

### DOM — 21 candidates, all expected (aggregate)

**Country default**: 🚀 `[ ]` Accept all

- ⬜ **Aggregate policy expectation** — DOM uses
  `aggregate_adam_to_fm`. 33 ADAM provinces roll up to 10 FM regions.
  All 21 low-IoU candidates here are correct attachments (each
  province lives inside its FM region) — IoU is low because each
  province is small relative to the region. This is the same shape
  as ISL/IRL/PRT. No per-row action.
  - **Status**: ⬜ pending
  - **Your comment**:

#### Reviewer findings

<!-- -->

---

### GLP — 2 NULL-source rows, by-design

**Country default**: 🚀 `[ ]` Accept all

- ⬜ **Policy expectation** — GLP is `fm_adm1_only`. ADAM has 1
  polygon for Guadeloupe (no Basse-Terre / Pointe-à-Pitre split);
  FM has 2. Both FM rows get `no_adam_at_adm1` by design. ADAM
  exposure attaches at adm0 only.
  - **Status**: ⬜ pending
  - **Your comment**:

#### Reviewer findings

<!-- -->

---

### GRL — 6 NULL-source rows, by-design

**Country default**: 🚀 `[ ]` Accept all

- ⬜ **Policy expectation** — GRL is `fm_adm1_only` because ADAM's
  Greenland coverage uses pre-2009 districts (Vestgrønland /
  Ostgrønland) that don't match FM's 6 post-reform municipalities.
  All 6 FM rows get `no_adam_at_adm1` by design. (GDACS-side handles
  this via shared_source — see GDACS section when we get to it.)
  - **Status**: ⬜ pending
  - **Your comment**:

#### Reviewer findings

<!-- -->

---

### IRL — 24 candidates, all expected (aggregate)

**Country default**: 🚀 `[ ]` Accept all

- ⬜ **Aggregate policy expectation** — IRL uses
  `aggregate_adam_to_fm`. 26 ADAM counties roll up to 4 FM provinces
  (Connacht, Leinster, Munster, Ulster). All 24 low-IoU candidates
  are correct county→province assignments; IoU is low because each
  county is small relative to the province. No per-row action.
  - **Status**: ⬜ pending
  - **Your comment**:

#### Reviewer findings

<!-- -->

---

### ISL — 71 candidates, all expected (aggregate)

**Country default**: 🚀 `[ ]` Accept all

- ⬜ **Aggregate policy expectation** — ISL uses
  `aggregate_adam_to_fm`. 75 Icelandic municipalities roll up to 8
  FM regions. All 71 low-IoU candidates here are correct
  municipality→region assignments; IoU is low because municipalities
  are small relative to regions. No per-row action.
  - **Status**: ⬜ pending
  - **Your comment**:

#### Reviewer findings

<!-- -->

---

### JAM — 2 NULL-source rows (offshore cays)

**Country default**: 🚀 `[ ]` Accept all

- ⬜ **Pedro Bank / Morant Cays** — both FM rows with no ADAM source
  (JAM_2-20250729, JAM_3-20250729). These are offshore-cay FM admin
  records — Pedro Bank is a submerged bank with two tiny cays; Morant
  Cays are 4 small islets. Both are Jamaican sovereign territory but
  not assigned to any parish. ADAM doesn't break Jamaica down to
  these offshore features. There's an existing `[[per_row_notes]]`
  fm_offshore_orphan caveat for each (single-FM, not multi-FM, so
  per_row_notes is the right vocabulary here, not shared_source).
  - **My recommendation**: NO ACTION. The existing per_row_notes
    cover the explanation. The `no_adam_at_adm1` caveat that
    auto-emits is technically less specific than the per_row_notes
    one — could promote per_row_notes to override the auto-caveat,
    but the practical difference is small (both say "no ADAM here").
  - **Status**: ⬜ pending
  - **Your comment**:

#### Reviewer findings

<!-- -->

---

### PRT — 9 candidates, all expected (aggregate)

**Country default**: 🚀 `[ ]` Accept all

- ⬜ **Aggregate policy expectation** — PRT uses
  `aggregate_adam_to_fm`. 29 ADAM districts/islands roll up to 20 FM
  regions. The 9 low-IoU candidates are all the individual Azores
  and Madeira islands (Ilha De Porto Santo, Ilha Da Graciosa, etc.)
  aggregating into FM "Região Autónoma" units. Correct geographic
  rollup; IoU is low because each island is small relative to its
  archipelago region. No per-row action.
  - **Caveat worth flagging separately**: FM's Portugal data has a
    UTF-8 mojibake issue — `RegiÃ£o AutÃ³noma da Madeira` should be
    `Região Autónoma da Madeira`. This is an FM-source data quality
    issue, not a lookup issue. Worth raising upstream with FieldMaps
    but doesn't affect the lookup mechanics.
  - **Status**: ⬜ pending
  - **Your comment**:

#### Reviewer findings

<!-- -->

---

### UMI — 1 candidate, no action

**Country default**: 🚀 `[ ]` Accept all

- ⬜ **Kingman Reef** (UMI_5-20250729) ← ADAM "Kingman Reef" (iou 0.019)
  - **Why flagged**: low IoU
  - **My recommendation**: NO ACTION. Kingman Reef is mostly
    submerged with ~3 acres above water; the FM polygon is tiny.
    Name match is exact; low IoU is a small-polygon artifact, not a
    mapping error.
  - **Status**: ⬜ pending
  - **Your comment**:

#### Reviewer findings

<!-- -->

---

### VGB — 3 NULL-source rows

**Country default**: 🚀 `[ ]` Accept all

- ⬜ **uncertain** (VGB-20200401-1), no ADAM source
  - **Why flagged**: NULL source
  - **My recommendation**: NO ACTION. FM's "uncertain" row is FM's
    own catch-all placeholder for unassigned-territory exposure.
    There's no real ADAM equivalent; `no_adam_at_adm1` caveat is fine.
  - **Status**: ⬜ pending
  - **Your comment**:

- ⬜ **Cooper Island** (VGB-20200401-3), no ADAM source
  - **Why flagged**: NULL source
  - **My recommendation**: NO ACTION. Cooper Island is a small
    privately-held island in the BVI; ADAM doesn't carry it as a
    separate polygon. `no_adam_at_adm1` is correct.
  - **Status**: ⬜ pending
  - **Your comment**:

- ⬜ **Great Camanoe** (VGB-20200401-4), no ADAM source
  - **Why flagged**: NULL source
  - **My recommendation**: NO ACTION. Great Camanoe is a small
    sparsely-inhabited island in the BVI; ADAM doesn't carry it as
    a separate polygon. `no_adam_at_adm1` is correct.
  - **Status**: ⬜ pending
  - **Your comment**:

#### Reviewer findings

<!-- -->

---

### VIR — 3 NULL-source rows, by-design

**Country default**: 🚀 `[ ]` Accept all

- ⬜ **Policy expectation** — VIR is `fm_adm1_only` because ADAM has
  1 polygon for the US Virgin Islands; FM has 3 (St. Croix, St. John,
  St. Thomas). All 3 FM rows get `no_adam_at_adm1` by design. ADAM
  exposure attaches at adm0 only.
  - **Status**: ⬜ pending
  - **Your comment**:

#### Reviewer findings

<!-- -->

---

## GDACS — deferred

Same approach will apply once ADAM is buttoned up. The GDACS
shared_source pass already covers the 9 known boundary-reform cases
(commit `9a02e3f`); a full per-country review is still pending.
