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
- [x] ATG / AIA / BHS / CYM / DOM / IRL / JAM / PRT / TCA / UMI / VGB / VIR — all `🚀 Accept all` findings applied as NO ACTION → `08834d8`
- [x] GRL — policy note rewritten with "ADAM boundaries not compatible with OCHA CODs"; propagates as caveat_note on all 6 FM rows → `08834d8`
- [x] JAM offshore-cay rows (Pedro Bank, Morant Cays) — names now populated via FM_ADM1_NAME_FALLBACK_ISOS (was null) → `08834d8`
- [x] SPM — single FM row now has a name (was null) via the same fallback → `08834d8`
- [x] KNA — country-wide boundary-misalignment caveat now applied to all 14 FM rows via new needs_manual_mapping policy_note propagation → `08834d8`
- [x] MSR — note rewritten to compare ADAM vs FM directly (was framing as WFP vs ADAM) → `08834d8`
- [x] MTQ — changed from `needs_manual_mapping` to `accept` (names align, all IoUs ≥ 0.87) → `08834d8`
- [x] PRI — added `[adam_overrides.PRI] fm_level = 1` so ADAM bridge uses FM adm1 (= whole island) not the GDACS-side fm_level=2 → `08834d8`
- [x] 33 missing iso3 policy entries seeded — see Phase 3 section below → `08834d8`
- [x] GLP + ISL — both `🚀 Accept all` ticked, NO ACTION applied (no TOML change) → `8cd4c53`
- [x] MSR — reviewer pushback: switched country_only → accept (admin-layer match is clean; settlement-level exposure rows fail to join naturally) → `5b80945`

---

## ADAM — active review

### AIA — 10 candidates, all expected

**Country default**: 🚀 `[x]` Accept all

- [x] **Aggregate policy expectation** — AIA uses `aggregate_adam_to_fm`.
  ADAM has 10 small villages all rolling up to FM's 1 island polygon.
  All 10 IoUs are low (0.003–0.25) because each village is tiny
  relative to the whole island. This is correct behavior for an
  aggregate policy.
  - **Status**: ✅ apply *(accepted)*
  - **Your comment**:

---
 
### ATG — 1 candidate, no action needed

**Country default**: 🚀 `[x]` Accept all

- [x] **Redonda** (ATG-20191128-2) ← ADAM "Redonda" (iou 0.12)
  - **Why flagged**: low IoU (<0.3)
  - **My recommendation**: NO ACTION. Redonda is a tiny uninhabited
    rocky islet ~50 km SW of Antigua. Low IoU is because the polygon
    is small in absolute terms, not because the mapping is wrong —
    name match is exact (ge "Redonda" → FM "Redonda").
  - **Status**: ✅ apply *(accepted)*
  - **Your comment**:

---

### BHS — 9 candidates worth eyeballing

**Country default**: 🚀 `[x]` Accept all

#### 7 FM units with no ADAM source (offshore cays / small islands)

These FM units have no ge_adm1 polygon representing them. ADAM
doesn't report data at this granularity. The `no_adam_at_adm1` caveat
already on each row is the right outcome.

- [x] **Berry Islands** (BHS-20201113-02), no ADAM source
  - **Why flagged**: NULL source
  - **My recommendation**: NO ACTION. ge_adm1 has no Berry Islands
    polygon; ADAM doesn't report exposure at this admin level. The
    `no_adam_at_adm1` caveat already explains this.
  - **Status**: ✅ apply *(accepted)*
  - **Your comment**:

- [x] **Black Point** (BHS-20201113-04), no ADAM source
  - Same as Berry Islands — small cay, no ge polygon, no action.
  - **Status**: ✅ apply *(accepted)*
  - **Your comment**:

- [x] **Grand Cay** (BHS-20201113-13), no ADAM source
  - Same — small cay, no action.
  - **Status**: ✅ apply *(accepted)*
  - **Your comment**:

- [x] **Harbour Island** (BHS-20201113-14), no ADAM source
  - Same — small cay in N. Eleuthera area, no action.
  - **Status**: ✅ apply *(accepted)*
  - **Your comment**:

- [x] **Moore's Island** (BHS-20201113-20), no ADAM source
  - Same — small Abaco cay, no action.
  - **Status**: ✅ apply *(accepted)*
  - **Your comment**:

- [x] **Ragged Island** (BHS-20201113-25), no ADAM source
  - Same — small SE Bahamas cay, no action.
  - **Status**: ✅ apply *(accepted)*
  - **Your comment**:

- [x] **Spanish Wells** (BHS-20201113-31), no ADAM source
  - Same — small N. Eleuthera cay, no action.
  - **Status**: ✅ apply *(accepted)*
  - **Your comment**:

#### 2 low-IoU rows worth a closer look

- [x] **North Andros** (BHS-20201113-23) ← ADAM "North Andros" (iou 0.26)
  - **Why flagged**: low IoU
  - **My recommendation**: NO ACTION. Name match is exact. Low IoU
    reflects that FM and ge draw the boundary between N/Central
    Andros differently — but ge "North Andros" → FM "North Andros"
    is the correct semantic mapping; that's why we explicitly
    remapped 901502 (ge Central Andros) to FM Central Andros earlier.
  - **Status**: ✅ apply *(accepted)*
  - **Your comment**:

- [x] **West Grand Bahama** (BHS-20201113-32) ← ADAM "West Grand Bahama" (iou 0.12)
  - **Why flagged**: low IoU
  - **My recommendation**: NO ACTION. Name match is exact. Same
    story as North Andros — ge and FM draw the West Grand Bahama
    boundary differently (in particular, ge "Freeport" overlaps part
    of it, which is why we remapped 901507 to FM City of Freeport
    earlier). The IoU here is low but the row is the right one.
  - **Status**: ✅ apply *(accepted)*
  - **Your comment**:

#### Reviewer findings

<!-- Add 🆕 findings here as `🆕 [ ] **...** ...` -->

---

### CYM — 6 NULL-source rows, by-design

**Country default**: 🚀 `[x]` Accept all

- [x] **Policy expectation** — CYM is `fm_adm1_only` because ADAM's CYM
  polygon set doesn't break down to FM's 6 districts (Bodden Town,
  East End, George Town, North Side, Sister Islands, West Bay). All
  6 NULL-source rows are by-design output of the fm_adm1_only policy;
  ADAM exposure attaches at adm0 only.
  - **Status**: ✅ apply *(accepted)*
  - **Your comment**:

#### Reviewer findings

<!-- -->

---

### DOM — 21 candidates, all expected (aggregate)

**Country default**: 🚀 `[x]` Accept all

- [x] **Aggregate policy expectation** — DOM uses
  `aggregate_adam_to_fm`. 33 ADAM provinces roll up to 10 FM regions.
  All 21 low-IoU candidates here are correct attachments (each
  province lives inside its FM region) — IoU is low because each
  province is small relative to the region. This is the same shape
  as ISL/IRL/PRT. No per-row action.
  - **Status**: ✅ apply *(accepted via 🚀 Accept all)*
  - **Your comment**:

#### Reviewer findings
Just aggregate all ADAM DOM units correctly to the FM unit they compose

---

### GLP — 2 NULL-source rows, by-design

**Country default**: 🚀 `[x]` Accept all

- [x] **Policy expectation** — GLP is `fm_adm1_only`. ADAM has 1
  polygon for Guadeloupe (no Basse-Terre / Pointe-à-Pitre split);
  FM has 2. Both FM rows get `no_adam_at_adm1` by design. ADAM
  exposure attaches at adm0 only.
  - **Status**: ✅ apply *(accepted via 🚀 Accept all)*
  - **Your comment**:

#### Reviewer findings

Not sure what's going on here , I guess this should just be reported at
admin 0 level if adm1 is the same?

**My response**: Today's behavior is functionally what you describe —
`fm_adm1_only` emits both FM rows at adm1 with `adam_admin_id = NULL`,
so ADAM exposure can't attach at adm1 (it only joins where
`adam_admin_id` is not null). Net effect: ADAM exposure for GLP shows
up at adm0 only, and the 2 FM adm1 rows are just there as a
self-describing "ADAM can't disaggregate to this level" marker for
the lookup.

If you'd rather drop the 2 adm1 rows entirely (so a `SELECT * FROM
lookup WHERE iso3='GLP'` returns just the adm0 row), the action would
change to `country_only`. The behavioral difference is purely
cosmetic — same downstream answer. Leaving as `fm_adm1_only` for now
since it surfaces the FM units to the lookup; flip the bit later if
you want a tidier table. **Accepted as-is — applied in `8cd4c53`.**

---

### GRL — 6 NULL-source rows, by-design

**Country default**: 🚀 `[ ]` Accept all

- [x] **Policy expectation** — GRL is `fm_adm1_only` because ADAM's
  Greenland coverage uses pre-2009 districts (Vestgrønland /
  Ostgrønland) that don't match FM's 6 post-reform municipalities.
  All 6 FM rows get `no_adam_at_adm1` by design. (GDACS-side handles
  this via shared_source — see GDACS section when we get to it.)
  - **Status**: ✅ apply *(addressed via reviewer-requested caveat)*
  - **Your comment**:

#### Reviewer findings

Yeah this is correct, but we need a way that if exposed a note comes up 
that says ADAM boundaries are note compatible with OCHA COD's 

**My response**: Done. Rewrote the `[adam_policy.GRL].note` to read:
*"ADAM admin boundaries for Greenland use pre-2009 reform districts
(Vestgrønland / Ostgrønland) that are not compatible with OCHA COD /
FieldMaps post-reform municipalities (Avannaata, Kujalleq, Northeast
Greenland National Park, Qeqertalik, Qeqqata, Sermersooq). No usable
per-FM-unit crosswalk; ADAM exposure attaches at country (adm0) level
only."* That note propagates as `caveat_note` on all 6 GRL FM rows
via the `fm_adm1_only` build path, so any downstream consumer that
reads the lookup gets the OCHA-CODs-incompatibility warning attached
to every GRL adm1 row. **Applied in this commit.**
---

### IRL — 24 candidates, all expected (aggregate)

**Country default**: 🚀 `[x]` Accept all

- [x] **Aggregate policy expectation** — IRL uses
  `aggregate_adam_to_fm`. 26 ADAM counties roll up to 4 FM provinces
  (Connacht, Leinster, Munster, Ulster). All 24 low-IoU candidates
  are correct county→province assignments; IoU is low because each
  county is small relative to the province. No per-row action.
  - **Status**: ✅ apply *(accepted via 🚀 Accept all)*
  - **Your comment**:

#### Reviewer findings

<!-- -->

---

### ISL — 71 candidates, all expected (aggregate)

**Country default**: 🚀 `[x]` Accept all

- [x] **Aggregate policy expectation** — ISL uses
  `aggregate_adam_to_fm`. 75 Icelandic municipalities roll up to 8
  FM regions. All 71 low-IoU candidates here are correct
  municipality→region assignments; IoU is low because municipalities
  are small relative to regions. No per-row action.
  - **Status**: ✅ apply *(accepted via 🚀 Accept all)*
  - **Your comment**:

#### Reviewer findings

**My response (no reviewer finding here, just noting the status)**:
You didn't tick 🚀 Accept all for ISL, but didn't write a comment
either. Same situation as DOM / IRL / PRT — aggregate countries where
the recommendation is "no action, low IoUs are expected." I'm leaving
ISL as `pending` originally. If you want me to apply the same NO ACTION
treatment as the other aggregate countries, say so in the next pass
or tick the box. **Accepted (NO ACTION) — applied in `8cd4c53`.**

---

### JAM — 2 NULL-source rows (offshore cays)

**Country default**: 🚀 `[x]` Accept all

- [x] **Pedro Bank / Morant Cays** — both FM rows with no ADAM source
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
  - **Status**: ✅ apply *(accepted via 🚀 Accept all)*
  - **Your comment**:

#### Reviewer findings

yeah it looks like these are unnamed even in FM, maybe we can add
the names and keep just FM/OCHNA repoorting for them (no adam, as is)

---

### KNA

#### Reviewer Finding

- the auto matching is fine, but when we report at admin 1 for any admin in 
country we should be able to access a footnote that basically says the 
boundaries are pretty misaligned so comparisons are not direct and
proceed with caution

**My response**: Done — and in a way that generalizes. Extended the
build script so any country with policy `needs_manual_mapping`
propagates its policy note as `caveat_note` on every emitted lookup
row (both source-attached and NULL-source). Then rewrote
`[adam_policy.KNA].note` to be exactly the cautionary text you want:
*"ADAM and FM both publish KNA at the 14-parish level, but parish
boundaries differ noticeably between the two layers... the auto-
matching is the best-IoU pick per ADAM polygon, but FM↔ADAM parish
boundaries are not directly comparable — proceed with caution when
reporting any KNA adm1 exposure number side-by-side."* All 14 KNA FM
rows now carry that caveat. **Applied in this commit.**

---

### MSR

#### Reviewer Finding

- the QMD note doesnt make sense it's comparing WFP & ADAM which are the same
thing. We should be comparing ADAM (WFP) with FM - unclear that you have 
the right policy or not ni the QMD

**My response (first pass)**: Rewrote the note to compare ADAM-side
parishes directly with FM parishes. Kept policy as `country_only`
because ADAM's exposure CSVs emit settlement-level names. Applied in
`08834d8`.

**My response (second pass — reviewer pushback)**: You noted that
the qmd panel shows 3 ADAM admin polygons cleanly matching 3 FM
parishes — `country_only` was over-conservative. The data is:

- **ADAM admin layer (boundary reference)**: 3 parishes (Saint
  Anthony, Saint Georges, Saint Peter), match FM cleanly with mean
  IoU 0.96
- **ADAM exposure CSV feed**: 7 settlement names (Plymouth, Salem,
  Brodericks, Central, East, North, South) that don't match the
  parish names

Switched MSR to `accept`. The 3 parish-level lookup rows are now
emitted with the clean spatial match. ADAM exposure rows with
settlement names just naturally fail to join the lookup (no matching
adam_admin_name in the lookup) and drop from adm1 reporting — same
practical outcome as `country_only` for the current settlement-level
feed, but cleaner representation and ready to attach immediately if
WFP starts emitting parish-level data for MSR. **Applied in commit
`5b80945`.**

---

### MTQ

#### Reviewer Finding

- QMD is confusing. The  adm names in table look perfectly aligned,
but you wrote "need_manual_mapping" not sure why? or does table reflect manual 
mapping that already occured?

**My response**: You're right, I had the wrong action. Inspected the
data: all 4 FM communes match cleanly to 4 ADAM polygons by name
(Fort-de-France, Le Marin, Saint-Pierre, La Trinité — minor accent
diffs only) with IoUs 0.87–0.95. `needs_manual_mapping` was wrong;
changed to `accept`. The QMD table reflects the matcher output
directly, so what you saw was the actual mapping (which was already
fine — the policy label just didn't match the data quality). New
policy note documents the change. **Applied in this commit.**

---

### PRI

#### Reviewer Finding

- not sure why you wrote FM level : "adm2" . We want to report at FM adm1 
and FM admin 1 in this case is the same as adm0 so we can just report any
exposure at country level for both ADAM and FM. 

**My response**: Caused by an inherited setting. `[overrides.PRI]
fm_level = 2` exists for the GDACS↔GADM bridge (FM stores PRI's 78
municipios at adm2; GDACS uses 8 senatorial districts at adm1).
`resolve_adam_fm_level` was inheriting that fm_level=2 because no
ADAM-specific override existed. Fixed by:

1. Adding a new `[adam_overrides]` block to the TOML, parallel to
   `[gdacs_overrides]`.
2. `resolve_adam_fm_level` now checks `[adam_overrides]` first, then
   falls back to `[overrides]`, then `[defaults]`.
3. Added `[adam_overrides.PRI] fm_level = 1` — so the ADAM bridge
   uses FM adm1 (= single whole-island polygon = same as adm0 for
   PRI's case).

Policy remains `country_only` because ADAM emits 1 row for PRI
(country-level only). End result: ADAM exposure attaches at adm0
only, the qmd no longer says "FM level: adm2" for PRI, and the
GDACS-side fm_level=2 behavior is untouched. **Applied in this
commit.**

---

### PRT — 9 candidates, all expected (aggregate)

**Country default**: 🚀 `[x]` Accept all

- [x] **Aggregate policy expectation** — PRT uses
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
  - **Status**: ✅ apply *(accepted via 🚀 Accept all)*
  - **Your comment**:

#### Reviewer findings

yes a good amount are direct 1 to 1, the others should be rolled up.
---


### SPM

#### Reviewer Finding

Looks like FM names are blank - can you look into this

**My response**: Fixed. FM's SPM record has `adm1_name = null` and
puts the actual name in `adm0_name` as "Saint Pierre and Miquelon
(Fr.)" — same data pattern as SJM and UMI. Added SPM and JAM to the
`FM_ADM1_NAME_FALLBACK_ISOS` allowlist so the build script falls back
to adm0_name (with the trailing " (XYZ)" annotation stripped) when
adm1_name is null. SPM's FM row in the lookup now shows fm_name =
"Saint Pierre and Miquelon" instead of NaN. **Applied in this
commit.**

---

### TCA

#### Reviewer Finding

Matches in QMD are correct

**My response**: Acknowledged, no change needed. TCA stays as
`accept` — the auto-matching is correct. Marked accepted.

---

### UMI — 1 candidate, no action

**Country default**: 🚀 `[x]` Accept all

- [x] **Kingman Reef** (UMI_5-20250729) ← ADAM "Kingman Reef" (iou 0.019)
  - **Why flagged**: low IoU
  - **My recommendation**: NO ACTION. Kingman Reef is mostly
    submerged with ~3 acres above water; the FM polygon is tiny.
    Name match is exact; low IoU is a small-polygon artifact, not a
    mapping error.
  - **Status**: ✅ apply *(accepted)*
  - **Your comment**:

#### Reviewer findings

<!-- -->

---

### VGB — 3 NULL-source rows

**Country default**: 🚀 `[x]` Accept all

- [x] **uncertain** (VGB-20200401-1), no ADAM source
  - **Why flagged**: NULL source
  - **My recommendation**: NO ACTION. FM's "uncertain" row is FM's
    own catch-all placeholder for unassigned-territory exposure.
    There's no real ADAM equivalent; `no_adam_at_adm1` caveat is fine.
  - **Status**: ✅ apply *(accepted via 🚀 Accept all)*
  - **Your comment**:

- [x] **Cooper Island** (VGB-20200401-3), no ADAM source
  - **Why flagged**: NULL source
  - **My recommendation**: NO ACTION. Cooper Island is a small
    privately-held island in the BVI; ADAM doesn't carry it as a
    separate polygon. `no_adam_at_adm1` is correct.
  - **Status**: ✅ apply *(accepted via 🚀 Accept all)*
  - **Your comment**:

- [x] **Great Camanoe** (VGB-20200401-4), no ADAM source
  - **Why flagged**: NULL source
  - **My recommendation**: NO ACTION. Great Camanoe is a small
    sparsely-inhabited island in the BVI; ADAM doesn't carry it as
    a separate polygon. `no_adam_at_adm1` is correct.
  - **Status**: ✅ apply *(accepted via 🚀 Accept all)*
  - **Your comment**:

#### Reviewer findings

<!-- -->

---

### VIR — 3 NULL-source rows, by-design

**Country default**: 🚀 `[x]` Accept all

- [x] **Policy expectation** — VIR is `fm_adm1_only` because ADAM has
  1 polygon for the US Virgin Islands; FM has 3 (St. Croix, St. John,
  St. Thomas). All 3 FM rows get `no_adam_at_adm1` by design. ADAM
  exposure attaches at adm0 only.
  - **Status**: ✅ apply *(accepted via 🚀 Accept all)*
  - **Your comment**:

#### Reviewer findings

<!-- -->

---

## Phase 3 — 33 newly-seeded ATLANTIC_ISO3 countries

These 33 iso3s had no explicit `[adam_policy]` entry before and were
silently falling through to `country_only`. They now have explicit
policies so they render in the qmd and the runtime behavior is
documented. Auto-classified from the matcher diagnostic; sensible
defaults but worth a second look as ADAM data starts flowing for
these countries.

**`accept` (20 countries, clean 1:1 or minor noise)**:
ABW · BEL · BLZ · COL · CPV · CRI · CUW · DEU · GBR · GIB · GTM ·
GUF · GUY · LCA · MAR · RUS · SUR · SWE · SXM · VEN

**`aggregate_adam_to_fm` (5 — ADAM finer than FM)**:
FRA · HND · JEY · NIC · NOR

**`fm_adm1_only` (3 — ADAM too coarse)**: GRD · LUX · SJM

**`needs_manual_mapping` (5 — country-wide caveat propagation)**:
BRB · PAN · SLV · TTO · VCT

Each entry has a brief rationale note in the TOML (see
`[adam_policy.<ISO3>]` blocks). All now appear in the qmd as their
own per-country panels. If you find an issue with any specific
country, add a section above with `### <ISO3>` and your finding;
I'll respond and apply in the next pass.

---

## GDACS — deferred

Same approach will apply once ADAM is buttoned up. The GDACS
shared_source pass already covers the 9 known boundary-reform cases
(commit `9a02e3f`); a full per-country review is still pending.
