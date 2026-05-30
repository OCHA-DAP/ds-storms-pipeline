# FM ↔ ADAM / GDACS review checklist

Persistent scratch space for review findings on the canonical lookups.
Edit this file as you scroll the qmd. Each entry should have:

- **a checkbox** (`[ ]` open, `[x]` applied)
- **what** the finding is (iso3, row, the diagnostic)
- **proposed action** as a TOML snippet you can copy from the qmd's
  per-country "Review-candidate snippets" block
- **status** if applied: link to the commit that applied it

Workflow:

1. Scroll `exploration/review_report_adam.html` (latest render)
2. For each panel, scan the per-row table + the "Review-candidate
   snippets" disclosure at the bottom
3. If a row needs attention, copy its snippet into the relevant
   country section below, edit the placeholders (`action`,
   `fm_pcode_override`, `note`)
4. Tell me you're done with a batch (or push the edited file)
5. I sweep the open items, apply them to
   `config/adm_level_config.toml`, rebuild + re-render, check the
   boxes here in the same commit

Items marked `[x]` are kept as audit trail — don't delete them.

---

## ADAM

### BHS

- [x] **ge "Central Abaco" (901501) → remap to FM Central Abaco** —
      applied in commit `6daba6e`. IoU pick was FM Hope Town (0.16); name match
      is unambiguous.
- [x] **ge "Central Andros" (901502) → remap to FM Central Andros** —
      applied in commit `6daba6e`.
- [x] **ge "Freeport" (901507) → remap to FM City of Freeport** —
      applied in commit `6daba6e`.
- [x] **ge "Under National Administration" (901518) → caveat (Nassau)** —
      applied in commit `6daba6e`.
- [x] **Shared-source caveats** (Central Abaco↔Hope Town,
      San Salvador↔Rum Cay, South Andros↔Mangrove Cay) — applied in
      `9a02e3f`.

### DOM

- [x] **ge "Under National Administration" (901058) → caveat** —
      small national-lands polygon (Sierra de Bahoruco / Lago
      Enriquillo), aggregate-spatially attaches to Región Enriquillo
      correctly but source name is opaque. Caveat applied.

### USA

- [x] **ge "Under National Administration" (902134) → drop** —
      placeholder for federal/unincorporated lands; IoU picked Michigan
      by spatial drift. Dropped.

### VGB

- [x] **ge "Other Islands" (39505) → drop** — catch-all placeholder.

### _open items go below_

<!-- Add per-iso3 H3 headings as you find things. Example:

### NIC

- [ ] **NIC fm_pcode=NIC-XXX (Río San Juan)** — IoU 0.05, looks wrong.
      ```toml
      [[adam_row_overrides]]
      iso3 = "NIC"
      adam_admin_id = ...
      action = "remap"
      fm_pcode_override = "NIC-XXX"
      note = "..."
      ```
-->

---

## GDACS

The GDACS lookup is more mature (it shipped in PR #17 first). Most
boundary-reform cases are now covered by `[[gdacs_shared_source]]`.
Open items here would be new findings since that work.

### _open items go below_

<!-- Same pattern as ADAM section above. -->
