"""Inputs needed to build the canonical FM↔ADAM admin lookup.

Mirror of :mod:`src.static.gdacs.inputs` for the ADAM side. ADAM (WFP)
reports per-event population exposure by admin name — no admin codes
in the CSV. The names come from WFP's own boundary layer
``ge_adm1.parquet`` (https://data.earthobservation.vam.wfp.org/
public-share/boundaries/ge_adm1.parquet), an OCHA/SALB-style global
COD composite. Same source as FieldMaps for many countries — but
*not* the same release, so admin granularity and naming diverge for
~10 countries (ISL: 75 ge_adm1 municipalities vs 8 FM regions; DOM:
33 provinces vs 10 regions; BHS: 22 vs 32 districts).

Scope (``ATLANTIC_ISO3``), FM loader (``load_fieldmaps_adm``),
column-name resolution (``fm_id_and_name_cols``), and the FM-side
adm0-name fallback for SJM/UMI are all reused from
:mod:`src.static.gdacs.inputs`. Two ADAM-specific helpers added:

- :func:`resolve_adam_fm_level` — which FM admin level to bridge to.
  Defaults to ``[overrides.<ISO3>].fm_level`` then
  ``[defaults].fm_level``. No ``[adam_overrides]`` block exists yet
  (YAGNI) — add one alongside ``gdacs_overrides`` if a country ever
  needs ADAM-specific level wiring distinct from GDACS.
- :func:`adam_policy_for` — convenience getter for
  ``[adam_policy.<ISO3>]``.
"""

from __future__ import annotations

# Re-export the shared pieces so callers can import everything from
# src.static.adam.inputs without crossing back into the gdacs module.
from src.static.gdacs.inputs import (  # noqa: F401
    ATLANTIC_ISO3,
    DEFAULT_CONFIG,
    FM_ADM1_NAME_FALLBACK_ISOS,
    fallback_fm_name_from_adm0,
    fm_id_and_name_cols,
    load_fieldmaps_adm,
    load_level_config,
)


def resolve_adam_fm_level(iso3: str, cfg: dict) -> int:
    """Look up the FM admin level to use for the FM↔ADAM bridge.

    Mirror of :func:`src.static.gdacs.inputs.resolve_gdacs_fm_level`
    but without the GDACS-specific override layer — ADAM uses the
    shared ``[overrides]`` block, falling back to ``[defaults]``.
    """
    ov = cfg.get("overrides", {}).get(iso3)
    if ov and "fm_level" in ov:
        return ov["fm_level"]
    return cfg["defaults"]["fm_level"]


def adam_policy_for(iso3: str, cfg: dict) -> dict:
    """Return ``[adam_policy.<ISO3>]`` table or ``{}`` if not set.

    Convenience getter so the build script doesn't repeat the
    ``cfg.get("adam_policy", {}).get(iso3, {})`` chain. Returns
    empty dict (NOT None) so ``policy.get("action", "country_only")``
    style lookups stay safe.
    """
    return cfg.get("adam_policy", {}).get(iso3, {})
