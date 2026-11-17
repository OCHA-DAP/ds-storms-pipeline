"""Refit the SWIO radii model and print COEF / SHARES for src/pipelines/swio_radii.py.

Reads Réunion's best track from the dev blob copy (or --path), fits on every
season with Réunion radii, and prints the constants as Python literals.

Usage
-----
    python scripts/fit_swio_radii.py [--path local.csv]
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.pipelines import swio, swio_radii  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--path", help="Local copy of the best-track file")
    a = ap.parse_args()
    if a.path:
        with open(a.path, "rb") as f:
            bt = swio.read_besttrack(f.read())
    else:
        bt = swio.load_besttrack("dev")
    coef, shares, dp_shift = swio_radii.fit(bt)
    print(f"DP_ERA_SHIFT = np.array([{', '.join(f'{x:.6f}' for x in dp_shift)}])")
    print("COEF = {")
    for (k, m), b in sorted(coef.items()):
        print(f"    ({k}, {m!r}): np.array([{', '.join(f'{x:.6f}' for x in b)}]),")
    print("}")
    print("SHARES = {")
    for k, s in sorted(shares.items()):
        print(f"    {k}: np.array([{', '.join(f'{x:.4f}' for x in s)}]),")
    print("}")
    for k in swio_radii.SPEEDS:
        R = swio_radii.mean_radius(bt, k)
        ok = (bt.season >= swio_radii.FIRST_RADII_SEASON[k]) & (bt.wind >= k) & (R > 0)
        print(f"# {k} kt: {int(ok.sum())} points, seasons {bt.season[ok].min()}-{bt.season[ok].max()}")


if __name__ == "__main__":
    main()
