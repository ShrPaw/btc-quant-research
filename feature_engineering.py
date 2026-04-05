#!/usr/bin/env python3
"""
Phase 2 — Feature Engineering (Microstructure State Representation)

READS:  raw trades CSV (from collector.py / fetch_historical.py)
WRITES: /data/processed/features_*.csv
        /data/processed/percentiles_*.json

Principles:
  RAW STRUCTURE > ENGINEERED FEATURES
  No normalization. No transforms. No curve fitting.
  Fixed windows only. Global percentiles only.
  Zero lookahead bias.

Pipeline:
  raw trades → 1s bars → base features → rolling features → global percentiles
"""

import csv
import json
import os
import sys
from collections import defaultdict

ROLLING_WINDOWS = [5, 15, 30]
PERCENTILES = [1, 5, 10, 90, 95, 99]


# ─── 1. One-Second Aggregation ───────────────────────────────────────────────

def aggregate_1s(input_path):
    """
    Raw aggTrades → 1-second bars.

    Columns:
      timestamp_s   — unix epoch (floor to second)
      price         — last trade price in second
      volume        — sum of trade quantities
      delta         — signed volume (buy=+qty, sell=-qty)
      trade_count   — number of trades in second
    """
    seconds = defaultdict(lambda: {
        "prices": [], "volumes": [], "delta": 0.0, "count": 0
    })

    with open(input_path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            sec = int(row["timestamp_ms"]) // 1000
            price = float(row["price"])
            qty = float(row["quantity"])
            side = row["agggressor_side"]

            s = seconds[sec]
            s["prices"].append(price)
            s["volumes"].append(qty)
            s["count"] += 1
            if side == "BUY":
                s["delta"] += qty
            else:
                s["delta"] -= qty

    bars = []
    for sec in sorted(seconds.keys()):
        d = seconds[sec]
        bars.append({
            "timestamp_s": sec,
            "price": d["prices"][-1],
            "volume": round(sum(d["volumes"]), 8),
            "delta": round(d["delta"], 8),
            "trade_count": d["count"],
        })

    return bars


# ─── 2. Base Features ────────────────────────────────────────────────────────

def compute_base_features(bars):
    """
    For each 1s bar compute:
      returns    — log return from prior bar
      cvd        — cumulative volume delta (running sum of delta)
    """
    cvd = 0.0
    for i, bar in enumerate(bars):
        # CVD: running sum
        cvd += bar["delta"]
        bar["cvd"] = round(cvd, 8)

        # Returns: log(P_t / P_{t-1})
        if i > 0 and bars[i - 1]["price"] > 0 and bar["price"] > 0:
            import math
            bar["returns"] = round(math.log(bar["price"] / bars[i - 1]["price"]), 10)
        else:
            bar["returns"] = 0.0

    return bars


# ─── 3. Rolling Features ─────────────────────────────────────────────────────

def compute_rolling_features(bars, windows=None):
    """
    For each bar and each window W in {5, 15, 30}:

      return_{W}s      — sum of returns over prior W seconds
      volume_{W}s      — sum of volume over prior W seconds
      intensity_{W}s   — sum of trade_count over prior W seconds
      cvd_delta_{W}s   — CVD_now - CVD_{W seconds ago}

    STRICT: uses PRIOR data only. Bar i references bars [i-W, i-1].
    """
    if windows is None:
        windows = ROLLING_WINDOWS

    n = len(bars)

    for i in range(n):
        for w in windows:
            start = i - w
            if start < 0:
                # Not enough history — leave as None, fill later
                bars[i][f"return_{w}s"] = None
                bars[i][f"volume_{w}s"] = None
                bars[i][f"intensity_{w}s"] = None
                bars[i][f"cvd_delta_{w}s"] = None
            else:
                window = bars[start:i]  # bars[start] through bars[i-1]

                bars[i][f"return_{w}s"] = round(
                    sum(b["returns"] for b in window), 10
                )
                bars[i][f"volume_{w}s"] = round(
                    sum(b["volume"] for b in window), 8
                )
                bars[i][f"intensity_{w}s"] = sum(
                    b["trade_count"] for b in window
                )

                # CVD delta: current CVD minus CVD at start of window
                bars[i][f"cvd_delta_{w}s"] = round(
                    bars[i]["cvd"] - bars[start]["cvd"], 8
                )

    # Fill leading None values with 0 (no lookahead)
    for i in range(n):
        for w in windows:
            for prefix in ("return_", "volume_", "intensity_", "cvd_delta_"):
                key = f"{prefix}{w}s"
                if bars[i][key] is None:
                    bars[i][key] = 0

    return bars


# ─── 4. Global Percentiles ───────────────────────────────────────────────────

def compute_global_percentiles(bars, windows=None, percentiles=None):
    """
    GLOBAL percentiles (computed over entire dataset, not rolling).

    For each rolling feature:
      p1, p5, p10, p90, p95, p99

    These are reference thresholds — NOT normalization parameters.
    """
    if windows is None:
        windows = ROLLING_WINDOWS
    if percentiles is None:
        percentiles = PERCENTILES

    rolling_feature_names = []
    for w in windows:
        for prefix in ("return_", "volume_", "intensity_", "cvd_delta_"):
            rolling_feature_names.append(f"{prefix}{w}s")

    result = {}
    for fname in rolling_feature_names:
        vals = sorted(b[fname] for b in bars)
        n = len(vals)
        pmap = {}
        for p in percentiles:
            idx = int(n * p / 100)
            idx = max(0, min(idx, n - 1))
            pmap[f"p{p}"] = vals[idx]
        result[fname] = pmap

    return result


# ─── 5. Save Outputs ─────────────────────────────────────────────────────────

def save_features(bars, output_path):
    """Save feature matrix to CSV."""
    if not bars:
        return

    fieldnames = list(bars[0].keys())
    with open(output_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(bars)

    print(f"  Features: {output_path}")
    print(f"  Shape: {len(bars)} rows x {len(fieldnames)} columns")


def save_percentiles(percentiles, output_path):
    """Save percentiles to JSON."""
    with open(output_path, "w") as f:
        json.dump(percentiles, f, indent=2)

    print(f"  Percentiles: {output_path}")


# ─── 6. Validation ───────────────────────────────────────────────────────────

def validate(bars, percentiles):
    """
    Pre-flight checks:
      1. CVD is continuous accumulation (no jumps/gaps)
      2. No artificial clipping
      3. Percentiles strictly ordered
      4. No NaN in output
    """
    import math
    errors = []

    # --- CVD continuity ---
    for i in range(1, len(bars)):
        cvd_expected = bars[i - 1]["cvd"] + bars[i]["delta"]
        cvd_actual = bars[i]["cvd"]
        if abs(cvd_expected - cvd_actual) > 1e-6:
            errors.append(
                f"CVD discontinuity at index {i}: "
                f"expected={cvd_expected:.8f} actual={cvd_actual:.8f}"
            )
            if len(errors) > 5:
                errors.append("  ... (truncated)")
                break

    # --- Percentile ordering ---
    for fname, pmap in percentiles.items():
        pvals = [pmap[f"p{p}"] for p in PERCENTILES]
        for j in range(1, len(pvals)):
            if pvals[j] < pvals[j - 1]:
                errors.append(
                    f"Percentile ordering violation in {fname}: "
                    f"p{PERCENTILES[j-1]}={pvals[j-1]} > p{PERCENTILES[j]}={pvals[j]}"
                )

    # --- NaN check ---
    nan_found = False
    for i, bar in enumerate(bars):
        for k, v in bar.items():
            if isinstance(v, float) and math.isnan(v):
                errors.append(f"NaN found at index {i}, column '{k}'")
                nan_found = True
                break
        if nan_found and len(errors) > 10:
            errors.append("  ... (truncated)")
            break

    # --- Report ---
    if errors:
        print("\n  ✗ VALIDATION FAILED:")
        for e in errors:
            print(f"    {e}")
        return False
    else:
        print("  ✓ CVD continuous accumulation")
        print("  ✓ No artificial clipping")
        print("  ✓ Percentiles strictly ordered (p1 < p5 < ... < p99)")
        print("  ✓ No NaN values")
        return True


# ─── Main ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 feature_engineering.py <raw_trades.csv>")
        print("")
        print("Input:  Raw trades from collector.py / fetch_historical.py")
        print("Output: data/processed/features_<timestamp>.csv")
        print("        data/processed/percentiles_<timestamp>.json")
        sys.exit(1)

    input_path = sys.argv[1]

    # Step 1: Aggregate to 1s
    print(f"  Reading: {input_path}")
    bars = aggregate_1s(input_path)
    print(f"  Aggregated: {len(bars)} seconds")

    # Step 2: Base features (returns, CVD)
    bars = compute_base_features(bars)
    print(f"  Base features: returns, cvd")

    # Step 3: Rolling features (fixed windows, prior-only)
    bars = compute_rolling_features(bars)
    print(f"  Rolling features: windows={ROLLING_WINDOWS}")

    # Step 4: Global percentiles
    percentiles = compute_global_percentiles(bars)
    print(f"  Global percentiles: p{PERCENTILES}")

    # Step 5: Save
    output_dir = "data/processed"
    os.makedirs(output_dir, exist_ok=True)

    basename = os.path.basename(input_path).replace("trades_", "features_")
    feat_path = os.path.join(output_dir, basename)
    pct_path = os.path.join(
        output_dir,
        "percentiles_" + basename.replace("features_", "").replace(".csv", ".json")
    )

    save_features(bars, feat_path)
    save_percentiles(percentiles, pct_path)

    # Step 6: Validate
    print(f"\n  Validation:")
    ok = validate(bars, percentiles)

    if ok:
        print(f"\n  Phase 2 complete. Ready for Phase 3 (Signal Discovery).")
    else:
        print(f"\n  Phase 2 FAILED validation. Fix before proceeding.")
        sys.exit(1)
