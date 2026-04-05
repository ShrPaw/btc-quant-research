#!/usr/bin/env python3
"""
Phase 3 — Data Integrity Audit

READS:  features CSV (from feature_engineering.py)
OUTPUT: summary report (printed) + optional stats CSV

Pure descriptive analysis. No modeling. No signals. No optimization.

Checks:
  1. Distribution properties (mean, std, skew, kurtosis)
  2. Temporal stability (drift across chunks)
  3. Regime shifts (high-vol, high-intensity distributions)
  4. Feature correlation (redundancy detection)
"""

import csv
import json
import math
import os
import sys


# ─── Load ─────────────────────────────────────────────────────────────────────

def load_features(path):
    """Load features CSV into list of dicts."""
    rows = []
    with open(path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            parsed = {}
            for k, v in row.items():
                try:
                    parsed[k] = float(v)
                except (ValueError, TypeError):
                    parsed[k] = v
            rows.append(parsed)
    return rows


def get_feature_names(rows):
    """Extract numeric feature column names (exclude timestamp)."""
    skip = {"timestamp_s", "timestamp_utc"}
    return [k for k in rows[0].keys()
            if k not in skip and isinstance(rows[0][k], (int, float))]


# ─── 1. Distribution Analysis ────────────────────────────────────────────────

def moment4(values):
    """Compute mean, std, skewness, kurtosis (excess)."""
    n = len(values)
    if n < 4:
        return {"mean": 0, "std": 0, "skewness": 0, "kurtosis": 0}

    mean = sum(values) / n
    m2 = sum((x - mean) ** 2 for x in values) / n
    std = math.sqrt(m2)

    if std < 1e-15:
        return {"mean": mean, "std": 0, "skewness": 0, "kurtosis": 0}

    m3 = sum((x - mean) ** 3 for x in values) / n
    m4 = sum((x - mean) ** 4 for x in values) / n

    skewness = m3 / (std ** 3)
    # Excess kurtosis (normal = 0)
    kurtosis = m4 / (std ** 4) - 3.0

    return {"mean": mean, "std": std, "skewness": skewness, "kurtosis": kurtosis}


def distribution_report(rows, feature_names):
    """Compute distribution stats for each feature."""
    report = {}
    for fname in feature_names:
        vals = [r[fname] for r in rows]
        report[fname] = moment4(vals)
    return report


def print_distribution_table(report):
    """Pretty-print distribution analysis."""
    print(f"\n{'='*80}")
    print(f"  1. DISTRIBUTION ANALYSIS")
    print(f"{'='*80}")
    print(f"  {'Feature':<25} {'Mean':>12} {'Std':>12} {'Skew':>10} {'Kurt':>10} Flags")
    print(f"  {'-'*78}")

    flags = []
    for fname, m in sorted(report.items()):
        f = []
        if abs(m["skewness"]) > 2:
            f.append(f"skew={m['skewness']:+.1f}")
        if m["kurtosis"] > 10:
            f.append(f"kurt={m['kurtosis']:.1f}")
        if m["std"] == 0:
            f.append("CONSTANT")

        flag_str = ", ".join(f) if f else ""
        print(f"  {fname:<25} {m['mean']:>12.6f} {m['std']:>12.6f} "
              f"{m['skewness']:>10.2f} {m['kurtosis']:>10.2f} {flag_str}")

        if f:
            flags.append((fname, f))

    if flags:
        print(f"\n  ⚠ Distribution flags:")
        for fname, f in flags:
            print(f"    {fname}: {', '.join(f)}")
    else:
        print(f"\n  ✓ No extreme distribution anomalies detected.")

    return flags


# ─── 2. Temporal Stability ───────────────────────────────────────────────────

def temporal_stability(rows, feature_names, n_chunks=5):
    """
    Split dataset into n_chunks equal parts.
    Recompute mean/std per chunk. Detect drift.
    """
    n = len(rows)
    chunk_size = n // n_chunks
    if chunk_size < 2:
        return {}

    chunks = []
    for i in range(n_chunks):
        start = i * chunk_size
        end = start + chunk_size if i < n_chunks - 1 else n
        chunks.append(rows[start:end])

    result = {}
    for fname in feature_names:
        chunk_stats = []
        for chunk in chunks:
            vals = [r[fname] for r in chunk]
            m = moment4(vals)
            chunk_stats.append({"mean": m["mean"], "std": m["std"]})

        # Drift: max mean - min mean across chunks, normalized by overall std
        all_vals = [r[fname] for r in rows]
        overall = moment4(all_vals)
        overall_std = overall["std"] if overall["std"] > 1e-15 else 1.0

        means = [c["mean"] for c in chunk_stats]
        mean_range = max(means) - min(means)
        drift_ratio = mean_range / overall_std

        result[fname] = {
            "chunks": chunk_stats,
            "mean_drift_range": mean_range,
            "drift_ratio": drift_ratio,
            "stable": drift_ratio < 2.0,
        }

    return result


def print_stability_table(stability):
    """Pretty-print temporal stability."""
    print(f"\n{'='*80}")
    print(f"  2. TEMPORAL STABILITY ({len(list(stability.values())[0]['chunks'])} chunks)")
    print(f"{'='*80}")
    print(f"  {'Feature':<25} {'Drift Ratio':>12} {'Status':>10}")
    print(f"  {'-'*50}")

    unstable = []
    for fname, s in sorted(stability.items()):
        status = "✓ stable" if s["stable"] else "✗ DRIFT"
        print(f"  {fname:<25} {s['drift_ratio']:>12.2f} {status:>10}")
        if not s["stable"]:
            unstable.append(fname)

    if unstable:
        print(f"\n  ⚠ Features with significant drift: {len(unstable)}")
        for f in unstable:
            print(f"    {f}: drift_ratio={stability[f]['drift_ratio']:.2f}")
    else:
        print(f"\n  ✓ All features temporally stable (drift ratio < 2σ).")

    return unstable


# ─── 3. Regime Detection (Descriptive) ───────────────────────────────────────

def regime_analysis(rows, feature_names):
    """
    Identify regimes:
      - High volatility: top 10% of |return_30s|
      - High intensity: top 10% of intensity_30s

    Compare feature distributions in-regime vs out-of-regime.
    """
    n = len(rows)

    # Need at least 10 rows
    if n < 10:
        return {}

    # Regime flags
    def top_10(values):
        sorted_vals = sorted(values)
        idx = int(len(sorted_vals) * 0.9)
        return sorted_vals[idx]

    regimes = {}

    # High volatility regime
    if "return_30s" in feature_names:
        abs_rets = [abs(r["return_30s"]) for r in rows]
        threshold = top_10(abs_rets)
        in_regime = [i for i in range(n) if abs_rets[i] >= threshold]
        out_regime = [i for i in range(n) if abs_rets[i] < threshold]
        regimes["high_vol"] = {"in": in_regime, "out": out_regime, "threshold": threshold}

    # High intensity regime
    if "intensity_30s" in feature_names:
        intensities = [r["intensity_30s"] for r in rows]
        threshold = top_10(intensities)
        in_regime = [i for i in range(n) if intensities[i] >= threshold]
        out_regime = [i for i in range(n) if intensities[i] < threshold]
        regimes["high_intensity"] = {"in": in_regime, "out": out_regime, "threshold": threshold}

    # Compare distributions in vs out of each regime
    regime_stats = {}
    for regime_name, regime in regimes.items():
        stats = {}
        for fname in feature_names:
            in_vals = [rows[i][fname] for i in regime["in"]]
            out_vals = [rows[i][fname] for i in regime["out"]]

            in_m = moment4(in_vals) if len(in_vals) >= 4 else None
            out_m = moment4(out_vals) if len(out_vals) >= 4 else None

            if in_m and out_m:
                # Mean shift: difference in means normalized by pooled std
                pooled_std = math.sqrt(
                    (in_m["std"] ** 2 + out_m["std"] ** 2) / 2
                ) if (in_m["std"] + out_m["std"]) > 1e-15 else 1.0

                mean_shift = (in_m["mean"] - out_m["mean"]) / pooled_std
                stats[fname] = {
                    "in_mean": in_m["mean"], "in_std": in_m["std"],
                    "out_mean": out_m["mean"], "out_std": out_m["std"],
                    "mean_shift": mean_shift,
                }
        regime_stats[regime_name] = stats

    return regime_stats


def print_regime_table(regime_stats):
    """Pretty-print regime analysis."""
    print(f"\n{'='*80}")
    print(f"  3. REGIME ANALYSIS (descriptive)")
    print(f"{'='*80}")

    for regime_name, stats in regime_stats.items():
        print(f"\n  Regime: {regime_name}")
        print(f"  {'Feature':<25} {'In-Regime':>12} {'Out-Regime':>12} {'Shift':>10}")
        print(f"  {'-'*62}")

        shifted = []
        for fname, s in sorted(stats.items()):
            shift = s["mean_shift"]
            flag = " ◄" if abs(shift) > 1.0 else ""
            print(f"  {fname:<25} {s['in_mean']:>12.6f} {s['out_mean']:>12.6f} "
                  f"{shift:>+10.2f}{flag}")
            if abs(shift) > 1.0:
                shifted.append((fname, shift))

        if shifted:
            print(f"\n    Features shifting >1σ in {regime_name}:")
            for f, s in shifted:
                print(f"      {f}: {s:+.2f}σ")
        else:
            print(f"\n    ✓ No features shift >1σ in {regime_name}.")


# ─── 4. Feature Correlation ──────────────────────────────────────────────────

def compute_correlation(rows, feature_names):
    """Pearson correlation matrix between all features."""
    n = len(rows)
    nf = len(feature_names)

    # Extract columns
    columns = {}
    for fname in feature_names:
        columns[fname] = [r[fname] for r in rows]

    # Compute means
    means = {}
    for fname in feature_names:
        means[fname] = sum(columns[fname]) / n

    # Compute correlation
    corr = {}
    for i, fi in enumerate(feature_names):
        corr[fi] = {}
        for fj in feature_names:
            xi = columns[fi]
            xj = columns[fj]
            mi = means[fi]
            mj = means[fj]

            cov = sum((xi[k] - mi) * (xj[k] - mj) for k in range(n)) / n
            si = math.sqrt(sum((xi[k] - mi) ** 2 for k in range(n)) / n)
            sj = math.sqrt(sum((xj[k] - mj) ** 2 for k in range(n)) / n)

            if si > 1e-15 and sj > 1e-15:
                corr[fi][fj] = round(cov / (si * sj), 6)
            else:
                corr[fi][fj] = 0.0

    return corr


def print_correlation_table(corr, feature_names):
    """Pretty-print correlation matrix and flag redundancy."""
    print(f"\n{'='*80}")
    print(f"  4. FEATURE CORRELATION")
    print(f"{'='*80}")

    # Find highly correlated pairs (|r| > 0.9)
    redundant = []
    seen = set()
    for fi in feature_names:
        for fj in feature_names:
            if fi >= fj:
                continue
            key = (fi, fj)
            if key in seen:
                continue
            seen.add(key)
            r = corr[fi][fj]
            if abs(r) > 0.9:
                redundant.append((fi, fj, r))

    if redundant:
        print(f"\n  ⚠ Highly correlated pairs (|r| > 0.9):")
        for fi, fj, r in sorted(redundant, key=lambda x: -abs(x[2])):
            print(f"    {fi} <-> {fj}: r = {r:+.4f}")
    else:
        print(f"\n  ✓ No redundant feature pairs (all |r| <= 0.9).")

    # Print condensed matrix
    print(f"\n  Correlation matrix (lower triangle):")
    short_names = [f.replace("_", "")[:10] for f in feature_names]
    header = f"  {'':>12}" + "".join(f"{sn:>12}" for sn in short_names)
    print(header)

    for i, fi in enumerate(feature_names):
        sn = short_names[i]
        row = f"  {sn:>12}"
        for j in range(i + 1):
            fj = feature_names[j]
            r = corr[fi][fj]
            if i == j:
                row += f"{'1.000':>12}"
            else:
                row += f"{r:>+12.3f}"
        print(row)

    return redundant


# ─── Save Stats CSV ──────────────────────────────────────────────────────────

def save_stats_csv(dist_report, stability, output_path):
    """Save per-feature stats to CSV."""
    feature_names = sorted(dist_report.keys())

    with open(output_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow([
            "feature", "mean", "std", "skewness", "kurtosis",
            "drift_ratio", "stable"
        ])
        for fname in feature_names:
            d = dist_report[fname]
            s = stability.get(fname, {})
            w.writerow([
                fname,
                f"{d['mean']:.10f}",
                f"{d['std']:.10f}",
                f"{d['skewness']:.4f}",
                f"{d['kurtosis']:.4f}",
                f"{s.get('drift_ratio', 0):.4f}",
                s.get("stable", True),
            ])

    print(f"\n  Stats CSV: {output_path}")


# ─── Main ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 data_audit.py <features.csv>")
        print("")
        print("Input:  Output of feature_engineering.py")
        print("Output: Summary report + optional stats CSV")
        sys.exit(1)

    input_path = sys.argv[1]

    print(f"  Loading: {input_path}")
    rows = load_features(input_path)
    feature_names = get_feature_names(rows)
    print(f"  Loaded {len(rows)} rows × {len(feature_names)} features")

    # 1. Distribution
    dist_report = distribution_report(rows, feature_names)
    dist_flags = print_distribution_table(dist_report)

    # 2. Temporal stability
    stability = temporal_stability(rows, feature_names)
    unstable = print_stability_table(stability)

    # 3. Regime analysis
    regime_stats = regime_analysis(rows, feature_names)
    print_regime_table(regime_stats)

    # 4. Correlation
    corr = compute_correlation(rows, feature_names)
    redundant = print_correlation_table(corr, feature_names)

    # Save stats
    output_dir = "data/processed"
    os.makedirs(output_dir, exist_ok=True)
    basename = os.path.basename(input_path).replace("features_", "audit_stats_")
    stats_path = os.path.join(output_dir, basename)
    save_stats_csv(dist_report, stability, stats_path)

    # Final verdict
    print(f"\n{'='*80}")
    print(f"  AUDIT SUMMARY")
    print(f"{'='*80}")
    print(f"  Rows analyzed:          {len(rows)}")
    print(f"  Features analyzed:      {len(feature_names)}")
    print(f"  Distribution flags:     {len(dist_flags)}")
    print(f"  Temporal drift:         {len(unstable)} features")
    print(f"  Redundant pairs:        {len(redundant)}")

    issues = len(dist_flags) + len(unstable) + len(redundant)
    if issues == 0:
        print(f"\n  ✓ DATASET STRUCTURALLY SOUND — ready for event definition.")
    else:
        print(f"\n  ⚠ {issues} issue(s) found — review before proceeding.")

    sys.exit(0 if issues == 0 else 0)  # advisory, not blocking
