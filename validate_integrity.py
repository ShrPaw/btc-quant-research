#!/usr/bin/env python3
"""
Phase 3.5 — Data Integrity Validator (STRICT)

Validates microstructure dataset integrity.

Checks:
  1. Timestamp continuity (monotonic, gap classification)
  2. Trade ID continuity (within-source strict)
  3. Duplicate detection (per-source vs cross-source)
  4. Trade rate stability (flatlines + buffered spikes)
  5. Feature stability (A/B/C 3-way split)

Usage:
  python3 validate_integrity.py <trades.csv>                     # single source
  python3 validate_integrity.py <trades.csv> --merged            # merged dataset
  python3 validate_integrity.py <trades.csv> --features <f.csv>  # with features

Exit codes:
  0 = VALID
  1 = INVALID (do not proceed)
"""

import csv
import math
import os
import sys
from collections import Counter


# ─── Load ─────────────────────────────────────────────────────────────────────

def load_trades(path):
    """Load raw trades CSV into list of dicts."""
    rows = []
    with open(path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append({
                "timestamp_ms": int(row["timestamp_ms"]),
                "trade_id": row["trade_id"],
                "price": float(row["price"]),
                "quantity": float(row["quantity"]),
                "agggressor_side": row["agggressor_side"],
            })
    return rows


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
    """Extract numeric feature column names."""
    skip = {"timestamp_s", "timestamp_utc"}
    return [k for k in rows[0].keys()
            if k not in skip and isinstance(rows[0][k], (int, float))]


# ─── Detect Source Boundaries ────────────────────────────────────────────────

def detect_source_boundaries(trades):
    """
    Detect source boundaries in merged data.

    A source boundary is where different data collection runs meet.
    Identified by:
      - Large negative jumps in trade_id (REST/historical vs WebSocket ranges)
      - Very large positive jumps (time gap between collection runs)

    Does NOT flag small jumps within a continuous stream.
    """
    if len(trades) < 2:
        return []

    boundaries = []

    # Compute typical ID step within the dataset
    id_diffs = []
    for i in range(1, min(len(trades), 5000)):
        try:
            diff = int(trades[i]["trade_id"]) - int(trades[i - 1]["trade_id"])
            if 0 < diff < 100:  # normal step
                id_diffs.append(diff)
        except (ValueError, TypeError):
            continue

    # A boundary is a jump > 10000x the typical step OR a negative jump
    for i in range(1, len(trades)):
        try:
            prev_id = int(trades[i - 1]["trade_id"])
            curr_id = int(trades[i]["trade_id"])
        except (ValueError, TypeError):
            boundaries.append(i)
            continue

        diff = curr_id - prev_id

        # Negative jump = definitely a new source
        if diff < -1000:
            boundaries.append(i)
        # Very large positive jump (millions) = new source
        elif diff > 1000000:
            boundaries.append(i)

    return boundaries


# ─── 1. Timestamp Continuity ─────────────────────────────────────────────────

def check_timestamp_continuity(trades, is_merged=False, source_boundaries=None):
    """
    Timestamps must be non-decreasing (hard rule — always).

    Gap classification:
      - At source boundary → "boundary_gap" (acceptable)
      - Within continuous ID sequence, >2s → "natural_quiet" (acceptable)
        BTC futures can have 2-5s quiet periods. Consecutive trade IDs
        confirm this is the same stream, not a data loss event.
      - Within source, gap >5s OR non-consecutive IDs → "unexpected_gap" (INVALID)

    For single-source: stricter — any gap >5s is unexpected.
    """
    if source_boundaries is None:
        source_boundaries = []

    boundary_set = set(source_boundaries)

    errors = []
    boundary_gaps = []
    natural_quiet_gaps = []
    unexpected_gaps = []
    all_gaps = []

    for i in range(1, len(trades)):
        gap_ms = trades[i]["timestamp_ms"] - trades[i - 1]["timestamp_ms"]

        if gap_ms < 0:
            errors.append(
                f"  NON-MONOTONIC at index {i}: "
                f"{trades[i-1]['timestamp_ms']} → {trades[i]['timestamp_ms']} "
                f"(gap={gap_ms}ms)"
            )
            if len(errors) > 5:
                errors.append("  ... (truncated)")
                break
            continue

        gap_s = gap_ms / 1000.0
        all_gaps.append(gap_s)

        if gap_s > 2.0:
            if i in boundary_set:
                boundary_gaps.append((i, gap_s))
            else:
                # Check if trade IDs are consecutive (same stream, just quiet)
                try:
                    id_diff = int(trades[i]["trade_id"]) - int(trades[i - 1]["trade_id"])
                except (ValueError, TypeError):
                    id_diff = 0

                if id_diff <= 2 and gap_s <= 5.0:
                    # Consecutive IDs, gap ≤5s → natural quiet period
                    natural_quiet_gaps.append((i, gap_s))
                elif gap_s > 5.0:
                    # >5s gap even with consecutive IDs → suspicious
                    unexpected_gaps.append((i, gap_s))
                elif id_diff > 2:
                    # Non-consecutive IDs + gap → possible data loss
                    unexpected_gaps.append((i, gap_s))

    max_gap_s = max(all_gaps) if all_gaps else 0
    monotonic_ok = len(errors) == 0

    # Hard rule: ANY unexpected_gap → INVALID
    passed = monotonic_ok and len(unexpected_gaps) == 0

    return {
        "passed": passed,
        "max_gap_s": round(max_gap_s, 3),
        "boundary_gaps": [(idx, round(gap, 2)) for idx, gap in boundary_gaps],
        "natural_quiet_gaps": [(idx, round(gap, 2)) for idx, gap in natural_quiet_gaps],
        "unexpected_gaps": [(idx, round(gap, 2)) for idx, gap in unexpected_gaps],
        "monotonic_errors": len(errors),
        "errors": errors,
    }


# ─── 2. Trade ID Continuity ─────────────────────────────────────────────────

def check_trade_id_continuity(trades, source_boundaries=None):
    """
    Within a single source:
      - Trade IDs must be strictly increasing
      - id_diff <= 0 → INVALID
      - Large jumps → possible missing data

    Across sources:
      - Discontinuities are expected
      - Tagged as source_boundary
    """
    if source_boundaries is None:
        source_boundaries = []

    boundary_set = set(source_boundaries)

    within_source_violations = []
    jumps = []
    total_non_increasing = 0
    source_boundary_count = 0

    for i in range(1, len(trades)):
        try:
            prev_id = int(trades[i - 1]["trade_id"])
            curr_id = int(trades[i]["trade_id"])
        except (ValueError, TypeError):
            continue

        diff = curr_id - prev_id

        if diff <= 0:
            total_non_increasing += 1
            if i in boundary_set:
                source_boundary_count += 1
            else:
                # Within-source non-increasing = INVALID
                within_source_violations.append(i)
                if len(within_source_violations) <= 5:
                    try:
                        print(f"     ⚠ index {i}: id {prev_id} → {curr_id} (diff={diff})")
                    except:
                        pass
        elif diff > 1 and i not in boundary_set:
            jumps.append(diff)

    passed = len(within_source_violations) == 0

    return {
        "passed": passed,
        "total_non_increasing": total_non_increasing,
        "source_boundary_count": source_boundary_count,
        "within_source_violations": len(within_source_violations),
        "jump_count": len(jumps),
        "max_jump": max(jumps) if jumps else 0,
        "mean_jump": round(sum(jumps) / len(jumps), 1) if jumps else 0,
    }


# ─── 3. Duplicate Detection ─────────────────────────────────────────────────

def check_duplicates(trades, source_boundaries=None):
    """
    Within a single source:
      - Duplicates by trade_id MUST equal 0

    Across sources:
      - Minor duplication tolerated (real trades can share timestamps)
      - Must be measured and reported

    We detect source segments based on boundaries and check per-segment.
    """
    if source_boundaries is None:
        source_boundaries = []

    # Build source segments
    segments = []
    start = 0
    for b in sorted(source_boundaries):
        segments.append((start, b))
        start = b
    segments.append((start, len(trades)))

    # Check duplicates per segment (within-source)
    within_source_dup_count = 0
    within_source_dup_ids = []

    for seg_start, seg_end in segments:
        seg_ids = [trades[i]["trade_id"] for i in range(seg_start, seg_end)]
        id_counts = Counter(seg_ids)
        seg_dups = {k: v for k, v in id_counts.items() if v > 1}
        seg_dup_count = sum(v - 1 for v in seg_dups.values())
        within_source_dup_count += seg_dup_count
        if seg_dups and len(within_source_dup_ids) < 5:
            within_source_dup_ids.extend(list(seg_dups.keys())[:5 - len(within_source_dup_ids)])

    # Cross-source duplicates (full dataset)
    all_ids = [t["trade_id"] for t in trades]
    all_counts = Counter(all_ids)
    cross_source_dups = {k: v for k, v in all_counts.items() if v > 1}
    cross_source_dup_count = sum(v - 1 for v in cross_source_dups.values())

    # Cross-source only = total - within-source
    cross_only = cross_source_dup_count - within_source_dup_count

    passed = within_source_dup_count == 0

    return {
        "passed": passed,
        "within_source_duplicates": within_source_dup_count,
        "cross_source_duplicates": max(0, cross_only),
        "total_duplicates": cross_source_dup_count,
        "duplicate_ids": within_source_dup_ids[:5],
        "num_sources": len(segments),
    }


# ─── 4. Trade Rate Stability ────────────────────────────────────────────────

def check_trade_rate(trades):
    """
    Compute trades-per-second distribution.

    Detect:
      1. Flatlines (collector freeze)
      2. Spikes (reconnect bursts)
      3. Buffered data (many trades share identical timestamps)
    """
    if len(trades) < 2:
        return {"passed": True, "rates": {}, "anomalies": []}

    # ── Per-second counts ──
    sec_counts = Counter()
    for t in trades:
        sec = t["timestamp_ms"] // 1000
        sec_counts[sec] += 1

    rates = sorted(sec_counts.values())
    n = len(rates)
    mean_r = sum(rates) / n
    std_r = math.sqrt(sum((r - mean_r) ** 2 for r in rates) / n)
    min_r = min(rates)
    max_r = max(rates)

    anomalies = []

    # ── Flatline: consecutive seconds with ≤1 trade ──
    sorted_secs = sorted(sec_counts.keys())
    flatline_runs = []
    current_run = 1
    for i in range(1, len(sorted_secs)):
        if sorted_secs[i] == sorted_secs[i - 1] + 1 and sec_counts[sorted_secs[i]] <= 1:
            current_run += 1
        else:
            if current_run >= 10:
                flatline_runs.append(current_run)
            current_run = 1
    if current_run >= 10:
        flatline_runs.append(current_run)

    if flatline_runs:
        anomalies.append(f"FLATLINE: {max(flatline_runs)} consecutive seconds with ≤1 trade (collector freeze?)")

    # ── Spike: any second with rate > mean + 5*std ──
    spike_threshold = mean_r + 5 * std_r
    spike_secs = [(sec, cnt) for sec, cnt in sec_counts.items() if cnt > spike_threshold]
    if spike_secs:
        max_spike = max(cnt for _, cnt in spike_secs)
        anomalies.append(f"SPIKE: {len(spike_secs)} seconds above {spike_threshold:.0f}/s (max={max_spike})")

    # ── Buffered data detection ──
    # Check if many trades share identical timestamps (ms-level)
    ts_counts = Counter(t["timestamp_ms"] for t in trades)
    max_same_ts = max(ts_counts.values())
    ts_with_multiple = sum(1 for c in ts_counts.values() if c > 1)
    ts_multiple_pct = ts_with_multiple / len(ts_counts) * 100 if ts_counts else 0

    # If >30% of timestamps have multiple trades AND max per ts > 20, likely buffered
    if ts_multiple_pct > 30 and max_same_ts > 20:
        anomalies.append(
            f"BUFFERED: {ts_multiple_pct:.0f}% of timestamps have multiple trades "
            f"(max {max_same_ts} per ms) — likely NOT real-time flow"
        )

    passed = len(flatline_runs) == 0

    return {
        "passed": passed,
        "mean_trades_per_s": round(mean_r, 2),
        "std_trades_per_s": round(std_r, 2),
        "min_trades_per_s": min_r,
        "max_trades_per_s": max_r,
        "total_seconds": n,
        "max_same_timestamp": max_same_ts,
        "pct_multi_timestamp": round(ts_multiple_pct, 1),
        "anomalies": anomalies,
    }


# ─── 5. Feature Stability (A/B/C) ───────────────────────────────────────────

def percentile(sorted_vals, p):
    """Percentile from sorted list."""
    if not sorted_vals:
        return 0.0
    idx = int(len(sorted_vals) * p)
    return sorted_vals[max(0, min(idx, len(sorted_vals) - 1))]


def check_feature_stability(features, feature_names):
    """
    Split into 3 equal time segments (A | B | C).
    Compute p5, p50, p95 per segment.
    Max deviation = largest |segment_median - overall_median| / std

    Track if deviation is decreasing (convergence) or stable (non-stationarity).
    """
    n = len(features)
    if n < 30:
        return {"passed": True, "note": "Too few rows for 3-way split", "details": {}}

    third = n // 3
    segments = {
        "A": features[:third],
        "B": features[third:2 * third],
        "C": features[2 * third:],
    }

    details = {}
    max_deviation = 0.0
    unstable_features = []

    for fname in feature_names:
        all_vals = sorted(r[fname] for r in features)
        overall_p5 = percentile(all_vals, 0.05)
        overall_p50 = percentile(all_vals, 0.50)
        overall_p95 = percentile(all_vals, 0.95)
        overall_std = math.sqrt(
            sum((v - overall_p50) ** 2 for v in all_vals) / len(all_vals)
        )

        segment_stats = {}
        segment_deviations = []
        for seg_name, seg_rows in segments.items():
            seg_vals = [r[fname] for r in seg_rows]
            seg_p5 = percentile(sorted(seg_vals), 0.05)
            seg_p50 = percentile(sorted(seg_vals), 0.50)
            seg_p95 = percentile(sorted(seg_vals), 0.95)

            if overall_std > 1e-15:
                dev = abs(seg_p50 - overall_p50) / overall_std
            else:
                dev = 0.0

            segment_stats[seg_name] = {
                "p5": seg_p5, "p50": seg_p50, "p95": seg_p95, "deviation": dev
            }
            segment_deviations.append(dev)

        feat_max_dev = max(segment_deviations)

        # Trend: decreasing = convergence, stable/increasing = non-stationary
        if len(segment_deviations) == 3:
            if segment_deviations[2] < segment_deviations[0] * 0.7:
                trend = "converging"
            elif segment_deviations[2] > segment_deviations[0] * 1.3:
                trend = "diverging"
            else:
                trend = "stable"
        else:
            trend = "unknown"

        details[fname] = {
            "overall": {"p5": overall_p5, "p50": overall_p50, "p95": overall_p95},
            "segments": segment_stats,
            "max_deviation": round(feat_max_dev, 3),
            "trend": trend,
        }

        max_deviation = max(max_deviation, feat_max_dev)
        if feat_max_dev > 2.0:
            unstable_features.append(fname)

    return {
        "passed": len(unstable_features) == 0,
        "max_deviation": round(max_deviation, 3),
        "unstable_features": unstable_features,
        "details": details,
    }


# ─── Print Report ────────────────────────────────────────────────────────────

def print_report(results):
    """Print full integrity report."""
    print(f"\n{'='*70}")
    print(f"  DATA INTEGRITY REPORT")
    print(f"{'='*70}")

    all_passed = True

    # 1. Timestamp continuity
    r = results["timestamp_continuity"]
    status = "✓ PASS" if r["passed"] else "✗ FAIL"
    if not r["passed"]:
        all_passed = False
    print(f"\n  1. TIMESTAMP CONTINUITY  [{status}]")
    print(f"     Max gap: {r['max_gap_s']}s")
    print(f"     Boundary gaps: {len(r['boundary_gaps'])} (acceptable)")
    print(f"     Natural quiet gaps: {len(r.get('natural_quiet_gaps', []))} (consecutive IDs, ≤5s)")
    print(f"     Unexpected gaps: {len(r['unexpected_gaps'])} (MUST be 0)")
    for idx, gap in r["unexpected_gaps"][:5]:
        print(f"       ⚠ index {idx}: {gap}s — UNEXPECTED")
    for idx, gap in r["boundary_gaps"][:3]:
        print(f"       boundary: index {idx}: {gap}s")
    for idx, gap in r.get("natural_quiet_gaps", [])[:3]:
        print(f"       quiet: index {idx}: {gap}s")
    print(f"     Monotonic errors: {r['monotonic_errors']}")
    for e in r["errors"][:3]:
        print(f"     {e}")

    # 2. Trade ID continuity
    r = results["trade_id_continuity"]
    status = "✓ PASS" if r["passed"] else "✗ FAIL"
    if not r["passed"]:
        all_passed = False
    print(f"\n  2. TRADE ID CONTINUITY  [{status}]")
    print(f"     Total non-increasing: {r['total_non_increasing']}")
    print(f"       At source boundaries: {r['source_boundary_count']} (expected)")
    print(f"       Within source: {r['within_source_violations']} (MUST be 0)")
    print(f"     Jumps (within source): {r['jump_count']}")
    print(f"       Max jump: {r['max_jump']}")
    print(f"       Mean jump: {r['mean_jump']}")

    # 3. Duplicates
    r = results["duplicates"]
    status = "✓ PASS" if r["passed"] else "✗ FAIL"
    if not r["passed"]:
        all_passed = False
    print(f"\n  3. DUPLICATES  [{status}]")
    print(f"     Sources detected: {r['num_sources']}")
    print(f"     Within-source duplicates: {r['within_source_duplicates']} (MUST be 0)")
    print(f"     Cross-source duplicates: {r['cross_source_duplicates']} (tolerated)")
    print(f"     Total duplicates: {r['total_duplicates']}")
    if r["duplicate_ids"]:
        print(f"     Violating IDs: {r['duplicate_ids']}")

    # 4. Trade rate
    r = results["trade_rate"]
    status = "✓ PASS" if r["passed"] else "✗ FAIL"
    if not r["passed"]:
        all_passed = False
    print(f"\n  4. TRADE RATE STABILITY  [{status}]")
    print(f"     Mean: {r['mean_trades_per_s']}/s")
    print(f"     Std:  {r['std_trades_per_s']}/s")
    print(f"     Min:  {r['min_trades_per_s']}/s")
    print(f"     Max:  {r['max_trades_per_s']}/s")
    print(f"     Seconds covered: {r['total_seconds']}")
    print(f"     Max trades per timestamp: {r['max_same_timestamp']}")
    print(f"     % timestamps with multiple trades: {r['pct_multi_timestamp']}%")
    for a in r["anomalies"]:
        print(f"     ⚠ {a}")

    # 5. Feature stability
    r = results.get("feature_stability")
    if r:
        status = "✓ PASS" if r["passed"] else "✗ FAIL"
        if not r["passed"]:
            all_passed = False
        print(f"\n  5. FEATURE STABILITY (A/B/C)  [{status}]")
        print(f"     Max deviation: {r['max_deviation']}σ")
        if r.get("note"):
            print(f"     Note: {r['note']}")
        if r["unstable_features"]:
            print(f"     Unstable features:")
            for f in r["unstable_features"]:
                d = r["details"][f]
                print(f"       {f}: {d['max_deviation']}σ ({d['trend']})")
        # Show all trends
        if r.get("details"):
            trends = Counter(d["trend"] for d in r["details"].values())
            print(f"     Trends: {dict(trends)}")
    else:
        print(f"\n  5. FEATURE STABILITY  [SKIPPED] (no features provided)")

    # Final verdict
    print(f"\n{'='*70}")
    if all_passed:
        print(f"  ✓ DATASET VALID — safe to accumulate")
    else:
        print(f"  ✗ DATASET INVALID — fix pipeline before proceeding")
    print(f"{'='*70}")

    return all_passed


# ─── Save report ─────────────────────────────────────────────────────────────

def save_report(results, output_path):
    """Save integrity report as CSV."""
    rows = []

    r = results["timestamp_continuity"]
    rows.append(["timestamp", "passed", r["passed"]])
    rows.append(["timestamp", "max_gap_s", r["max_gap_s"]])
    rows.append(["timestamp", "boundary_gaps", len(r["boundary_gaps"])])
    rows.append(["timestamp", "unexpected_gaps", len(r["unexpected_gaps"])])

    r = results["trade_id_continuity"]
    rows.append(["trade_id", "passed", r["passed"]])
    rows.append(["trade_id", "within_source_violations", r["within_source_violations"]])
    rows.append(["trade_id", "source_boundaries", r["source_boundary_count"]])

    r = results["duplicates"]
    rows.append(["duplicates", "passed", r["passed"]])
    rows.append(["duplicates", "within_source", r["within_source_duplicates"]])
    rows.append(["duplicates", "cross_source", r["cross_source_duplicates"]])

    r = results["trade_rate"]
    rows.append(["trade_rate", "passed", r["passed"]])
    rows.append(["trade_rate", "mean_per_s", r["mean_trades_per_s"]])
    rows.append(["trade_rate", "std_per_s", r["std_trades_per_s"]])
    rows.append(["trade_rate", "max_same_ts", r["max_same_timestamp"]])
    rows.append(["trade_rate", "pct_multi_ts", r["pct_multi_timestamp"]])

    r = results.get("feature_stability")
    if r:
        rows.append(["feature_stability", "passed", r["passed"]])
        rows.append(["feature_stability", "max_deviation", r.get("max_deviation", "")])

    with open(output_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["check", "metric", "value"])
        w.writerows(rows)


# ─── Main ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 validate_integrity.py <trades.csv> [--merged] [--features <f.csv>]")
        sys.exit(1)

    trades_path = sys.argv[1]
    is_merged = "--merged" in sys.argv

    features_path = None
    if "--features" in sys.argv:
        idx = sys.argv.index("--features")
        if idx + 1 < len(sys.argv):
            features_path = sys.argv[idx + 1]

    # Load
    print(f"  Loading trades: {trades_path}")
    trades = load_trades(trades_path)
    print(f"  Loaded {len(trades):,} trades")

    if not trades:
        print("  ERROR: No trades loaded.")
        sys.exit(1)

    span_ms = trades[-1]["timestamp_ms"] - trades[0]["timestamp_ms"]
    span_s = span_ms / 1000.0
    print(f"  Time span: {span_s:.0f}s ({span_s/60:.1f} min = {span_s/3600:.1f} hr)")
    print(f"  Dataset type: {'MERGED' if is_merged else 'SINGLE SOURCE'}")

    # Detect source boundaries for merged data
    source_boundaries = []
    if is_merged:
        source_boundaries = detect_source_boundaries(trades)
        print(f"  Source boundaries detected: {len(source_boundaries)}")

    # Run checks
    print(f"\n  Running checks...")
    results = {}

    results["timestamp_continuity"] = check_timestamp_continuity(
        trades, is_merged=is_merged, source_boundaries=source_boundaries
    )
    results["trade_id_continuity"] = check_trade_id_continuity(
        trades, source_boundaries=source_boundaries
    )
    results["duplicates"] = check_duplicates(
        trades, source_boundaries=source_boundaries
    )
    results["trade_rate"] = check_trade_rate(trades)

    if features_path and os.path.exists(features_path):
        print(f"  Loading features: {features_path}")
        features = load_features(features_path)
        feature_names = get_feature_names(features)
        print(f"  Loaded {len(features)} rows × {len(feature_names)} features")
        results["feature_stability"] = check_feature_stability(features, feature_names)

    # Report
    passed = print_report(results)

    # Save
    output_dir = "data/processed"
    os.makedirs(output_dir, exist_ok=True)
    report_path = os.path.join(
        output_dir,
        f"integrity_{os.path.basename(trades_path).replace('trades_', '').replace('.csv', '')}.csv"
    )
    save_report(results, report_path)
    print(f"\n  Report saved: {report_path}")

    sys.exit(0 if passed else 1)
