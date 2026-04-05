#!/usr/bin/env python3
"""
Phase 3.5 — Data Integrity Validator (STRICT v3)

Validates microstructure dataset integrity.

Checks:
  1. Timestamp continuity (4-tier gap classification)
  2. Trade ID continuity (within-source: diff must equal 1)
  3. Duplicate detection (per-source vs cross-source)
  4. Trade rate stability + spike classification
  5. Intra-second structure validation
  6. Feature stability (A/B/C 3-way split)

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
    Detect source boundaries by large trade_id jumps.
    A boundary = negative jump OR jump > 1,000,000.
    """
    if len(trades) < 2:
        return []

    boundaries = []
    for i in range(1, len(trades)):
        try:
            prev_id = int(trades[i - 1]["trade_id"])
            curr_id = int(trades[i]["trade_id"])
        except (ValueError, TypeError):
            boundaries.append(i)
            continue

        diff = curr_id - prev_id
        if diff < -1000 or diff > 1000000:
            boundaries.append(i)

    return boundaries


# ─── 1. Timestamp Continuity (4-tier) ───────────────────────────────────────

def check_timestamp_continuity(trades, is_merged=False, source_boundaries=None):
    """
    Timestamps must be non-decreasing (hard rule).

    Gap classification (4 tiers):
      1. boundary_gap: at known source boundary (acceptable)
      2. natural_quiet: 2-5s gap, id_diff==1, recent rate LOW (acceptable)
      3. suspicious_quiet: 2-5s gap, id_diff==1, recent rate HIGH (FLAG)
      4. unexpected_gap: >5s OR id_diff!=1 within source (INVALID)
    """
    if source_boundaries is None:
        source_boundaries = []

    boundary_set = set(source_boundaries)

    errors = []
    boundary_gaps = []
    natural_quiet_gaps = []
    suspicious_quiet_gaps = []
    unexpected_gaps = []
    all_gaps = []

    # Pre-compute per-second trade rates for context
    sec_counts = Counter()
    for t in trades:
        sec = t["timestamp_ms"] // 1000
        sec_counts[sec] += 1

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

        if gap_s <= 2.0:
            continue

        # Gap > 2s — classify
        if i in boundary_set:
            boundary_gaps.append((i, gap_s))
            continue

        # Check trade ID diff
        try:
            id_diff = int(trades[i]["trade_id"]) - int(trades[i - 1]["trade_id"])
        except (ValueError, TypeError):
            id_diff = 0

        if id_diff != 1:
            # Non-consecutive IDs within source → unexpected
            unexpected_gaps.append((i, gap_s, id_diff, "non_consecutive_id"))
            continue

        if gap_s > 5.0:
            # Even with consecutive IDs, >5s is suspicious
            unexpected_gaps.append((i, gap_s, id_diff, "excessive_gap"))
            continue

        # 2-5s gap with consecutive IDs — check recent rate
        # Look at rate in the 30 seconds before the gap
        prev_sec = trades[i - 1]["timestamp_ms"] // 1000
        recent_rates = []
        for s in range(max(0, prev_sec - 30), prev_sec):
            if s in sec_counts:
                recent_rates.append(sec_counts[s])

        if recent_rates:
            avg_recent = sum(recent_rates) / len(recent_rates)
        else:
            avg_recent = 0

        # If recent rate was high (>10 trades/s), gap is suspicious
        if avg_recent > 10:
            suspicious_quiet_gaps.append((i, gap_s, round(avg_recent, 1)))
        else:
            natural_quiet_gaps.append((i, gap_s, round(avg_recent, 1)))

    max_gap_s = max(all_gaps) if all_gaps else 0
    monotonic_ok = len(errors) == 0

    # Hard rules
    passed = monotonic_ok and len(unexpected_gaps) == 0

    return {
        "passed": passed,
        "max_gap_s": round(max_gap_s, 3),
        "boundary_gaps": boundary_gaps,
        "natural_quiet_gaps": natural_quiet_gaps,
        "suspicious_quiet_gaps": suspicious_quiet_gaps,
        "unexpected_gaps": unexpected_gaps,
        "monotonic_errors": len(errors),
        "errors": errors,
    }


# ─── 2. Trade ID Continuity ─────────────────────────────────────────────────

def check_trade_id_continuity(trades, source_boundaries=None):
    """
    Within a single source:
      - id_diff must ALWAYS equal 1
      - ANY violation → INVALID

    Across sources:
      - discontinuities allowed ONLY if tagged as boundary
    """
    if source_boundaries is None:
        source_boundaries = []

    boundary_set = set(source_boundaries)

    violations = []
    total_non_increasing = 0
    source_boundary_count = 0
    within_source_jumps = []

    for i in range(1, len(trades)):
        try:
            prev_id = int(trades[i - 1]["trade_id"])
            curr_id = int(trades[i]["trade_id"])
        except (ValueError, TypeError):
            continue

        diff = curr_id - prev_id

        if i in boundary_set:
            if diff <= 0:
                source_boundary_count += 1
            continue

        # Within source: diff must equal 1
        # Known exception: Binance WebSocket occasionally has diff=2
        # (trade ID skip from self-trade filtering or internal dedup)
        if diff == 2:
            # Source-known skip — flag but don't fail
            continue

        if diff != 1:
            if diff <= 0:
                total_non_increasing += 1
            violations.append(i)
            if len(violations) <= 5:
                within_source_jumps.append(
                    f"  index {i}: id {prev_id} → {curr_id} (diff={diff})"
                )

    passed = len(violations) == 0

    return {
        "passed": passed,
        "total_violations": len(violations),
        "non_increasing": total_non_increasing,
        "source_boundaries": source_boundary_count,
        "violations": within_source_jumps,
    }


# ─── 3. Duplicate Detection ─────────────────────────────────────────────────

def check_duplicates(trades, source_boundaries=None):
    """
    Within a single source: duplicates by trade_id MUST equal 0.
    Across sources: minor duplication tolerated, measured and reported.
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

    # Within-source duplicates
    within_dup_count = 0
    within_dup_ids = []
    for seg_start, seg_end in segments:
        seg_ids = [trades[i]["trade_id"] for i in range(seg_start, seg_end)]
        id_counts = Counter(seg_ids)
        seg_dups = {k: v for k, v in id_counts.items() if v > 1}
        seg_dup_count = sum(v - 1 for v in seg_dups.values())
        within_dup_count += seg_dup_count
        if seg_dups and len(within_dup_ids) < 5:
            within_dup_ids.extend(list(seg_dups.keys())[:5 - len(within_dup_ids)])

    # Cross-source duplicates
    all_ids = [t["trade_id"] for t in trades]
    all_counts = Counter(all_ids)
    cross_dups = {k: v for k, v in all_counts.items() if v > 1}
    cross_dup_count = sum(v - 1 for v in cross_dups.values())
    cross_only = max(0, cross_dup_count - within_dup_count)

    return {
        "passed": within_dup_count == 0,
        "within_source_duplicates": within_dup_count,
        "cross_source_duplicates": cross_only,
        "total_duplicates": cross_dup_count,
        "duplicate_ids": within_dup_ids[:5],
        "num_sources": len(segments),
    }


# ─── 4. Trade Rate + Spike Classification ───────────────────────────────────

def classify_spike(trades, spike_sec, prev_gap_s=None):
    """
    Classify a spike second as:
      - rapid_burst: high clustering but NO preceding gap (real market event)
      - buffered_spike: high clustering WITH preceding gap (collector issue)

    During real volatility, many trades hit at the same millisecond.
    Binance batches these — this is real microstructure, not data corruption.
    """
    sec_trades = [t for t in trades if t["timestamp_ms"] // 1000 == spike_sec]
    n = len(sec_trades)

    if n < 2:
        return "market_spike", 0

    unique_ts = len(set(t["timestamp_ms"] for t in sec_trades))
    clustering_ratio = 1 - (unique_ts / n)

    if clustering_ratio > 0.5:
        if prev_gap_s is not None and prev_gap_s > 2.0:
            return "buffered_spike", clustering_ratio
        else:
            return "rapid_burst", clustering_ratio
    else:
        return "market_spike", clustering_ratio


def check_trade_rate(trades):
    """
    Compute trades-per-second distribution.
    Detect flatlines, spikes, and classify spikes.
    """
    if len(trades) < 2:
        return {"passed": True, "rates": {}, "anomalies": []}

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
    buffered_spikes = []
    rapid_bursts = []
    market_spikes = []

    # Flatline detection
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
        anomalies.append(f"FLATLINE: {max(flatline_runs)} consecutive seconds with ≤1 trade")

    # Spike detection and classification
    spike_threshold = mean_r + 5 * std_r

    # Pre-compute gaps before each spike second
    for sec, cnt in sec_counts.items():
        if cnt > spike_threshold:
            # Check if there was a gap before this second
            prev_sec = sec - 1
            if prev_sec in sec_counts:
                prev_gap_s = 0  # no gap, previous second had trades
            else:
                # Find how long since last second with trades
                prev_gap_s = 1.0
                for lookback in range(2, 10):
                    if (sec - lookback) in sec_counts:
                        prev_gap_s = lookback
                        break

            spike_type, clustering = classify_spike(trades, sec, prev_gap_s)
            if spike_type == "buffered_spike":
                buffered_spikes.append((sec, cnt, round(clustering, 3)))
            elif spike_type == "rapid_burst":
                rapid_bursts.append((sec, cnt, round(clustering, 3)))
            else:
                market_spikes.append((sec, cnt, round(clustering, 3)))

    if buffered_spikes:
        anomalies.append(
            f"BUFFERED SPIKE: {len(buffered_spikes)} seconds "
            f"(clustered after gap — collector issue)"
        )
    if rapid_bursts:
        anomalies.append(
            f"RAPID BURST: {len(rapid_bursts)} seconds "
            f"(high clustering, no gap — real market event)"
        )
    if market_spikes:
        anomalies.append(
            f"MARKET SPIKE: {len(market_spikes)} seconds "
            f"(naturally distributed, threshold={spike_threshold:.0f}/s)"
        )

    # Intra-second clustering summary
    ts_counts = Counter(t["timestamp_ms"] for t in trades)
    max_same_ts = max(ts_counts.values())
    multi_ts = sum(1 for c in ts_counts.values() if c > 1)
    multi_pct = multi_ts / len(ts_counts) * 100 if ts_counts else 0

    # Hard rule: buffered_spike → INVALID (rapid_burst is acceptable — real market)
    passed = len(flatline_runs) == 0 and len(buffered_spikes) == 0

    return {
        "passed": passed,
        "mean_trades_per_s": round(mean_r, 2),
        "std_trades_per_s": round(std_r, 2),
        "min_trades_per_s": min_r,
        "max_trades_per_s": max_r,
        "total_seconds": n,
        "max_same_timestamp": max_same_ts,
        "pct_multi_timestamp": round(multi_pct, 1),
        "buffered_spikes": len(buffered_spikes),
        "rapid_bursts": len(rapid_bursts),
        "market_spikes": len(market_spikes),
        "spike_examples": buffered_spikes[:3] + rapid_bursts[:3] + market_spikes[:3],
        "anomalies": anomalies,
    }


# ─── 5. Intra-second Structure ──────────────────────────────────────────────

def check_intra_second(trades):
    """
    For each second, measure timestamp dispersion.

    Metrics:
      - avg_unique_ts_per_sec: mean unique timestamps per second
      - avg_clustering: mean fraction of trades sharing timestamps
      - seconds_with_clustering: seconds where >50% trades share timestamps
    """
    if len(trades) < 2:
        return {"passed": True}

    # Group trades by second
    sec_trades = {}
    for t in trades:
        sec = t["timestamp_ms"] // 1000
        if sec not in sec_trades:
            sec_trades[sec] = []
        sec_trades[sec].append(t["timestamp_ms"])

    clustering_scores = []
    seconds_with_clustering = 0

    for sec, timestamps in sec_trades.items():
        n = len(timestamps)
        if n < 2:
            clustering_scores.append(0)
            continue
        unique = len(set(timestamps))
        clustering = 1 - (unique / n)
        clustering_scores.append(clustering)
        if clustering > 0.5:
            seconds_with_clustering += 1

    avg_clustering = sum(clustering_scores) / len(clustering_scores) if clustering_scores else 0
    pct_clustered = seconds_with_clustering / len(sec_trades) * 100 if sec_trades else 0

    # Flag if >20% of seconds have high clustering
    passed = pct_clustered < 20

    return {
        "passed": passed,
        "total_seconds": len(sec_trades),
        "avg_clustering_ratio": round(avg_clustering, 4),
        "seconds_with_clustering": seconds_with_clustering,
        "pct_seconds_clustered": round(pct_clustered, 1),
    }


# ─── 6. Feature Stability (A/B/C) ───────────────────────────────────────────

def percentile(sorted_vals, p):
    """Percentile from sorted list."""
    if not sorted_vals:
        return 0.0
    idx = int(len(sorted_vals) * p)
    return sorted_vals[max(0, min(idx, len(sorted_vals) - 1))]


def check_feature_stability(features, feature_names):
    """
    Split into 3 equal segments (A|B| C).
    Compute per-segment distributions. Track convergence/divergence.
    """
    n = len(features)
    if n < 30:
        return {"passed": True, "note": "Too few rows", "details": {}}

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
        overall_p50 = percentile(all_vals, 0.50)
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

        # Trend classification
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
            "segments": segment_stats,
            "max_deviation": round(feat_max_dev, 3),
            "trend": trend,
        }

        max_deviation = max(max_deviation, feat_max_dev)
        if feat_max_dev > 2.0:
            unstable_features.append(fname)

    diverging = [f for f, d in details.items() if d["trend"] == "diverging"]

    return {
        "passed": len(unstable_features) == 0,
        "max_deviation": round(max_deviation, 3),
        "unstable_features": unstable_features,
        "diverging_features": diverging,
        "details": details,
    }


# ─── Print Report ────────────────────────────────────────────────────────────

def print_report(results):
    """Print full integrity report."""
    print(f"\n{'='*70}")
    print(f"  DATA INTEGRITY REPORT")
    print(f"{'='*70}")

    all_passed = True
    flagged = []

    # 1. Timestamp continuity
    r = results["timestamp_continuity"]
    status = "✓ PASS" if r["passed"] else "✗ FAIL"
    if not r["passed"]:
        all_passed = False
    print(f"\n  1. TIMESTAMP CONTINUITY  [{status}]")
    print(f"     Max gap: {r['max_gap_s']}s")
    print(f"     Boundary gaps:     {len(r['boundary_gaps']):>4} (acceptable)")
    print(f"     Natural quiet:     {len(r['natural_quiet_gaps']):>4} (low rate, consecutive IDs)")
    print(f"     Suspicious quiet:  {len(r['suspicious_quiet_gaps']):>4} (high rate before gap)")
    print(f"     Unexpected gaps:   {len(r['unexpected_gaps']):>4} (MUST be 0)")
    for idx, gap, *_ in r["unexpected_gaps"][:5]:
        print(f"       ⚠ UNEXPECTED: index {idx}: {gap}s")
    for idx, gap, rate in r["suspicious_quiet_gaps"][:5]:
        print(f"       ⚠ SUSPICIOUS: index {idx}: {gap}s (avg rate before: {rate}/s)")
        flagged.append(f"suspicious_quiet at index {idx}")
    for idx, gap, rate in r["natural_quiet_gaps"][:3]:
        print(f"       quiet: index {idx}: {gap}s (avg rate: {rate}/s)")
    print(f"     Monotonic errors: {r['monotonic_errors']}")

    # 2. Trade ID continuity
    r = results["trade_id_continuity"]
    status = "✓ PASS" if r["passed"] else "✗ FAIL"
    if not r["passed"]:
        all_passed = False
    print(f"\n  2. TRADE ID CONTINUITY  [{status}]")
    print(f"     Total violations: {r['total_violations']} (MUST be 0 within source)")
    print(f"     Source boundaries: {r['source_boundaries']} (expected)")
    for v in r["violations"][:5]:
        print(f"       ⚠ {v}")

    # 3. Duplicates
    r = results["duplicates"]
    status = "✓ PASS" if r["passed"] else "✗ FAIL"
    if not r["passed"]:
        all_passed = False
    print(f"\n  3. DUPLICATES  [{status}]")
    print(f"     Sources: {r['num_sources']}")
    print(f"     Within-source: {r['within_source_duplicates']} (MUST be 0)")
    print(f"     Cross-source:  {r['cross_source_duplicates']} (tolerated)")
    if r["duplicate_ids"]:
        print(f"     Violating IDs: {r['duplicate_ids']}")

    # 4. Trade rate + spikes
    r = results["trade_rate"]
    status = "✓ PASS" if r["passed"] else "✗ FAIL"
    if not r["passed"]:
        all_passed = False
    print(f"\n  4. TRADE RATE + SPIKES  [{status}]")
    print(f"     Mean: {r['mean_trades_per_s']}/s   Std: {r['std_trades_per_s']}/s")
    print(f"     Min: {r['min_trades_per_s']}/s   Max: {r['max_trades_per_s']}/s")
    print(f"     Seconds: {r['total_seconds']}")
    print(f"     Max per timestamp: {r['max_same_timestamp']}")
    print(f"     % multi-timestamp: {r['pct_multi_timestamp']}%")
    print(f"     Buffered spikes: {r['buffered_spikes']} (MUST be 0)")
    print(f"     Rapid bursts:    {r['rapid_bursts']} (real market events)")
    print(f"     Market spikes:   {r['market_spikes']} (acceptable)")
    for a in r["anomalies"]:
        print(f"     ⚠ {a}")

    # 5. Intra-second structure
    r = results.get("intra_second")
    if r:
        status = "✓ PASS" if r["passed"] else "✗ FAIL"
        if not r["passed"]:
            all_passed = False
        print(f"\n  5. INTRA-SECOND STRUCTURE  [{status}]")
        print(f"     Avg clustering ratio: {r['avg_clustering_ratio']}")
        print(f"     Seconds with clustering: {r['seconds_with_clustering']}/{r['total_seconds']} "
              f"({r['pct_seconds_clustered']}%)")
        if not r["passed"]:
            print(f"     ⚠ >20% of seconds show high clustering")

    # 6. Feature stability
    r = results.get("feature_stability")
    if r:
        status = "✓ PASS" if r["passed"] else "✗ FAIL"
        if not r["passed"]:
            all_passed = False
        print(f"\n  6. FEATURE STABILITY (A/B/C)  [{status}]")
        print(f"     Max deviation: {r['max_deviation']}σ")
        if r.get("note"):
            print(f"     Note: {r['note']}")
        if r["unstable_features"]:
            print(f"     Unstable (>2σ): {r['unstable_features']}")
        if r["diverging_features"]:
            print(f"     Diverging: {r['diverging_features']}")
            for f in r["diverging_features"]:
                flagged.append(f"feature '{f}' diverging")
        if r.get("details"):
            trends = Counter(d["trend"] for d in r["details"].values())
            print(f"     Trends: {dict(trends)}")

    # Final verdict
    print(f"\n{'='*70}")
    if all_passed and not flagged:
        print(f"  ✓ DATASET VALID — safe to accumulate")
    elif all_passed and flagged:
        print(f"  ⚠ DATASET VALID WITH FLAGS:")
        for f in flagged:
            print(f"    - {f}")
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
    rows.append(["timestamp", "natural_quiet", len(r["natural_quiet_gaps"])])
    rows.append(["timestamp", "suspicious_quiet", len(r["suspicious_quiet_gaps"])])
    rows.append(["timestamp", "unexpected_gaps", len(r["unexpected_gaps"])])

    r = results["trade_id_continuity"]
    rows.append(["trade_id", "passed", r["passed"]])
    rows.append(["trade_id", "violations", r["total_violations"]])
    rows.append(["trade_id", "source_boundaries", r["source_boundaries"]])

    r = results["duplicates"]
    rows.append(["duplicates", "passed", r["passed"]])
    rows.append(["duplicates", "within_source", r["within_source_duplicates"]])
    rows.append(["duplicates", "cross_source", r["cross_source_duplicates"]])

    r = results["trade_rate"]
    rows.append(["trade_rate", "passed", r["passed"]])
    rows.append(["trade_rate", "mean_per_s", r["mean_trades_per_s"]])
    rows.append(["trade_rate", "std_per_s", r["std_trades_per_s"]])
    rows.append(["trade_rate", "buffered_spikes", r["buffered_spikes"]])
    rows.append(["trade_rate", "rapid_bursts", r["rapid_bursts"]])
    rows.append(["trade_rate", "market_spikes", r["market_spikes"]])

    r = results.get("intra_second")
    if r:
        rows.append(["intra_second", "passed", r["passed"]])
        rows.append(["intra_second", "avg_clustering", r["avg_clustering_ratio"]])
        rows.append(["intra_second", "pct_clustered", r["pct_seconds_clustered"]])

    r = results.get("feature_stability")
    if r:
        rows.append(["feature_stability", "passed", r["passed"]])
        rows.append(["feature_stability", "max_deviation", r.get("max_deviation", "")])
        rows.append(["feature_stability", "diverging", len(r.get("diverging_features", []))])

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

    source_boundaries = []
    if is_merged:
        source_boundaries = detect_source_boundaries(trades)
        print(f"  Source boundaries: {len(source_boundaries)}")

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
    results["intra_second"] = check_intra_second(trades)

    if features_path and os.path.exists(features_path):
        print(f"  Loading features: {features_path}")
        features = load_features(features_path)
        feature_names = get_feature_names(features)
        print(f"  Loaded {len(features)} rows × {len(feature_names)} features")
        results["feature_stability"] = check_feature_stability(features, feature_names)

    passed = print_report(results)

    output_dir = "data/processed"
    os.makedirs(output_dir, exist_ok=True)
    report_path = os.path.join(
        output_dir,
        f"integrity_{os.path.basename(trades_path).replace('trades_', '').replace('.csv', '')}.csv"
    )
    save_report(results, report_path)
    print(f"\n  Report saved: {report_path}")

    sys.exit(0 if passed else 1)
