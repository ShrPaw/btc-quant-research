#!/usr/bin/env python3
"""
Phase 3.5 — Data Accumulation Pipeline

Continuously collects and processes BTCUSDT microstructure data.

Usage:
  python3 accumulate.py              # Full pipeline: fetch + merge + process + validate
  python3 accumulate.py --process    # Merge existing raw data + process + validate
  python3 accumulate.py --fetch      # Fetch historical batches only

Deduplication policy:
  - Within SAME source: dedup by trade_id (strict)
  - ACROSS sources: do NOT aggressively dedup
  - Safer to allow minor duplication than remove real trades

Storage rules:
  - Raw data is source of truth (partitioned, append-only)
  - Merged dataset is TEMPORARY
"""

import csv
import glob
import json
import math
import os
import subprocess
import sys
import time
from collections import Counter


DATA_RAW = "data/raw"
DATA_PROC = "data/processed"


def fetch_historical_batches(num_batches=50, delay_s=2):
    """Fetch multiple batches of recent trades via REST."""
    import urllib.request

    os.makedirs(DATA_RAW, exist_ok=True)

    all_trades = []
    seen_ids = set()

    print(f"  Fetching {num_batches} batches (delay={delay_s}s)...")

    for i in range(num_batches):
        url = "https://fapi.binance.com/fapi/v1/trades?symbol=BTCUSDT&limit=1000"
        req = urllib.request.Request(url, headers={"User-Agent": "btc-quant-research/1.0"})

        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                batch = json.loads(resp.read().decode())
        except Exception as e:
            print(f"    Batch {i+1}: fetch error: {e}")
            time.sleep(delay_s)
            continue

        # Within this fetch, dedup by trade_id
        new = [t for t in batch if t["id"] not in seen_ids]
        for t in new:
            seen_ids.add(t["id"])
        all_trades.extend(new)

        if (i + 1) % 10 == 0 or i == 0:
            print(f"    Batch {i+1}/{num_batches}: {len(new)} new (total: {len(all_trades)})")

        if i < num_batches - 1:
            time.sleep(delay_s)

    if not all_trades:
        print("  No trades fetched.")
        return None

    all_trades.sort(key=lambda t: t["id"])

    ts = time.strftime("%Y%m%d_%H%M%S")
    path = os.path.join(DATA_RAW, f"trades_hist_{ts}.csv")

    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["timestamp_ms", "timestamp_utc", "price", "quantity",
                     "is_buyer_maker", "agggressor_side", "trade_id"])
        for t in all_trades:
            ts_ms = t["time"]
            ts_utc = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(ts_ms / 1000)) + f".{ts_ms % 1000:03d}"
            side = "SELL" if t["isBuyerMaker"] else "BUY"
            w.writerow([ts_ms, ts_utc, t["price"], t["qty"], t["isBuyerMaker"], side, t["id"]])

    span = (all_trades[-1]["time"] - all_trades[0]["time"]) / 1000
    print(f"  Saved {len(all_trades)} trades to {path}")
    print(f"  Time span: {span:.0f}s ({span/60:.1f} min)")

    return path


def merge_raw_csvs():
    """
    Merge all raw trade CSVs into a single time-sorted dataset.

    Deduplication policy:
      - Within same source file: dedup by trade_id (strict)
      - Across sources: concatenate, sort by timestamp
      - Cross-source dedup: ONLY remove exact (trade_id) duplicates
      - Do NOT use composite keys for cross-source dedup
        (real trades can share identical timestamp/price/qty/side)

    Reads:  data/raw/trades_*.csv
    Writes: data/raw/trades_merged.csv (TEMPORARY)

    Rules:
      - Raw data NEVER overwritten
      - Merged dataset is NOT source of truth
    """
    pattern = os.path.join(DATA_RAW, "trades_*.csv")
    files = sorted(glob.glob(pattern))
    files = [f for f in files if "merged" not in f]

    if not files:
        print("  No raw trade files found.")
        return None

    print(f"  Merging {len(files)} raw files...")

    all_rows = []
    total_rows_in = 0
    per_file_stats = []
    global_seen_ids = set()
    cross_source_dup_count = 0

    for fpath in files:
        basename = os.path.basename(fpath)
        file_ids = set()
        file_rows_in = 0
        file_rows_out = 0
        file_within_dup = 0

        with open(fpath, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                file_rows_in += 1
                total_rows_in += 1
                tid = row["trade_id"]

                # Within-source dedup by trade_id
                if tid in file_ids:
                    file_within_dup += 1
                    continue
                file_ids.add(tid)

                # Cross-source: only skip if exact trade_id already seen
                if tid in global_seen_ids:
                    cross_source_dup_count += 1
                    continue
                global_seen_ids.add(tid)

                all_rows.append(row)
                file_rows_out += 1

        per_file_stats.append({
            "file": basename,
            "rows_in": file_rows_in,
            "rows_out": file_rows_out,
            "within_dup": file_within_dup,
        })

    # Sort by timestamp (NOT by trade_id)
    all_rows.sort(key=lambda r: int(r["timestamp_ms"]))

    total_dups = sum(s["within_dup"] for s in per_file_stats) + cross_source_dup_count

    # ── Merge log ──
    span_s = 0
    if all_rows:
        span_s = (int(all_rows[-1]["timestamp_ms"]) - int(all_rows[0]["timestamp_ms"])) / 1000

    print(f"\n  [MERGE]")
    print(f"  files: {len(files)}")
    print(f"  rows_in: {total_rows_in}")
    print(f"  rows_out: {len(all_rows)}")
    print(f"  duplicates_removed: {total_dups}")
    print(f"    within_source: {sum(s['within_dup'] for s in per_file_stats)}")
    print(f"    cross_source: {cross_source_dup_count}")
    print(f"  time_span_seconds: {span_s:.1f}")

    print()
    for s in per_file_stats:
        print(f"    {s['file']}: {s['rows_in']} in → {s['rows_out']} out "
              f"(within_dup={s['within_dup']})")

    # ── Trade rate check ──
    if all_rows:
        sec_counts = Counter()
        for r in all_rows:
            sec = int(r["timestamp_ms"]) // 1000
            sec_counts[sec] += 1

        rates = sorted(sec_counts.values())
        mean_r = sum(rates) / len(rates)
        std_r = math.sqrt(sum((r - mean_r) ** 2 for r in rates) / len(rates))

        # Intra-second clustering
        ts_counts = Counter(int(r["timestamp_ms"]) for r in all_rows)
        max_same_ts = max(ts_counts.values())
        multi_ts = sum(1 for c in ts_counts.values() if c > 1)
        multi_pct = multi_ts / len(ts_counts) * 100 if ts_counts else 0

        print(f"\n  [TRADE RATE]")
        print(f"  mean: {mean_r:.1f}/s  std: {std_r:.1f}  min: {min(rates)}  max: {max(rates)}")
        print(f"  max_same_timestamp: {max_same_ts}  pct_multi: {multi_pct:.1f}%")

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
            print(f"  ⚠ FLATLINE: {max(flatline_runs)} consecutive seconds with ≤1 trade")
        if multi_pct > 30 and max_same_ts > 20:
            print(f"  ⚠ BUFFERED: {multi_pct:.0f}% timestamps have multiple trades (max {max_same_ts}/ms)")

    # Write merged
    output_path = os.path.join(DATA_RAW, "trades_merged.csv")
    with open(output_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=[
            "timestamp_ms", "timestamp_utc", "price", "quantity",
            "is_buyer_maker", "agggressor_side", "trade_id"
        ])
        w.writeheader()
        w.writerows(all_rows)

    print(f"\n  Merged: {len(all_rows):,} trades → {output_path}")
    if all_rows:
        print(f"  Time span: {span_s:.0f}s ({span_s/60:.1f} min = {span_s/3600:.1f} hr)")

    return output_path


def run_feature_engineering(input_path):
    """Run feature_engineering.py on merged data."""
    print(f"\n  Running feature engineering...")
    result = subprocess.run(
        [sys.executable, "feature_engineering.py", input_path],
        capture_output=True, text=True
    )
    print(result.stdout)
    if result.stderr:
        print(result.stderr)
    return result.returncode == 0


def run_integrity_validation(trades_path, features_path=None):
    """Run validate_integrity.py on merged data."""
    print(f"\n  Running integrity validation...")
    cmd = [sys.executable, "validate_integrity.py", trades_path, "--merged"]
    if features_path and os.path.exists(features_path):
        cmd.extend(["--features", features_path])

    result = subprocess.run(cmd, capture_output=True, text=True)
    print(result.stdout)
    if result.stderr:
        print(result.stderr)
    return result.returncode == 0


def run_audit(features_path):
    """Run data_audit.py on features."""
    print(f"\n  Running data audit...")
    result = subprocess.run(
        [sys.executable, "data_audit.py", features_path],
        capture_output=True, text=True
    )
    print(result.stdout)
    if result.stderr:
        print(result.stderr)
    return result.returncode == 0


def accumulation_report():
    """Print current accumulation status."""
    print(f"\n{'='*60}")
    print(f"  ACCUMULATION STATUS")
    print(f"{'='*60}")

    raw_files = glob.glob(os.path.join(DATA_RAW, "trades_*.csv"))
    raw_files = [f for f in raw_files if "merged" not in f]
    merged = os.path.join(DATA_RAW, "trades_merged.csv")
    feat_files = glob.glob(os.path.join(DATA_PROC, "features_*.csv"))
    integrity_files = glob.glob(os.path.join(DATA_PROC, "integrity_*.csv"))

    print(f"  Raw trade files:     {len(raw_files)}")

    if os.path.exists(merged):
        with open(merged) as f:
            rows = sum(1 for _ in f) - 1
        print(f"  Merged trades:       {rows:,}")
    else:
        print(f"  Merged trades:       (not yet merged)")

    if feat_files:
        latest = max(feat_files, key=os.path.getmtime)
        with open(latest) as f:
            feat_rows = sum(1 for _ in f) - 1
        print(f"  Latest features:     {feat_rows:,} rows ({os.path.basename(latest)})")

    print(f"  Feature files:       {len(feat_files)}")
    print(f"  Integrity reports:   {len(integrity_files)}")


# ─── Main ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "--full"

    if mode == "--fetch":
        fetch_historical_batches(num_batches=50)

    elif mode == "--process":
        merged = merge_raw_csvs()
        if merged:
            feat_path = os.path.join(
                DATA_PROC,
                os.path.basename(merged).replace("trades_", "features_")
            )
            run_feature_engineering(merged)
            if os.path.exists(feat_path):
                run_integrity_validation(merged, feat_path)
            else:
                run_integrity_validation(merged)

    elif mode == "--full":
        print("  [1/5] Fetching historical batches...")
        fetch_historical_batches(num_batches=50)

        print(f"\n  [2/5] Merging raw datasets...")
        merged = merge_raw_csvs()

        if merged:
            print(f"\n  [3/5] Feature engineering...")
            run_feature_engineering(merged)

            feat_path = os.path.join(
                DATA_PROC,
                os.path.basename(merged).replace("trades_", "features_")
            )

            print(f"\n  [4/5] Integrity validation...")
            if os.path.exists(feat_path):
                valid = run_integrity_validation(merged, feat_path)
            else:
                valid = run_integrity_validation(merged)

            if valid:
                print(f"\n  [5/5] Data audit...")
                run_audit(feat_path)
            else:
                print(f"\n  [5/5] SKIPPED — integrity check failed")

        accumulation_report()

    else:
        print(f"Unknown mode: {mode}")
        print("Usage: python3 accumulate.py [--full|--fetch|--process]")
        sys.exit(1)
