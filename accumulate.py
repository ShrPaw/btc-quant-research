#!/usr/bin/env python3
"""
Phase 3.5 — Data Accumulation Pipeline

Continuously collects and processes BTCUSDT microstructure data.

Usage:
  python3 accumulate.py              # Full pipeline: fetch + merge + process + validate
  python3 accumulate.py --process    # Merge existing raw data + process + validate
  python3 accumulate.py --fetch      # Fetch historical batches only

Pipeline:
  1. Fetch historical trades (REST, multiple batches)
  2. Merge all raw CSVs into single dataset (with validation log)
  3. Run feature_engineering.py on merged data
  4. Run validate_integrity.py
  5. Print accumulation report

NOTE: Live collector (collector.py) should run separately in parallel.
      Merged dataset is TEMPORARY. Raw partitioned data is source of truth.
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

# Percentiles policy: only recompute when dataset doubles or +30 min new data
PERCENTILES_MIN_INTERVAL_S = 1800  # 30 minutes
LAST_PERCENTILES_SIZE = 0
LAST_PERCENTILES_TIME = 0


def fetch_historical_batches(num_batches=50, delay_s=2):
    """
    Fetch multiple batches of recent trades via REST.
    Deduplicates by trade_id.
    Saves to data/raw/trades_hist_<timestamp>.csv
    """
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
    Merge all raw trade CSVs into a single deduplicated dataset.

    Reads:  data/raw/trades_*.csv (append-only, partitioned)
    Writes: data/raw/trades_merged.csv (TEMPORARY, not source of truth)

    Rules:
      - Deduplicates by trade_id (keeps first occurrence)
      - Sorts by timestamp
      - Raw data NEVER overwritten
    """
    pattern = os.path.join(DATA_RAW, "trades_*.csv")
    files = sorted(glob.glob(pattern))

    if not files:
        print("  No raw trade files found.")
        return None

    # Exclude merged file itself
    files = [f for f in files if "merged" not in f]

    print(f"  Merging {len(files)} raw files...")

    # Deduplicate by composite key: (timestamp_ms, price, quantity, aggressor_side, trade_id)
    # Trade IDs are NOT globally monotonic across data sources (REST vs WebSocket).
    seen_keys = set()
    all_rows = []
    total_rows_in = 0
    duplicates_removed = 0
    per_file = []

    for fpath in files:
        with open(fpath, "r") as f:
            reader = csv.DictReader(f)
            file_count = 0
            file_new = 0
            for row in reader:
                file_count += 1
                total_rows_in += 1
                # Composite dedup key (trade_id alone is insufficient across sources)
                key = (
                    row["timestamp_ms"],
                    row["price"],
                    row["quantity"],
                    row["agggressor_side"],
                    row["trade_id"],
                )
                if key not in seen_keys:
                    seen_keys.add(key)
                    all_rows.append(row)
                    file_new += 1
                else:
                    duplicates_removed += 1
            per_file.append((os.path.basename(fpath), file_count, file_new))

    # Sort by timestamp (NOT by trade_id — IDs are source-dependent)
    all_rows.sort(key=lambda r: int(r["timestamp_ms"]))

    # ── Merge validation log ──
    print(f"\n  [MERGE]")
    print(f"  files: {len(files)}")
    print(f"  rows_in: {total_rows_in}")
    print(f"  rows_out: {len(all_rows)}")
    print(f"  duplicates_removed: {duplicates_removed}")

    if all_rows:
        span_s = (int(all_rows[-1]["timestamp_ms"]) - int(all_rows[0]["timestamp_ms"])) / 1000
        print(f"  time_span_seconds: {span_s:.1f}")
    else:
        span_s = 0
        print(f"  time_span_seconds: 0")

    print()
    for fname, count, new in per_file:
        print(f"    {fname}: {count} rows → {new} new")

    # ── Trade rate check on merged data ──
    if all_rows:
        sec_counts = Counter()
        for r in all_rows:
            sec = int(r["timestamp_ms"]) // 1000
            sec_counts[sec] += 1

        rates = sorted(sec_counts.values())
        mean_r = sum(rates) / len(rates)
        std_r = math.sqrt(sum((r - mean_r) ** 2 for r in rates) / len(rates))

        print(f"\n  [TRADE RATE]")
        print(f"  mean: {mean_r:.1f}/s  std: {std_r:.1f}  min: {min(rates)}  max: {max(rates)}")

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

    # Write merged
    output_path = os.path.join(DATA_RAW, "trades_merged.csv")
    with open(output_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=[
            "timestamp_ms", "timestamp_utc", "price", "quantity",
            "is_buyer_maker", "agggressor_side", "trade_id"
        ])
        w.writeheader()
        w.writerows(all_rows)

    print(f"\n  Merged: {len(all_rows):,} unique trades → {output_path}")
    if all_rows:
        print(f"  Time span: {span_s:.0f}s ({span_s/60:.1f} min = {span_s/3600:.1f} hr)")

    return output_path


def should_recompute_percentiles(current_size):
    """
    Percentiles policy:
      - Recompute when dataset size doubles
      - OR at least +30 minutes of new data
      - Otherwise: treat as unstable, skip
    """
    global LAST_PERCENTILES_SIZE, LAST_PERCENTILES_TIME

    now = time.time()

    if LAST_PERCENTILES_SIZE == 0:
        return True

    size_doubled = current_size >= 2 * LAST_PERCENTILES_SIZE
    time_elapsed = (now - LAST_PERCENTILES_TIME) >= PERCENTILES_MIN_INTERVAL_S

    if size_doubled or time_elapsed:
        return True

    return False


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
    cmd = [sys.executable, "validate_integrity.py", trades_path]
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
        # Step 1: Fetch additional data
        print("  [1/5] Fetching historical batches...")
        fetch_historical_batches(num_batches=50)

        # Step 2: Merge all raw data
        print(f"\n  [2/5] Merging raw datasets...")
        merged = merge_raw_csvs()

        if merged:
            # Step 3: Feature engineering
            print(f"\n  [3/5] Feature engineering...")
            run_feature_engineering(merged)

            feat_path = os.path.join(
                DATA_PROC,
                os.path.basename(merged).replace("trades_", "features_")
            )

            # Step 4: Integrity validation
            print(f"\n  [4/5] Integrity validation...")
            if os.path.exists(feat_path):
                valid = run_integrity_validation(merged, feat_path)
            else:
                valid = run_integrity_validation(merged)

            # Step 5: Audit (only if valid)
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
