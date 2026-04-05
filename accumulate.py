#!/usr/bin/env python3
"""
Phase 3.5 — Data Accumulation Pipeline

Continuously collects and processes BTCUSDT microstructure data.

Usage:
  python3 accumulate.py              # Full pipeline: fetch + process
  python3 accumulate.py --process    # Process existing raw data only
  python3 accumulate.py --fetch      # Fetch historical batches only

Pipeline:
  1. Fetch historical trades (REST, multiple batches)
  2. Merge all raw CSVs into single dataset
  3. Run feature_engineering.py on merged data
  4. Save percentiles
  5. Print accumulation report

NOTE: Live collector (collector.py) should run separately in parallel.
"""

import csv
import glob
import json
import os
import subprocess
import sys
import time


DATA_RAW = "data/raw"
DATA_PROC = "data/processed"


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

    Reads:  data/raw/trades_*.csv
    Writes: data/raw/trades_merged.csv

    Deduplicates by trade_id (keeps first occurrence).
    Sorts by timestamp.
    """
    pattern = os.path.join(DATA_RAW, "trades_*.csv")
    files = sorted(glob.glob(pattern))

    if not files:
        print("  No raw trade files found.")
        return None

    print(f"  Merging {len(files)} raw files...")

    seen_ids = set()
    all_rows = []

    for fpath in files:
        with open(fpath, "r") as f:
            reader = csv.DictReader(f)
            count = 0
            for row in reader:
                tid = row["trade_id"]
                if tid not in seen_ids:
                    seen_ids.add(tid)
                    all_rows.append(row)
                    count += 1
        print(f"    {os.path.basename(fpath)}: {count} new trades")

    # Sort by timestamp
    all_rows.sort(key=lambda r: int(r["timestamp_ms"]))

    output_path = os.path.join(DATA_RAW, "trades_merged.csv")
    with open(output_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=[
            "timestamp_ms", "timestamp_utc", "price", "quantity",
            "is_buyer_maker", "agggressor_side", "trade_id"
        ])
        w.writeheader()
        w.writerows(all_rows)

    span = (int(all_rows[-1]["timestamp_ms"]) - int(all_rows[0]["timestamp_ms"])) / 1000
    print(f"  Merged: {len(all_rows)} unique trades → {output_path}")
    print(f"  Time span: {span:.0f}s ({span/60:.1f} min = {span/3600:.1f} hr)")

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
    merged = os.path.join(DATA_RAW, "trades_merged.csv")
    feat_files = glob.glob(os.path.join(DATA_PROC, "features_*.csv"))

    print(f"  Raw trade files:     {len(raw_files)}")

    if os.path.exists(merged):
        with open(merged) as f:
            rows = sum(1 for _ in f) - 1  # minus header
        print(f"  Merged trades:       {rows:,}")
    else:
        print(f"  Merged trades:       (not yet merged)")

    if feat_files:
        latest = max(feat_files, key=os.path.getmtime)
        with open(latest) as f:
            feat_rows = sum(1 for _ in f) - 1
        print(f"  Latest features:     {feat_rows:,} rows ({os.path.basename(latest)})")

    print(f"  Feature files:       {len(feat_files)}")


# ─── Main ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "--full"

    if mode == "--fetch":
        fetch_historical_batches(num_batches=50)

    elif mode == "--process":
        merged = merge_raw_csvs()
        if merged:
            run_feature_engineering(merged)

    elif mode == "--full":
        # Step 1: Fetch additional data
        print("  [1/4] Fetching historical batches...")
        fetch_historical_batches(num_batches=50)

        # Step 2: Merge all raw data
        print(f"\n  [2/4] Merging raw datasets...")
        merged = merge_raw_csvs()

        if merged:
            # Step 3: Feature engineering
            print(f"\n  [3/4] Feature engineering...")
            run_feature_engineering(merged)

            # Step 4: Audit
            print(f"\n  [4/4] Data audit...")
            feat_path = os.path.join(DATA_PROC, os.path.basename(merged).replace("trades_", "features_"))
            if os.path.exists(feat_path):
                run_audit(feat_path)

        accumulation_report()

    else:
        print(f"Unknown mode: {mode}")
        print("Usage: python3 accumulate.py [--full|--fetch|--process]")
        sys.exit(1)
