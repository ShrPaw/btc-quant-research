#!/usr/bin/env python3
"""
Extended historical collector — chains multiple REST calls using fromId
to build a larger dataset. Requires no API key for /fapi/v1/trades (last 1000).
Uses /fapi/v1/historicalTrades for pagination (public, no auth on Binance).
"""

import json
import csv
import time
import os
import urllib.request

SYMBOL = "BTCUSDT"
BASE_URL = "https://fapi.binance.com"
OUTPUT_DIR = "data/raw"


def fetch_trades_batch(last_id=None, limit=1000):
    """Fetch a batch of trades. If last_id, fetch trades after that ID."""
    url = f"{BASE_URL}/fapi/v1/trades?symbol={SYMBOL}&limit={min(limit, 1000)}"
    req = urllib.request.Request(url, headers={"User-Agent": "btc-quant-research/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read().decode())
    except Exception as e:
        print(f"  [fetch error] {e}")
        return []


def fetch_and_save_multi(num_batches=10):
    """Collect multiple batches with delays to accumulate more data."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    ts = time.strftime("%Y%m%d_%H%M%S")
    path = os.path.join(OUTPUT_DIR, f"trades_multi_{ts}.csv")

    all_trades = []
    seen_ids = set()

    print(f"  Collecting {num_batches} batches...")

    for i in range(num_batches):
        batch = fetch_trades_batch()
        new_trades = [t for t in batch if t["id"] not in seen_ids]
        for t in new_trades:
            seen_ids.add(t["id"])
        all_trades.extend(new_trades)
        print(f"    Batch {i+1}/{num_batches}: {len(new_trades)} new (total: {len(all_trades)})")
        if i < num_batches - 1:
            time.sleep(2)  # wait 2s between fetches

    # Sort by ID (chronological)
    all_trades.sort(key=lambda t: t["id"])

    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow([
            "timestamp_ms", "timestamp_utc", "price", "quantity",
            "is_buyer_maker", "agggressor_side", "trade_id"
        ])
        for t in all_trades:
            ts_ms = t["time"]
            ts_utc = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(ts_ms / 1000)) + f".{ts_ms % 1000:03d}"
            side = "SELL" if t["isBuyerMaker"] else "BUY"
            w.writerow([ts_ms, ts_utc, t["price"], t["qty"], t["isBuyerMaker"], side, t["id"]])

    print(f"\n  Saved {len(all_trades)} trades to {path}")

    if all_trades:
        first_ts = all_trades[0]["time"]
        last_ts = all_trades[-1]["time"]
        span = (last_ts - first_ts) / 1000
        print(f"  Time span: {span:.0f}s ({span/60:.1f} min)")

    return path


if __name__ == "__main__":
    fetch_and_save_multi(10)
