#!/usr/bin/env python3
"""
Binance Futures Historical Trade Fetcher
Pulls recent aggregate trades via REST API (up to 1000 per request).
Use this to bootstrap initial dataset before running live collector.
"""

import json
import csv
import time
import os
import urllib.request

SYMBOL = "BTCUSDT"
BASE_URL = "https://fapi.binance.com"
OUTPUT_DIR = "data/raw"


def fetch_recent_trades(limit=1000):
    """GET /fapi/v1/trades — last 1000 trades, no auth needed."""
    url = f"{BASE_URL}/fapi/v1/trades?symbol={SYMBOL}&limit={min(limit, 1000)}"
    req = urllib.request.Request(url, headers={"User-Agent": "btc-quant-research/1.0"})
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read().decode())


def fetch_historical_trades(from_id=None, limit=1000):
    """GET /fapi/v1/historicalTrades — needs API key but no signature."""
    url = f"{BASE_URL}/fapi/v1/historicalTrades?symbol={SYMBOL}&limit={min(limit, 1000)}"
    if from_id:
        url += f"&fromId={from_id}"
    req = urllib.request.Request(url, headers={"User-Agent": "btc-quant-research/1.0"})
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read().decode())


def save_trades(trades, path):
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow([
            "timestamp_ms", "timestamp_utc", "price", "quantity",
            "is_buyer_maker", "agggressor_side", "trade_id"
        ])
        for t in trades:
            ts_ms = t["time"]
            ts_utc = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(ts_ms / 1000)) + f".{ts_ms % 1000:03d}"
            side = "SELL" if t["isBuyerMaker"] else "BUY"
            w.writerow([ts_ms, ts_utc, t["price"], t["qty"], t["isBuyerMaker"], side, t["id"]])


if __name__ == "__main__":
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    ts = time.strftime("%Y%m%d_%H%M%S")

    print("  Fetching recent trades (REST /fapi/v1/trades)...")
    trades = fetch_recent_trades(1000)
    print(f"  Got {len(trades)} trades")

    path = os.path.join(OUTPUT_DIR, f"trades_hist_{ts}.csv")
    save_trades(trades, path)
    print(f"  Saved to: {path}")

    # Show time range
    if trades:
        first = trades[0]["time"]
        last = trades[-1]["time"]
        span = (last - first) / 1000
        print(f"  Time range: {span:.1f}s ({span/60:.1f} min)")
        print(f"  First: {time.strftime('%H:%M:%S', time.gmtime(first/1000))}")
        print(f"  Last:  {time.strftime('%H:%M:%S', time.gmtime(last/1000))}")
