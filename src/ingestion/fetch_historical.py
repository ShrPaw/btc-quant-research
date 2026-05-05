"""
Binance Futures Historical Trade Fetcher
Pulls recent aggregate trades via REST API (up to 1000 per request).
Use this to bootstrap initial dataset before running live collector.

Adapted from fetch_historical.py — uses urllib (no external dependencies).
"""
import json
import csv
import time
import os
import urllib.request

from src.utils.config import SYMBOL, TRADES_ENDPOINT, MAX_TRADES_PER_REQUEST


def fetch_recent_trades(limit=None):
    """GET /fapi/v1/trades — last 1000 trades, no auth needed."""
    if limit is None:
        limit = MAX_TRADES_PER_REQUEST
    limit = min(limit, MAX_TRADES_PER_REQUEST)
    url = f"{TRADES_ENDPOINT}?symbol={SYMBOL}&limit={limit}"
    req = urllib.request.Request(url, headers={"User-Agent": "btc-quant-research/1.0"})
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read().decode())


def trades_to_rows(trades):
    """Convert Binance API response to standard row format."""
    rows = []
    for t in trades:
        ts_ms = t["time"]
        ts_utc = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(ts_ms / 1000)) + f".{ts_ms % 1000:03d}"
        side = "SELL" if t["isBuyerMaker"] else "BUY"
        rows.append({
            "timestamp_ms": str(ts_ms),
            "timestamp_utc": ts_utc,
            "price": str(t["price"]),
            "quantity": str(t["qty"]),
            "is_buyer_maker": str(t["isBuyerMaker"]),
            "agggressor_side": side,
            "trade_id": str(t["id"]),
        })
    return rows


def save_trades(rows, output_path):
    """Save trade rows to CSV."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    fieldnames = [
        "timestamp_ms", "timestamp_utc", "price", "quantity",
        "is_buyer_maker", "agggressor_side", "trade_id"
    ]
    with open(output_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)
    return output_path


def fetch_and_save(output_path=None):
    """Fetch recent trades and save to CSV. Returns (rows, path)."""
    if output_path is None:
        from src.utils.io import timestamp_filename
        from src.utils.config import DATA_RAW
        os.makedirs(DATA_RAW, exist_ok=True)
        output_path = os.path.join(DATA_RAW, timestamp_filename("trades_hist"))

    trades = fetch_recent_trades()
    rows = trades_to_rows(trades)
    save_trades(rows, output_path)

    if rows:
        first = int(rows[0]["timestamp_ms"])
        last = int(rows[-1]["timestamp_ms"])
        span = (last - first) / 1000
        print(f"  Fetched {len(rows)} trades, span: {span:.1f}s ({span/60:.1f} min)")

    return rows, output_path


if __name__ == "__main__":
    rows, path = fetch_and_save()
    print(f"  Saved to: {path}")
