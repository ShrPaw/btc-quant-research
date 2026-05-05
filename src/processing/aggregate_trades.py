"""
Trade Aggregation — Raw tick trades → 1-second bars

Reads raw trade CSV, computes per-second aggregated metrics:
  - buy_vol, sell_vol, net_delta, cvd_cumulative
  - buy_trades, sell_trades, total_trades
  - price_open, price_high, price_low, price_close
  - vwap, total_volume

Adapted from compute_cvd.py with cleaner interface.
"""
import csv
import os
import time
from collections import defaultdict


def aggregate_trades_to_1s(input_path):
    """
    Aggregate raw tick trades into 1-second bars.

    Args:
        input_path: Path to raw trades CSV

    Returns:
        list of dicts, one per second, with aggregated metrics
    """
    seconds = defaultdict(lambda: {
        "buy_vol": 0.0, "sell_vol": 0.0, "buy_count": 0, "sell_count": 0,
        "prices": [], "vols": []
    })

    with open(input_path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            sec = int(row["timestamp_ms"]) // 1000
            price = float(row["price"])
            qty = float(row["quantity"])
            side = row["agggressor_side"]

            seconds[sec]["prices"].append(price)
            seconds[sec]["vols"].append(qty)

            if side == "BUY":
                seconds[sec]["buy_vol"] += qty
                seconds[sec]["buy_count"] += 1
            else:
                seconds[sec]["sell_vol"] += qty
                seconds[sec]["sell_count"] += 1

    sorted_secs = sorted(seconds.keys())
    bars = []
    cvd_cumulative = 0.0

    for sec in sorted_secs:
        d = seconds[sec]
        net = d["buy_vol"] - d["sell_vol"]
        cvd_cumulative += net

        prices = d["prices"]
        vols = d["vols"]
        total_vol = sum(vols)

        if total_vol > 0:
            vwap = sum(p * v for p, v in zip(prices, vols)) / total_vol
        else:
            vwap = prices[0]

        ts_utc = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(sec))

        bars.append({
            "timestamp_s": sec,
            "timestamp_utc": ts_utc,
            "buy_vol": round(d["buy_vol"], 6),
            "sell_vol": round(d["sell_vol"], 6),
            "net_delta": round(net, 6),
            "cvd_cumulative": round(cvd_cumulative, 6),
            "buy_trades": d["buy_count"],
            "sell_trades": d["sell_count"],
            "total_trades": d["buy_count"] + d["sell_count"],
            "price_open": prices[0],
            "price_high": max(prices),
            "price_low": min(prices),
            "price_close": prices[-1],
            "vwap": round(vwap, 2),
            "total_volume": round(total_vol, 6),
        })

    return bars


def save_1s_metrics(bars, output_path):
    """Save 1-second aggregated bars to CSV."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    fieldnames = [
        "timestamp_s", "timestamp_utc",
        "buy_vol", "sell_vol", "net_delta", "cvd_cumulative",
        "buy_trades", "sell_trades", "total_trades",
        "price_open", "price_high", "price_low", "price_close",
        "vwap", "total_volume"
    ]
    with open(output_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(bars)
    return output_path


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python -m src.processing.aggregate_trades <raw_trades.csv>")
        sys.exit(1)

    bars = aggregate_trades_to_1s(sys.argv[1])
    print(f"  Aggregated {len(bars)} seconds of data")

    from src.utils.io import timestamp_filename
    from src.utils.config import DATA_PROCESSED
    out = os.path.join(DATA_PROCESSED, timestamp_filename("metrics_1s"))
    save_1s_metrics(bars, out)
    print(f"  Saved to: {out}")
