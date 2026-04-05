#!/usr/bin/env python3
"""
CVD & Trade Intensity Computation
Reads raw trade CSV, computes 1-second aggregated metrics.
"""

import csv
import os
import sys
from collections import defaultdict


def compute_metrics(input_path, output_dir="data/processed"):
    """
    From raw tick trades, compute per-second:
    - buy_volume, sell_volume
    - trade_count
    - cvd_delta (buy_vol - sell_vol)
    - price_open, price_close, price_high, price_low
    - vwap (volume-weighted avg price)
    """
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(
        output_dir,
        os.path.basename(input_path).replace("trades_", "metrics_1s_")
    )

    # Aggregate per second
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

    # Sort by second, compute cumulative
    sorted_secs = sorted(seconds.keys())

    with open(output_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "timestamp_s", "timestamp_utc",
            "buy_vol", "sell_vol", "net_delta", "cvd_cumulative",
            "buy_trades", "sell_trades", "total_trades",
            "price_open", "price_high", "price_low", "price_close", "vwap",
            "total_volume"
        ])

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

            import time
            ts_utc = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(sec))

            writer.writerow([
                sec, ts_utc,
                round(d["buy_vol"], 6), round(d["sell_vol"], 6),
                round(net, 6), round(cvd_cumulative, 6),
                d["buy_count"], d["sell_count"],
                d["buy_count"] + d["sell_count"],
                prices[0], max(prices), min(prices), prices[-1],
                round(vwap, 2), round(total_vol, 6)
            ])

    return output_path


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 compute_cvd.py <raw_trades.csv>")
        sys.exit(1)

    path = sys.argv[1]
    out = compute_metrics(path)
    print(f"  Metrics written to: {out}")
