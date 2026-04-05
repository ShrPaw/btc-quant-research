#!/usr/bin/env python3
"""
Liquidation Cascade Proxy Detector

Event definition:
  ΔCVD_30s ≤ P5 (rolling, prior-only)
  AND return_30s ≤ 0
  AND trade_intensity_30s ≥ P75 (rolling, prior-only)

All percentiles computed on PRIOR data only (no lookahead).
Trigger at CLOSE of the 30s window where condition becomes TRUE.
"""

import csv
import os
import sys
import time
from collections import deque


def load_metrics(path):
    """Load 1s metrics CSV into list of dicts."""
    rows = []
    with open(path) as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append({
                "ts": int(r["timestamp_s"]),
                "ts_utc": r["timestamp_utc"],
                "buy_vol": float(r["buy_vol"]),
                "sell_vol": float(r["sell_vol"]),
                "net_delta": float(r["net_delta"]),
                "cvd_cum": float(r["cvd_cumulative"]),
                "buy_trades": int(r["buy_trades"]),
                "sell_trades": int(r["sell_trades"]),
                "total_trades": int(r["total_trades"]),
                "price_open": float(r["price_open"]),
                "price_high": float(r["price_high"]),
                "price_low": float(r["price_low"]),
                "price_close": float(r["price_close"]),
                "vwap": float(r["vwap"]),
                "total_volume": float(r["total_volume"]),
            })
    return rows


def detect_liquidation_proxy(metrics, window_s=30, lookback_windows=50):
    """
    Scan metrics for liquidation proxy events.

    For each 30s window ending at index i:
      - delta_cvd_30s = sum of net_delta in window
      - return_30s = (close_price[i] - close_price[i-window+1]) / close_price[i-window+1]
      - trade_intensity_30s = sum of total_trades in window

    Percentiles computed on prior N windows only (rolling).

    Returns list of event dicts.
    """
    if len(metrics) < window_s + lookback_windows:
        print(f"  ERROR: Need at least {window_s + lookback_windows}s of data, got {len(metrics)}s")
        return []

    events = []

    # Pre-compute rolling windows
    cvd_window = deque(maxlen=window_s)
    trade_window = deque(maxlen=window_s)

    # Store computed window stats for percentile calculation
    window_stats = []  # list of (delta_cvd, return_30s, intensity)

    for i, row in enumerate(metrics):
        cvd_window.append(row["net_delta"])
        trade_window.append(row["total_trades"])

        if i < window_s - 1:
            continue

        # Window metrics
        delta_cvd = sum(cvd_window)
        intensity = sum(trade_window)

        # Return: price at start vs end of window
        price_start = metrics[i - window_s + 1]["price_close"]
        price_end = row["price_close"]
        ret_30s = (price_end - price_start) / price_start if price_start > 0 else 0

        # Check if we have enough prior windows for percentile calc
        if len(window_stats) >= lookback_windows:
            # Extract prior values only
            prior_cvd = [w[0] for w in window_stats[-lookback_windows:]]
            prior_intensity = [w[2] for w in window_stats[-lookback_windows:]]

            # Sort for percentile
            sorted_cvd = sorted(prior_cvd)
            sorted_intensity = sorted(prior_intensity)

            p5_idx = int(len(sorted_cvd) * 0.05)
            p75_idx = int(len(sorted_intensity) * 0.75)

            p5_cvd = sorted_cvd[p5_idx]
            p75_intensity = sorted_intensity[p75_idx]

            # Event check
            if delta_cvd <= p5_cvd and ret_30s <= 0 and intensity >= p75_intensity:
                events.append({
                    "event_idx": len(events) + 1,
                    "window_end_ts": row["ts"],
                    "window_end_utc": row["ts_utc"],
                    "window_start_ts": metrics[i - window_s + 1]["ts"],
                    "window_start_utc": metrics[i - window_s + 1]["ts_utc"],
                    "delta_cvd_30s": round(delta_cvd, 6),
                    "return_30s": round(ret_30s, 8),
                    "trade_intensity_30s": intensity,
                    "price_start": price_start,
                    "price_end": price_end,
                    "cvd_at_trigger": row["cvd_cum"],
                    "p5_cvd_threshold": round(p5_cvd, 6),
                    "p75_intensity_threshold": p75_intensity,
                })

        window_stats.append((delta_cvd, ret_30s, intensity))

    return events


def save_events(events, output_path):
    """Save detected events to CSV."""
    if not events:
        print("  No events detected.")
        return

    with open(output_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=events[0].keys())
        w.writeheader()
        w.writerows(events)

    print(f"  Events saved to: {output_path}")
    print(f"  Total events: {len(events)}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 detect_liquidations.py <metrics_1s.csv> [window_s] [lookback]")
        sys.exit(1)

    metrics_path = sys.argv[1]
    window = int(sys.argv[2]) if len(sys.argv) > 2 else 30
    lookback = int(sys.argv[3]) if len(sys.argv) > 3 else 50

    print(f"  Loading: {metrics_path}")
    metrics = load_metrics(metrics_path)
    print(f"  Loaded {len(metrics)} seconds of data")
    print(f"  Window: {window}s | Lookback: {lookback} windows")
    print(f"  Conditions: ΔCVD_30s ≤ P5 AND return_30s ≤ 0 AND intensity_30s ≥ P75")

    events = detect_liquidation_proxy(metrics, window, lookback)

    os.makedirs("data/events", exist_ok=True)
    out = os.path.join("data/events", "liquidation_proxy_" + os.path.basename(metrics_path).replace("metrics_1s_", ""))
    save_events(events, out)

    if events:
        print(f"\n  First 3 events:")
        for e in events[:3]:
            print(f"    #{e['event_idx']} {e['window_end_utc']} | CVD={e['delta_cvd_30s']} | ret={e['return_30s']:.6f} | intensity={e['trade_intensity_30s']}")


def deduplicate_cascades(events, min_gap_s=60):
    """
    Group consecutive triggers into cascades.
    Keep only the FIRST trigger of each cascade.
    Cascade = events within min_gap_s of each other.
    """
    if not events:
        return []

    cascades = []
    current = [events[0]]

    for e in events[1:]:
        if e["window_end_ts"] - current[-1]["window_end_ts"] <= min_gap_s:
            current.append(e)
        else:
            cascades.append(current[0])  # keep first trigger
            current = [e]

    cascades.append(current[0])
    return cascades
