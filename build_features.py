#!/usr/bin/env python3
"""
Phase 2 — Feature Engineering

Reads 1s metrics CSV (from compute_cvd.py) and produces a feature matrix
for edge discovery. All rolling features use PRIOR data only (no lookahead).

Anti-overfitting principles:
  - No future data in any calculation
  - Winsorize outliers at 1st/99th percentile
  - Z-score normalization using expanding mean/std (not batch)

Usage:
  python3 build_features.py data/processed/metrics_1s_<timestamp>.csv
"""

import csv
import os
import sys
import math
from collections import deque


# ─── Helpers ──────────────────────────────────────────────────────────────────

def safe_log(x):
    """Log with guard against zero/negative."""
    return math.log(x) if x > 0 else 0.0


def linear_slope(values):
    """OLS slope of values over their index positions. Returns slope per-step."""
    n = len(values)
    if n < 2:
        return 0.0
    # Using simplified OLS: slope = cov(x,y) / var(x)
    # x = [0, 1, ..., n-1], y = values
    sum_x = n * (n - 1) / 2.0
    sum_x2 = (n - 1) * n * (2 * n - 1) / 6.0
    sum_y = sum(values)
    sum_xy = sum(i * v for i, v in enumerate(values))
    denom = n * sum_x2 - sum_x * sum_x
    if denom == 0:
        return 0.0
    return (n * sum_xy - sum_x * sum_y) / denom


def percentile(sorted_vals, p):
    """Percentile from pre-sorted list."""
    if not sorted_vals:
        return 0.0
    idx = int(len(sorted_vals) * p)
    idx = max(0, min(idx, len(sorted_vals) - 1))
    return sorted_vals[idx]


def winsorize_clip(value, low, high):
    """Clip value to [low, high] range."""
    return max(low, min(high, value))


# ─── Feature computation ─────────────────────────────────────────────────────

def compute_features(metrics, winsor_bounds=None):
    """
    From 1s metrics, compute feature matrix.

    Parameters:
        metrics: list of dicts from load_metrics()
        winsor_bounds: dict of {feature_name: (low, high)} for clipping.
                       If None, computed from the data (first pass).

    Returns:
        (features_list, winsor_bounds)
        features_list: list of dicts, one per second
        winsor_bounds: dict for reproducible clipping on new data
    """
    n = len(metrics)
    if n < 310:  # need at least 300s + margin for longest window
        print(f"  WARNING: Only {n}s of data. Need ≥310s for all features. Partial features only.")

    # Extract raw arrays for fast access
    prices = [m["price_close"] for m in metrics]
    net_deltas = [m["net_delta"] for m in metrics]
    cvd_cums = [m["cvd_cum"] for m in metrics]
    total_vols = [m["total_volume"] for m in metrics]
    total_trades = [m["total_trades"] for m in metrics]
    buy_vols = [m["buy_vol"] for m in metrics]
    sell_vols = [m["sell_vol"] for m in metrics]

    # Window sizes
    windows_ret = [1, 5, 30, 60]
    windows_vol = [30, 60, 300]
    windows_cvd_slope = [10, 30, 60]
    windows_delta_mom = [10, 30]
    windows_vroc = [30]

    # Max window needed
    max_win = max(max(windows_ret), max(windows_vol), max(windows_cvd_slope),
                  max(windows_delta_mom), max(windows_vroc), 60, 300)

    # Rolling buffers
    buf_price = deque(maxlen=max_win + 1)
    buf_net_delta = deque(maxlen=max_win)
    buf_cvd = deque(maxlen=max_win)
    buf_vol = deque(maxlen=max_win)
    buf_trades = deque(maxlen=max_win)
    buf_buy_vol = deque(maxlen=max_win)
    buf_sell_vol = deque(maxlen=max_win)

    # Expanding stats for z-score (trade intensity)
    cum_trades_sum = 0.0
    cum_trades_sq = 0.0
    cum_count = 0

    raw_features = []

    for i in range(n):
        m = metrics[i]
        buf_price.append(prices[i])
        buf_net_delta.append(net_deltas[i])
        buf_cvd.append(cvd_cums[i])
        buf_vol.append(total_vols[i])
        buf_trades.append(total_trades[i])
        buf_buy_vol.append(buy_vols[i])
        buf_sell_vol.append(sell_vols[i])

        feat = {
            "timestamp_s": m["ts"],
            "timestamp_utc": m["ts_utc"],
            "price_close": prices[i],
        }

        # ── Log returns ──
        for w in windows_ret:
            if len(buf_price) > w:
                ret = safe_log(buf_price[-1]) - safe_log(buf_price[-1 - w])
                feat[f"ret_{w}s"] = ret
            else:
                feat[f"ret_{w}s"] = None

        # ── Realized volatility (std of 1s returns) ──
        for w in windows_vol:
            if len(buf_price) > w + 1:
                window_prices = list(buf_price)[-(w + 1):]
                rets = [safe_log(window_prices[j + 1]) - safe_log(window_prices[j])
                        for j in range(len(window_prices) - 1)]
                mean_r = sum(rets) / len(rets)
                var_r = sum((r - mean_r) ** 2 for r in rets) / len(rets)
                feat[f"realized_vol_{w}s"] = math.sqrt(var_r)
            else:
                feat[f"realized_vol_{w}s"] = None

        # ── CVD slope (linear regression per step) ──
        for w in windows_cvd_slope:
            if len(buf_cvd) >= w:
                window_cvd = list(buf_cvd)[-w:]
                feat[f"cvd_slope_{w}s"] = linear_slope(window_cvd)
            else:
                feat[f"cvd_slope_{w}s"] = None

        # ── CVD-Price divergence (30s) ──
        # Divergence = normalized CVD change - normalized price change
        # When CVD rises but price falls (or vice versa), that's divergence
        div_w = 30
        if len(buf_cvd) >= div_w and len(buf_price) > div_w:
            cvd_change = buf_cvd[-1] - buf_cvd[-div_w]
            price_change = safe_log(buf_price[-1]) - safe_log(buf_price[-div_w])
            # Normalize cvd_change by its absolute value to get direction signal
            cvd_norm = math.copysign(1.0, cvd_change) if cvd_change != 0 else 0.0
            price_norm = math.copysign(1.0, price_change) if price_change != 0 else 0.0
            feat["cvd_price_divergence_30s"] = cvd_norm - price_norm  # range [-2, 2]
        else:
            feat["cvd_price_divergence_30s"] = None

        # ── Trade intensity z-score (expanding, 60s min) ──
        cum_count += 1
        cum_trades_sum += total_trades[i]
        cum_trades_sq += total_trades[i] ** 2

        if cum_count >= 60:
            mean_t = cum_trades_sum / cum_count
            var_t = cum_trades_sq / cum_count - mean_t ** 2
            std_t = math.sqrt(max(var_t, 1e-10))
            feat["trade_intensity_zscore"] = (total_trades[i] - mean_t) / std_t
        else:
            feat["trade_intensity_zscore"] = None

        # ── Net delta momentum (sum of net_delta over window) ──
        for w in windows_delta_mom:
            if len(buf_net_delta) >= w:
                feat[f"net_delta_mom_{w}s"] = sum(list(buf_net_delta)[-w:])
            else:
                feat[f"net_delta_mom_{w}s"] = None

        # ── Volume rate of change ──
        for w in windows_vroc:
            if len(buf_vol) >= w:
                recent_vol = sum(list(buf_vol)[-w:])
                if len(buf_vol) >= 2 * w:
                    prior_vol = sum(list(buf_vol)[-2 * w:-w])
                    if prior_vol > 0:
                        feat[f"vroc_{w}s"] = (recent_vol - prior_vol) / prior_vol
                    else:
                        feat[f"vroc_{w}s"] = None
                else:
                    feat[f"vroc_{w}s"] = None
            else:
                feat[f"vroc_{w}s"] = None

        # ── Efficiency ratio (30s) ──
        # = |net price movement| / sum of |each-step movement|
        eff_w = 30
        if len(buf_price) > eff_w:
            window_p = list(buf_price)[-(eff_w + 1):]
            net_move = abs(window_p[-1] - window_p[0])
            path_move = sum(abs(window_p[j + 1] - window_p[j]) for j in range(len(window_p) - 1))
            feat[f"efficiency_ratio_{eff_w}s"] = net_move / path_move if path_move > 0 else 0.0
        else:
            feat[f"efficiency_ratio_{eff_w}s"] = None

        # ── Buy/sell volume imbalance (instantaneous) ──
        bv = buy_vols[i]
        sv = sell_vols[i]
        total_bs = bv + sv
        feat["vol_imbalance"] = (bv - sv) / total_bs if total_bs > 0 else 0.0

        # ── Price distance from VWAP (1s) ──
        vwap = m["vwap"]
        if vwap > 0:
            feat["price_vwap_dist"] = (prices[i] - vwap) / vwap
        else:
            feat["price_vwap_dist"] = 0.0

        raw_features.append(feat)

    # ── Winsorization pass ──
    feature_names = [k for k in raw_features[0].keys()
                     if k not in ("timestamp_s", "timestamp_utc", "price_close")]

    if winsor_bounds is None:
        winsor_bounds = {}
        for fname in feature_names:
            vals = sorted([f[fname] for f in raw_features if f[fname] is not None])
            if len(vals) >= 100:
                lo = percentile(vals, 0.01)
                hi = percentile(vals, 0.99)
                winsor_bounds[fname] = (lo, hi)
            else:
                winsor_bounds[fname] = (None, None)

    # Apply winsorization and fill None with 0
    for f in raw_features:
        for fname in feature_names:
            if f[fname] is None:
                f[fname] = 0.0
            elif winsor_bounds.get(fname, (None, None)) != (None, None):
                lo, hi = winsor_bounds[fname]
                f[fname] = winsorize_clip(f[fname], lo, hi)

    return raw_features, winsor_bounds


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


def save_features(features, output_path):
    """Save feature matrix to CSV."""
    if not features:
        print("  No features to save.")
        return

    fieldnames = list(features[0].keys())
    with open(output_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(features)

    print(f"  Features saved to: {output_path}")
    print(f"  Shape: {len(features)} rows × {len(fieldnames)} columns")
    print(f"  Feature columns: {len(fieldnames) - 3} (excl. timestamp_s, timestamp_utc, price_close)")


def save_winsor_bounds(bounds, output_path):
    """Save winsorization bounds for reproducibility."""
    with open(output_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["feature", "lower_bound", "upper_bound"])
        for fname, (lo, hi) in sorted(bounds.items()):
            w.writerow([fname, f"{lo:.10f}" if lo is not None else "",
                        f"{hi:.10f}" if hi is not None else ""])
    print(f"  Winsor bounds saved to: {output_path}")


def print_feature_summary(features):
    """Quick sanity check on feature distribution."""
    if not features:
        return

    feature_names = [k for k in features[0].keys()
                     if k not in ("timestamp_s", "timestamp_utc", "price_close")]

    print(f"\n  Feature Summary ({len(feature_names)} features):")
    print(f"  {'Feature':<30} {'Mean':>12} {'Std':>12} {'Min':>12} {'Max':>12}")
    print(f"  {'-' * 78}")

    for fname in feature_names:
        vals = [f[fname] for f in features]
        mean_v = sum(vals) / len(vals)
        std_v = math.sqrt(sum((v - mean_v) ** 2 for v in vals) / len(vals))
        min_v = min(vals)
        max_v = max(vals)
        print(f"  {fname:<30} {mean_v:>12.6f} {std_v:>12.6f} {min_v:>12.6f} {max_v:>12.6f}")


# ─── Main ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 build_features.py <metrics_1s.csv>")
        print("")
        print("Input:  Output of compute_cvd.py (1s aggregated metrics)")
        print("Output: data/features/features_<timestamp>.csv")
        print("        data/features/winsor_bounds_<timestamp>.csv")
        sys.exit(1)

    metrics_path = sys.argv[1]
    print(f"  Loading: {metrics_path}")
    metrics = load_metrics(metrics_path)
    print(f"  Loaded {len(metrics)} seconds of data")

    print(f"  Computing features (anti-overfit: prior-only rolling windows)...")
    features, bounds = compute_features(metrics)

    output_dir = "data/features"
    os.makedirs(output_dir, exist_ok=True)

    basename = os.path.basename(metrics_path).replace("metrics_1s_", "features_")
    feat_path = os.path.join(output_dir, basename)
    bounds_path = os.path.join(output_dir, "winsor_bounds_" + basename.replace("features_", ""))

    save_features(features, feat_path)
    save_winsor_bounds(bounds, bounds_path)
    print_feature_summary(features)

    print(f"\n  Done. Use {feat_path} as input for Phase 3 (Signal Discovery).")
