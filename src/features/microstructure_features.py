"""
Microstructure Feature Engineering

Computes 18 features from 1-second aggregated bars:
  - Log returns at multiple scales (1s, 5s, 30s, 60s)
  - Realized volatility (30s, 60s, 300s)
  - CVD linear regression slopes (10s, 30s, 60s)
  - CVD-price divergence
  - Trade intensity z-score (expanding)
  - Net delta momentum (10s, 30s)
  - Volume rate of change (30s)
  - Efficiency ratio (30s)
  - Volume imbalance
  - Price-VWAP distance

All rolling windows use PRIOR data only (zero lookahead bias).
Outliers winsorized at 1st/99th percentile.
"""
import math
from collections import defaultdict


def compute_returns(bars):
    """Compute log returns between consecutive bars."""
    for i, bar in enumerate(bars):
        if i > 0 and bars[i - 1]["price_close"] > 0 and bar["price_close"] > 0:
            bar["returns"] = round(math.log(bar["price_close"] / bars[i - 1]["price_close"]), 10)
        else:
            bar["returns"] = 0.0
    return bars


def compute_cvd(bars):
    """Compute cumulative volume delta from net_delta."""
    cvd = 0.0
    for bar in bars:
        cvd += bar["net_delta"]
        bar["cvd"] = round(cvd, 8)
    return bars


def compute_rolling_features(bars, windows=None):
    """
    Compute rolling features using PRIOR data only.

    For each bar i and window W:
      - return_Ws:    sum of returns over bars [i-W, i-1]
      - volume_Ws:    sum of total_volume over bars [i-W, i-1]
      - intensity_Ws: sum of total_trades over bars [i-W, i-1]
      - cvd_delta_Ws: CVD[i] - CVD[i-W]
    """
    if windows is None:
        from src.utils.config import ROLLING_WINDOWS
        windows = ROLLING_WINDOWS

    n = len(bars)

    for i in range(n):
        for w in windows:
            start = i - w
            if start < 0:
                bars[i][f"return_{w}s"] = 0.0
                bars[i][f"volume_{w}s"] = 0.0
                bars[i][f"intensity_{w}s"] = 0
                bars[i][f"cvd_delta_{w}s"] = 0.0
            else:
                window = bars[start:i]
                bars[i][f"return_{w}s"] = round(sum(b["returns"] for b in window), 10)
                bars[i][f"volume_{w}s"] = round(sum(b["total_volume"] for b in window), 8)
                bars[i][f"intensity_{w}s"] = sum(b["total_trades"] for b in window)
                bars[i][f"cvd_delta_{w}s"] = round(bars[i]["cvd"] - bars[start]["cvd"], 8)

    return bars


def compute_realized_volatility(bars, windows=None):
    """Compute realized volatility (std of 1s returns) over rolling windows."""
    if windows is None:
        windows = [30, 60, 300]

    n = len(bars)
    for i in range(n):
        for w in windows:
            start = i - w
            if start < 0:
                bars[i][f"realized_vol_{w}s"] = 0.0
            else:
                rets = [b["returns"] for b in bars[start:i]]
                mean_r = sum(rets) / len(rets)
                var = sum((r - mean_r) ** 2 for r in rets) / len(rets)
                bars[i][f"realized_vol_{w}s"] = round(math.sqrt(var), 12)

    return bars


def compute_cvd_slope(bars, windows=None):
    """Compute CVD linear regression slope (OLS) over rolling windows."""
    if windows is None:
        windows = [10, 30, 60]

    n = len(bars)
    for i in range(n):
        for w in windows:
            start = i - w
            if start < 0:
                bars[i][f"cvd_slope_{w}s"] = 0.0
            else:
                cvd_vals = [b["cvd"] for b in bars[start:i]]
                m = len(cvd_vals)
                if m < 2:
                    bars[i][f"cvd_slope_{w}s"] = 0.0
                    continue
                x_mean = (m - 1) / 2
                y_mean = sum(cvd_vals) / m
                num = sum((j - x_mean) * (cvd_vals[j] - y_mean) for j in range(m))
                den = sum((j - x_mean) ** 2 for j in range(m))
                slope = num / den if den > 1e-15 else 0.0
                bars[i][f"cvd_slope_{w}s"] = round(slope, 10)

    return bars


def compute_cvd_price_divergence(bars, window=30):
    """Compute CVD direction − price direction over window."""
    n = len(bars)
    for i in range(n):
        start = i - window
        if start < 0:
            bars[i]["cvd_price_divergence_30s"] = 0.0
        else:
            cvd_change = bars[i]["cvd"] - bars[start]["cvd"]
            price_change = bars[i]["price_close"] - bars[start]["price_close"]

            cvd_dir = 1 if cvd_change > 0 else (-1 if cvd_change < 0 else 0)
            price_dir = 1 if price_change > 0 else (-1 if price_change < 0 else 0)
            bars[i]["cvd_price_divergence_30s"] = cvd_dir - price_dir

    return bars


def compute_trade_intensity_zscore(bars):
    """Compute expanding z-score of trade count."""
    n = len(bars)
    running_sum = 0.0
    running_sum_sq = 0.0

    for i in range(n):
        count = bars[i]["total_trades"]
        running_sum += count
        running_sum_sq += count ** 2

        mean = running_sum / (i + 1)
        var = running_sum_sq / (i + 1) - mean ** 2
        std = math.sqrt(var) if var > 1e-15 else 1.0

        bars[i]["trade_intensity_zscore"] = round((count - mean) / std, 6)

    return bars


def compute_net_delta_momentum(bars, windows=None):
    """Compute sum of net_delta over rolling windows."""
    if windows is None:
        windows = [10, 30]

    n = len(bars)
    for i in range(n):
        for w in windows:
            start = i - w
            if start < 0:
                bars[i][f"net_delta_mom_{w}s"] = 0.0
            else:
                bars[i][f"net_delta_mom_{w}s"] = round(
                    sum(b["net_delta"] for b in bars[start:i]), 8
                )

    return bars


def compute_volume_rate_of_change(bars, window=30):
    """Compute volume rate of change vs prior window."""
    n = len(bars)
    for i in range(n):
        start = i - window
        prev_start = start - window
        if prev_start < 0:
            bars[i]["vroc_30s"] = 0.0
        else:
            current_vol = sum(b["total_volume"] for b in bars[start:i])
            prior_vol = sum(b["total_volume"] for b in bars[prev_start:start])
            if prior_vol > 1e-15:
                bars[i]["vroc_30s"] = round((current_vol - prior_vol) / prior_vol, 8)
            else:
                bars[i]["vroc_30s"] = 0.0

    return bars


def compute_efficiency_ratio(bars, window=30):
    """Compute efficiency ratio: |net movement| / sum of |step movements|."""
    n = len(bars)
    for i in range(n):
        start = i - window
        if start < 0:
            bars[i]["efficiency_ratio_30s"] = 0.0
        else:
            prices = [b["price_close"] for b in bars[start:i]]
            net_movement = abs(prices[-1] - prices[0])
            step_movement = sum(abs(prices[j] - prices[j-1]) for j in range(1, len(prices)))
            if step_movement > 1e-15:
                bars[i]["efficiency_ratio_30s"] = round(net_movement / step_movement, 8)
            else:
                bars[i]["efficiency_ratio_30s"] = 0.0

    return bars


def compute_volume_imbalance(bars):
    """Compute instant volume imbalance: (buy - sell) / (buy + sell)."""
    for bar in bars:
        total = bar["buy_vol"] + bar["sell_vol"]
        if total > 1e-15:
            bar["vol_imbalance"] = round((bar["buy_vol"] - bar["sell_vol"]) / total, 8)
        else:
            bar["vol_imbalance"] = 0.0
    return bars


def compute_price_vwap_distance(bars):
    """Compute instant price-VWAP distance: (price - vwap) / vwap."""
    for bar in bars:
        if bar["vwap"] > 1e-15:
            bar["price_vwap_dist"] = round(
                (bar["price_close"] - bar["vwap"]) / bar["vwap"], 8
            )
        else:
            bar["price_vwap_dist"] = 0.0
    return bars


def winsorize(bars, feature_names, lower=None, upper=None):
    """Winsorize features at given percentiles. Returns (bounds, modified_bars)."""
    if lower is None:
        from src.utils.config import WINSOR_LOWER
        lower = WINSOR_LOWER
    if upper is None:
        from src.utils.config import WINSOR_UPPER
        upper = WINSOR_UPPER

    bounds = {}
    for fname in feature_names:
        vals = sorted(b[fname] for b in bars)
        n = len(vals)
        lo = vals[max(0, int(n * lower))]
        hi = vals[min(n - 1, int(n * upper))]
        bounds[fname] = {"lower": lo, "upper": hi}

        for bar in bars:
            if bar[fname] < lo:
                bar[fname] = lo
            elif bar[fname] > hi:
                bar[fname] = hi

    return bounds, bars


def build_feature_matrix(input_path):
    """
    Full feature engineering pipeline.

    Args:
        input_path: Path to raw trades CSV

    Returns:
        (bars, bounds) — feature matrix rows and winsorization bounds
    """
    from src.processing.aggregate_trades import aggregate_trades_to_1s

    # Step 1: Aggregate to 1s bars
    bars = aggregate_trades_to_1s(input_path)

    # Step 2: Base features
    bars = compute_returns(bars)
    bars = compute_cvd(bars)

    # Step 3: Rolling features
    bars = compute_rolling_features(bars)
    bars = compute_realized_volatility(bars)
    bars = compute_cvd_slope(bars)
    bars = compute_cvd_price_divergence(bars)
    bars = compute_trade_intensity_zscore(bars)
    bars = compute_net_delta_momentum(bars)
    bars = compute_volume_rate_of_change(bars)
    bars = compute_efficiency_ratio(bars)
    bars = compute_volume_imbalance(bars)
    bars = compute_price_vwap_distance(bars)

    # Step 4: Winsorize
    feature_names = [
        "returns", "cvd",
        "return_5s", "return_15s", "return_30s",
        "volume_5s", "volume_15s", "volume_30s",
        "intensity_5s", "intensity_15s", "intensity_30s",
        "cvd_delta_5s", "cvd_delta_15s", "cvd_delta_30s",
        "realized_vol_30s", "realized_vol_60s", "realized_vol_300s",
        "cvd_slope_10s", "cvd_slope_30s", "cvd_slope_60s",
        "cvd_price_divergence_30s", "trade_intensity_zscore",
        "net_delta_mom_10s", "net_delta_mom_30s",
        "vroc_30s", "efficiency_ratio_30s",
        "vol_imbalance", "price_vwap_dist",
    ]
    bounds, bars = winsorize(bars, feature_names)

    return bars, bounds
