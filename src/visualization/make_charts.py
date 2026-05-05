"""
Chart Generator — Portfolio-ready visualization for BTC market data.

Generates clean PNG charts showing pipeline outputs.
Uses matplotlib only. No crazy styling. No false annotations.

Charts:
  - price_over_time.png      — cleaned price series
  - volume_over_time.png     — buy/sell volume aggregation
  - returns_over_time.png    — log returns over time
  - rolling_volatility.png   — realized volatility features
  - cvd_over_time.png        — cumulative volume delta (if available)
  - feature_preview.png      — multi-panel feature overview
"""
import os
import sys


def check_matplotlib():
    """Check if matplotlib is available."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        return True
    except ImportError:
        return False


def load_sample_data():
    """Load sample data for chart generation. Prefers processed features, falls back to raw."""
    import csv

    # Try processed features first
    features_path = os.path.join("data", "processed", "research_dataset_sample.csv")
    if not os.path.exists(features_path):
        features_path = os.path.join("data", "processed", "features.csv")
    if os.path.exists(features_path):
        rows = []
        with open(features_path) as f:
            reader = csv.DictReader(f)
            for row in reader:
                parsed = {}
                for k, v in row.items():
                    try:
                        parsed[k] = float(v)
                    except (ValueError, TypeError):
                        parsed[k] = v
                rows.append(parsed)
        if rows and "price_close" in rows[0]:
            return rows

    # Fall back to sample data (raw) — aggregate first
    sample_path = os.path.join("data", "sample", "sample_market_data.csv")
    if not os.path.exists(sample_path):
        print(f"  ⚠ No data found. Run scripts/run_pipeline.py first.")
        return None

    from src.processing.aggregate_trades import aggregate_trades_to_1s
    bars = aggregate_trades_to_1s(sample_path)
    if bars:
        from src.features.microstructure_features import compute_returns, compute_cvd
        bars = compute_returns(bars)
        bars = compute_cvd(bars)
    return bars


# ── Individual chart generators ──────────────────────────────────────────────


def generate_price_chart(rows, output_path):
    """Price over time — cleaned price series after processing."""
    import matplotlib.pyplot as plt

    timestamps = list(range(len(rows)))
    prices = [r["price_close"] for r in rows]

    fig, ax = plt.subplots(figsize=(12, 5))
    ax.plot(timestamps, prices, linewidth=0.8, color="#2196F3")
    ax.set_title("BTCUSDT Price (1s bars)", fontsize=14, fontweight="bold")
    ax.set_xlabel("Time (seconds)")
    ax.set_ylabel("Price (USDT)")
    ax.grid(True, alpha=0.3)
    ax.ticklabel_format(style="plain", axis="y")
    fig.tight_layout()
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"      → {output_path}")


def generate_volume_chart(rows, output_path):
    """Volume over time — buy vs sell volume aggregated from tick trades."""
    import matplotlib.pyplot as plt

    timestamps = list(range(len(rows)))
    buy_vol = [r["buy_vol"] for r in rows]
    sell_vol = [r["sell_vol"] for r in rows]

    fig, ax = plt.subplots(figsize=(12, 4))
    ax.bar(timestamps, buy_vol, alpha=0.7, color="#4CAF50", label="Buy Volume", width=1.0)
    ax.bar(timestamps, [-v for v in sell_vol], alpha=0.7, color="#F44336", label="Sell Volume", width=1.0)
    ax.set_title("BTCUSDT Volume by Side (1s bars)", fontsize=14, fontweight="bold")
    ax.set_xlabel("Time (seconds)")
    ax.set_ylabel("Volume (BTC)")
    ax.legend()
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"      → {output_path}")


def generate_returns_over_time_chart(rows, output_path):
    """Returns over time — log returns plotted as a time series."""
    import matplotlib.pyplot as plt

    timestamps = list(range(len(rows)))
    returns = [r.get("returns", 0) for r in rows]

    fig, ax = plt.subplots(figsize=(12, 4))
    ax.plot(timestamps, returns, linewidth=0.5, color="#2196F3", alpha=0.8)
    ax.axhline(y=0, color="black", linewidth=0.5)
    ax.set_title("BTCUSDT Log Returns (1s)", fontsize=14, fontweight="bold")
    ax.set_xlabel("Time (seconds)")
    ax.set_ylabel("Log Return")
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"      → {output_path}")


def generate_volatility_chart(rows, output_path):
    """Rolling volatility — realized volatility at multiple scales."""
    import matplotlib.pyplot as plt

    timestamps = list(range(len(rows)))
    vol30 = [r.get("realized_vol_30s", 0) for r in rows]
    vol60 = [r.get("realized_vol_60s", 0) for r in rows]

    fig, ax = plt.subplots(figsize=(12, 4))
    ax.plot(timestamps, vol30, linewidth=0.8, color="#9C27B0", label="30s Vol", alpha=0.8)
    ax.plot(timestamps, vol60, linewidth=0.8, color="#E91E63", label="60s Vol", alpha=0.8)
    ax.set_title("BTCUSDT Realized Volatility", fontsize=14, fontweight="bold")
    ax.set_xlabel("Time (seconds)")
    ax.set_ylabel("Volatility (σ)")
    ax.legend()
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"      → {output_path}")


def generate_cvd_chart(rows, output_path):
    """CVD over time — cumulative volume delta tracking buy/sell pressure."""
    import matplotlib.pyplot as plt

    timestamps = list(range(len(rows)))
    cvd = [r.get("cvd_cumulative", r.get("cvd", 0)) for r in rows]

    fig, ax = plt.subplots(figsize=(12, 4))
    ax.plot(timestamps, cvd, linewidth=1.0, color="#FF9800")
    ax.fill_between(timestamps, cvd, alpha=0.2, color="#FF9800")
    ax.set_title("BTCUSDT Cumulative Volume Delta (CVD)", fontsize=14, fontweight="bold")
    ax.set_xlabel("Time (seconds)")
    ax.set_ylabel("CVD (BTC)")
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"      → {output_path}")


def generate_feature_preview_chart(rows, output_path):
    """Feature preview — multi-panel overview of key engineered features."""
    import matplotlib.pyplot as plt

    timestamps = list(range(len(rows)))

    fig, axes = plt.subplots(3, 2, figsize=(14, 10))
    fig.suptitle("Feature Preview — Engineered Features from Raw Trade Data",
                 fontsize=15, fontweight="bold")

    # Panel 1: Price
    ax = axes[0, 0]
    prices = [r["price_close"] for r in rows]
    ax.plot(timestamps, prices, linewidth=0.7, color="#2196F3")
    ax.set_title("Price (1s bars)", fontsize=11)
    ax.set_ylabel("USDT")
    ax.grid(True, alpha=0.3)

    # Panel 2: Returns
    ax = axes[0, 1]
    returns = [r.get("returns", 0) for r in rows]
    ax.plot(timestamps, returns, linewidth=0.4, color="#4CAF50", alpha=0.8)
    ax.axhline(y=0, color="black", linewidth=0.3)
    ax.set_title("Log Returns", fontsize=11)
    ax.set_ylabel("Return")
    ax.grid(True, alpha=0.3)

    # Panel 3: CVD
    ax = axes[1, 0]
    cvd = [r.get("cvd_cumulative", r.get("cvd", 0)) for r in rows]
    ax.plot(timestamps, cvd, linewidth=0.8, color="#FF9800")
    ax.fill_between(timestamps, cvd, alpha=0.15, color="#FF9800")
    ax.set_title("Cumulative Volume Delta", fontsize=11)
    ax.set_ylabel("CVD (BTC)")
    ax.grid(True, alpha=0.3)

    # Panel 4: Rolling Volatility
    ax = axes[1, 1]
    vol30 = [r.get("realized_vol_30s", 0) for r in rows]
    vol60 = [r.get("realized_vol_60s", 0) for r in rows]
    ax.plot(timestamps, vol30, linewidth=0.6, color="#9C27B0", label="30s", alpha=0.8)
    ax.plot(timestamps, vol60, linewidth=0.6, color="#E91E63", label="60s", alpha=0.8)
    ax.set_title("Realized Volatility", fontsize=11)
    ax.set_ylabel("σ")
    ax.legend(fontsize=8)
    ax.grid(True, alpha=0.3)

    # Panel 5: Volume Imbalance
    ax = axes[2, 0]
    imbalance = [r.get("vol_imbalance", 0) for r in rows]
    colors = ["#4CAF50" if v >= 0 else "#F44336" for v in imbalance]
    ax.bar(timestamps, imbalance, color=colors, alpha=0.6, width=1.0)
    ax.axhline(y=0, color="black", linewidth=0.3)
    ax.set_title("Volume Imbalance", fontsize=11)
    ax.set_ylabel("(buy−sell)/(buy+sell)")
    ax.set_xlabel("Time (seconds)")
    ax.grid(True, alpha=0.3)

    # Panel 6: Trade Intensity Z-Score
    ax = axes[2, 1]
    zscore = [r.get("trade_intensity_zscore", 0) for r in rows]
    ax.plot(timestamps, zscore, linewidth=0.5, color="#00BCD4", alpha=0.8)
    ax.axhline(y=0, color="black", linewidth=0.3)
    ax.axhline(y=2, color="red", linewidth=0.3, linestyle="--", alpha=0.5)
    ax.axhline(y=-2, color="red", linewidth=0.3, linestyle="--", alpha=0.5)
    ax.set_title("Trade Intensity Z-Score", fontsize=11)
    ax.set_ylabel("Z-Score")
    ax.set_xlabel("Time (seconds)")
    ax.grid(True, alpha=0.3)

    fig.tight_layout(rect=[0, 0, 1, 0.96])
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"      → {output_path}")


# ── Main entry point ────────────────────────────────────────────────────────


def generate_all_charts(output_dir=None):
    """Generate all portfolio charts."""
    if output_dir is None:
        output_dir = os.path.join("assets", "charts")
    os.makedirs(output_dir, exist_ok=True)

    if not check_matplotlib():
        print("  ⚠ matplotlib not installed. Install with: pip install matplotlib")
        print("  Skipping chart generation.")
        return False

    rows = load_sample_data()
    if not rows:
        return False

    print(f"  Generating charts from {len(rows)} rows...")

    # Required charts
    generate_price_chart(rows, os.path.join(output_dir, "price_over_time.png"))
    generate_volume_chart(rows, os.path.join(output_dir, "volume_over_time.png"))
    generate_returns_over_time_chart(rows, os.path.join(output_dir, "returns_over_time.png"))
    generate_volatility_chart(rows, os.path.join(output_dir, "rolling_volatility.png"))

    # CVD — only if data available
    has_cvd = any(r.get("cvd_cumulative", r.get("cvd", 0)) != 0 for r in rows)
    if has_cvd:
        generate_cvd_chart(rows, os.path.join(output_dir, "cvd_over_time.png"))
    else:
        print("      ⚠ CVD data not found — skipping cvd_over_time.png")

    # Feature preview
    generate_feature_preview_chart(rows, os.path.join(output_dir, "feature_preview.png"))

    count = 5 if has_cvd else 4
    print(f"\n  ✓ {count} charts saved to {output_dir}/")
    return True


if __name__ == "__main__":
    generate_all_charts()
