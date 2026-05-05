"""
Chart Generator — Portfolio-ready visualization for BTC market data.

Generates clean PNG charts showing pipeline outputs.
Uses matplotlib for static chart generation.
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

    # Load raw and aggregate
    from src.processing.aggregate_trades import aggregate_trades_to_1s
    bars = aggregate_trades_to_1s(sample_path)
    if bars:
        from src.features.microstructure_features import compute_returns, compute_cvd
        bars = compute_returns(bars)
        bars = compute_cvd(bars)
    return bars


def generate_price_chart(rows, output_path):
    """Generate price over time chart."""
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
    """Generate volume over time chart."""
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


def generate_delta_chart(rows, output_path):
    """Generate net delta over time chart."""
    import matplotlib.pyplot as plt

    timestamps = list(range(len(rows)))
    delta = [r["net_delta"] for r in rows]
    colors = ["#4CAF50" if d >= 0 else "#F44336" for d in delta]

    fig, ax = plt.subplots(figsize=(12, 4))
    ax.bar(timestamps, delta, color=colors, alpha=0.7, width=1.0)
    ax.set_title("BTCUSDT Net Delta (buy_vol − sell_vol per second)", fontsize=14, fontweight="bold")
    ax.set_xlabel("Time (seconds)")
    ax.set_ylabel("Net Delta (BTC)")
    ax.axhline(y=0, color="black", linewidth=0.5)
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"      → {output_path}")


def generate_cvd_chart(rows, output_path):
    """Generate CVD over time chart."""
    import matplotlib.pyplot as plt

    timestamps = list(range(len(rows)))
    cvd = [r["cvd_cumulative"] for r in rows]

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


def generate_volatility_chart(rows, output_path):
    """Generate rolling volatility chart."""
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


def generate_returns_chart(rows, output_path):
    """Generate returns distribution chart."""
    import matplotlib.pyplot as plt

    returns = [r.get("returns", 0) for r in rows]
    returns = [r for r in returns if r != 0]

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.hist(returns, bins=80, color="#2196F3", alpha=0.7, edgecolor="white", linewidth=0.3)
    ax.set_title("Distribution of 1s Log Returns", fontsize=14, fontweight="bold")
    ax.set_xlabel("Log Return")
    ax.set_ylabel("Frequency")
    ax.axvline(x=0, color="black", linewidth=0.5)
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"      → {output_path}")


def generate_feature_correlation_heatmap(rows, output_path):
    """Generate feature correlation heatmap."""
    import matplotlib.pyplot as plt
    import math

    feature_names = [
        "returns", "net_delta", "vol_imbalance", "price_vwap_dist",
        "trade_intensity_zscore", "realized_vol_30s", "cvd_slope_30s"
    ]
    feature_names = [f for f in feature_names if f in rows[0]]

    n = len(rows)
    columns = {fname: [r[fname] for r in rows] for fname in feature_names}
    means = {fname: sum(columns[fname]) / n for fname in feature_names}

    # Compute correlation matrix
    nf = len(feature_names)
    corr_matrix = [[0.0] * nf for _ in range(nf)]
    for i, fi in enumerate(feature_names):
        for j, fj in enumerate(feature_names):
            xi, xj = columns[fi], columns[fj]
            mi, mj = means[fi], means[fj]
            cov = sum((xi[k] - mi) * (xj[k] - mj) for k in range(n)) / n
            si = math.sqrt(sum((xi[k] - mi) ** 2 for k in range(n)) / n)
            sj = math.sqrt(sum((xj[k] - mj) ** 2 for k in range(n)) / n)
            if si > 1e-15 and sj > 1e-15:
                corr_matrix[i][j] = cov / (si * sj)

    fig, ax = plt.subplots(figsize=(8, 7))
    im = ax.imshow(corr_matrix, cmap="RdBu_r", vmin=-1, vmax=1, aspect="auto")

    short_names = [f.replace("_", "\n") for f in feature_names]
    ax.set_xticks(range(nf))
    ax.set_yticks(range(nf))
    ax.set_xticklabels(short_names, fontsize=8, rotation=45, ha="right")
    ax.set_yticklabels(short_names, fontsize=8)

    # Add text annotations
    for i in range(nf):
        for j in range(nf):
            val = corr_matrix[i][j]
            color = "white" if abs(val) > 0.6 else "black"
            ax.text(j, i, f"{val:.2f}", ha="center", va="center", fontsize=7, color=color)

    ax.set_title("Feature Correlation Matrix", fontsize=14, fontweight="bold")
    fig.colorbar(im, ax=ax, shrink=0.8)
    fig.tight_layout()
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"      → {output_path}")


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

    generate_price_chart(rows, os.path.join(output_dir, "price_over_time.png"))
    generate_volume_chart(rows, os.path.join(output_dir, "volume_over_time.png"))
    generate_delta_chart(rows, os.path.join(output_dir, "delta_over_time.png"))
    generate_cvd_chart(rows, os.path.join(output_dir, "cvd_over_time.png"))
    generate_volatility_chart(rows, os.path.join(output_dir, "rolling_volatility.png"))
    generate_returns_chart(rows, os.path.join(output_dir, "returns_distribution.png"))
    generate_feature_correlation_heatmap(rows, os.path.join(output_dir, "feature_correlation.png"))

    print(f"\n  ✓ 7 charts saved to {output_dir}/")
    return True


if __name__ == "__main__":
    generate_all_charts()
