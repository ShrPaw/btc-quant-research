#!/usr/bin/env python3
"""
Portfolio Asset Generator — Create PNG charts for portfolio/resume.

Generates clean, professional charts showing pipeline outputs.
Uses sample data (no large raw datasets required).

Charts:
  - price_over_time.png      — cleaned price series
  - volume_over_time.png     — buy/sell volume aggregation
  - returns_over_time.png    — log returns over time
  - rolling_volatility.png   — realized volatility features
  - cvd_over_time.png        — cumulative volume delta (if available)
  - feature_preview.png      — multi-panel feature overview

Usage:
  python3 scripts/generate_portfolio_assets.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def main():
    from src.utils.logging_utils import PipelineLogger

    log = PipelineLogger("assets")
    log.header("Portfolio Asset Generator")

    # Check matplotlib
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        log.success("matplotlib available")
    except ImportError:
        log.error("matplotlib not installed")
        log.info("Install with: pip install matplotlib")
        log.info("Then re-run: python3 scripts/generate_portfolio_assets.py")
        return

    # Check data
    processed = os.path.join("data", "processed", "research_dataset_sample.csv")
    sample = os.path.join("data", "sample", "sample_market_data.csv")
    if not os.path.exists(processed) and not os.path.exists(sample):
        log.info("No data found. Run scripts/run_pipeline.py first.")
        return

    # Generate charts
    log.set_stage(1)
    log.stage("Generating portfolio charts")

    from src.visualization.make_charts import generate_all_charts
    success = generate_all_charts()

    if success:
        log.header("Portfolio Assets Complete")
        log.info("Charts saved to: assets/charts/")
        log.info("")
        log.info("Generated charts:")
        log.info("  1. price_over_time.png      — cleaned price series")
        log.info("  2. volume_over_time.png     — buy/sell volume aggregation")
        log.info("  3. returns_over_time.png    — log returns time series")
        log.info("  4. rolling_volatility.png   — realized volatility features")
        log.info("  5. cvd_over_time.png        — cumulative volume delta")
        log.info("  6. feature_preview.png      — multi-panel feature overview")
        log.info("")
        log.info("Use these in:")
        log.info("  - Upwork portfolio")
        log.info("  - GitHub README")
        log.info("  - Resume/CV attachments")
        log.info("  - LinkedIn project posts")
    else:
        log.error("Chart generation failed")


if __name__ == "__main__":
    main()
