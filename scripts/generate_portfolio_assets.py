#!/usr/bin/env python3
"""
Portfolio Asset Generator — Create PNG charts for portfolio/resume.

Generates clean, professional charts showing pipeline outputs.
Uses sample data (no large raw datasets required).

Usage:
  python scripts/generate_portfolio_assets.py
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
        log.info("Then re-run: python scripts/generate_portfolio_assets.py")
        return

    # Check sample data
    sample_path = os.path.join("data", "sample", "sample_market_data.csv")
    if not os.path.exists(sample_path):
        log.info("Sample data not found. Running pipeline to generate...")
        log.info("Execute: python scripts/run_pipeline.py")
        log.info("Then re-run this script.")
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
        log.info("Recommended portfolio images:")
        log.info("  1. price_over_time.png      — shows data processing capability")
        log.info("  2. volume_over_time.png     — shows market microstructure understanding")
        log.info("  3. cvd_over_time.png        — shows advanced metric computation")
        log.info("  4. rolling_volatility.png   — shows feature engineering skills")
        log.info("  5. feature_correlation.png  — shows analytical depth")
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
