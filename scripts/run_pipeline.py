#!/usr/bin/env python3
"""
Pipeline Runner — Execute the full BTC quant research pipeline.

Stages:
  1. Load sample or raw data
  2. Clean data
  3. Aggregate trades to 1-second bars
  4. Build feature matrix
  5. Save processed dataset
  6. Print summary statistics

Usage:
  python scripts/run_pipeline.py                          # Use sample data
  python scripts/run_pipeline.py data/raw/trades_*.csv    # Use specific file
"""
import os
import sys
import time

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def main():
    from src.utils.logging_utils import PipelineLogger
    from src.utils.config import DATA_RAW, DATA_PROCESSED, DATA_SAMPLE
    from src.utils.io import ensure_dir

    log = PipelineLogger("pipeline")
    log.header("BTC Quant Research — Data Pipeline")
    start_time = time.time()

    # ── Stage 1: Load data ────────────────────────────────────────────────────
    log.set_stage(1)
    log.stage("Loading market data")

    # Determine input source
    if len(sys.argv) > 1:
        input_path = sys.argv[1]
    else:
        # Try sample data first, then raw
        input_path = os.path.join(DATA_SAMPLE, "sample_market_data.csv")
        if not os.path.exists(input_path):
            # Try to find any raw data
            import glob
            raw_files = glob.glob(os.path.join(DATA_RAW, "trades_*.csv"))
            raw_files = [f for f in raw_files if "merged" not in f]
            if raw_files:
                input_path = max(raw_files, key=os.path.getmtime)
            else:
                log.info("No input data found.")
                log.info("")
                log.info("Options:")
                log.info("  1. Run: python scripts/run_pipeline.py <path_to_trades.csv>")
                log.info("  2. Fetch live data first:")
                log.info("     python -c 'from src.ingestion.fetch_historical import fetch_and_save; fetch_and_save()'")
                log.info("     python scripts/run_pipeline.py")
                log.info("")
                log.info("Generating sample data for demonstration...")
                input_path = generate_sample_data(log)
                if input_path is None:
                    return

    if not os.path.exists(input_path):
        log.error(f"File not found: {input_path}")
        return

    log.info(f"Source: {input_path}")

    # Read raw data
    from src.utils.io import read_csv_rows
    raw_rows = read_csv_rows(input_path)
    log.result(f"{len(raw_rows):,} rows loaded")

    # ── Stage 2: Clean data ───────────────────────────────────────────────────
    log.set_stage(2)
    log.stage("Cleaning data")

    from src.processing.clean_data import clean_trades, print_cleaning_report
    cleaned, report = clean_trades(raw_rows)
    print_cleaning_report(report)
    log.result(f"{len(cleaned):,} rows after cleaning")

    # Save cleaned data temporarily
    import csv
    import tempfile
    cleaned_path = os.path.join(DATA_PROCESSED, "temp_cleaned.csv")
    ensure_dir(DATA_PROCESSED)
    fieldnames = ["timestamp_ms", "timestamp_utc", "price", "quantity",
                  "is_buyer_maker", "agggressor_side", "trade_id"]
    with open(cleaned_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(cleaned)

    # ── Stage 3: Aggregate trades ─────────────────────────────────────────────
    log.set_stage(3)
    log.stage("Aggregating trades to 1-second bars")

    from src.processing.aggregate_trades import aggregate_trades_to_1s, save_1s_metrics
    bars = aggregate_trades_to_1s(cleaned_path)
    log.info(f"1-second bars: {len(bars):,}")

    # Save 1s metrics
    metrics_path = os.path.join(DATA_PROCESSED, "metrics_1s.csv")
    save_1s_metrics(bars, metrics_path)
    log.result(f"Saved: {metrics_path}")

    # ── Stage 4: Build features ───────────────────────────────────────────────
    log.set_stage(4)
    log.stage("Building features: price, volume, delta, CVD, returns, volatility...")

    from src.features.microstructure_features import build_feature_matrix
    features, bounds = build_feature_matrix(cleaned_path)
    log.info(f"Features per bar: {len(features[0].keys()) if features else 0}")
    log.info(f"Bars with features: {len(features):,}")

    # ── Stage 5: Save processed dataset ───────────────────────────────────────
    log.set_stage(5)
    log.stage("Saving processed dataset")

    # Save as research_dataset_sample.csv (portfolio output name)
    if features:
        import json
        feat_fieldnames = list(features[0].keys())
        output_name = "research_dataset_sample.csv"
        feat_path = os.path.join(DATA_PROCESSED, output_name)
        with open(feat_path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=feat_fieldnames)
            w.writeheader()
            w.writerows(features)
        log.info(f"Output: data/processed/{output_name}")
        log.info(f"Shape: {len(features)} rows × {len(feat_fieldnames)} columns")

        # Save winsor bounds
        bounds_path = os.path.join(DATA_PROCESSED, "winsor_bounds.json")
        with open(bounds_path, "w") as f:
            json.dump(bounds, f, indent=2)

    log.result("Dataset saved")

    # ── Stage 6: Summary statistics ───────────────────────────────────────────
    log.set_stage(6)
    log.stage("Summary statistics")

    if features:
        prices = [f["price_close"] for f in features]
        volumes = [f["total_volume"] for f in features]
        deltas = [f["net_delta"] for f in features]

        log.info(f"Price range:  {min(prices):,.2f} — {max(prices):,.2f}")
        log.info(f"Total volume: {sum(volumes):,.4f} BTC")
        log.info(f"Mean delta:   {sum(deltas)/len(deltas):+.6f} BTC/s")
        log.info(f"Time span:    {len(features):,} seconds ({len(features)/60:.1f} min)")

        # Print feature list
        feature_names = [k for k in features[0].keys()
                         if k not in ("timestamp_s", "timestamp_utc")]
        log.info(f"Features generated: {', '.join(feature_names[:10])}...")
        log.info(f"  ({len(feature_names)} total columns)")

    # Clean up temp file
    if os.path.exists(cleaned_path):
        os.remove(cleaned_path)

    elapsed = time.time() - start_time
    log.header(f"Pipeline Complete ({elapsed:.1f}s)")
    print(f"  Processed: {input_path}")
    print(f"  Features:  {len(features):,} rows")
    print(f"  Output:    data/processed/research_dataset_sample.csv")


def generate_sample_data(log):
    """Generate a small sample dataset for demonstration."""
    import csv
    import time
    import random

    log.info("Generating synthetic sample data (1000 trades, ~5 min)...")

    sample_dir = os.path.join("data", "sample")
    os.makedirs(sample_dir, exist_ok=True)
    output_path = os.path.join(sample_dir, "sample_market_data.csv")

    # Generate realistic-ish BTCUSDT trades
    base_price = 67000.0
    base_ts = 1775351900000  # 2026-04-05 ~01:18 UTC

    rows = []
    price = base_price
    for i in range(1000):
        ts_ms = base_ts + i * 300  # ~300ms between trades
        ts_utc = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(ts_ms / 1000)) + f".{ts_ms % 1000:03d}"

        # Random walk price
        price += random.gauss(0, 0.5)
        price = max(price, base_price - 100)

        qty = round(random.expovariate(0.5) * 0.01, 6)
        qty = max(qty, 0.001)

        is_buyer_maker = random.random() < 0.5
        side = "SELL" if is_buyer_maker else "BUY"

        rows.append({
            "timestamp_ms": str(ts_ms),
            "timestamp_utc": ts_utc,
            "price": str(round(price, 2)),
            "quantity": str(qty),
            "is_buyer_maker": str(is_buyer_maker),
            "agggressor_side": side,
            "trade_id": str(1000000 + i),
        })

    fieldnames = ["timestamp_ms", "timestamp_utc", "price", "quantity",
                  "is_buyer_maker", "agggressor_side", "trade_id"]
    with open(output_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)

    log.info(f"Sample data: {output_path} ({len(rows)} trades)")
    return output_path


if __name__ == "__main__":
    main()
