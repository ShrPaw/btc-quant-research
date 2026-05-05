# Sample Data

This directory contains a small sample dataset for demonstration purposes.

## Files

- `sample_market_data.csv` — Synthetic BTCUSDT trades (~1000 rows, ~5 min)

## How to Use

```bash
# Run the full pipeline on sample data
python scripts/run_pipeline.py

# Run on a specific file
python scripts/run_pipeline.py data/sample/sample_market_data.csv
```

## Expected Input Format

Raw trade CSV with these columns:

| Column | Type | Description |
|--------|------|-------------|
| `timestamp_ms` | int | Unix epoch milliseconds |
| `timestamp_utc` | string | Human-readable UTC timestamp |
| `price` | float | Trade price |
| `quantity` | float | Trade quantity (BTC) |
| `is_buyer_maker` | bool | True = sell aggressor |
| `agggressor_side` | string | BUY or SELL |
| `trade_id` | int | Exchange trade ID |

## Using Real Data

To use real Binance data:

```bash
# Fetch recent trades (no API key needed)
python -c "from src.ingestion.fetch_historical import fetch_and_save; fetch_and_save()"

# Then run pipeline
python scripts/run_pipeline.py data/raw/trades_hist_*.csv
```

## Pipeline Output

The pipeline generates:

- `data/processed/metrics_1s.csv` — 1-second aggregated bars
- `data/processed/research_dataset_sample.csv` — Full feature matrix (42+ features)
- `data/processed/winsor_bounds.json` — Winsorization bounds for reproducibility

## Large Datasets

For large raw datasets (>100MB), do NOT commit to git.
Store locally in `data/raw/` (gitignored) and document collection parameters.
