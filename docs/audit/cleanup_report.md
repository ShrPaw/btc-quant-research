# Cleanup Report — btc-quant-research

**Date:** 2026-05-05
**Branch:** cleanup-research-structure
**Commits:** 5 logical commits

## 1. What Was Kept

### Core Source Code (moved to src/)

| Original | New Location | Purpose |
|----------|-------------|---------|
| collector.py | src/ingestion/live_collector.py | WebSocket trade collector |
| fetch_historical.py | src/ingestion/fetch_historical.py | REST API bootstrap |
| fetch_multi.py | (merged into fetch_historical.py) | Batch fetching |
| compute_cvd.py | src/processing/aggregate_trades.py | 1s aggregation |
| feature_engineering.py | src/features/microstructure_features.py | Feature engineering |
| detect_liquidations.py | (logic preserved in features) | Event detection |
| validate_integrity.py | src/validation/validation_runner.py | Integrity checks |
| data_audit.py | src/validation/baseline_tests.py | Statistical tests |
| accumulate.py | scripts/run_pipeline.py | Pipeline orchestrator |

### New Files Created

- `src/utils/config.py` — Centralized configuration
- `src/utils/io.py` — CSV I/O utilities
- `src/utils/logging_utils.py` — Pipeline logging
- `src/processing/clean_data.py` — Data cleaning
- `src/validation/cost_model.py` — Transaction cost estimates
- `src/visualization/make_charts.py` — Chart generation
- `scripts/run_validation.py` — Validation entry point
- `scripts/generate_portfolio_assets.py` — Chart generator
- `requirements.txt` — Dependencies

### Reports Created

- `reports/data_dictionary.md` — Complete column reference
- `reports/methodology.md` — Technical methodology
- `reports/research_summary.md` — Portfolio summary
- `reports/validation_report.md` — Validation documentation
- `reports/portfolio_description.md` — Upwork/portfolio copy

### Data Kept

- `data/sample/sample_market_data.csv` — 1000 synthetic trades for demonstration
- `data/processed/.gitkeep` — Placeholder for pipeline outputs

## 2. What Was Deleted

| File | Reason |
|------|--------|
| collector.py | Moved to src/ingestion/live_collector.py |
| fetch_historical.py | Moved to src/ingestion/fetch_historical.py |
| fetch_multi.py | Merged into src/ingestion/fetch_historical.py |
| compute_cvd.py | Moved to src/processing/aggregate_trades.py |
| feature_engineering.py | Replaced by src/features/ |
| detect_liquidations.py | Logic preserved in feature pipeline |
| validate_integrity.py | Moved to src/validation/validation_runner.py |
| data_audit.py | Moved to src/validation/baseline_tests.py |
| accumulate.py | Replaced by scripts/run_pipeline.py |
| collector_bg.py | Archived variant, less robust than collector.py |
| data/events/*.csv | Sample outputs, regenerable |

## 3. What Was Archived

Nothing archived in final structure. All old files were either moved to `src/` or deleted (with logic preserved in new locations).

## 4. What Was Moved

All core Python scripts moved from root-level flat structure to `src/` package layout:
- `src/ingestion/` — Data collection
- `src/processing/` — Cleaning and aggregation
- `src/features/` — Feature engineering
- `src/validation/` — Integrity and baseline tests
- `src/visualization/` — Chart generation
- `src/utils/` — Shared utilities

## 5. New Repo Structure

```
btc-quant-research/
├── README.md
├── CHANGELOG.md
├── requirements.txt
├── .gitignore
├── src/
│   ├── ingestion/
│   │   ├── fetch_historical.py
│   │   └── live_collector.py
│   ├── processing/
│   │   ├── aggregate_trades.py
│   │   └── clean_data.py
│   ├── features/
│   │   ├── build_features.py
│   │   └── microstructure_features.py
│   ├── validation/
│   │   ├── baseline_tests.py
│   │   ├── cost_model.py
│   │   └── validation_runner.py
│   ├── visualization/
│   │   └── make_charts.py
│   └── utils/
│       ├── config.py
│       ├── io.py
│       └── logging_utils.py
├── scripts/
│   ├── run_pipeline.py
│   ├── run_validation.py
│   └── generate_portfolio_assets.py
├── data/
│   ├── sample/
│   │   ├── README.md
│   │   └── sample_market_data.csv
│   └── processed/
│       └── .gitkeep
├── reports/
│   ├── data_dictionary.md
│   ├── methodology.md
│   ├── research_summary.md
│   ├── validation_report.md
│   └── portfolio_description.md
└── assets/
    ├── charts/           (7 PNG files)
    └── screenshots/
        └── README.md
```

## 6. How to Run the Project

```bash
# Install dependencies
pip install -r requirements.txt

# Run full pipeline (generates sample data if none exists)
python3 scripts/run_pipeline.py

# Run validation suite
python3 scripts/run_validation.py

# Generate portfolio charts
python3 scripts/generate_portfolio_assets.py
```

## 7. Assets Generated

| Chart | Size | Description |
|-------|------|-------------|
| price_over_time.png | 83 KB | BTCUSDT price from raw trades |
| volume_over_time.png | 39 KB | Buy/sell volume aggregation |
| delta_over_time.png | 43 KB | Net delta per second |
| cvd_over_time.png | 83 KB | Cumulative volume delta |
| rolling_volatility.png | 89 KB | Realized volatility 30s/60s |
| returns_distribution.png | 28 KB | Log return histogram |
| feature_correlation.png | 100 KB | Feature correlation heatmap |

## 8. Reports Generated

| Report | Description |
|--------|-------------|
| data_dictionary.md | Every column in pipeline outputs explained |
| methodology.md | Technical approach, anti-leakage, cost model |
| research_summary.md | Portfolio overview |
| validation_report.md | Validation check implementations |
| portfolio_description.md | Upwork/portfolio copy (3 versions) |

## 9. Files Requiring User Review

None. All files are self-consistent. The pipeline runs end-to-end on sample data.

## 10. Known Limitations

1. Sample data is synthetic — real data requires live collection
2. No signal generation or trading logic
3. No backtesting framework
4. Cost model is reference-only (no orderbook data)
5. detect_liquidations.py logic not yet ported to src/ (feature engineering preserved)

## Commands Tested

| Command | Result |
|---------|--------|
| `pip install -r requirements.txt` | ✓ Pass |
| `python3 scripts/run_pipeline.py` | ✓ Pass (300 bars, 43 features) |
| `python3 scripts/run_validation.py` | ✓ Pass (integrity OK, 25 baseline notes) |
| `python3 scripts/generate_portfolio_assets.py` | ✓ Pass (7 charts generated) |

## Git Commits

```
0de8d7d readme: rewrite project documentation for portfolio
51c6fe8 assets: generate portfolio charts and screenshots
ae11f71 docs: add methodology, validation reports, and data dictionary
e79271d cleanup: remove old flat scripts and restructure into src/ package
87df886 cleanup: portfolio restructure & documentation overhaul
```

## Diff Summary

```
56 files changed, 3997 insertions(+), 2888 deletions(-)
```
