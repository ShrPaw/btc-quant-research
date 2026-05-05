# BTC Quant Research

**Python Market Data Pipeline & Research Validation System**

This repository demonstrates how raw crypto market data can be collected, cleaned, aggregated, transformed into features, and prepared for validation workflows.

## What This Project Does

```
Raw Binance WebSocket trades
    → Clean (remove invalid rows)
    → Aggregate to 1-second bars (OHLC, volume, delta, CVD, VWAP)
    → Build 18 features (returns, volatility, CVD slopes, divergence, z-scores)
    → Winsorize outliers (1st/99th percentile, bounds saved)
    → Validate integrity (timestamps, duplicates, rates, stability)
    → Output: structured, validated dataset
```

## What Problem This Solves

Raw exchange data is noisy, unstructured, and arrives at tick-level granularity. This pipeline transforms that raw data into clean, feature-rich, validation-ready datasets suitable for quantitative research.

## Project Architecture

```
src/
├── ingestion/          # Data collection
│   ├── fetch_historical.py    # REST API bootstrap
│   └── live_collector.py      # WebSocket real-time stream
├── processing/         # Cleaning & aggregation
│   ├── clean_data.py          # Data quality checks
│   └── aggregate_trades.py    # Tick → 1s bar aggregation
├── features/           # Feature engineering
│   ├── build_features.py      # Entry point
│   └── microstructure_features.py  # 18 feature calculations
├── validation/         # Integrity & baseline tests
│   ├── baseline_tests.py      # Statistical checks
│   ├── cost_model.py          # Transaction cost estimates
│   └── validation_runner.py   # Orchestrator
├── visualization/      # Chart generation
│   └── make_charts.py         # Portfolio-ready PNG charts
└── utils/              # Shared utilities
    ├── config.py              # Constants & paths
    ├── io.py                  # CSV I/O helpers
    └── logging_utils.py       # Pipeline logging

scripts/
├── run_pipeline.py            # Full pipeline entry point
├── run_validation.py          # Validation suite
└── generate_portfolio_assets.py  # Chart generator

data/
├── sample/             # Sample data for demonstration
└── processed/          # Pipeline outputs (gitignored)

reports/
├── data_dictionary.md         # Column reference
├── methodology.md             # Technical methodology
├── research_summary.md        # Portfolio summary
├── validation_report.md       # Validation documentation
└── portfolio_description.md   # Upwork/portfolio copy

assets/
├── charts/             # Generated PNG charts
└── screenshots/        # Portfolio screenshots
```

## Data Pipeline

### Stage 1: Ingestion
Collect raw aggregate trades from Binance Futures (BTCUSDT). Two modes:
- **REST bootstrap** — Fetch last 1000 trades instantly (no auth)
- **WebSocket live** — Real-time buffered collection with auto-reconnect

### Stage 2: Cleaning
Remove invalid trades: missing fields, zero quantity/price, invalid side. All removals counted and reported.

### Stage 3: Aggregation
Aggregate tick trades into 1-second bars with: OHLC prices, buy/sell volume, net delta, cumulative volume delta (CVD), VWAP, trade counts.

### Stage 4: Feature Engineering
Compute 18 microstructure features:
- **Returns** — Log returns at 1s, 5s, 15s, 30s
- **Volatility** — Realized vol at 30s, 60s, 300s
- **CVD** — Slopes (OLS), deltas, price divergence
- **Volume** — Rolling, rate of change, imbalance
- **Intensity** — Rolling counts, expanding z-score
- **Price** — Efficiency ratio, VWAP distance

### Stage 5: Validation
6-check integrity suite: timestamp ordering, duplicates, missing values, constant features, temporal drift, feature correlation.

## Features Generated

| Category | Features | Anti-Leakage |
|----------|----------|--------------|
| Returns | 1s, 5s, 15s, 30s log returns | Prior window only |
| Volatility | 30s, 60s, 300s realized vol | Prior window only |
| CVD | Slopes, deltas, divergence | Prior window only |
| Volume | Rolling, RoC, imbalance | Prior window only |
| Intensity | Counts, z-score | Expanding (not rolling) |
| Price | Efficiency, VWAP dist | Instant (no lookahead) |

All rolling windows use `bars[start:i]` — excludes current bar. Winsorization bounds saved for train/test consistency.

## Validation Approach

- **Timestamp ordering** — Non-decreasing timestamps
- **Duplicate detection** — No duplicate timestamps
- **Missing values** — NaN/null audit per feature
- **Constant features** — Zero-variance detection
- **Temporal stability** — Drift across data chunks
- **Feature correlation** — Redundancy detection (|r| > 0.9)
- **Lookahead precautions** — Structural verification (rolling windows, expanding stats, fixed bounds)

## How to Run

```bash
# Install dependencies
pip install -r requirements.txt

# Run full pipeline (generates sample data if none exists)
python scripts/run_pipeline.py

# Run on specific file
python scripts/run_pipeline.py data/raw/trades_*.csv

# Run validation suite
python scripts/run_validation.py

# Generate portfolio charts
python scripts/generate_portfolio_assets.py
```

## Portfolio Screenshots

Generate charts with `python scripts/generate_portfolio_assets.py`:

- `price_over_time.png` — BTCUSDT price from raw trade data
- `volume_over_time.png` — Buy/sell volume aggregation
- `delta_over_time.png` — Net delta (buy − sell per second)
- `cvd_over_time.png` — Cumulative volume delta
- `rolling_volatility.png` — Realized volatility at multiple scales
- `returns_distribution.png` — Log return distribution
- `feature_correlation.png` — Feature correlation heatmap

## Reports

| Report | Description |
|--------|-------------|
| [Data Dictionary](reports/data_dictionary.md) | Every column explained |
| [Methodology](reports/methodology.md) | Technical approach |
| [Research Summary](reports/research_summary.md) | Portfolio overview |
| [Validation Report](reports/validation_report.md) | Check implementations |
| [Portfolio Description](reports/portfolio_description.md) | Upwork/portfolio copy |

## Limitations

- No signal generation or trading logic implemented
- No backtesting framework
- Cost model is reference-only (no orderbook data)
- Sample data is synthetic (real data requires live collection)
- No real-time execution simulation

## Skills Demonstrated

- **Python** — Core language, stdlib-first design
- **Data Engineering** — Pipeline design, CSV I/O, data cleaning
- **Market Microstructure** — CVD, delta, VWAP, trade classification
- **Feature Engineering** — Rolling/expanding stats, OLS regression, z-scores
- **Data Validation** — Integrity checks, baseline tests, leakage prevention
- **Automation** — End-to-end pipeline orchestration
- **Documentation** — Data dictionary, methodology, research reports

## Disclaimer

**This repository does not claim to produce a profitable trading strategy.** It demonstrates market data processing, feature engineering, validation workflows, and research discipline.

## License

Research project by Nicolas Bustamante / ShrPaw.
