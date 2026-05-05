# Python Market Data Pipeline & Research Validation System

## Summary

This project transforms raw crypto market data into structured datasets for analysis, validation, and research workflows.

It demonstrates end-to-end data engineering: from data collection through cleaning, aggregation, feature engineering, integrity validation, and reproducible output generation.

## What This Project Does

```
Raw Binance aggTrades (tick-level)
    ↓
Clean (remove invalid rows, missing fields, zero values)
    ↓
Aggregate to 1-second bars (OHLC, volume, delta, CVD, VWAP)
    ↓
Build 44 features (returns, volatility, CVD slopes, divergence, z-scores)
    ↓
Winsorize outliers (1st/99th percentile, bounds saved)
    ↓
Validate integrity (timestamps, duplicates, missing values, stability)
    ↓
Output: structured, validated dataset + portfolio charts
```

## What Problem This Solves

Raw exchange data is noisy, unstructured, and arrives at tick-level granularity — thousands of individual trades per minute. This pipeline transforms that raw data into clean, feature-rich, validation-ready datasets suitable for quantitative research.

## Core Capabilities

- **Raw trade data processing** — Collection from Binance Futures WebSocket and REST APIs
- **Data cleaning** — Remove invalid rows (missing fields, zero quantity/price, invalid side)
- **1-second aggregation** — OHLC prices, buy/sell volume, net delta, CVD, VWAP, trade counts
- **Feature engineering** — 44 features including returns, volatility, CVD slopes, volume metrics, z-scores
- **Anti-leakage design** — All rolling features use prior data only; winsorization bounds saved for consistency
- **Data validation** — Integrity checks (timestamps, duplicates, missing values), baseline statistical tests
- **Portfolio charts** — 6 clean PNG charts showing pipeline outputs
- **Reproducible pipeline** — Runs on included sample data, no API keys required

## Features Generated

| Category | Features |
|----------|----------|
| Returns | 1s log returns, absolute returns |
| Volatility | 30s, 60s, 300s realized volatility |
| CVD | Slopes (10s, 30s, 60s), deltas (5s, 15s, 30s), divergence |
| Volume | Rolling (5s, 15s, 30s), rate of change, imbalance |
| Intensity | Rolling counts (5s, 15s, 30s), expanding z-score |
| Price | Efficiency ratio, VWAP distance |

## Outputs

| Output | Description |
|--------|-------------|
| `data/processed/research_dataset_sample.csv` | 300 rows × 44 columns of engineered features |
| `data/processed/metrics_1s.csv` | 1-second aggregated bars |
| `assets/charts/*.png` | 6 portfolio-ready charts |
| `reports/validation_report.md` | Auto-generated validation results |

## Validation Approach

- Timestamp ordering and monotonicity
- Duplicate detection
- Missing value and constant feature detection
- Temporal stability (drift across chunks)
- Feature correlation (redundancy detection)
- Returns sanity checks
- Structural lookahead leakage verification

## Skills Demonstrated

- **Python** — Core language, stdlib-first design
- **Data Engineering** — Pipeline design, CSV I/O, data cleaning
- **Market Microstructure** — CVD, delta, VWAP, trade classification
- **Feature Engineering** — Rolling/expanding stats, OLS regression, z-scores
- **Data Validation** — Integrity checks, baseline tests, leakage prevention
- **Automation** — End-to-end pipeline orchestration
- **Documentation** — Data dictionary, methodology, research reports

## Important Disclaimer

**This repository does not claim to produce a profitable trading strategy.** It demonstrates market data processing, feature engineering, validation workflows, and research discipline.
