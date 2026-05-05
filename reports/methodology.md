# Methodology

## Overview

This project implements a Python data pipeline for processing raw cryptocurrency market data into structured, validated datasets for quantitative research.

## Data Source

**Exchange:** Binance Futures (USDT-M)  
**Symbol:** BTCUSDT  
**Data Type:** Aggregate trades (aggTrades)  
**Endpoint:** `wss://fstream.binance.com/ws/btcusdt@aggTrade` (WebSocket) / `https://fapi.binance.com/fapi/v1/trades` (REST)  
**Authentication:** None required (public market data)  
**Rate:** ~2-3 trades/second (calm), 50-200+ trades/second (volatile)

## Data Collection

### Live Collection (`src/ingestion/live_collector.py`)
- Connects to Binance Futures WebSocket
- Receives real-time aggTrade events
- Buffers 50 trades or 5 seconds before flushing to CSV
- Auto-reconnects on connection failure
- Heartbeat monitoring every 5 seconds

### Historical Bootstrap (`src/ingestion/fetch_historical.py`)
- REST API call to `/fapi/v1/trades` (last 1000 trades)
- No authentication required
- Used for quick dataset initialization

## Data Cleaning (`src/processing/clean_data.py`)

Before aggregation, raw trades are cleaned:

1. **Missing fields** — Rows with missing required columns are removed
2. **Zero quantity** — Trades with qty ≤ 0 are removed
3. **Zero price** — Trades with price ≤ 0 are removed
4. **Invalid side** — Trades without BUY/SELL aggressor side are removed

All removals are counted and reported.

## Aggregation (`src/processing/aggregate_trades.py`)

Raw tick trades are aggregated into 1-second bars:

- **OHLC:** Open, High, Low, Close prices per second
- **Volume:** Total, buy, and sell volume (BTC)
- **Delta:** Net signed volume (buy_vol − sell_vol)
- **CVD:** Cumulative Volume Delta (running sum of delta)
- **VWAP:** Volume-weighted average price per second
- **Trade counts:** Buy, sell, and total trades per second

## Feature Engineering (`src/features/microstructure_features.py`)

44 columns are computed from 1-second bars (42 engineered features). All features follow strict anti-leakage rules.

### Feature Categories

**Returns:**
- Log returns at 1s, 5s, 15s, 30s scales
- Absolute returns (magnitude of price movement)
- Computed as: log(P_t / P_{t-W})

**Volatility:**
- Realized volatility at 30s, 60s, 300s windows
- Computed as: std(1s returns) over rolling window

**CVD Metrics:**
- CVD slope (OLS regression) at 10s, 30s, 60s
- CVD delta (change) at 5s, 15s, 30s
- CVD-price divergence at 30s

**Volume:**
- Rolling volume at 5s, 15s, 30s
- Volume rate of change (30s)
- Instant volume imbalance

**Trade Intensity:**
- Rolling trade count at 5s, 15s, 30s
- Expanding z-score of trade count

**Price:**
- Efficiency ratio (30s)
- Price-VWAP distance

### Anti-Leakage Design

**Rolling windows:** All rolling features use `bars[start:i]` — the slice excludes the current bar `i`, using only prior data.

**Expanding statistics:** Trade intensity z-score uses expanding (not rolling) mean and std, so each bar's z-score is computed from all prior data.

**Winsorization:** Outliers are clipped at the 1st and 99th percentiles. Bounds are saved separately (`winsor_bounds_*.json`) for reproducible clipping on new data.

**No future returns:** No feature calculation uses future price or future returns.

## Validation (`src/validation/`)

### Integrity Checks (`validation_runner.py`)

1. **Timestamp ordering** — Timestamps must be non-decreasing
2. **Duplicate detection** — No duplicate timestamps within a dataset
3. **Missing values** — All features checked for NaN/null
4. **Constant features** — Features with zero variance detected

### Baseline Tests (`baseline_tests.py`)

1. **Distribution analysis** — Mean, std, skewness, kurtosis per feature
2. **Temporal stability** — Mean drift across data chunks
3. **Feature correlation** — Highly correlated pairs (|r| > 0.9) flagged
4. **Missing value audit** — Count and percentage per feature

### Lookahead Precautions (Structural)

Verified by code structure (not runtime):
- Rolling windows reference `bars[start:i]` (prior only)
- Expanding z-scores use running statistics
- Winsorization uses fixed bounds (not per-window)
- No future returns in any feature calculation

## Cost Model (`src/validation/cost_model.py`)

Provides reference cost estimates for research context:

- **Transaction fee:** 0.04% taker fee (Binance Futures default)
- **Slippage:** Fixed 0.01% estimate (orderbook data needed for accurate modeling)
- **Cost-aware metrics:** Gross vs net return estimates

**Note:** The cost model is a reference only. It does NOT model realistic execution, orderbook impact, or position sizing.

## What This Project Does NOT Claim

**This repository does not claim to produce a profitable trading strategy.** It demonstrates market data processing, feature engineering, validation workflows, and research discipline.

Specifically, this project:
- Does NOT claim any trading profitability
- Does NOT implement signal generation or entry/exit logic
- Does NOT model realistic execution costs
- Does NOT provide investment advice
- Does NOT backtest any strategy

The project focuses exclusively on data engineering, feature construction, and validation methodology.

## Reproducibility

All pipeline steps are reproducible:

1. Raw data is the source of truth (never overwritten)
2. All features use prior data only (no lookahead)
3. Winsorization bounds are saved for consistency
4. Pipeline is orchestrated by `scripts/run_pipeline.py`
5. Validation is run by `scripts/run_validation.py`
6. Sample data is provided for demonstration
