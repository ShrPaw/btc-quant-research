# BTC Quant Research — Python Market Data Pipeline & Research Validation System

A data engineering project that transforms raw BTCUSDT crypto market data into structured, validated datasets with microstructure features for quantitative research.

**This is a research data pipeline — not a trading strategy.** It demonstrates data collection, cleaning, aggregation, feature engineering, integrity validation, and reproducible analysis workflows.

## What This Project Does

```
Raw Binance WebSocket trades
    → 1-second aggregation (OHLC, volume, delta, CVD, VWAP, trade counts)
    → 18 microstructure features (returns, volatility, CVD slopes, divergence, z-scores)
    → 6-check integrity validation
    → Distribution & regime audit
    → Validated, reproducible dataset
```

## Key Features

- **Real-time data collection** from Binance Futures WebSocket with auto-reconnect and buffered writes
- **Historical bootstrap** via REST API (no authentication required for public data)
- **1-second bar aggregation** with CVD (Cumulative Volume Delta), VWAP, OHLC
- **18 engineered features** — all rolling windows use prior data only (zero lookahead bias)
- **Winsorization** at 1st/99th percentile with bounds saved for train/test consistency
- **Strict 6-check integrity validation**: timestamp continuity, trade ID continuity, duplicate detection, trade rate spike classification, intra-second structure, feature stability (A/B/C split)
- **Distribution audit**: mean/std/skew/kurtosis, temporal drift detection, regime analysis, feature correlation
- **Liquidation proxy detection** via extreme CVD spikes (rolling percentile thresholds)
- **Full pipeline orchestration**: fetch → merge → feature engineering → validation → audit in one command

## Project Structure

```
btc-quant-research/
├── collector.py              # Live WebSocket trade stream → CSV
├── fetch_historical.py       # REST API bootstrap (last 1000 trades)
├── fetch_multi.py            # Extended historical collection (batched)
├── compute_cvd.py            # Raw trades → 1s CVD/metrics aggregation
├── feature_engineering.py    # 1s bars → 18-feature matrix + percentiles
├── detect_liquidations.py    # Liquidation cascade proxy detector
├── validate_integrity.py     # 6-check strict data integrity validator
├── data_audit.py             # Distribution, stability, regime, correlation audit
├── accumulate.py             # Full pipeline orchestrator
├── data/
│   ├── raw/                  # Raw trade data (gitignored — too large)
│   ├── processed/            # Computed metrics & features (gitignored — regenerable)
│   └── events/               # Detected events (sample outputs kept)
├── docs/
│   ├── audit/                # Repository audit documentation
│   └── archive/              # Archived variants
└── README.md
```

## Quick Start

```bash
# Install dependency
pip install websocket-client

# Option A: Full pipeline (fetch + merge + process + validate)
python3 accumulate.py

# Option B: Step by step
python3 fetch_historical.py                              # Bootstrap data
python3 compute_cvd.py data/raw/trades_<timestamp>.csv   # Aggregate to 1s
python3 feature_engineering.py data/raw/trades_*.csv     # Build features
python3 validate_integrity.py data/raw/trades_*.csv --merged --features data/processed/features_*.csv

# Option C: Live collection
python3 collector.py   # Ctrl+C to stop, auto-flushes buffer
```

## Data Schemas

### Raw Trades (`data/raw/trades_*.csv`)

| Column | Type | Description |
|--------|------|-------------|
| `timestamp_ms` | int | Unix epoch milliseconds |
| `timestamp_utc` | string | Human-readable UTC |
| `price` | float | Trade price |
| `quantity` | float | Trade quantity (BTC) |
| `is_buyer_maker` | bool | True = sell aggressor |
| `agggressor_side` | string | BUY or SELL (taker side) |
| `trade_id` | int | Binance aggregate trade ID |

### 1s Metrics (`data/processed/metrics_1s_*.csv`)

| Column | Type | Description |
|--------|------|-------------|
| `timestamp_s` | int | Unix epoch seconds |
| `buy_vol` / `sell_vol` | float | Aggressive buy/sell volume (BTC) |
| `net_delta` | float | buy_vol − sell_vol per second |
| `cvd_cumulative` | float | Running cumulative volume delta |
| `price_open/high/low/close` | float | OHLC per second |
| `vwap` | float | Volume-weighted average price |
| `total_trades` | int | Trade count in second |

### Features (`data/processed/features_*.csv`)

18 features computed from 1s metrics. All rolling windows use **prior data only** (no lookahead). Outliers winsorized at 1st/99th percentile.

| Feature | Window | Description |
|---------|--------|-------------|
| `ret_{1,5,30,60}s` | 1s–60s | Log returns at multiple scales |
| `realized_vol_{30,60,300}s` | 30s–300s | Realized volatility (std of 1s returns) |
| `cvd_slope_{10,30,60}s` | 10s–60s | CVD linear regression slope (OLS) |
| `cvd_price_divergence_30s` | 30s | CVD direction − price direction |
| `trade_intensity_zscore` | expanding | Z-score of trade count (expanding stats) |
| `net_delta_mom_{10,30}s` | 10s–30s | Sum of net_delta over window |
| `vroc_30s` | 30s | Volume rate of change |
| `efficiency_ratio_30s` | 30s | |net movement| / sum of |step movements| |
| `vol_imbalance` | instant | (buy_vol − sell_vol) / (buy_vol + sell_vol) |
| `price_vwap_dist` | instant | (price − vwap) / vwap |

## Integrity Validation

The `validate_integrity.py` script runs 6 strict checks:

1. **Timestamp Continuity** — 4-tier gap classification (boundary, natural quiet, suspicious, unexpected)
2. **Trade ID Continuity** — 3-tier classification with contextual validation
3. **Duplicate Detection** — within-source (must be 0) vs cross-source (tolerated, measured)
4. **Trade Rate + Spike Classification** — market bursts vs buffered spikes (collector artifacts)
5. **Intra-second Structure** — timestamp clustering analysis
6. **Feature Stability** — A/B/C 3-way split convergence/divergence tracking

Exit code 0 = VALID, 1 = INVALID. No advisory-only soft failures.

## Anti-Overfitting Design

- No future data in any rolling calculation
- Winsorization bounds saved separately for reproducible clipping on new data
- Expanding (not rolling) z-score statistics
- All features are simple, interpretable, and economically motivated
- Fixed windows only — no adaptive or data-driven window selection

## Data Source

- **Exchange**: Binance Futures (USDT-M)
- **Symbol**: BTCUSDT
- **Endpoint**: `wss://fstream.binance.com/ws/btcusdt@aggTrade`
- **Auth**: None required (public data)
- **Liquidation data**: Binance does not expose a public liquidation feed — proxy implemented via extreme CVD spikes

## Dependencies

- Python 3.8+
- `websocket-client` (only for live collection)
- All other modules use stdlib only (`csv`, `json`, `math`, `collections`, `urllib`)

## License

Research project by Nicolas Bustamante / ShrPaw.
