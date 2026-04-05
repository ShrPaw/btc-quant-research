# BTC Quant Research — Data Pipeline

Microstructure research engine for BTCUSDT (Binance Futures).

## Status

**Phase 0 — Data Foundation** ✅  
**Phase 2 — Feature Engineering** ✅

## Structure

```
btc-quant-research/
├── collector.py            # Live WebSocket trade stream → CSV
├── collector_bg.py         # Background variant (no buffer)
├── fetch_historical.py     # REST API bootstrap (last 1000 trades)
├── fetch_multi.py          # Extended historical collection
├── compute_cvd.py          # Tick trades → 1s CVD/metrics aggregation
├── detect_liquidations.py  # Liquidation cascade proxy detector
├── build_features.py       # 1s metrics → feature matrix (Phase 2)
├── data/
│   ├── raw/                # Raw trade data (tick-level)
│   │   └── trades_*.csv
│   ├── processed/          # Computed metrics (1s aggregation)
│   │   └── metrics_1s_*.csv
│   ├── events/             # Detected events (liquidation proxies)
│   │   └── liquidation_proxy_*.csv
│   └── features/           # Feature matrices (Phase 2)
│       ├── features_*.csv
│       └── winsor_bounds_*.csv
└── README.md
```

## Quick Start

```bash
# Install dependency
pip install websocket-client

# Option A: Bootstrap with REST (instant, last 1000 trades)
python3 fetch_historical.py

# Option B: Live stream (run for as long as you want data)
python3 collector.py
# Ctrl+C to stop, auto-flushes buffer

# Compute CVD & metrics
python3 compute_cvd.py data/raw/trades_<timestamp>.csv

# Build feature matrix
python3 build_features.py data/processed/metrics_1s_<timestamp>.csv

# Detect liquidation proxies (optional)
python3 detect_liquidations.py data/processed/metrics_1s_<timestamp>.csv
```

## Data Schemas

### Raw Trades (`data/raw/trades_*.csv`)

| Column | Type | Description |
|--------|------|-------------|
| `timestamp_ms` | int | Unix epoch milliseconds |
| `timestamp_utc` | string | Human-readable UTC |
| `price` | float | Trade price |
| `quantity` | float | Trade quantity (BTC) |
| `is_buyer_maker` | bool | True = sell aggressor (taker is seller) |
| `agggressor_side` | string | BUY or SELL (taker side) |
| `trade_id` | int | Binance aggregate trade ID |

### 1s Metrics (`data/processed/metrics_1s_*.csv`)

| Column | Type | Description |
|--------|------|-------------|
| `timestamp_s` | int | Unix epoch seconds |
| `timestamp_utc` | string | Human-readable UTC |
| `buy_vol` | float | Aggressive buy volume (BTC) |
| `sell_vol` | float | Aggressive sell volume (BTC) |
| `net_delta` | float | buy_vol - sell_vol per second |
| `cvd_cumulative` | float | Running cumulative volume delta |
| `buy_trades` | int | Count of buy-aggressor trades |
| `sell_trades` | int | Count of sell-aggressor trades |
| `total_trades` | int | Total trades in second |
| `price_open` | float | First trade price |
| `price_high` | float | Max price in second |
| `price_low` | float | Min price in second |
| `price_close` | float | Last trade price |
| `vwap` | float | Volume-weighted average price |
| `total_volume` | float | Total volume traded |

### Features (`data/features/features_*.csv`)

18 features computed from 1s metrics. All rolling windows use **prior data only** (no lookahead). Outliers winsorized at 1st/99th percentile.

| Feature | Window | Description |
|---------|--------|-------------|
| `ret_{1,5,30,60}s` | 1s–60s | Log returns at multiple scales |
| `realized_vol_{30,60,300}s` | 30s–300s | Realized volatility (std of 1s returns) |
| `cvd_slope_{10,30,60}s` | 10s–60s | CVD linear regression slope (OLS) |
| `cvd_price_divergence_30s` | 30s | CVD direction − price direction (range: −2 to 2) |
| `trade_intensity_zscore` | expanding | Z-score of trade count (expanding mean/std) |
| `net_delta_mom_{10,30}s` | 10s–30s | Sum of net_delta over window |
| `vroc_30s` | 30s | Volume rate of change (vs prior 30s) |
| `efficiency_ratio_30s` | 30s | |net movement| / sum of |step movements| |
| `vol_imbalance` | instant | (buy_vol − sell_vol) / (buy_vol + sell_vol) |
| `price_vwap_dist` | instant | (price − vwap) / vwap |

**Winsor bounds** (`winsor_bounds_*.csv`): Saved separately for reproducible clipping on new data during inference.

## Anti-Overfitting Protocol

- No future data in any rolling calculation
- Winsorization at 1st/99th percentile (bounds saved for train/test consistency)
- Expanding (not rolling) z-score statistics
- All features are simple, interpretable, and economically motivated

## Exchange & Data Source

- **Exchange**: Binance Futures (USDT-M)
- **Symbol**: BTCUSDT
- **Endpoint**: `wss://fstream.binance.com/ws/btcusdt@aggTrade`
- **Auth**: None required (public data)
- **Liquidation data**: Binance does not expose a public liquidation feed.
  Proxy implemented via extreme CVD spikes (P5 threshold).

## Data Rate

Typical flow: ~2-3 trades/second during calm periods.  
Volatile periods: 50-200+ trades/second.

## Notes

- No overengineering. Local-first. CSV for portability.
- Minimum 310s of data recommended for full feature coverage.
- Pipeline: `collector.py` → `compute_cvd.py` → `build_features.py` → [Phase 3: Signal Discovery]
