# BTC Quant Research — Data Pipeline

Microstructure research engine for BTCUSDT (Binance Futures).

## Status

**Phase 0 — Data Foundation** ✅ (in progress)

## Structure

```
btc-quant-research/
├── collector.py            # Live WebSocket trade stream → CSV
├── fetch_historical.py     # REST API bootstrap (last 1000 trades)
├── compute_cvd.py          # Tick trades → 1s CVD/metrics aggregation
├── data/
│   ├── raw/                # Raw trade data (tick-level)
│   │   └── trades_*.csv
│   └── processed/          # Computed metrics (1s aggregation)
│       └── metrics_1s_*.csv
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

## Exchange & Data Source

- **Exchange**: Binance Futures (USDT-M)
- **Symbol**: BTCUSDT
- **Endpoint**: `wss://fstream.binance.com/ws/btcusdt@aggTrade`
- **Auth**: None required (public data)
- **Liquidation data**: Binance does not expose a public liquidation feed.
  Proxy will be implemented via extreme CVD spikes (P5 threshold).

## Data Rate

Typical flow: ~2-3 trades/second during calm periods.
Volatile periods: 50-200+ trades/second.

## Notes

- No overengineering. Local-first. CSV for portability.
- CVD proxy for liquidations will be implemented once sufficient tick data is collected.
- Protocol: see RESEARCH_PROTOCOL.md (Phase 1+ requires this data layer).
