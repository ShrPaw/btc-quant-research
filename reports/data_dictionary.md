# Data Dictionary

Complete reference for all columns in the BTC quant research data pipeline.

## Raw Trades (`data/raw/trades_*.csv`)

| Column | Meaning | Source | Notes |
|--------|---------|--------|-------|
| `timestamp_ms` | Unix epoch milliseconds | Binance aggTrade API | Trade execution time |
| `timestamp_utc` | Human-readable UTC timestamp | Derived from timestamp_ms | Format: YYYY-MM-DD HH:MM:SS.mmm |
| `price` | Trade execution price | Binance aggTrade API | USDT price |
| `quantity` | Trade quantity in BTC | Binance aggTrade API | Always positive |
| `is_buyer_maker` | Whether buyer was the maker | Binance aggTrade API | True = sell aggressor (taker is seller) |
| `agggressor_side` | Taker side (BUY/SELL) | Derived from is_buyer_maker | BUY = buy aggressor, SELL = sell aggressor |
| `trade_id` | Binance aggregate trade ID | Binance aggTrade API | Unique per trade, monotonically increasing |

## 1-Second Metrics (`data/processed/metrics_1s_*.csv`)

| Column | Meaning | Source | Notes |
|--------|---------|--------|-------|
| `timestamp_s` | Unix epoch seconds | Aggregated from timestamp_ms | Floor to nearest second |
| `timestamp_utc` | Human-readable UTC | Derived from timestamp_s | Format: YYYY-MM-DD HH:MM:SS |
| `buy_vol` | Aggressive buy volume (BTC) | Sum of quantity for BUY trades in second | Taker buy volume |
| `sell_vol` | Aggressive sell volume (BTC) | Sum of quantity for SELL trades in second | Taker sell volume |
| `net_delta` | buy_vol − sell_vol per second | Computed | Signed volume imbalance |
| `cvd_cumulative` | Running cumulative volume delta | Sum of net_delta | Tracks persistent buying/selling pressure |
| `buy_trades` | Count of buy-aggressor trades | Count | Integer |
| `sell_trades` | Count of sell-aggressor trades | Count | Integer |
| `total_trades` | Total trades in second | buy_trades + sell_trades | Integer |
| `price_open` | First trade price in second | Min timestamp | |
| `price_high` | Max price in second | Max of all prices | |
| `price_low` | Min price in second | Min of all prices | |
| `price_close` | Last trade price in second | Max timestamp | |
| `vwap` | Volume-weighted average price | Σ(price × qty) / Σ(qty) | Per-second VWAP |
| `total_volume` | Total volume traded in second | Sum of all quantities | BTC |

## Features (`data/processed/research_dataset_sample.csv`)

All features use **prior data only** (zero lookahead bias). Outliers winsorized at 1st/99th percentile.

| Column | Meaning | Source | Notes |
|--------|---------|--------|-------|
| `returns` | 1-second log return | log(price_close[t] / price_close[t-1]) | |
| `abs_returns` | Absolute log return | abs(returns) | Magnitude of price movement |
| `cvd` | Cumulative volume delta | Running sum of net_delta | Same as cvd_cumulative |
| `return_5s` | 5-second log return | Sum of returns over prior 5s | |
| `return_15s` | 15-second log return | Sum of returns over prior 15s | |
| `return_30s` | 30-second log return | Sum of returns over prior 30s | |
| `volume_5s` | 5-second volume | Sum of total_volume over prior 5s | BTC |
| `volume_15s` | 15-second volume | Sum of total_volume over prior 15s | BTC |
| `volume_30s` | 30-second volume | Sum of total_volume over prior 30s | BTC |
| `intensity_5s` | 5-second trade count | Sum of total_trades over prior 5s | |
| `intensity_15s` | 15-second trade count | Sum of total_trades over prior 15s | |
| `intensity_30s` | 30-second trade count | Sum of total_trades over prior 30s | |
| `cvd_delta_5s` | 5-second CVD change | CVD[t] − CVD[t-5] | |
| `cvd_delta_15s` | 15-second CVD change | CVD[t] − CVD[t-15] | |
| `cvd_delta_30s` | 30-second CVD change | CVD[t] − CVD[t-30] | |
| `realized_vol_30s` | 30-second realized volatility | Std of 1s returns over prior 30s | |
| `realized_vol_60s` | 60-second realized volatility | Std of 1s returns over prior 60s | |
| `realized_vol_300s` | 300-second realized volatility | Std of 1s returns over prior 300s | |
| `cvd_slope_10s` | 10-second CVD slope | OLS regression slope of CVD over prior 10s | |
| `cvd_slope_30s` | 30-second CVD slope | OLS regression slope of CVD over prior 30s | |
| `cvd_slope_60s` | 60-second CVD slope | OLS regression slope of CVD over prior 60s | |
| `cvd_price_divergence_30s` | CVD-price divergence | CVD direction − price direction | Range: −2 to 2 |
| `trade_intensity_zscore` | Trade count z-score | Expanding mean/std of total_trades | Detects abnormal activity |
| `net_delta_mom_10s` | 10-second delta momentum | Sum of net_delta over prior 10s | |
| `net_delta_mom_30s` | 30-second delta momentum | Sum of net_delta over prior 30s | |
| `vroc_30s` | Volume rate of change | (Current 30s vol − Prior 30s vol) / Prior 30s vol | |
| `efficiency_ratio_30s` | Price efficiency | |Net movement| / Σ|Step movements| | Range: 0 to 1 |
| `vol_imbalance` | Instant volume imbalance | (buy_vol − sell_vol) / (buy_vol + sell_vol) | Range: −1 to 1 |
| `price_vwap_dist` | Price-VWAP distance | (price − vwap) / vwap | |

## Event Labels (Optional — Not Currently Generated)

Event detection (e.g., liquidation proxies via extreme CVD spikes) is not implemented in the current pipeline. If added, events would include:

| Column | Meaning | Notes |
|--------|---------|-------|
| `event_idx` | Event sequence number | Auto-increment |
| `window_end_ts` | Event timestamp | Epoch seconds |
| `delta_cvd_30s` | CVD change in window | Sum of net_delta |
| `return_30s` | Price return in window | (close − open) / open |
| `trade_intensity_30s` | Trade count in window | Sum of total_trades |

## Target Variables (Optional — Not Currently Generated)

No target/label variables are generated by the current pipeline. If signal generation is added, targets would be documented here.

## Winsorization Bounds (`data/processed/winsor_bounds.json`)

JSON dict mapping feature names to `{"lower": float, "upper": float}`.
Used for reproducible clipping on new data during inference.
