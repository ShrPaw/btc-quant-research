# Validation Report

**Generated:** 2026-05-05 21:03:13 UTC
**Dataset:** `research_dataset_sample.csv`
**Rows:** 300
**Columns:** 44
**Feature columns:** 42

## Columns Generated

- `timestamp_s`
- `timestamp_utc`
- `buy_vol`
- `sell_vol`
- `net_delta`
- `cvd_cumulative`
- `buy_trades`
- `sell_trades`
- `total_trades`
- `price_open`
- `price_high`
- `price_low`
- `price_close`
- `vwap`
- `total_volume`
- `returns`
- `abs_returns`
- `cvd`
- `return_5s`
- `volume_5s`
- `intensity_5s`
- `cvd_delta_5s`
- `return_15s`
- `volume_15s`
- `intensity_15s`
- `cvd_delta_15s`
- `return_30s`
- `volume_30s`
- `intensity_30s`
- `cvd_delta_30s`
- `realized_vol_30s`
- `realized_vol_60s`
- `realized_vol_300s`
- `cvd_slope_10s`
- `cvd_slope_30s`
- `cvd_slope_60s`
- `cvd_price_divergence_30s`
- `trade_intensity_zscore`
- `net_delta_mom_10s`
- `net_delta_mom_30s`
- `vroc_30s`
- `efficiency_ratio_30s`
- `vol_imbalance`
- `price_vwap_dist`

## Missing Values

No missing values found in any column.

## Duplicate Timestamps

No duplicate timestamps found.

## Timestamp Ordering

Timestamps are correctly sorted in non-decreasing order.

## Returns Sanity

- Non-zero returns: 299
- Mean return: 0.0000012542
- Min return: -0.0000258121
- Max return: 0.0000323755
- Sanity: PASS

## Validation Results

- Timestamp ordering: **PASS**
- Duplicate detection: **PASS**
- Features analyzed: 42
- Missing value issues: 0
- Constant features: 1
- Unstable features: 10
- Redundant pairs (|r| > 0.9): 14
- Overall: **PASSED**

## Anti-Leakage Precautions

All precautions verified by code structure (not runtime):

- ✓ rolling_windows_use_prior_only: All rolling features use bars[start:i] — excludes current bar
- ✓ expanding_zscore: Trade intensity z-score uses expanding mean/std, not rolling
- ✓ winsor_fixed_bounds: Winsorization bounds saved for train/test consistency
- ✓ no_future_returns_in_features: No future returns used in any feature calculation

## Known Limitations

- Sample data is synthetic (real data requires live collection)
- No signal generation or trading logic implemented
- No backtesting framework
- Cost model is reference-only (no orderbook data)
- No real-time execution simulation

## Disclaimer

**This repository does not claim to produce a profitable trading strategy.**
It demonstrates market data processing, feature engineering, validation workflows, and research discipline.
