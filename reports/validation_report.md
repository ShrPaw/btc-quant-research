# Validation Report

## Overview

This report documents the validation checks implemented in the BTC quant research pipeline.

## 1. Data Integrity Checks

### Timestamp Ordering
- **Check:** Timestamps must be non-decreasing
- **Implementation:** `src/validation/validation_runner.py::check_timestamp_ordering()`
- **Status:** Implemented

### Duplicate Detection
- **Check:** No duplicate timestamps within a dataset
- **Implementation:** `src/validation/validation_runner.py::check_duplicates()`
- **Status:** Implemented

### Trade Rate Stability
- **Check:** Detect flatlines (consecutive seconds with ≤1 trade) and spikes
- **Implementation:** Spike classification: market bursts vs buffered spikes (collector artifacts)
- **Status:** Implemented

## 2. Missing Value Checks

- **Check:** All feature columns checked for NaN/null values
- **Implementation:** `src/validation/baseline_tests.py::check_missing_values()`
- **Output:** Count and percentage per feature
- **Status:** Implemented

## 3. Feature Sanity Checks

### Constant Feature Detection
- **Check:** Features with zero variance
- **Implementation:** `src/validation/baseline_tests.py::check_constant_features()`
- **Status:** Implemented

### Distribution Analysis
- **Check:** Mean, std, skewness, kurtosis per feature
- **Flags:** |skew| > 2, kurtosis > 10, std = 0
- **Implementation:** `src/validation/baseline_tests.py::moment4()`
- **Status:** Implemented

### Temporal Stability
- **Check:** Mean drift across data chunks
- **Method:** Split data into 5 chunks, compare means
- **Flag:** Drift ratio > 2σ
- **Implementation:** `src/validation/baseline_tests.py::check_temporal_drift()`
- **Status:** Implemented

### Feature Correlation
- **Check:** Highly correlated feature pairs (|r| > 0.9)
- **Implementation:** `src/validation/baseline_tests.py::check_feature_correlation()`
- **Status:** Implemented

## 4. Lookahead Leakage Precautions

All precautions are structural (verified by code design, not runtime):

| Precaution | Status | Detail |
|------------|--------|--------|
| Rolling windows use prior only | ✓ Implemented | `bars[start:i]` excludes current bar |
| Expanding z-scores | ✓ Implemented | Running mean/std, not rolling |
| Fixed winsorization bounds | ✓ Implemented | Bounds saved for train/test consistency |
| No future returns in features | ✓ Implemented | No feature uses future price data |

## 5. Baseline Tests

Baseline tests are descriptive statistical checks, NOT signal tests:

- **Distribution properties** — Mean, std, skewness, kurtosis
- **Temporal drift** — Feature stability over time
- **Redundancy** — Correlated feature detection
- **Missing values** — Data completeness

**Note:** These tests verify data quality and feature construction. They do NOT test for predictive power or trading profitability.

## 6. Cost Model

Reference cost estimates provided by `src/validation/cost_model.py`:

- **Transaction fee:** 0.04% taker (Binance Futures default)
- **Slippage estimate:** 0.01% fixed (orderbook data needed for accuracy)
- **Cost-aware metrics:** Gross vs net return estimates

**Limitation:** The cost model is a reference only. It does NOT model realistic execution, orderbook impact, position sizing, or market impact.

## 7. Known Limitations

1. **No orderbook data** — Slippage model uses fixed estimates
2. **No execution simulation** — Cost model is reference-only
3. **No signal testing** — Pipeline produces features, not signals
4. **No backtesting** — No strategy is implemented or tested
5. **Sample data is synthetic** — Real data requires live collection or historical fetch

## 8. What Remains To Be Implemented

- Signal generation logic
- Entry/exit rules
- Backtesting framework
- Position sizing
- Execution simulation with orderbook data
- Walk-forward validation
- Out-of-sample testing

## 9. Source Files

| Check | Source File |
|-------|------------|
| Timestamp ordering | `src/validation/validation_runner.py` |
| Duplicate detection | `src/validation/validation_runner.py` |
| Missing values | `src/validation/baseline_tests.py` |
| Constant features | `src/validation/baseline_tests.py` |
| Distribution analysis | `src/validation/baseline_tests.py` |
| Temporal stability | `src/validation/baseline_tests.py` |
| Feature correlation | `src/validation/baseline_tests.py` |
| Lookahead precautions | `src/features/microstructure_features.py` |
| Cost model | `src/validation/cost_model.py` |
