# File Classification — btc-quant-research

**Date:** 2026-05-05
**Branch:** cleanup-research-structure

## Classification Summary

| Category | Count |
|----------|-------|
| KEEP_CORE | 9 |
| KEEP_DOCS | 1 |
| KEEP_REPORT | 3 |
| KEEP | 1 |
| MOVE_ARCHIVE | 1 |
| DELETE | 0 |

## KEEP_CORE — Core Pipeline Source Code

| Path | Role | Reason |
|------|------|--------|
| collector.py | Data Collection | Foreground WebSocket trade collector with buffered writes, auto-reconnect, heartbeat monitoring |
| fetch_historical.py | Data Collection | REST API bootstrap — fetches last 1000 trades for quick-start dataset |
| fetch_multi.py | Data Collection | Extended historical collection via multiple REST batches with deduplication |
| compute_cvd.py | Aggregation | Raw tick trades → 1-second CVD/metrics aggregation (buy_vol, sell_vol, net_delta, vwap, OHLC) |
| feature_engineering.py | Feature Engineering | Raw trades → 18-feature matrix (returns, volatility, CVD slopes, divergence, z-scores) with winsor bounds |
| detect_liquidations.py | Event Detection | Liquidation cascade proxy detector using rolling CVD/return/intensity percentiles |
| validate_integrity.py | Validation | 6-check strict data integrity validator (timestamps, trade IDs, duplicates, rate spikes, intra-second, feature stability) |
| data_audit.py | Audit | Distribution analysis, temporal stability, regime detection, feature correlation |
| accumulate.py | Orchestration | Full pipeline orchestrator: fetch → merge → feature engineering → validation → audit |

## KEEP_DOCS — Documentation

| Path | Reason |
|------|--------|
| README.md | Project documentation (needs update: references build_features.py which doesn't exist; should be feature_engineering.py) |

## KEEP_REPORT — Sample Validation Outputs

| Path | Reason |
|------|--------|
| data/events/liquidation_proxy_v1.csv | Sample output: single-source liquidation proxy detection (2 events) |
| data/events/liquidation_proxy_merged.csv | Sample output: merged liquidation proxy events (20 events) |
| data/events/liquidation_proxy_dedup_merged.csv | Sample output: deduplicated cascade events (2 events) |

## KEEP — Configuration

| Path | Reason |
|------|--------|
| .gitignore | Correctly excludes data/raw/, data/processed/, __pycache__/, *.pyc |

## MOVE_ARCHIVE — Relocate to docs/archive/

| Path | Reason |
|------|--------|
| collector_bg.py | Near-duplicate of collector.py. Background variant (no buffer, writes every trade immediately). Less robust than collector.py. Useful as reference but not part of the clean pipeline. |

## DELETE

None. All files serve a purpose or are archived.

## GITIGNORE — Already Handled

The existing `.gitignore` correctly excludes:
- `data/raw/` — raw trade data (too large for repo)
- `data/processed/` — computed metrics/features (regenerable)
- `__pycache__/` — Python cache
- `*.pyc` — compiled Python

## Notes

1. **collector_bg.py** is a simplified variant of **collector.py**. The main collector has buffering, heartbeat, and better reconnection. The background variant writes every trade immediately (higher I/O, no buffer). Moving to archive preserves it as reference without cluttering the core pipeline.

2. **fetch_multi.py** overlaps with `accumulate.py --fetch` mode. However, it's a useful standalone script for quick extended collection, so it stays in KEEP_CORE.

3. **README.md** needs correction: it references `build_features.py` but the actual file is `feature_engineering.py`.

4. All 3 CSV files in `data/events/` are small sample outputs that demonstrate the pipeline working — valuable for portfolio demonstration.
