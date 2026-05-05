# File Inventory — btc-quant-research

**Date:** 2026-05-05
**Branch:** cleanup-research-structure
**Total files:** 15 (excluding .git)

## Full File Tree

```
.
├── .gitignore                              (45 B)
├── README.md                               (5.2 KB)
├── accumulate.py                           (13 KB)
├── collector_bg.py                         (2.4 KB)
├── collector.py                            (3.9 KB)
├── compute_cvd.py                          (3.1 KB)
├── data_audit.py                           (16 KB)
├── detect_liquidations.py                  (6.5 KB)
├── feature_engineering.py                  (11 KB)
├── fetch_historical.py                     (2.5 KB)
├── fetch_multi.py                          (2.6 KB)
├── validate_integrity.py                   (32 KB)
├── data/
│   └── events/
│       ├── liquidation_proxy_dedup_merged.csv  (439 B)
│       ├── liquidation_proxy_merged.csv        (2.6 KB)
│       └── liquidation_proxy_v1.csv            (437 B)
└── docs/
    └── audit/
```

## File Count by Extension

| Extension | Count |
|-----------|-------|
| .py       | 10    |
| .csv      | 3     |
| .md       | 1     |
| (none)    | 1     |

## Largest Files

| File | Size |
|------|------|
| validate_integrity.py | 32 KB |
| data_audit.py | 16 KB |
| accumulate.py | 13 KB |
| feature_engineering.py | 11 KB |
| detect_liquidations.py | 6.5 KB |

## Detected Python Scripts (10)

| Script | Purpose |
|--------|---------|
| collector.py | Live WebSocket trade stream (foreground, buffered) |
| collector_bg.py | Live WebSocket trade stream (background, no buffer) |
| fetch_historical.py | REST API bootstrap (last 1000 trades) |
| fetch_multi.py | Extended historical collection (multiple batches) |
| compute_cvd.py | Raw trades → 1s CVD/metrics aggregation |
| detect_liquidations.py | Liquidation cascade proxy detector |
| feature_engineering.py | Raw trades → feature matrix (18 features) |
| data_audit.py | Distribution, stability, regime, correlation analysis |
| validate_integrity.py | Strict 6-check data integrity validation |
| accumulate.py | Full pipeline orchestrator (fetch→merge→process→validate) |

## Detected Datasets (3)

| File | Rows | Description |
|------|------|-------------|
| data/events/liquidation_proxy_v1.csv | 2 | Single-source liquidation proxy events |
| data/events/liquidation_proxy_merged.csv | 20 | Merged liquidation proxy events |
| data/events/liquidation_proxy_dedup_merged.csv | 2 | Deduplicated cascade events |

## Detected Notebooks

None.

## Detected Reports

None (reports are printed to stdout, not saved as files).

## Detected Logs

None tracked. (stream_status.txt is generated at runtime by collector_bg.py but not committed.)

## Detected Images/Assets

None.

## Detected Cache/Build/Temp Files

None in repository. `.gitignore` correctly excludes `__pycache__/` and `*.pyc`.

## Detected Unrelated Files

None. All files are part of the BTC microstructure research pipeline.

## Detected Duplicate/Obsolete Files

| File | Issue |
|------|-------|
| collector_bg.py | Near-duplicate of collector.py (background variant) |
| fetch_multi.py | Overlaps with accumulate.py --fetch mode |

## Detected Potential Secrets

None. Grep for API_KEY, SECRET, TOKEN, PASSWORD, PRIVATE returned no matches.

## Notes

- `.gitignore` correctly excludes `data/raw/`, `data/processed/`, `__pycache__/`, `*.pyc`
- README references `build_features.py` which does not exist (actual file: `feature_engineering.py`)
- All CSV files in `data/events/` are small sample outputs (< 3 KB each)
- No virtual environments, node_modules, or dependency lock files committed
