# File Inventory — btc-quant-research

**Date:** 2026-05-06
**Branch:** upwork-portfolio-cleanup
**Total files (excl. .git/):** 50

## File Count by Extension

| Extension | Count | Purpose |
|-----------|-------|---------|
| `.py`     | 24    | Source code, scripts |
| `.png`    | 12    | Charts, screenshots |
| `.md`     | 10    | Documentation |
| `.txt`    | 1     | Dependencies |
| `.csv`    | 1     | Sample data |
| `.gitkeep`| 1     | Directory placeholder |
| `.gitignore` | 1 | Git exclusions |

## Full File Tree

```
btc-quant-research/
├── README.md                           # Main project documentation
├── CHANGELOG.md                        # Version history
├── requirements.txt                    # Python dependencies
├── .gitignore                          # Git exclusion rules
│
├── src/                                # Source package (24 .py files)
│   ├── __init__.py                     # Package root
│   ├── ingestion/                      # Data collection
│   │   ├── __init__.py
│   │   ├── fetch_historical.py         # REST API bootstrap
│   │   └── live_collector.py           # WebSocket real-time stream
│   ├── processing/                     # Cleaning & aggregation
│   │   ├── __init__.py
│   │   ├── aggregate_trades.py         # Tick → 1s bar aggregation
│   │   └── clean_data.py              # Data quality checks
│   ├── features/                       # Feature engineering
│   │   ├── __init__.py
│   │   ├── build_features.py           # Entry point
│   │   └── microstructure_features.py  # 18 feature calculations
│   ├── validation/                     # Integrity & baseline tests
│   │   ├── __init__.py
│   │   ├── baseline_tests.py           # Statistical checks
│   │   ├── cost_model.py              # Transaction cost estimates
│   │   └── validation_runner.py        # Orchestrator
│   ├── visualization/                  # Chart generation
│   │   ├── __init__.py
│   │   └── make_charts.py             # Portfolio-ready PNG charts
│   └── utils/                          # Shared utilities
│       ├── __init__.py
│       ├── config.py                   # Constants & paths
│       ├── io.py                       # CSV I/O helpers
│       └── logging_utils.py           # Pipeline logging
│
├── scripts/                            # Entry point scripts
│   ├── run_pipeline.py                 # Full pipeline
│   ├── run_validation.py              # Validation suite
│   ├── generate_portfolio_assets.py    # Chart generator
│   └── generate_validation_screenshots.py  # Terminal screenshots
│
├── data/
│   ├── sample/                         # Demo data
│   │   ├── README.md
│   │   └── sample_market_data.csv      # 1000 synthetic trades
│   └── processed/                      # Pipeline outputs (gitignored)
│       └── .gitkeep
│
├── reports/                            # Documentation
│   ├── data_dictionary.md              # Column reference
│   ├── methodology.md                  # Technical methodology
│   ├── research_summary.md             # Portfolio overview
│   ├── validation_report.md            # Validation documentation
│   └── portfolio_description.md        # Upwork/portfolio copy
│
├── assets/
│   ├── charts/                         # Generated PNG charts (7 files)
│   │   ├── README.md
│   │   ├── price_over_time.png
│   │   ├── volume_over_time.png
│   │   ├── delta_over_time.png
│   │   ├── cvd_over_time.png
│   │   ├── rolling_volatility.png
│   │   ├── returns_distribution.png
│   │   └── feature_correlation.png
│   └── screenshots/                    # Terminal-style screenshots (5 files)
│       ├── README.md
│       ├── pipeline_output.png
│       ├── validation_output.png
│       ├── validation_summary.png
│       ├── correlation_analysis.png
│       └── lookahead_precautions.png
│
├── docs/
│   └── audit/                          # Cleanup documentation
│       ├── file_inventory.md           # This file
│       ├── file_classification.md      # File classification
│       ├── cleanup_plan.md             # Cleanup plan
│       └── cleanup_report.md           # Post-cleanup report
│
└── archive/                            # Archived/unused files
    └── README.md
```

## Python Scripts (24 files)

### Source Package (16 files)
| File | Purpose |
|------|---------|
| `src/__init__.py` | Package root |
| `src/ingestion/__init__.py` | Ingestion package |
| `src/ingestion/fetch_historical.py` | REST API trade fetcher |
| `src/ingestion/live_collector.py` | WebSocket live collector |
| `src/processing/__init__.py` | Processing package |
| `src/processing/aggregate_trades.py` | 1s bar aggregation |
| `src/processing/clean_data.py` | Data cleaning |
| `src/features/__init__.py` | Features package |
| `src/features/build_features.py` | Feature entry point |
| `src/features/microstructure_features.py` | 18 feature calculations |
| `src/validation/__init__.py` | Validation package |
| `src/validation/baseline_tests.py` | Statistical tests |
| `src/validation/cost_model.py` | Cost estimation |
| `src/validation/validation_runner.py` | Validation orchestrator |
| `src/visualization/__init__.py` | Visualization package |
| `src/visualization/make_charts.py` | Chart generation |

### Utility Modules (4 files)
| File | Purpose |
|------|---------|
| `src/utils/__init__.py` | Utils package |
| `src/utils/config.py` | Configuration constants |
| `src/utils/io.py` | CSV I/O utilities |
| `src/utils/logging_utils.py` | Pipeline logging |

### Entry Point Scripts (4 files)
| File | Purpose |
|------|---------|
| `scripts/run_pipeline.py` | Full pipeline |
| `scripts/run_validation.py` | Validation suite |
| `scripts/generate_portfolio_assets.py` | Chart generator |
| `scripts/generate_validation_screenshots.py` | Terminal screenshots |

## Datasets (1 file)
| File | Rows | Description |
|------|------|-------------|
| `data/sample/sample_market_data.csv` | 1000 | Synthetic BTCUSDT trades |

## Charts (7 files)
| File | Description |
|------|-------------|
| `price_over_time.png` | BTCUSDT price from raw trades |
| `volume_over_time.png` | Buy/sell volume aggregation |
| `delta_over_time.png` | Net delta per second |
| `cvd_over_time.png` | Cumulative volume delta |
| `rolling_volatility.png` | Realized volatility 30s/60s |
| `returns_distribution.png` | Log return histogram |
| `feature_correlation.png` | Feature correlation heatmap |

## Screenshots (5 files)
| File | Description |
|------|-------------|
| `pipeline_output.png` | Terminal-style pipeline output |
| `validation_output.png` | Terminal-style validation output |
| `validation_summary.png` | Validation results card |
| `correlation_analysis.png` | Redundant feature pairs |
| `lookahead_precautions.png` | Anti-leakage verification |

## Large Files
None. All files under 1 MB.

## Potential Secrets
None found. No API keys, tokens, passwords, or credentials.

## Root-Level Files
| File | Status |
|------|--------|
| `README.md` | ✅ Required |
| `CHANGELOG.md` | ✅ Useful |
| `requirements.txt` | ✅ Required |
| `.gitignore` | ✅ Required |
