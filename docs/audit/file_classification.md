# File Classification — btc-quant-research

**Date:** 2026-05-06

## Classification Legend

- **KEEP_CORE** — Core source code for data pipeline
- **KEEP_SAMPLE** — Small sample/demo data
- **KEEP_REPORT** — Research report or documentation
- **KEEP_ASSET** — Charts/screenshots for portfolio
- **KEEP_DOC** — README, docs, data dictionary
- **ARCHIVE** — Old experiments, not part of clean public project
- **DELETE** — Cache, temp, junk
- **GITIGNORE** — Large data, generated outputs, local files

## Classification Results

### KEEP_CORE (20 files)
| File | Role |
|------|------|
| `src/__init__.py` | Package root |
| `src/ingestion/__init__.py` | Package init |
| `src/ingestion/fetch_historical.py` | REST data fetcher |
| `src/ingestion/live_collector.py` | WebSocket collector |
| `src/processing/__init__.py` | Package init |
| `src/processing/aggregate_trades.py` | 1s bar aggregation |
| `src/processing/clean_data.py` | Data cleaning |
| `src/features/__init__.py` | Package init |
| `src/features/build_features.py` | Feature entry point |
| `src/features/microstructure_features.py` | 18 features |
| `src/validation/__init__.py` | Package init |
| `src/validation/baseline_tests.py` | Statistical tests |
| `src/validation/cost_model.py` | Cost estimation |
| `src/validation/validation_runner.py` | Validation orchestrator |
| `src/visualization/__init__.py` | Package init |
| `src/visualization/make_charts.py` | Chart generation |
| `src/utils/__init__.py` | Package init |
| `src/utils/config.py` | Configuration |
| `src/utils/io.py` | CSV I/O |
| `src/utils/logging_utils.py` | Logging |

### KEEP_SAMPLE (2 files)
| File | Role |
|------|------|
| `data/sample/sample_market_data.csv` | 1000 synthetic trades |
| `data/sample/README.md` | Sample data docs |

### KEEP_REPORT (5 files)
| File | Role |
|------|------|
| `reports/data_dictionary.md` | Column reference |
| `reports/methodology.md` | Technical methodology |
| `reports/research_summary.md` | Portfolio overview |
| `reports/validation_report.md` | Validation docs |
| `reports/portfolio_description.md` | Upwork copy |

### KEEP_ASSET (12 files)
| File | Role |
|------|------|
| `assets/charts/price_over_time.png` | Price chart |
| `assets/charts/volume_over_time.png` | Volume chart |
| `assets/charts/delta_over_time.png` | Delta chart |
| `assets/charts/cvd_over_time.png` | CVD chart |
| `assets/charts/rolling_volatility.png` | Volatility chart |
| `assets/charts/returns_distribution.png` | Returns chart |
| `assets/charts/feature_correlation.png` | Correlation heatmap |
| `assets/screenshots/pipeline_output.png` | Pipeline terminal |
| `assets/screenshots/validation_output.png` | Validation terminal |
| `assets/screenshots/validation_summary.png` | Results card |
| `assets/screenshots/correlation_analysis.png` | Correlation detail |
| `assets/screenshots/lookahead_precautions.png` | Anti-leakage |

### KEEP_DOC (6 files)
| File | Role |
|------|------|
| `README.md` | Main docs |
| `CHANGELOG.md` | Version history |
| `requirements.txt` | Dependencies |
| `.gitignore` | Git exclusions |
| `assets/screenshots/README.md` | Screenshot docs |
| `data/processed/.gitkeep` | Directory placeholder |

### KEEP_SCRIPTS (4 files)
| File | Role |
|------|------|
| `scripts/run_pipeline.py` | Pipeline entry point |
| `scripts/run_validation.py` | Validation entry point |
| `scripts/generate_portfolio_assets.py` | Chart generator |
| `scripts/generate_validation_screenshots.py` | Screenshot generator |

### ARCHIVE (1 file)
| File | Role |
|------|------|
| `archive/README.md` | Archive documentation |

### DELETE (0 files)
No files flagged for deletion.

### GITIGNORE (managed by .gitignore)
- `data/raw/` — Raw trade data
- `data/processed/` — Generated outputs
- `__pycache__/` — Python cache
- `.env` — Environment variables
- `*.log` — Log files

## Summary

| Category | Count |
|----------|-------|
| KEEP_CORE | 20 |
| KEEP_SAMPLE | 2 |
| KEEP_REPORT | 5 |
| KEEP_ASSET | 12 |
| KEEP_DOC | 6 |
| KEEP_SCRIPTS | 4 |
| ARCHIVE | 1 |
| DELETE | 0 |
| **Total** | **50** |
