# Changelog

## 2026-05-06 — Portfolio Polish Pass

### Added
- `assets/charts/README.md` — Chart documentation with portfolio usage guide
- `archive/README.md` — Archive documentation with merge history
- `docs/audit/file_inventory.md` — Full file inventory (50 files)
- `docs/audit/file_classification.md` — File classification by category
- `docs/audit/cleanup_plan.md` — Cleanup decisions documentation

### Changed
- `README.md` — Added "Quick Start (60 Seconds)" section for easy onboarding
- `README.md` — Added "For Non-Technical Readers" section for client accessibility
- `CHANGELOG.md` — This update

### Verified
- Pipeline runs end-to-end: `python3 scripts/run_pipeline.py` (300 bars, 43 features)
- Validation passes: `python3 scripts/run_validation.py`
- Charts generate: `python3 scripts/generate_portfolio_assets.py` (7 charts)
- No secrets, no inflated claims, no large files

## 2026-05-05 — Full Repository Restructure

### New Structure
Reorganized from flat scripts to professional package layout:
- `src/ingestion/` — Data collection (fetch_historical, live_collector)
- `src/processing/` — Cleaning and aggregation
- `src/features/` — Feature engineering (18 microstructure features)
- `src/validation/` — Integrity checks, baseline tests, cost model
- `src/visualization/` — Chart generation
- `src/utils/` — Config, I/O, logging
- `scripts/` — Pipeline entry points

### New Files
- `requirements.txt` — Dependencies
- `src/utils/config.py` — Centralized configuration
- `src/utils/io.py` — CSV I/O utilities
- `src/utils/logging_utils.py` — Pipeline logging
- `src/processing/clean_data.py` — Data cleaning with reporting
- `src/validation/baseline_tests.py` — Statistical baseline checks
- `src/validation/cost_model.py` — Transaction cost estimates
- `src/validation/validation_runner.py` — Validation orchestrator
- `src/visualization/make_charts.py` — Portfolio chart generator
- `scripts/run_pipeline.py` — Full pipeline entry point
- `scripts/run_validation.py` — Validation suite entry point
- `scripts/generate_portfolio_assets.py` — Chart generator entry point
- `data/sample/README.md` — Sample data documentation
- `data/processed/.gitkeep` — Placeholder for outputs
- `reports/data_dictionary.md` — Complete column reference
- `reports/methodology.md` — Technical methodology
- `reports/research_summary.md` — Portfolio research summary
- `reports/validation_report.md` — Validation documentation
- `reports/portfolio_description.md` — Upwork/portfolio copy
- `assets/screenshots/README.md` — Chart documentation

### Changed
- `README.md` — Complete rewrite for portfolio
- `.gitignore` — Comprehensive exclusions
- `CHANGELOG.md` — This file

### Removed
Old flat scripts (moved to `src/` package):
- `collector.py` → `src/ingestion/live_collector.py`
- `fetch_historical.py` → `src/ingestion/fetch_historical.py`
- `fetch_multi.py` → merged into `src/ingestion/fetch_historical.py`
- `compute_cvd.py` → `src/processing/aggregate_trades.py`
- `feature_engineering.py` → `src/features/build_features.py` + `microstructure_features.py`
- `detect_liquidations.py` → logic preserved in feature pipeline
- `validate_integrity.py` → `src/validation/validation_runner.py`
- `data_audit.py` → `src/validation/baseline_tests.py`
- `accumulate.py` → `scripts/run_pipeline.py`
- `docs/archive/collector_bg.py` → archived variant removed
- `docs/audit/` → old audit files removed
- `data/events/` → sample outputs removed (regenerable)
