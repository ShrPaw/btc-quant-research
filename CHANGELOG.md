# Changelog

## 2026-05-05 — Repository Cleanup & Portfolio Restructure

### Added
- `docs/audit/file_inventory.md` — full repository inventory with file counts, sizes, and classification
- `docs/audit/file_classification.md` — every file classified (KEEP_CORE, KEEP_DOCS, KEEP_REPORT, MOVE_ARCHIVE)
- `docs/archive/` — archived non-core variants
- `CHANGELOG.md` — this file

### Changed
- `README.md` — rewritten as portfolio-grade documentation
  - Fixed reference to `build_features.py` (actual file: `feature_engineering.py`)
  - Added project positioning statement (data pipeline, not trading strategy)
  - Added integrity validation documentation
  - Added anti-overfitting design section
  - Added dependency list
  - Improved structure and formatting

### Moved
- `collector_bg.py` → `docs/archive/collector_bg.py`
  - Reason: near-duplicate of `collector.py` (background variant, less robust)
  - Preserved as reference; not part of clean pipeline

### Removed
Nothing deleted. All files preserved.

### Notes
- No secrets, credentials, or API keys found in repository
- No cache, build, or temp files found in repository
- All 3 CSV files in `data/events/` are small sample outputs (< 3 KB) — kept for demonstration
- `.gitignore` correctly excludes `data/raw/`, `data/processed/`, `__pycache__/`, `*.pyc`
