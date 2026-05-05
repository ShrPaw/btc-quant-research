# Archive

This directory contains files that are not part of the main project but are preserved for reference.

## Purpose

Old experiments, alternative implementations, and development artifacts are kept here rather than deleted. This preserves development history without cluttering the main project structure.

## Current Contents

| File | Reason |
|------|--------|
| `generate_validation_screenshots.py` | Terminal-style screenshot generator. Not part of core pipeline. Used to create portfolio screenshots during development. |

## What Was Merged (Not Archived)

| Original File | Merged Into | Notes |
|---------------|-------------|-------|
| `collector.py` | `src/ingestion/live_collector.py` | WebSocket collector |
| `fetch_historical.py` | `src/ingestion/fetch_historical.py` | REST API fetcher |
| `fetch_multi.py` | `src/ingestion/fetch_historical.py` | Batch fetching |
| `compute_cvd.py` | `src/processing/aggregate_trades.py` | 1s aggregation |
| `feature_engineering.py` | `src/features/microstructure_features.py` | Feature engineering |
| `detect_liquidations.py` | Logic in feature pipeline | Event detection |
| `validate_integrity.py` | `src/validation/validation_runner.py` | Integrity checks |
| `data_audit.py` | `src/validation/baseline_tests.py` | Statistical tests |
| `accumulate.py` | `scripts/run_pipeline.py` | Pipeline orchestrator |
