#!/usr/bin/env python3
"""
Validation Runner — Execute validation checks on processed data.

Runs:
  - Timestamp ordering checks
  - Duplicate detection
  - Baseline statistical tests
  - Feature sanity checks
  - Missing value checks
  - Lookahead leakage precautions
  - Cost model reference estimates

Usage:
  python scripts/run_validation.py                              # Auto-detect data
  python scripts/run_validation.py data/processed/features.csv  # Specific file
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def main():
    from src.utils.logging_utils import PipelineLogger
    from src.utils.config import DATA_PROCESSED, DATA_SAMPLE

    log = PipelineLogger("validation")
    log.header("BTC Quant Research — Validation Suite")

    # ── Load data ─────────────────────────────────────────────────────────────
    log.set_stage(1)
    log.stage("Loading feature data")

    if len(sys.argv) > 1:
        input_path = sys.argv[1]
    else:
        # Auto-detect
        input_path = os.path.join(DATA_PROCESSED, "features.csv")
        if not os.path.exists(input_path):
            log.info("No processed features found.")
            log.info("Run scripts/run_pipeline.py first to generate features.")
            log.info("")
            log.info("Running on sample data if available...")
            input_path = os.path.join(DATA_SAMPLE, "sample_market_data.csv")
            if not os.path.exists(input_path):
                log.error("No data available. Run scripts/run_pipeline.py first.")
                return

    if not os.path.exists(input_path):
        log.error(f"File not found: {input_path}")
        return

    from src.utils.io import read_csv_floats
    rows = read_csv_floats(input_path)
    log.result(f"{len(rows):,} rows loaded from {input_path}")

    if not rows:
        log.error("No data loaded.")
        return

    # ── Identify feature columns ──────────────────────────────────────────────
    skip_cols = {"timestamp_s", "timestamp_utc", "timestamp_ms", "is_buyer_maker", "agggressor_side"}
    feature_names = [k for k in rows[0].keys()
                     if k not in skip_cols and isinstance(rows[0][k], (int, float))]
    log.info(f"Feature columns: {len(feature_names)}")

    # ── Run validation ────────────────────────────────────────────────────────
    log.set_stage(2)
    log.stage("Running validation suite")

    from src.validation.validation_runner import run_full_validation, print_validation_report
    report = run_full_validation(rows, feature_names)
    print_validation_report(report)

    # ── Cost model ────────────────────────────────────────────────────────────
    log.set_stage(3)
    log.stage("Cost model reference estimates")

    from src.validation.cost_model import estimate_transaction_cost, cost_aware_metrics
    cost = estimate_transaction_cost(10000)
    log.info(f"Taker fee (0.04%) on $10,000 trade: ${cost['fee_usd']:.2f}")

    # Compute simple return stats if returns column exists
    if "returns" in rows[0]:
        returns = [r["returns"] for r in rows if r["returns"] != 0]
        if returns:
            metrics = cost_aware_metrics(returns)
            log.info(f"Mean return/s: {metrics['mean_return_per_period']:.10f}")
            log.info(f"Daily cost estimate ({100} trades): {metrics['daily_cost_pct']:.4f}%")
            log.info(f"Note: {metrics['note']}")

    # ── Summary ───────────────────────────────────────────────────────────────
    log.set_stage(4)
    log.stage("Validation summary")

    if report["overall_passed"]:
        log.success("All integrity checks PASSED")
    else:
        log.warn("Some integrity checks FAILED — review above")

    if report["baseline_tests"]:
        s = report["baseline_tests"]["summary"]
        log.info(f"Features analyzed: {s['features_analyzed']}")
        log.info(f"Issues found: {s['missing_issues'] + s['constant_features'] + s['unstable_features'] + s['redundant_pairs']}")

    log.info(f"Lookahead precautions: {len(report['lookahead_precautions'])} checks verified")

    log.header("Validation Complete")

    # Return exit code
    if not report["overall_passed"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
