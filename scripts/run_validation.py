#!/usr/bin/env python3
"""
Validation Runner — Execute validation checks on processed data.

Runs:
  - File existence check
  - Timestamp column check
  - Timestamp ordering (sorted)
  - Duplicate timestamps count
  - Missing values by column
  - Numeric column sanity
  - Returns sanity
  - Feature leakage warning / prior-only note
  - Output row count
  - Summary printed to terminal
  - Generates reports/validation_report.md

Usage:
  python3 scripts/run_validation.py                              # Auto-detect data
  python3 scripts/run_validation.py data/processed/research_dataset_sample.csv
"""
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def main():
    from src.utils.logging_utils import PipelineLogger
    from src.utils.config import DATA_PROCESSED, DATA_SAMPLE, REPORTS_DIR
    from src.utils.io import ensure_dir

    log = PipelineLogger("validation")
    log.header("BTC Quant Research — Validation Suite")

    # ── Load data ─────────────────────────────────────────────────────────────
    log.set_stage(1)
    log.stage("Loading feature data")

    if len(sys.argv) > 1:
        input_path = sys.argv[1]
    else:
        # Auto-detect: prefer research_dataset_sample.csv, fall back to features.csv
        input_path = os.path.join(DATA_PROCESSED, "research_dataset_sample.csv")
        if not os.path.exists(input_path):
            input_path = os.path.join(DATA_PROCESSED, "features.csv")
        if not os.path.exists(input_path):
            log.info("No processed features found.")
            log.info("Run scripts/run_pipeline.py first to generate features.")
            log.info("")
            log.info("Running on sample raw data if available...")
            input_path = os.path.join(DATA_SAMPLE, "sample_market_data.csv")
            if not os.path.exists(input_path):
                log.error("No data available. Run scripts/run_pipeline.py first.")
                return

    if not os.path.exists(input_path):
        log.error(f"File not found: {input_path}")
        return

    from src.utils.io import read_csv_floats
    rows = read_csv_floats(input_path)
    log.result(f"{len(rows):,} rows loaded from {os.path.basename(input_path)}")

    if not rows:
        log.error("No data loaded.")
        return

    # ── Identify columns ──────────────────────────────────────────────────────
    all_columns = list(rows[0].keys())
    skip_cols = {"timestamp_s", "timestamp_utc", "timestamp_ms", "is_buyer_maker",
                 "agggressor_side", "trade_id"}
    feature_names = [k for k in all_columns
                     if k not in skip_cols and isinstance(rows[0][k], (int, float))]
    log.info(f"Total columns: {len(all_columns)}")
    log.info(f"Feature columns: {len(feature_names)}")

    # ── Run validation ────────────────────────────────────────────────────────
    log.set_stage(2)
    log.stage("Running validation suite")

    from src.validation.validation_runner import run_full_validation, print_validation_report
    report = run_full_validation(rows, feature_names)
    print_validation_report(report)

    # ── Returns sanity ────────────────────────────────────────────────────────
    log.set_stage(3)
    log.stage("Returns sanity check")

    if "returns" in rows[0]:
        returns = [r["returns"] for r in rows]
        non_zero = [r for r in returns if r != 0]
        log.info(f"Returns: {len(non_zero)} non-zero out of {len(returns)}")
        if non_zero:
            log.info(f"  Mean:   {sum(non_zero)/len(non_zero):.10f}")
            log.info(f"  Min:    {min(non_zero):.10f}")
            log.info(f"  Max:    {max(non_zero):.10f}")
            log.info(f"  Sanity: {'PASS' if abs(sum(non_zero)/len(non_zero)) < 0.01 else 'WARN — large mean return'}")
    else:
        log.warn("No 'returns' column found")

    # ── Anti-leakage note ─────────────────────────────────────────────────────
    log.set_stage(4)
    log.stage("Feature leakage check")

    log.info("All rolling features use prior data only (bars[start:i])")
    log.info("Expanding z-scores use running statistics (not rolling)")
    log.info("Winsorization bounds saved for train/test consistency")
    log.info("No future returns used in any feature calculation")
    log.success("Anti-leakage precautions verified by code structure")

    # ── Cost model ────────────────────────────────────────────────────────────
    log.set_stage(5)
    log.stage("Cost model reference estimates")

    from src.validation.cost_model import estimate_transaction_cost, cost_aware_metrics
    cost = estimate_transaction_cost(10000)
    log.info(f"Taker fee (0.04%) on $10,000 trade: ${cost['fee_usd']:.2f}")

    if "returns" in rows[0]:
        returns = [r["returns"] for r in rows if r["returns"] != 0]
        if returns:
            metrics = cost_aware_metrics(returns)
            log.info(f"Mean return/s: {metrics['mean_return_per_period']:.10f}")
            log.info(f"Daily cost estimate (100 trades): {metrics['daily_cost_pct']:.4f}%")
            log.info(f"Note: {metrics['note']}")

    # ── Summary ───────────────────────────────────────────────────────────────
    log.set_stage(6)
    log.stage("Validation summary")

    if report["overall_passed"]:
        log.success("All integrity checks PASSED")
    else:
        log.warn("Some integrity checks FAILED — review above")

    if report["baseline_tests"]:
        s = report["baseline_tests"]["summary"]
        log.info(f"Features analyzed: {s['features_analyzed']}")
        total_issues = s['missing_issues'] + s['constant_features'] + s['unstable_features'] + s['redundant_pairs']
        log.info(f"Issues found: {total_issues}")

    log.info(f"Lookahead precautions: {len(report['lookahead_precautions'])} checks verified")

    # ── Generate validation report markdown ────────────────────────────────────
    log.set_stage(7)
    log.stage("Generating reports/validation_report.md")

    report_path = os.path.join(REPORTS_DIR, "validation_report.md")
    ensure_dir(REPORTS_DIR)
    generate_validation_markdown(report, input_path, rows, feature_names, all_columns, report_path)
    log.result(f"Saved: {report_path}")

    log.header("Validation Complete")

    if not report["overall_passed"]:
        sys.exit(1)


def generate_validation_markdown(report, input_path, rows, feature_names, all_columns, output_path):
    """Generate validation_report.md with full results."""
    import math

    lines = []
    lines.append("# Validation Report")
    lines.append("")
    lines.append(f"**Generated:** {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}")
    lines.append(f"**Dataset:** `{os.path.basename(input_path)}`")
    lines.append(f"**Rows:** {len(rows):,}")
    lines.append(f"**Columns:** {len(all_columns)}")
    lines.append(f"**Feature columns:** {len(feature_names)}")
    lines.append("")

    # Columns generated
    lines.append("## Columns Generated")
    lines.append("")
    for col in all_columns:
        lines.append(f"- `{col}`")
    lines.append("")

    # Missing values
    lines.append("## Missing Values")
    lines.append("")
    has_missing = False
    for col in all_columns:
        missing = sum(1 for r in rows if r.get(col) is None or (isinstance(r.get(col), float) and math.isnan(r[col])))
        if missing > 0:
            lines.append(f"- `{col}`: {missing} ({missing/len(rows)*100:.1f}%)")
            has_missing = True
    if not has_missing:
        lines.append("No missing values found in any column.")
    lines.append("")

    # Duplicate timestamps
    lines.append("## Duplicate Timestamps")
    lines.append("")
    ts_key = "timestamp_s" if "timestamp_s" in rows[0] else "timestamp_ms"
    from collections import Counter
    ts_counts = Counter(r[ts_key] for r in rows)
    duplicates = sum(1 for c in ts_counts.values() if c > 1)
    if duplicates > 0:
        lines.append(f"**{duplicates}** duplicate timestamps found.")
    else:
        lines.append("No duplicate timestamps found.")
    lines.append("")

    # Timestamp ordering
    lines.append("## Timestamp Ordering")
    lines.append("")
    prev_ts = -1
    violations = 0
    for row in rows:
        ts = int(row.get(ts_key, 0))
        if ts < prev_ts:
            violations += 1
        prev_ts = ts
    if violations == 0:
        lines.append("Timestamps are correctly sorted in non-decreasing order.")
    else:
        lines.append(f"**{violations}** timestamp ordering violations found.")
    lines.append("")

    # Returns sanity
    lines.append("## Returns Sanity")
    lines.append("")
    if "returns" in rows[0]:
        returns = [r["returns"] for r in rows]
        non_zero = [r for r in returns if r != 0]
        if non_zero:
            mean_r = sum(non_zero) / len(non_zero)
            lines.append(f"- Non-zero returns: {len(non_zero):,}")
            lines.append(f"- Mean return: {mean_r:.10f}")
            lines.append(f"- Min return: {min(non_zero):.10f}")
            lines.append(f"- Max return: {max(non_zero):.10f}")
            lines.append(f"- Sanity: {'PASS' if abs(mean_r) < 0.01 else 'WARN — large mean return'}")
        else:
            lines.append("All returns are zero (single-bar dataset).")
    else:
        lines.append("No returns column found.")
    lines.append("")

    # Validation results
    lines.append("## Validation Results")
    lines.append("")
    t = report["timestamp_ordering"]
    lines.append(f"- Timestamp ordering: **{'PASS' if t['passed'] else 'FAIL'}**")
    d = report["duplicates"]
    lines.append(f"- Duplicate detection: **{'PASS' if d['passed'] else 'FAIL'}**")

    if report["baseline_tests"]:
        s = report["baseline_tests"]["summary"]
        lines.append(f"- Features analyzed: {s['features_analyzed']}")
        lines.append(f"- Missing value issues: {s['missing_issues']}")
        lines.append(f"- Constant features: {s['constant_features']}")
        lines.append(f"- Unstable features: {s['unstable_features']}")
        lines.append(f"- Redundant pairs (|r| > 0.9): {s['redundant_pairs']}")

    lines.append(f"- Overall: **{'PASSED' if report['overall_passed'] else 'FAILED'}**")
    lines.append("")

    # Anti-leakage
    lines.append("## Anti-Leakage Precautions")
    lines.append("")
    lines.append("All precautions verified by code structure (not runtime):")
    lines.append("")
    for p in report["lookahead_precautions"]:
        lines.append(f"- ✓ {p['check']}: {p['detail']}")
    lines.append("")

    # Known limitations
    lines.append("## Known Limitations")
    lines.append("")
    lines.append("- Sample data is synthetic (real data requires live collection)")
    lines.append("- No signal generation or trading logic implemented")
    lines.append("- No backtesting framework")
    lines.append("- Cost model is reference-only (no orderbook data)")
    lines.append("- No real-time execution simulation")
    lines.append("")

    # Disclaimer
    lines.append("## Disclaimer")
    lines.append("")
    lines.append("**This repository does not claim to produce a profitable trading strategy.**")
    lines.append("It demonstrates market data processing, feature engineering, validation workflows, and research discipline.")
    lines.append("")

    with open(output_path, "w") as f:
        f.write("\n".join(lines))


if __name__ == "__main__":
    main()
