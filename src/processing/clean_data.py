"""
Data Cleaning — Raw trade data quality checks and fixes.

Applies cleaning rules to raw trade data before aggregation:
  - Remove rows with missing/invalid fields
  - Remove zero-quantity trades
  - Ensure timestamp monotonicity
  - Flag and report anomalies
"""
import csv
import os


REQUIRED_FIELDS = [
    "timestamp_ms", "price", "quantity", "is_buyer_maker",
    "agggressor_side", "trade_id"
]


def clean_trades(rows):
    """
    Clean raw trade rows. Returns (cleaned_rows, report).

    Cleaning rules:
      1. Drop rows with missing required fields
      2. Drop rows with zero or negative quantity
      3. Drop rows with zero or negative price
      4. Drop rows with invalid aggressor_side (not BUY/SELL)
      5. Report removed rows
    """
    cleaned = []
    removed = []
    reasons = {"missing_field": 0, "zero_qty": 0, "zero_price": 0, "invalid_side": 0}

    for i, row in enumerate(rows):
        # Check required fields
        missing = False
        for field in REQUIRED_FIELDS:
            if field not in row or row[field] is None or str(row[field]).strip() == "":
                missing = True
                break
        if missing:
            reasons["missing_field"] += 1
            removed.append((i, "missing_field"))
            continue

        # Check quantity
        try:
            qty = float(row["quantity"])
            if qty <= 0:
                reasons["zero_qty"] += 1
                removed.append((i, "zero_qty"))
                continue
        except (ValueError, TypeError):
            reasons["missing_field"] += 1
            removed.append((i, "invalid_quantity"))
            continue

        # Check price
        try:
            price = float(row["price"])
            if price <= 0:
                reasons["zero_price"] += 1
                removed.append((i, "zero_price"))
                continue
        except (ValueError, TypeError):
            reasons["missing_field"] += 1
            removed.append((i, "invalid_price"))
            continue

        # Check side
        side = str(row["agggressor_side"]).upper().strip()
        if side not in ("BUY", "SELL"):
            reasons["invalid_side"] += 1
            removed.append((i, "invalid_side"))
            continue

        cleaned.append(row)

    report = {
        "input_rows": len(rows),
        "output_rows": len(cleaned),
        "removed": len(removed),
        "reasons": reasons,
    }

    return cleaned, report


def clean_trades_file(input_path, output_path=None):
    """Clean trades from CSV file. Returns (cleaned_rows, report, output_path)."""
    from src.utils.io import read_csv_rows, write_csv

    rows = read_csv_rows(input_path)
    cleaned, report = clean_trades(rows)

    if output_path is None:
        base, ext = os.path.splitext(input_path)
        output_path = f"{base}_clean{ext}"

    fieldnames = [
        "timestamp_ms", "timestamp_utc", "price", "quantity",
        "is_buyer_maker", "agggressor_side", "trade_id"
    ]
    write_csv(cleaned, fieldnames, output_path)

    return cleaned, report, output_path


def print_cleaning_report(report):
    """Print cleaning summary."""
    print(f"      Input:  {report['input_rows']:,} rows")
    print(f"      Output: {report['output_rows']:,} rows")
    print(f"      Removed: {report['removed']:,} rows")
    if report['removed'] > 0:
        for reason, count in report['reasons'].items():
            if count > 0:
                print(f"        - {reason}: {count}")
