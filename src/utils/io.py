"""
I/O utilities — CSV reading, writing, and path management.
"""
import csv
import os
import glob


def ensure_dir(path):
    """Create directory if it doesn't exist."""
    os.makedirs(path, exist_ok=True)
    return path


def read_csv_rows(path):
    """Read CSV into list of dicts."""
    rows = []
    with open(path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    return rows


def read_csv_floats(path, float_cols=None):
    """Read CSV into list of dicts with float conversion for numeric columns."""
    rows = []
    with open(path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            parsed = {}
            for k, v in row.items():
                if float_cols and k in float_cols:
                    try:
                        parsed[k] = float(v)
                    except (ValueError, TypeError):
                        parsed[k] = v
                else:
                    try:
                        parsed[k] = float(v)
                    except (ValueError, TypeError):
                        parsed[k] = v
            rows.append(parsed)
    return rows


def write_csv(rows, fieldnames, output_path):
    """Write list of dicts to CSV."""
    ensure_dir(os.path.dirname(output_path))
    with open(output_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)
    return output_path


def find_latest(pattern):
    """Find the most recently modified file matching a glob pattern."""
    files = glob.glob(pattern)
    if not files:
        return None
    return max(files, key=os.path.getmtime)


def timestamp_filename(prefix, ext="csv"):
    """Generate a timestamped filename."""
    import time
    ts = time.strftime("%Y%m%d_%H%M%S")
    return f"{prefix}_{ts}.{ext}"
