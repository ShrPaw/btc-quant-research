"""
Feature Builder — Entry point for feature engineering pipeline.

Reads raw trades CSV → builds feature matrix → saves output.
"""
import csv
import json
import os
import sys


def build_and_save(input_path, output_dir=None):
    """Build features from raw trades and save to CSV."""
    from src.features.microstructure_features import build_feature_matrix
    from src.utils.config import DATA_PROCESSED

    if output_dir is None:
        output_dir = DATA_PROCESSED
    os.makedirs(output_dir, exist_ok=True)

    print(f"  Reading: {input_path}")
    bars, bounds = build_feature_matrix(input_path)
    print(f"  Aggregated: {len(bars)} seconds")

    # Save feature matrix
    if bars:
        fieldnames = list(bars[0].keys())
        basename = os.path.basename(input_path).replace("trades_", "features_")
        feat_path = os.path.join(output_dir, basename)

        with open(feat_path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            w.writerows(bars)
        print(f"  Features: {feat_path}")
        print(f"  Shape: {len(bars)} rows × {len(fieldnames)} columns")

        # Save winsorization bounds
        bounds_path = os.path.join(
            output_dir,
            "winsor_bounds_" + basename.replace("features_", "").replace(".csv", ".json")
        )
        with open(bounds_path, "w") as f:
            json.dump(bounds, f, indent=2)
        print(f"  Winsor bounds: {bounds_path}")

        return bars, bounds, feat_path

    return bars, bounds, None


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python -m src.features.build_features <raw_trades.csv>")
        sys.exit(1)

    bars, bounds, path = build_and_save(sys.argv[1])
    if path:
        print(f"\n  Done. {len(bars)} feature rows → {path}")
