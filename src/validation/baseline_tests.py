"""
Baseline Tests — Simple statistical checks on feature data.

These are descriptive sanity checks, not signal tests:
  - Distribution properties (mean, std, skew, kurtosis)
  - Temporal stability (drift across chunks)
  - Feature correlation (redundancy detection)
  - Missing value checks
  - Constant feature detection
"""
import math
from collections import Counter


def moment4(values):
    """Compute mean, std, skewness, kurtosis (excess)."""
    n = len(values)
    if n < 4:
        return {"mean": 0, "std": 0, "skewness": 0, "kurtosis": 0}

    mean = sum(values) / n
    m2 = sum((x - mean) ** 2 for x in values) / n
    std = math.sqrt(m2)

    if std < 1e-15:
        return {"mean": mean, "std": 0, "skewness": 0, "kurtosis": 0}

    m3 = sum((x - mean) ** 3 for x in values) / n
    m4 = sum((x - mean) ** 4 for x in values) / n

    skewness = m3 / (std ** 3)
    kurtosis = m4 / (std ** 4) - 3.0

    return {"mean": mean, "std": std, "skewness": skewness, "kurtosis": kurtosis}


def check_missing_values(rows, feature_names):
    """Check for missing or NaN values in features."""
    issues = []
    for fname in feature_names:
        missing = 0
        for row in rows:
            v = row.get(fname)
            if v is None or (isinstance(v, float) and math.isnan(v)):
                missing += 1
        if missing > 0:
            issues.append({"feature": fname, "missing_count": missing, "pct": missing / len(rows) * 100})
    return issues


def check_constant_features(rows, feature_names):
    """Detect features with zero variance."""
    constants = []
    for fname in feature_names:
        vals = [row[fname] for row in rows if isinstance(row.get(fname), (int, float))]
        if len(vals) < 2:
            continue
        if max(vals) - min(vals) < 1e-15:
            constants.append(fname)
    return constants


def check_temporal_drift(rows, feature_names, n_chunks=5):
    """Split data into chunks and detect mean drift."""
    n = len(rows)
    chunk_size = n // n_chunks
    if chunk_size < 2:
        return {}

    results = {}
    for fname in feature_names:
        chunks = []
        for i in range(n_chunks):
            start = i * chunk_size
            end = start + chunk_size if i < n_chunks - 1 else n
            chunk_vals = [rows[j][fname] for j in range(start, end)]
            chunks.append(moment4(chunk_vals))

        all_vals = [r[fname] for r in rows]
        overall = moment4(all_vals)
        overall_std = overall["std"] if overall["std"] > 1e-15 else 1.0

        means = [c["mean"] for c in chunks]
        mean_range = max(means) - min(means)
        drift_ratio = mean_range / overall_std

        results[fname] = {
            "drift_ratio": drift_ratio,
            "stable": drift_ratio < 2.0,
        }

    return results


def check_feature_correlation(rows, feature_names, threshold=0.9):
    """Find highly correlated feature pairs."""
    n = len(rows)
    columns = {fname: [r[fname] for r in rows] for fname in feature_names}
    means = {fname: sum(columns[fname]) / n for fname in feature_names}

    redundant = []
    seen = set()
    for i, fi in enumerate(feature_names):
        for fj in feature_names[i+1:]:
            key = (fi, fj)
            if key in seen:
                continue
            seen.add(key)

            xi, xj = columns[fi], columns[fj]
            mi, mj = means[fi], means[fj]

            cov = sum((xi[k] - mi) * (xj[k] - mj) for k in range(n)) / n
            si = math.sqrt(sum((xi[k] - mi) ** 2 for k in range(n)) / n)
            sj = math.sqrt(sum((xj[k] - mj) ** 2 for k in range(n)) / n)

            if si > 1e-15 and sj > 1e-15:
                r = cov / (si * sj)
                if abs(r) > threshold:
                    redundant.append((fi, fj, round(r, 4)))

    return redundant


def run_baseline_tests(rows, feature_names):
    """Run all baseline tests. Returns comprehensive report."""
    report = {}

    # Distribution
    report["distributions"] = {fname: moment4([r[fname] for r in rows]) for fname in feature_names}

    # Missing values
    report["missing_values"] = check_missing_values(rows, feature_names)

    # Constant features
    report["constant_features"] = check_constant_features(rows, feature_names)

    # Temporal drift
    report["temporal_drift"] = check_temporal_drift(rows, feature_names)

    # Correlation
    report["redundant_pairs"] = check_feature_correlation(rows, feature_names)

    # Summary
    unstable = [f for f, d in report["temporal_drift"].items() if not d["stable"]]
    report["summary"] = {
        "features_analyzed": len(feature_names),
        "missing_issues": len(report["missing_values"]),
        "constant_features": len(report["constant_features"]),
        "unstable_features": len(unstable),
        "redundant_pairs": len(report["redundant_pairs"]),
    }

    return report


def print_baseline_report(report):
    """Print baseline test results."""
    s = report["summary"]
    print(f"      Features analyzed:    {s['features_analyzed']}")
    print(f"      Missing value issues: {s['missing_issues']}")
    print(f"      Constant features:    {s['constant_features']}")
    print(f"      Unstable features:    {s['unstable_features']}")
    print(f"      Redundant pairs:      {s['redundant_pairs']}")

    if report["missing_values"]:
        print(f"\n      Missing values:")
        for issue in report["missing_values"]:
            print(f"        {issue['feature']}: {issue['missing_count']} ({issue['pct']:.1f}%)")

    if report["constant_features"]:
        print(f"\n      Constant features: {report['constant_features']}")

    if report["redundant_pairs"]:
        print(f"\n      Highly correlated pairs (|r| > 0.9):")
        for fi, fj, r in sorted(report["redundant_pairs"], key=lambda x: -abs(x[2])):
            print(f"        {fi} <-> {fj}: r = {r:+.4f}")

    total_issues = s['missing_issues'] + s['constant_features'] + s['unstable_features'] + s['redundant_pairs']
    if total_issues == 0:
        print(f"\n      ✓ All baseline checks passed")
    else:
        print(f"\n      ⚠ {total_issues} issue(s) found — review before proceeding")
