"""
routes/quality.py — Quality scoring and anomaly detection
"""

import numpy as np
from fastapi import APIRouter
from backend.db import get_schema

router = APIRouter(prefix="/api/quality", tags=["quality"])


def compute_iqr_anomalies(schema: dict):
    """Run IQR + Z-score anomaly detection on null rates across columns."""
    anomalies = []
    for table_name, t in schema.items():
        null_rates = [c["nullPctNum"] for c in t["columns"]]
        if len(null_rates) < 3:
            continue

        arr = np.array(null_rates)
        q1, q3 = np.percentile(arr, [25, 75])
        iqr = q3 - q1
        upper_fence = q3 + 1.5 * iqr

        mean = np.mean(arr)
        std = np.std(arr) if np.std(arr) > 0 else 1

        for col in t["columns"]:
            rate = col["nullPctNum"]
            if rate > upper_fence and rate > 10:
                z = abs((rate - mean) / std)
                severity = "Critical" if z > 4 else "High" if z > 3 else "Medium"
                anomalies.append({
                    "table": table_name,
                    "column": col["name"],
                    "null_rate": rate,
                    "null_display": col["nullPct"],
                    "z_score": round(z, 1),
                    "severity": severity,
                    "iqr_fence": round(upper_fence, 2),
                })

    # Sort by z_score desc
    anomalies.sort(key=lambda x: x["z_score"], reverse=True)
    return anomalies


@router.get("")
def get_quality():
    """Return overall quality metrics, per-table bars, and anomaly detection results."""
    schema = get_schema()

    # Overall stats
    qualities = [t["quality"] for t in schema.values()]
    avg_quality = round(np.mean(qualities), 1) if qualities else 0

    # Completeness = average across all tables weighted by col count
    all_null_rates = []
    for t in schema.values():
        for c in t["columns"]:
            all_null_rates.append(c["nullPctNum"])
    avg_null = np.mean(all_null_rates) if all_null_rates else 0
    completeness = round((1 - avg_null / 100) * 100, 1)

    # Count stale tables (simulated — tables with "timestamp" cols we can check)
    stale_count = sum(1 for t in schema.values() if t["quality"] < 70)

    # Anomalies
    anomalies = compute_iqr_anomalies(schema)

    # Per-table quality bars
    table_bars = []
    for name, t in schema.items():
        table_bars.append({
            "name": name,
            "quality": t["quality"],
            "status": t["status"],
            "color": t["color"],
        })
    table_bars.sort(key=lambda x: x["quality"], reverse=True)

    # Anomaly sparkline — pick the worst anomaly for sparkline simulation
    sparkline = None
    if anomalies:
        worst = anomalies[0]
        # Simulate 7-day null rate history (first 6 days normal, last day spike)
        base = max(0.5, worst["null_rate"] * 0.07)
        sparkline = {
            "table": worst["table"],
            "column": worst["column"],
            "current_rate": worst["null_rate"],
            "z_score": worst["z_score"],
            "values": [
                round(base * 0.9, 2),
                round(base * 1.1, 2),
                round(base * 0.95, 2),
                round(base * 1.05, 2),
                round(base * 1.0, 2),
                round(base * 1.15, 2),
                worst["null_rate"],
            ],
            "labels": ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun⚠"],
        }

    return {
        "overall": avg_quality,
        "completeness": completeness,
        "freshness": 74.7,  # simulated — would need write timestamps in production
        "stale_tables": stale_count,
        "total_tables": len(schema),
        "anomalies": anomalies[:10],
        "table_bars": table_bars,
        "sparkline": sparkline,
    }
