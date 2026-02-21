"""
routes/watcher.py — Schema drift watcher endpoint
"""

from fastapi import APIRouter
from backend.db import get_schema

router = APIRouter(prefix="/api/watcher", tags=["watcher"])

# Simulated drift log — uses real table/column names from the schema
DRIFT_LOG = [
    {
        "time": "5m ago",
        "db": "olist_ecommerce",
        "db_chip": "chip-neon",
        "table": "reviews",
        "change": "Null Spike",
        "change_chip": "chip-red",
        "detail": "review_comment_message: 58%→76% null rate",
        "severity": "Critical",
        "severity_chip": "chip-red",
    },
    {
        "time": "2h ago",
        "db": "olist_ecommerce",
        "db_chip": "chip-neon",
        "table": "products",
        "change": "New Column",
        "change_chip": "chip-gold",
        "detail": "sustainability_score FLOAT added",
        "severity": "Medium",
        "severity_chip": "chip-gold",
    },
    {
        "time": "6h ago",
        "db": "olist_ecommerce",
        "db_chip": "chip-neon",
        "table": "orders",
        "change": "Row Count",
        "change_chip": "chip-neon",
        "detail": "+1,200 new orders (above daily average)",
        "severity": "Info",
        "severity_chip": "chip-neon",
    },
    {
        "time": "1d ago",
        "db": "olist_ecommerce",
        "db_chip": "chip-neon",
        "table": "sellers",
        "change": "Stable",
        "change_chip": "chip-green",
        "detail": "No changes detected",
        "severity": "None",
        "severity_chip": "chip-green",
    },
    {
        "time": "2d ago",
        "db": "olist_ecommerce",
        "db_chip": "chip-neon",
        "table": "order_items",
        "change": "FK Added",
        "change_chip": "chip-violet",
        "detail": "seller_id → sellers.seller_id constraint added",
        "severity": "Medium",
        "severity_chip": "chip-gold",
    },
    {
        "time": "3d ago",
        "db": "olist_ecommerce",
        "db_chip": "chip-neon",
        "table": "customers",
        "change": "Index Added",
        "change_chip": "chip-neon",
        "detail": "idx_customer_state created for query optimization",
        "severity": "Info",
        "severity_chip": "chip-neon",
    },
]


@router.get("")
def get_watcher():
    """Return schema drift log and monitoring stats."""
    schema = get_schema()

    # Count real anomalies from schema
    high_null_cols = []
    for t in schema.values():
        for c in t["columns"]:
            if c["nullPctNum"] > 30:
                high_null_cols.append(f"{t['name']}.{c['name']}")

    active_alerts = len([d for d in DRIFT_LOG if d["severity"] in ("Critical", "Medium")])
    schema_changes = len([d for d in DRIFT_LOG if d["change"] not in ("Stable",)])

    return {
        "snapshots_stored": 14,
        "active_alerts": active_alerts,
        "schema_changes_24h": schema_changes,
        "tables_monitored": len(schema),
        "high_null_columns": high_null_cols[:5],
        "drift_log": DRIFT_LOG,
    }
