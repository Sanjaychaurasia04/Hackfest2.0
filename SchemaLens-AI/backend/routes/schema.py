"""
routes/schema.py — Schema metadata endpoints
"""

from fastapi import APIRouter
from backend.db import get_schema

router = APIRouter(prefix="/api/schema", tags=["schema"])


@router.get("")
def list_tables():
    """Return all tables with summary stats."""
    schema = get_schema()
    result = []
    for name, t in schema.items():
        result.append({
            "name": name,
            "rows": t["rows"],
            "rowsNum": t["rowsNum"],
            "cols": t["cols"],
            "quality": t["quality"],
            "status": t["status"],
            "color": t["color"],
            "db": t["db"],
            "ai_context": t["ai_context"],
        })
    # Sort by quality desc
    result.sort(key=lambda x: x["quality"], reverse=True)
    return {"tables": result, "total": len(result)}


@router.get("/{table_name}")
def get_table(table_name: str):
    """Return detailed column info for a specific table."""
    schema = get_schema()
    if table_name not in schema:
        return {"error": f"Table '{table_name}' not found"}

    t = schema[table_name]
    return {
        "name": table_name,
        "rows": t["rows"],
        "rowsNum": t["rowsNum"],
        "cols": t["cols"],
        "quality": t["quality"],
        "status": t["status"],
        "color": t["color"],
        "db": t["db"],
        "ai_context": t["ai_context"],
        "columns": t["columns"],
    }
