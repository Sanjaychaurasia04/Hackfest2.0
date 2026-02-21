"""
routes/export.py — Export catalog as JSON or Markdown
"""

import json
from datetime import datetime
from fastapi import APIRouter
from fastapi.responses import Response
from backend.db import get_schema

router = APIRouter(prefix="/api/export", tags=["export"])


@router.get("/json")
def export_json():
    """Export full schema catalog as JSON."""
    schema = get_schema()
    catalog = {
        "generated": datetime.utcnow().isoformat() + "Z",
        "generator": "SchemaLens AI v1.0",
        "dataset": "Olist Brazilian E-Commerce",
        "database": "olist_ecommerce",
        "summary": {
            "total_tables": len(schema),
            "total_columns": sum(t["cols"] for t in schema.values()),
            "total_rows": sum(t["rowsNum"] for t in schema.values()),
            "avg_quality_score": round(
                sum(t["quality"] for t in schema.values()) / len(schema), 1
            ) if schema else 0,
        },
        "tables": [],
    }

    for name, t in schema.items():
        catalog["tables"].append({
            "name": name,
            "database": t["db"],
            "rows": t["rows"],
            "columns_count": t["cols"],
            "quality_score": t["quality"],
            "status": t["status"],
            "ai_context": t["ai_context"].replace("<strong>", "**").replace("</strong>", "**"),
            "columns": [
                {
                    "name": c["name"],
                    "type": c["type"],
                    "flags": c["flags"],
                    "null_pct": c["nullPctNum"],
                    "cardinality": c["cardinalityNum"],
                    "annotation": c["note"],
                }
                for c in t["columns"]
            ],
        })

    json_str = json.dumps(catalog, indent=2)
    return Response(
        content=json_str,
        media_type="application/json",
        headers={
            "Content-Disposition": "attachment; filename=schemalens-catalog.json"
        },
    )


@router.get("/markdown")
def export_markdown():
    """Export full schema catalog as Markdown."""
    schema = get_schema()
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    total_rows = sum(t["rowsNum"] for t in schema.values())
    avg_quality = round(sum(t["quality"] for t in schema.values()) / len(schema), 1) if schema else 0

    lines = [
        "# SchemaLens AI — Data Catalog",
        f"_Generated: {now} by SchemaLens AI_",
        "",
        "## Dataset: Olist Brazilian E-Commerce",
        "",
        "## Summary",
        f"- **Total Tables:** {len(schema)}",
        f"- **Total Rows:** {total_rows:,}",
        f"- **Total Columns:** {sum(t['cols'] for t in schema.values())}",
        f"- **Avg Quality Score:** {avg_quality}%",
        "",
        "## Tables",
        "",
    ]

    for name, t in schema.items():
        # Strip HTML from ai_context
        import re
        ctx = re.sub(r"<[^>]+>", "", t["ai_context"])

        lines += [
            f"### {name}",
            f"**Rows:** {t['rows']}  |  **Columns:** {t['cols']}  |  **Quality:** {t['quality']}%  |  **Status:** {t['status']}",
            "",
            f"> {ctx}",
            "",
            "| Column | Type | Flags | Null% | Cardinality | Annotation |",
            "|--------|------|-------|-------|-------------|------------|",
        ]

        for c in t["columns"]:
            flags_str = ", ".join(c["flags"]) if c["flags"] else "—"
            note = c["note"].replace("|", "\\|")  # escape pipes in markdown tables
            lines.append(
                f"| {c['name']} | {c['type']} | {flags_str} | {c['nullPctNum']:.1f}% | {c['cardinality']} | {note} |"
            )

        lines += ["", "---", ""]

    md_content = "\n".join(lines)
    return Response(
        content=md_content,
        media_type="text/markdown",
        headers={
            "Content-Disposition": "attachment; filename=schemalens-catalog.md"
        },
    )
