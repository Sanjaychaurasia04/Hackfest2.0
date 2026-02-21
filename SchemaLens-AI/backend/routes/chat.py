"""
routes/chat.py — AI Chat endpoint proxying to OpenAI GPT-4o
"""

import os
from fastapi import APIRouter
from pydantic import BaseModel
from typing import List, Optional
from openai import AsyncOpenAI
from backend.db import get_schema, AI_CONTEXT

router = APIRouter(prefix="/api/chat", tags=["chat"])

client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY", ""))


def build_schema_context(schema: dict) -> str:
    """Build a comprehensive schema context string for the AI."""
    lines = [
        "You are SchemaLens AI, an intelligent database schema assistant for the Olist Brazilian E-Commerce dataset.",
        "",
        "DATABASE: olist_ecommerce (SQLite/PostgreSQL-compatible)",
        "This is a real Brazilian e-commerce marketplace dataset with orders from 2016-2018.",
        "",
        "TABLES:",
    ]

    for name, t in schema.items():
        col_summaries = []
        for c in t["columns"]:
            flag_str = f" [{','.join(c['flags'])}]" if c["flags"] else ""
            col_summaries.append(f"  - {c['name']} ({c['type']}{flag_str}) null={c['nullPctNum']}% card={c['cardinality']}")

        lines.append(f"\n{name} ({t['rows']} rows, quality={t['quality']}%):")
        lines.extend(col_summaries[:12])  # limit to first 12 cols to save tokens

    lines += [
        "",
        "KEY RELATIONSHIPS:",
        "- orders.customer_id → customers.customer_id",
        "- order_items.order_id → orders.order_id",
        "- order_items.product_id → products.product_id",
        "- order_items.seller_id → sellers.seller_id",
        "- payments.order_id → orders.order_id",
        "- reviews.order_id → orders.order_id",
        "- products.product_category_name → category_translation.product_category_name",
        "",
        "IMPORTANT NOTES:",
        "- All monetary values are in BRL (Brazilian Real)",
        "- Timestamps are in Brazilian time (UTC-3)",
        "- reviews.review_comment_message has ~59% nulls (expected — optional field)",
        "- products.product_category_name is in Portuguese — JOIN category_translation for English",
        "- customers.customer_id is order-scoped; use customer_unique_id for true customer dedup",
        "- geolocation has multiple rows per zip prefix — use AVG(lat/lng) when joining",
        "",
        "WHEN GENERATING SQL:",
        "1. Write clean SQL (PostgreSQL/SQLite compatible)",
        "2. Use actual column and table names from above",
        "3. Add -- comments explaining logic",
        "4. After the SQL, provide numbered explanations of each clause",
        "5. Mention any schema caveats (null handling, currency, timezone, etc.)",
        "6. Format SQL in ```sql code blocks",
        "",
        "Keep responses concise but complete. Be the world's best data analyst assistant.",
    ]
    return "\n".join(lines)


class ChatMessage(BaseModel):
    role: str
    content: str


class ChatRequest(BaseModel):
    message: str
    history: Optional[List[ChatMessage]] = []


@router.post("")
async def chat(req: ChatRequest):
    """Proxy chat to GPT-4o with full schema context."""
    schema = get_schema()
    system_context = build_schema_context(schema)

    # Build messages
    messages = [{"role": "system", "content": system_context}]

    # Add history (last 8 turns to stay within token limits)
    for msg in (req.history or [])[-8:]:
        messages.append({"role": msg.role, "content": msg.content})

    # Add current message
    messages.append({"role": "user", "content": req.message})

    api_key = os.getenv("OPENAI_API_KEY", "")
    if not api_key or api_key.startswith("sk-placeholder"):
        # Return a demo response if no API key
        demo_reply = generate_demo_reply(req.message, schema)
        return {"reply": demo_reply, "model": "demo-mode"}

    try:
        response = await client.chat.completions.create(
            model="gpt-4o",
            max_tokens=1200,
            messages=messages,
        )
        reply = response.choices[0].message.content
        return {"reply": reply, "model": "gpt-4o"}
    except Exception as e:
        demo_reply = generate_demo_reply(req.message, schema)
        return {"reply": demo_reply, "model": "demo-fallback", "error": str(e)}


def generate_demo_reply(message: str, schema: dict) -> str:
    """Generate a relevant demo response when API key is unavailable."""
    msg_lower = message.lower()
    tables = list(schema.keys())
    total_rows = sum(t["rowsNum"] for t in schema.values())

    if any(w in msg_lower for w in ["revenue", "sales", "payment", "money", "brl"]):
        return """Great question! Here's the SQL to analyze revenue from the Olist dataset:

```sql
SELECT 
    DATE_TRUNC('month', o.order_purchase_timestamp) AS month,
    COUNT(DISTINCT o.order_id) AS total_orders,
    -- Sum all payment values for completed orders
    ROUND(SUM(p.payment_value), 2) AS gross_revenue_brl,
    ROUND(AVG(p.payment_value), 2) AS avg_order_value_brl
FROM orders o
JOIN payments p ON o.order_id = p.order_id
WHERE o.order_status = 'delivered'        -- Only count delivered orders
  AND o.order_purchase_timestamp IS NOT NULL
GROUP BY 1
ORDER BY 1;
```

**Line-by-line explanation:**
1. `DATE_TRUNC('month', ...)` — Groups by calendar month for time-series trending
2. `COUNT(DISTINCT order_id)` — Order count (DISTINCT because payments table can have multiple rows per order)
3. `SUM(payment_value)` — Total gross revenue in BRL
4. `WHERE status = 'delivered'` — Exclude cancelled/pending orders from revenue

**Caveats:** `payment_value` is in BRL (Brazilian Real). Payments table has one row per payment method — SUM gives true order total including installments."""

    elif any(w in msg_lower for w in ["customer", "user", "buyer", "ltv", "lifetime"]):
        return """Here's SQL for customer lifetime value analysis:

```sql
SELECT 
    c.customer_unique_id,
    c.customer_state,
    COUNT(DISTINCT o.order_id) AS total_orders,
    -- Sum all payments for this customer
    ROUND(SUM(p.payment_value), 2) AS lifetime_value_brl,
    MIN(o.order_purchase_timestamp) AS first_order_date,
    MAX(o.order_purchase_timestamp) AS last_order_date
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
JOIN payments p ON o.order_id = p.order_id
WHERE o.order_status = 'delivered'
GROUP BY c.customer_unique_id, c.customer_state
ORDER BY lifetime_value_brl DESC
LIMIT 100;
```

**Key caveat:** Use `customer_unique_id` (not `customer_id`) — the orders table uses order-scoped customer_id, so the same real customer can appear multiple times. `customer_unique_id` is the true de-duplicated identifier."""

    elif any(w in msg_lower for w in ["category", "product", "catalog"]):
        return """Here's how to analyze sales by product category (with English names):

```sql
SELECT 
    COALESCE(ct.product_category_name_english, p.product_category_name, 'Unknown') AS category,
    COUNT(DISTINCT oi.order_id) AS orders,
    ROUND(SUM(oi.price), 2) AS gross_revenue_brl,
    ROUND(AVG(oi.price), 2) AS avg_item_price,
    COUNT(DISTINCT oi.product_id) AS unique_products
FROM order_items oi
JOIN products p ON oi.product_id = p.product_id
LEFT JOIN category_translation ct 
    ON p.product_category_name = ct.product_category_name  -- Portuguese → English
JOIN orders o ON oi.order_id = o.order_id
WHERE o.order_status = 'delivered'
GROUP BY 1
ORDER BY gross_revenue_brl DESC;
```

**Schema note:** `product_category_name` in the products table is in **Portuguese**. The LEFT JOIN to `category_translation` maps it to English. `COALESCE` handles the ~0.3% of products with null categories."""

    elif any(w in msg_lower for w in ["null", "quality", "missing", "anomaly"]):
        # Find actual worst column
        worst_col = None
        worst_rate = 0
        for t in schema.values():
            for c in t["columns"]:
                if c["nullPctNum"] > worst_rate:
                    worst_rate = c["nullPctNum"]
                    worst_col = (t["name"], c["name"])

        return f"""Based on the real data analysis, here are the tables with highest null rates:

The worst column is **{worst_col[0] if worst_col else 'reviews'}.{worst_col[1] if worst_col else 'review_comment_message'}** with **{worst_rate:.1f}%** null rate.

```sql
-- Null rate audit across key columns
SELECT 
    'orders' AS table_name,
    'order_approved_at' AS column_name,
    ROUND(100.0 * SUM(CASE WHEN order_approved_at IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS null_pct
FROM orders
UNION ALL
SELECT 'reviews', 'review_comment_message',
    ROUND(100.0 * SUM(CASE WHEN review_comment_message IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2)
FROM reviews
UNION ALL
SELECT 'products', 'product_category_name',
    ROUND(100.0 * SUM(CASE WHEN product_category_name IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2)
FROM products
ORDER BY null_pct DESC;
```

**SchemaLens detected {len([c for t in schema.values() for c in t['columns'] if c['nullPctNum'] > 10])} columns with >10% null rate** using IQR anomaly detection. The `review_comment_message` high null rate is **expected** (optional field — most customers don't write text reviews)."""

    else:
        return f"""I have full context on **{len(tables)} tables** and **{total_rows:,} total rows** in the Olist Brazilian E-Commerce dataset.

Here's a quick schema overview:

| Table | Rows | Quality |
|-------|------|---------|
""" + "\n".join(f"| {t['name']} | {t['rows']} | {t['quality']}% |" for t in schema.values()) + """

Ask me to generate SQL, explain relationships, find data quality issues, or analyze any business question. For example:
- *"Show monthly revenue trend for 2018"*
- *"Which product categories have the highest average rating?"*
- *"Find customers who placed more than 3 orders"*"""
