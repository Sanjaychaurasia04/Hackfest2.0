"""
db.py — Load Olist CSVs into SQLite and compute real schema statistics.
"""

import os
import sqlite3
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, Any

ARCHIVE_DIR = Path(__file__).parent.parent / "archive"
DB_PATH = Path(__file__).parent / "schemalens.db"

# Friendly table name mapping (CSV filename → clean name)
TABLE_MAP = {
    "olist_orders_dataset": "orders",
    "olist_customers_dataset": "customers",
    "olist_order_items_dataset": "order_items",
    "olist_order_payments_dataset": "payments",
    "olist_order_reviews_dataset": "reviews",
    "olist_products_dataset": "products",
    "olist_sellers_dataset": "sellers",
    "olist_geolocation_dataset": "geolocation",
    "product_category_name_translation": "category_translation",
}

# AI-generated business context for each table
AI_CONTEXT = {
    "orders": (
        "The <strong>orders</strong> table is the central transaction record with "
        "<strong>99K+ purchases</strong>. Each row is a customer order, linking to "
        "<strong>customers</strong> via customer_id. Contains order_status, purchase timestamp, "
        "and estimated/actual delivery dates. Perfect for fulfillment SLA analysis, "
        "funnel metrics, and delivery performance tracking."
    ),
    "customers": (
        "The <strong>customers</strong> table is the customer master record. Contains "
        "geographic data (city, state, zip_code) and unique customer identifiers. "
        "Use <strong>customer_state</strong> for regional breakdowns and "
        "<strong>customer_unique_id</strong> for true customer-level (vs order-level) analysis. "
        "Growing dataset with customers across all 27 Brazilian states."
    ),
    "order_items": (
        "The <strong>order_items</strong> table is your revenue line record — each row is "
        "a product within an order. Contains price, freight_value, and shipping dates. "
        "JOIN to <strong>orders</strong> via order_id and <strong>products</strong> via product_id. "
        "Use SUM(price) for gross revenue, SUM(freight_value) for logistics cost analysis."
    ),
    "payments": (
        "The <strong>payments</strong> table is the financial ledger. Links to orders 1:N "
        "(one order can have split payments). Contains payment_type (credit_card, boleto, etc.), "
        "installments, and payment_value. High completeness — well-maintained data with "
        "minimal nulls across all columns."
    ),
    "reviews": (
        "The <strong>reviews</strong> table stores customer feedback post-purchase. "
        "review_score (1-5) distribution is bimodal (1★ and 5★ dominate — classic 'J-curve' pattern). "
        "review_comment_message has <strong>high null rate</strong> (expected — most customers don't write text). "
        "review_creation_date and answer_timestamp allow response-time SLA analysis."
    ),
    "products": (
        "The <strong>products</strong> table is the product catalog. "
        "product_category_name uses Portuguese — JOIN to <strong>category_translation</strong> "
        "for English names. product_photos_qty and measurement columns have elevated null rates (~16%). "
        "Use for category analysis, product mix reporting, and catalog health checks."
    ),
    "sellers": (
        "The <strong>sellers</strong> table contains marketplace seller data. "
        "All geographic columns (city, state, zip_code) are fully populated — excellent data quality. "
        "JOIN to <strong>order_items</strong> via seller_id for seller performance and GMV attribution. "
        "Useful for marketplace health dashboards and seller onboarding analytics."
    ),
    "geolocation": (
        "The <strong>geolocation</strong> table maps Brazilian zip codes to lat/lng coordinates. "
        "Extremely large table (~1M rows) with multiple lat/lng entries per zip prefix. "
        "Use for geographic distance calculations and delivery route analysis. "
        "Average across duplicates when joining to get representative coordinates per zip."
    ),
    "category_translation": (
        "The <strong>category_translation</strong> table maps Portuguese product category names "
        "to English equivalents. Small reference table (71 rows) — JOIN to "
        "<strong>products</strong> on product_category_name for English-language reporting. "
        "Fully complete with no null values."
    ),
}

# Type color mapping for frontend
TYPE_COLORS = {
    "object": "tt-text",
    "int64": "tt-int",
    "float64": "tt-num",
    "datetime64[ns]": "tt-ts",
    "bool": "tt-bool",
    "Int64": "tt-int",
}

_schema_cache: Dict[str, Any] = {}


def get_db_connection():
    return sqlite3.connect(str(DB_PATH))


def get_pandas_dtype_class(dtype_str: str) -> str:
    for key, val in TYPE_COLORS.items():
        if key in dtype_str:
            return val
    return "tt-text"


def detect_pk_fk(table_name: str, col_name: str) -> list:
    """Detect primary/foreign keys based on naming conventions."""
    flags = []
    # PK detection
    if col_name == f"{table_name}_id" or (col_name.endswith("_id") and col_name == f"{table_name[:-1]}_id"):
        flags.append("PK")
    # Specific table PKs
    pk_map = {
        "orders": "order_id",
        "customers": "customer_id",
        "order_items": None,  # composite key
        "payments": None,
        "reviews": "review_id",
        "products": "product_id",
        "sellers": "seller_id",
        "geolocation": None,
        "category_translation": None,
    }
    if pk_map.get(table_name) == col_name:
        if "PK" not in flags:
            flags.append("PK")

    # FK detection
    fk_rules = {
        "order_id": "orders",
        "customer_id": "customers",
        "product_id": "products",
        "seller_id": "sellers",
        "customer_unique_id": "customers",
    }
    if col_name in fk_rules and "PK" not in flags:
        flags.append(f"FK→{fk_rules[col_name]}")

    return flags


def generate_annotation(table_name: str, col_name: str, dtype_str: str, null_pct: float, cardinality: int) -> str:
    """Generate a business annotation for a column."""
    annotations = {
        # orders
        ("orders", "order_id"): "Unique order identifier (UUID). Primary key.",
        ("orders", "customer_id"): "Links to customers table for buyer details.",
        ("orders", "order_status"): "Enum: created, approved, invoiced, processing, shipped, delivered, unavailable, cancelled.",
        ("orders", "order_purchase_timestamp"): "When the customer placed the order (BRT timezone). Key for time-series analysis.",
        ("orders", "order_approved_at"): "Payment approval timestamp. Diff with purchase = approval latency.",
        ("orders", "order_delivered_carrier_date"): "When courier picked up the package from seller.",
        ("orders", "order_delivered_customer_date"): "Actual delivery date. Diff with estimate = delivery accuracy SLA.",
        ("orders", "order_estimated_delivery_date"): "Estimated delivery date shown to customer at checkout.",
        # customers
        ("customers", "customer_id"): "Order-level customer ID (not unique per person — use customer_unique_id for dedup).",
        ("customers", "customer_unique_id"): "True unique customer identifier. Use for LTV, cohort, and retention analysis.",
        ("customers", "customer_zip_code_prefix"): "First 5 digits of Brazilian postal code.",
        ("customers", "customer_city"): "City name. Note: may contain inconsistencies due to free-text entry.",
        ("customers", "customer_state"): "2-letter Brazilian state code (e.g. SP, RJ, MG).",
        # order_items
        ("order_items", "order_id"): "Links to orders table.",
        ("order_items", "order_item_id"): "Item sequence within order (1-based). Max value = number of items in order.",
        ("order_items", "product_id"): "Links to products catalog.",
        ("order_items", "seller_id"): "Links to sellers table — which seller fulfilled this item.",
        ("order_items", "shipping_limit_date"): "Deadline for seller to hand off to carrier.",
        ("order_items", "price"): "Item price in BRL. SUM for gross revenue. Use with freight for total order value.",
        ("order_items", "freight_value"): "Shipping cost in BRL. Charged to customer. Use for logistics margin analysis.",
        # payments
        ("payments", "order_id"): "Links to orders table. One order can have multiple payment rows (installments).",
        ("payments", "payment_sequential"): "Payment sequence within order (for split/installment payments).",
        ("payments", "payment_type"): "Enum: credit_card, boleto, voucher, debit_card. credit_card is dominant (~74%).",
        ("payments", "payment_installments"): "Number of credit card installments. Key metric for credit risk analysis.",
        ("payments", "payment_value"): "Payment amount in BRL. SUM per order to get total paid.",
        # reviews
        ("reviews", "review_id"): "Unique review identifier.",
        ("reviews", "order_id"): "Links to orders. Some orders have multiple reviews — use latest by creation date.",
        ("reviews", "review_score"): "Rating 1-5 stars. Bimodal distribution (1★ and 5★ dominate — J-curve pattern).",
        ("reviews", "review_comment_title"): "Short review title. High null rate is expected — optional field.",
        ("reviews", "review_comment_message"): "Full review text. High null rate (~59%) expected. Use for NLP/sentiment.",
        ("reviews", "review_creation_date"): "When the review survey was sent to the customer.",
        ("reviews", "review_answer_timestamp"): "When the customer submitted their review.",
        # products
        ("products", "product_id"): "Unique product identifier (UUID).",
        ("products", "product_category_name"): "Category in Portuguese. JOIN category_translation for English names.",
        ("products", "product_name_lenght"): "Character count of product name (note: intentional typo in source data).",
        ("products", "product_description_lenght"): "Character count of product description.",
        ("products", "product_photos_qty"): "Number of product photos. Null = no photo data available.",
        ("products", "product_weight_g"): "Weight in grams. Used by logistics for freight calculation.",
        ("products", "product_length_cm"): "Package length in cm.",
        ("products", "product_height_cm"): "Package height in cm.",
        ("products", "product_width_cm"): "Package width in cm.",
        # sellers
        ("sellers", "seller_id"): "Unique seller identifier (UUID).",
        ("sellers", "seller_zip_code_prefix"): "Seller location zip prefix. Use for logistics distance calc.",
        ("sellers", "seller_city"): "Seller city. Fully populated — no nulls.",
        ("sellers", "seller_state"): "Seller state (2-letter code). Fully populated.",
        # geolocation
        ("geolocation", "geolocation_zip_code_prefix"): "5-digit zip prefix. Multiple rows per zip — aggregate by AVG lat/lng.",
        ("geolocation", "geolocation_lat"): "Latitude. Multiple entries per zip — use AVG for centroid.",
        ("geolocation", "geolocation_lng"): "Longitude. Multiple entries per zip — use AVG for centroid.",
        ("geolocation", "geolocation_city"): "City name for this zip prefix.",
        ("geolocation", "geolocation_state"): "State code for this zip prefix.",
    }
    key = (table_name, col_name)
    if key in annotations:
        return annotations[key]

    # Generic fallback
    if null_pct > 30:
        return f"⚠ High null rate ({null_pct:.1f}%) — expected for optional field or data quality issue."
    if null_pct > 10:
        return f"Moderate null rate ({null_pct:.1f}%). Validate before using in core metrics."
    if "timestamp" in col_name or "date" in col_name:
        return f"Timestamp column. Parse as datetime for time-series analysis."
    if "_id" in col_name:
        return f"Identifier column. {cardinality:,} unique values."
    return f"{cardinality:,} unique values. {dtype_str} column."


def compute_quality_score(df: pd.DataFrame) -> float:
    """Compute overall quality score 0-100 based on completeness and consistency."""
    null_rate = df.isnull().mean().mean()
    completeness = (1 - null_rate) * 100

    # Penalty for very high null columns
    high_null_cols = (df.isnull().mean() > 0.5).sum()
    penalty = high_null_cols * 3

    score = max(0, min(100, completeness - penalty))
    return round(score, 1)


def load_all_csvs():
    """Load all Olist CSVs into SQLite and compute stats. Returns schema cache."""
    global _schema_cache
    if _schema_cache:
        return _schema_cache

    conn = get_db_connection()

    schema = {}
    for csv_stem, table_name in TABLE_MAP.items():
        csv_path = ARCHIVE_DIR / f"{csv_stem}.csv"
        if not csv_path.exists():
            print(f"⚠ CSV not found: {csv_path}")
            continue

        print(f"📊 Loading {table_name}...")
        try:
            # Load into pandas
            df = pd.read_csv(str(csv_path), low_memory=False)

            # Store in SQLite (chunked for large tables)
            df.to_sql(table_name, conn, if_exists="replace", index=False, chunksize=10000)

            # Compute stats
            row_count = len(df)
            col_count = len(df.columns)
            quality = compute_quality_score(df)

            # Per-column stats
            columns = []
            for col in df.columns:
                col_data = df[col]
                null_pct = col_data.isnull().mean() * 100
                cardinality = int(col_data.nunique())
                dtype_str = str(col_data.dtype)
                type_class = get_pandas_dtype_class(dtype_str)
                flags = detect_pk_fk(table_name, col)

                # Format null display
                null_display = f"⚠ {null_pct:.1f}%" if null_pct > 10 else f"{null_pct:.1f}%"

                annotation = generate_annotation(table_name, col, dtype_str, null_pct, cardinality)

                # Short type display
                type_display_map = {
                    "object": "VARCHAR",
                    "int64": "BIGINT",
                    "float64": "NUMERIC",
                    "datetime64[ns]": "TIMESTAMPTZ",
                    "bool": "BOOLEAN",
                }
                type_display = type_display_map.get(dtype_str, dtype_str.upper()[:12])

                columns.append({
                    "name": col,
                    "type": type_display,
                    "typeClass": type_class,
                    "flags": flags,
                    "nullPct": null_display,
                    "nullPctNum": round(null_pct, 2),
                    "cardinality": f"{cardinality:,}",
                    "cardinalityNum": cardinality,
                    "note": annotation,
                    "dtype": dtype_str,
                })

            # Determine status
            if quality >= 90:
                status = "Healthy"
            elif quality >= 75:
                status = "Good"
            elif quality >= 60:
                status = "Warning"
            else:
                status = "Alert"

            # Color map
            color_map = {
                "Healthy": "var(--acid)",
                "Good": "var(--neon)",
                "Warning": "var(--gold)",
                "Alert": "var(--rose)",
            }

            schema[table_name] = {
                "name": table_name,
                "rows": f"{row_count:,}",
                "rowsNum": row_count,
                "cols": col_count,
                "quality": quality,
                "status": status,
                "color": color_map[status],
                "db": "olist_ecommerce",
                "ai_context": AI_CONTEXT.get(table_name, "AI context being generated..."),
                "columns": columns,
            }
            print(f"  ✓ {table_name}: {row_count:,} rows, {col_count} cols, quality={quality}%")

        except Exception as e:
            print(f"  ✗ Error loading {table_name}: {e}")

    conn.close()
    _schema_cache = schema
    print(f"\n✅ Loaded {len(schema)} tables into SQLite")
    return schema


def get_schema():
    return _schema_cache
