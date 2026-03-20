"""
Simplified test verifier for Multi-Currency LIFO Inventory Costing task.
Validates table existence, schema, data quality, and basic business rules
using the self-contained sample database.
"""
import duckdb
import os

DATABASE_PATH = "/app/database/retail.duckdb"
DBT_PROJECT_PATH = "/app/dbt_transforms"


def require(condition, msg):
    if not condition:
        raise AssertionError(msg)


# ---------------------------------------------------------------------------
# Schema validation
# ---------------------------------------------------------------------------

def test_tables_exist():
    conn = duckdb.connect(DATABASE_PATH, read_only=True)
    try:
        for table in ['purchase_costs_usd', 'sale_cogs', 'inventory_turnover_metrics']:
            count = conn.execute(f"""
                SELECT COUNT(*) FROM information_schema.tables
                WHERE table_schema = 'main' AND LOWER(table_name) = '{table}'
            """).fetchone()[0]
            require(count == 1, f"Table '{table}' does not exist")
        print("OK tables exist")
    finally:
        conn.close()


def test_purchase_costs_schema():
    conn = duckdb.connect(DATABASE_PATH, read_only=True)
    try:
        cols = {c[0].lower() for c in conn.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema='main' AND LOWER(table_name)='purchase_costs_usd'
        """).fetchall()}
        required = {'purchase_id','sku','quantity','original_currency',
                     'original_unit_cost','purchase_date','exchange_rate',
                     'unit_cost_usd','total_cost_usd'}
        missing = required - cols
        require(not missing, f"purchase_costs_usd missing columns: {missing}")
    finally:
        conn.close()


def test_sale_cogs_schema():
    conn = duckdb.connect(DATABASE_PATH, read_only=True)
    try:
        cols = {c[0].lower() for c in conn.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema='main' AND LOWER(table_name)='sale_cogs'
        """).fetchall()}
        required = {'sale_id','order_id','sku','sale_date','quantity_sold',
                     'cogs_usd','avg_unit_cost','batches_consumed',
                     'inventory_shortfall','fallback_cost_usd',
                     'total_estimated_cogs','costing_method'}
        missing = required - cols
        require(not missing, f"sale_cogs missing columns: {missing}")
    finally:
        conn.close()


def test_inventory_turnover_schema():
    conn = duckdb.connect(DATABASE_PATH, read_only=True)
    try:
        cols = {c[0].lower() for c in conn.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema='main' AND LOWER(table_name)='inventory_turnover_metrics'
        """).fetchall()}
        required = {'sku','total_purchased_qty','total_purchased_cost_usd',
                     'total_sold_qty','total_cogs_usd','remaining_inventory_qty',
                     'remaining_inventory_cost_usd','weighted_avg_purchase_cost',
                     'weighted_avg_sale_cost','inventory_turnover_ratio','gross_margin_pct'}
        missing = required - cols
        require(not missing, f"inventory_turnover_metrics missing columns: {missing}")
    finally:
        conn.close()


def test_intermediate_models_exist():
    conn = duckdb.connect(DATABASE_PATH, read_only=True)
    try:
        for model in ['int_finance__exchange_rates_daily',
                       'int_procurement__purchases_enriched',
                       'int_orders__sales_enriched']:
            count = conn.execute(f"""
                SELECT COUNT(*) FROM information_schema.tables
                WHERE table_schema='main' AND LOWER(table_name)='{model}'
            """).fetchone()[0]
            require(count >= 1, f"Intermediate model '{model}' not found")
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Data quality
# ---------------------------------------------------------------------------

def test_purchase_costs_row_count():
    conn = duckdb.connect(DATABASE_PATH, read_only=True)
    try:
        count = conn.execute("SELECT COUNT(*) FROM main.purchase_costs_usd").fetchone()[0]
        require(count == 7, f"Expected 7 purchase rows, got {count}")
    finally:
        conn.close()


def test_sale_cogs_row_count():
    conn = duckdb.connect(DATABASE_PATH, read_only=True)
    try:
        count = conn.execute("SELECT COUNT(*) FROM main.sale_cogs").fetchone()[0]
        require(count == 5, f"Expected 5 sale rows, got {count}")
    finally:
        conn.close()


def test_no_null_required_fields():
    conn = duckdb.connect(DATABASE_PATH, read_only=True)
    try:
        n = conn.execute("""
            SELECT COUNT(*) FROM main.purchase_costs_usd
            WHERE purchase_id IS NULL OR sku IS NULL OR quantity IS NULL
               OR exchange_rate IS NULL OR unit_cost_usd IS NULL OR total_cost_usd IS NULL
        """).fetchone()[0]
        require(n == 0, f"purchase_costs_usd has {n} rows with NULLs")

        n = conn.execute("""
            SELECT COUNT(*) FROM main.sale_cogs
            WHERE sale_id IS NULL OR sku IS NULL OR quantity_sold IS NULL
               OR cogs_usd IS NULL OR costing_method IS NULL
        """).fetchone()[0]
        require(n == 0, f"sale_cogs has {n} rows with NULLs")
    finally:
        conn.close()


def test_positive_quantities():
    conn = duckdb.connect(DATABASE_PATH, read_only=True)
    try:
        n = conn.execute("SELECT COUNT(*) FROM main.purchase_costs_usd WHERE quantity <= 0").fetchone()[0]
        require(n == 0, "purchase_costs_usd has non-positive quantities")
        n = conn.execute("SELECT COUNT(*) FROM main.sale_cogs WHERE quantity_sold <= 0").fetchone()[0]
        require(n == 0, "sale_cogs has non-positive quantities")
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Business rules
# ---------------------------------------------------------------------------

def test_usd_exchange_rate_is_one():
    conn = duckdb.connect(DATABASE_PATH, read_only=True)
    try:
        violations = conn.execute("""
            SELECT COUNT(*) FROM main.purchase_costs_usd
            WHERE original_currency = 'USD' AND ABS(exchange_rate - 1.0) > 0.000001
        """).fetchone()[0]
        require(violations == 0, "USD purchases should have exchange_rate = 1.0")
    finally:
        conn.close()


def test_cost_math():
    conn = duckdb.connect(DATABASE_PATH, read_only=True)
    try:
        violations = conn.execute("""
            SELECT COUNT(*) FROM main.purchase_costs_usd
            WHERE ABS(unit_cost_usd - ROUND(original_unit_cost * exchange_rate, 4)) > 0.0001
               OR ABS(total_cost_usd - ROUND(unit_cost_usd * quantity, 2)) > 0.01
        """).fetchone()[0]
        require(violations == 0, "Cost calculation mismatch detected")
    finally:
        conn.close()


def test_costing_method_values():
    conn = duckdb.connect(DATABASE_PATH, read_only=True)
    try:
        invalid = conn.execute("""
            SELECT COUNT(*) FROM main.sale_cogs
            WHERE costing_method NOT IN ('LIFO_FULL', 'LIFO_PARTIAL', 'LIFO_NONE')
        """).fetchone()[0]
        require(invalid == 0, "Invalid costing_method values found")
    finally:
        conn.close()


def test_costing_method_classification():
    conn = duckdb.connect(DATABASE_PATH, read_only=True)
    try:
        # LIFO_FULL => shortfall = 0
        v = conn.execute("""
            SELECT COUNT(*) FROM main.sale_cogs
            WHERE costing_method = 'LIFO_FULL' AND inventory_shortfall != 0
        """).fetchone()[0]
        require(v == 0, "LIFO_FULL should have shortfall=0")

        # LIFO_PARTIAL => shortfall > 0 AND batches > 0
        v = conn.execute("""
            SELECT COUNT(*) FROM main.sale_cogs
            WHERE costing_method = 'LIFO_PARTIAL'
              AND (inventory_shortfall <= 0 OR batches_consumed <= 0)
        """).fetchone()[0]
        require(v == 0, "LIFO_PARTIAL classification error")

        # LIFO_NONE => batches = 0
        v = conn.execute("""
            SELECT COUNT(*) FROM main.sale_cogs
            WHERE costing_method = 'LIFO_NONE' AND batches_consumed != 0
        """).fetchone()[0]
        require(v == 0, "LIFO_NONE should have batches_consumed=0")
    finally:
        conn.close()


def test_total_estimated_cogs_formula():
    conn = duckdb.connect(DATABASE_PATH, read_only=True)
    try:
        v = conn.execute("""
            SELECT COUNT(*) FROM main.sale_cogs
            WHERE ABS(total_estimated_cogs - (cogs_usd + fallback_cost_usd)) > 0.01
        """).fetchone()[0]
        require(v == 0, "total_estimated_cogs != cogs_usd + fallback_cost_usd")
    finally:
        conn.close()


def test_shortfall_exists_for_sku_c():
    """SKU-C: purchased 75, sold 100 => shortfall of 25."""
    conn = duckdb.connect(DATABASE_PATH, read_only=True)
    try:
        row = conn.execute("""
            SELECT inventory_shortfall, costing_method, fallback_cost_usd
            FROM main.sale_cogs WHERE sku = 'SKU-C'
        """).fetchone()
        require(row is not None, "No sale_cogs row for SKU-C")
        require(int(row[0]) == 25, f"SKU-C shortfall should be 25, got {row[0]}")
        require(row[1] == 'LIFO_PARTIAL', f"SKU-C should be LIFO_PARTIAL, got {row[1]}")
        require(float(row[2]) > 0, "SKU-C fallback_cost_usd should be > 0")
    finally:
        conn.close()


def test_inventory_turnover_row_count():
    conn = duckdb.connect(DATABASE_PATH, read_only=True)
    try:
        count = conn.execute("SELECT COUNT(*) FROM main.inventory_turnover_metrics").fetchone()[0]
        require(count == 3, f"Expected 3 SKUs in turnover metrics, got {count}")
    finally:
        conn.close()


def test_inventory_balance():
    """consumed quantity should never exceed purchased quantity."""
    conn = duckdb.connect(DATABASE_PATH, read_only=True)
    try:
        violations = conn.execute("""
            WITH p AS (SELECT sku, SUM(quantity) AS total_purchased FROM main.purchase_costs_usd GROUP BY sku),
                 s AS (SELECT sku, SUM(quantity_sold - inventory_shortfall) AS total_consumed FROM main.sale_cogs GROUP BY sku)
            SELECT COUNT(*) FROM p JOIN s ON p.sku = s.sku WHERE s.total_consumed > p.total_purchased
        """).fetchone()[0]
        require(violations == 0, "Consumed > purchased for some SKU")
    finally:
        conn.close()
