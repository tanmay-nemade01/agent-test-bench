#!/bin/bash
set -e

# ===========================================================================
# STEP 1: Create DuckDB database with sample data
# ===========================================================================
mkdir -p /app/database
python3 << 'PYEOF'
import duckdb

conn = duckdb.connect('/app/database/retail.duckdb')

# Create schemas
conn.execute("CREATE SCHEMA IF NOT EXISTS finance")
conn.execute("CREATE SCHEMA IF NOT EXISTS procurement")
conn.execute("CREATE SCHEMA IF NOT EXISTS orders")

# --- Exchange Rates ---
conn.execute("""
CREATE TABLE IF NOT EXISTS finance.currency_exchange_rates (
    effective_date DATE,
    from_currency VARCHAR,
    to_currency VARCHAR,
    exchange_rate DOUBLE
)
""")
conn.execute("""
INSERT INTO finance.currency_exchange_rates VALUES
    ('2024-01-01', 'EUR', 'USD', 1.10),
    ('2024-01-15', 'EUR', 'USD', 1.11),
    ('2024-02-01', 'EUR', 'USD', 1.12),
    ('2024-03-01', 'EUR', 'USD', 1.08),
    ('2024-04-01', 'EUR', 'USD', 1.09),
    ('2024-01-01', 'GBP', 'USD', 1.27),
    ('2024-02-01', 'GBP', 'USD', 1.25),
    ('2024-03-01', 'GBP', 'USD', 1.30)
""")

# --- Purchase Orders ---
conn.execute("""
CREATE TABLE IF NOT EXISTS procurement.purchase_orders (
    po_id VARCHAR,
    currency_code VARCHAR,
    ordered_at TIMESTAMP
)
""")
conn.execute("""
INSERT INTO procurement.purchase_orders VALUES
    ('PO-001', 'EUR', '2024-01-15'),
    ('PO-002', 'USD', '2024-02-10'),
    ('PO-003', 'GBP', '2024-03-05'),
    ('PO-004', 'EUR', '2024-04-01')
""")

# --- Purchase Order Lines ---
conn.execute("""
CREATE TABLE IF NOT EXISTS procurement.purchase_order_lines (
    po_line_id VARCHAR,
    po_id VARCHAR,
    sku VARCHAR,
    quantity_received INTEGER,
    unit_price DOUBLE
)
""")
conn.execute("""
INSERT INTO procurement.purchase_order_lines VALUES
    ('POL-001', 'PO-001', 'SKU-A', 100, 10.00),
    ('POL-002', 'PO-001', 'SKU-B',  50, 20.00),
    ('POL-003', 'PO-002', 'SKU-A', 200, 12.00),
    ('POL-004', 'PO-002', 'SKU-C',  75, 30.00),
    ('POL-005', 'PO-003', 'SKU-A', 150,  8.00),
    ('POL-006', 'PO-003', 'SKU-B', 100, 15.00),
    ('POL-007', 'PO-004', 'SKU-A',  80, 11.00)
""")

# --- Sales Orders ---
conn.execute("""
CREATE TABLE IF NOT EXISTS orders.orders (
    order_id VARCHAR,
    ordered_at TIMESTAMP
)
""")
conn.execute("""
INSERT INTO orders.orders VALUES
    ('ORD-001', '2024-02-20'),
    ('ORD-002', '2024-03-15'),
    ('ORD-003', '2024-04-10')
""")

# --- Order Lines ---
conn.execute("""
CREATE TABLE IF NOT EXISTS orders.order_lines (
    order_line_id VARCHAR,
    order_id VARCHAR,
    sku VARCHAR,
    quantity_ordered INTEGER
)
""")
conn.execute("""
INSERT INTO orders.order_lines VALUES
    ('OL-001', 'ORD-001', 'SKU-A', 120),
    ('OL-002', 'ORD-001', 'SKU-B',  30),
    ('OL-003', 'ORD-002', 'SKU-A', 200),
    ('OL-004', 'ORD-002', 'SKU-C', 100),
    ('OL-005', 'ORD-003', 'SKU-A',  50)
""")

conn.close()
print("Database created with sample data.")
PYEOF

# ===========================================================================
# STEP 2: Set up dbt project
# ===========================================================================
mkdir -p /app/dbt_transforms
cd /app/dbt_transforms

cat <<EOF > profiles.yml
dbt_transforms:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: '/app/database/retail.duckdb'
      schema: 'main'
EOF

cat <<EOF > dbt_project.yml
name: 'dbt_transforms'
version: '1.0.0'
config-version: 2
profile: 'dbt_transforms'
model-paths: ["models"]
analysis-paths: ["analyses"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]
target-path: "target"
clean-targets: ["target", "dbt_packages"]
models:
  dbt_transforms:
    staging:
      +materialized: view
    intermediate:
      +materialized: view
    marts:
      +materialized: table
EOF

cat <<EOF > packages.yml
packages: []
EOF

# ===========================================================================
# STEP 3: Create source definitions and staging models
# ===========================================================================
mkdir -p models/staging models/intermediate models/marts

cat > models/staging/_sources.yml << 'EOF'
version: 2
sources:
  - name: finance
    schema: finance
    tables:
      - name: currency_exchange_rates
  - name: procurement
    schema: procurement
    tables:
      - name: purchase_orders
      - name: purchase_order_lines
  - name: orders
    schema: orders
    tables:
      - name: orders
      - name: order_lines
EOF

cat > models/staging/stg_finance__currency_exchange_rates.sql << 'EOF'
select
    effective_date,
    from_currency,
    to_currency,
    exchange_rate
from {{ source('finance', 'currency_exchange_rates') }}
EOF

cat > models/staging/stg_procurement__purchase_orders.sql << 'EOF'
select
    po_id,
    currency_code,
    ordered_at
from {{ source('procurement', 'purchase_orders') }}
EOF

cat > models/staging/stg_procurement__purchase_order_lines.sql << 'EOF'
select
    po_line_id,
    po_id,
    sku,
    quantity_received,
    unit_price
from {{ source('procurement', 'purchase_order_lines') }}
EOF

cat > models/staging/stg_orders__orders.sql << 'EOF'
select
    order_id,
    ordered_at
from {{ source('orders', 'orders') }}
EOF

cat > models/staging/stg_orders__order_lines.sql << 'EOF'
select
    order_line_id,
    order_id,
    sku,
    quantity_ordered
from {{ source('orders', 'order_lines') }}
EOF

# ===========================================================================
# STEP 4: Create intermediate models
# ===========================================================================

cat > models/intermediate/int_finance__exchange_rates_daily.sql << 'EOF'
{{ config(materialized='view') }}
select
    effective_date as rate_date,
    from_currency,
    to_currency,
    exchange_rate as rate_to_usd
from {{ ref('stg_finance__currency_exchange_rates') }}
where to_currency = 'USD'
EOF

cat > models/intermediate/int_procurement__purchases_enriched.sql << 'EOF'
{{ config(materialized='view') }}
select
    l.po_line_id as purchase_id,
    l.po_id,
    l.sku,
    l.quantity_received as quantity,
    l.unit_price as unit_cost,
    h.currency_code as currency,
    cast(h.ordered_at as date) as purchase_date
from {{ ref('stg_procurement__purchase_order_lines') }} l
inner join {{ ref('stg_procurement__purchase_orders') }} h
    on l.po_id = h.po_id
where l.sku is not null
  and l.quantity_received is not null
  and l.quantity_received > 0
  and l.unit_price is not null
  and l.unit_price > 0
EOF

cat > models/intermediate/int_orders__sales_enriched.sql << 'EOF'
{{ config(materialized='view') }}
select
    l.order_line_id as sale_id,
    l.order_id,
    l.sku,
    l.quantity_ordered as quantity_sold,
    cast(h.ordered_at as date) as sale_date
from {{ ref('stg_orders__order_lines') }} l
inner join {{ ref('stg_orders__orders') }} h
    on l.order_id = h.order_id
where l.sku is not null
  and l.quantity_ordered is not null
  and l.quantity_ordered > 0
EOF

# ===========================================================================
# STEP 5: Create mart models
# ===========================================================================

cat > models/marts/purchase_costs_usd.sql << 'EOF'
{{ config(materialized='table') }}

with purchases as (
    select * from {{ ref('int_procurement__purchases_enriched') }}
),
rates as (
    select * from {{ ref('int_finance__exchange_rates_daily') }}
),
purchase_with_rate as (
    select
        p.purchase_id, p.sku, p.quantity, p.currency, p.unit_cost, p.purchase_date,
        r.rate_to_usd as exchange_rate,
        row_number() over (partition by p.purchase_id order by r.rate_date desc) as rate_rank
    from purchases p
    left join rates r
        on p.currency = r.from_currency
        and r.rate_date <= p.purchase_date
    where p.currency = 'USD' or r.rate_to_usd is not null
),
final as (
    select
        purchase_id, sku,
        cast(quantity as integer) as quantity,
        currency as original_currency,
        unit_cost as original_unit_cost,
        purchase_date,
        case when currency = 'USD' then 1.000000 else round(exchange_rate, 6) end as exchange_rate,
        round(cast(unit_cost as double) * case when currency = 'USD' then 1.0 else exchange_rate end, 4) as unit_cost_usd,
        round(cast(unit_cost as double) * case when currency = 'USD' then 1.0 else exchange_rate end * quantity, 2) as total_cost_usd
    from purchase_with_rate
    where rate_rank = 1 or currency = 'USD'
)
select * from final order by purchase_date, purchase_id
EOF

cat > models/marts/sale_cogs.sql << 'EOF'
{{ config(materialized='table') }}

with purchase_costs as (
    select
        purchase_id, sku,
        cast(quantity as integer) as quantity,
        unit_cost_usd, total_cost_usd, purchase_date,
        coalesce(sum(quantity) over (partition by sku order by purchase_date desc, purchase_id desc rows between unbounded preceding and 1 preceding), 0) as cum_qty_start,
        sum(quantity) over (partition by sku order by purchase_date desc, purchase_id desc) as cum_qty_end
    from {{ ref('purchase_costs_usd') }}
),
sku_weighted_avg as (
    select sku, sum(total_cost_usd) / nullif(sum(quantity), 0) as weighted_avg_cost
    from {{ ref('purchase_costs_usd') }}
    group by sku
),
sales as (
    select
        sale_id, order_id, sku,
        cast(quantity_sold as integer) as quantity_sold,
        sale_date,
        coalesce(sum(quantity_sold) over (partition by sku order by sale_date, sale_id rows between unbounded preceding and 1 preceding), 0) as cum_sold_start,
        sum(quantity_sold) over (partition by sku order by sale_date, sale_id) as cum_sold_end
    from {{ ref('int_orders__sales_enriched') }}
),
matched as (
    select
        s.sale_id, s.order_id, s.sku, s.sale_date, s.quantity_sold,
        p.purchase_id, p.unit_cost_usd,
        case when p.purchase_id is not null then greatest(0, least(s.cum_sold_end, p.cum_qty_end) - greatest(s.cum_sold_start, p.cum_qty_start)) else 0 end as qty_from_batch
    from sales s
    left join purchase_costs p
        on s.sku = p.sku
        and p.cum_qty_start < s.cum_sold_end
        and p.cum_qty_end > s.cum_sold_start
),
sale_cogs_calc as (
    select
        sale_id, order_id, sku, sale_date, quantity_sold,
        coalesce(sum(case when qty_from_batch > 0 then qty_from_batch * unit_cost_usd else 0 end), 0) as cogs_usd,
        coalesce(count(distinct case when qty_from_batch > 0 then purchase_id end), 0) as batches_consumed,
        coalesce(sum(case when qty_from_batch > 0 then qty_from_batch else 0 end), 0) as qty_fulfilled
    from matched
    group by sale_id, order_id, sku, sale_date, quantity_sold
),
final as (
    select
        c.sale_id, c.order_id, c.sku, c.sale_date, c.quantity_sold,
        round(c.cogs_usd, 2) as cogs_usd,
        case when c.qty_fulfilled > 0 then round(c.cogs_usd / c.qty_fulfilled, 4) else 0.0000 end as avg_unit_cost,
        cast(c.batches_consumed as integer) as batches_consumed,
        cast(c.quantity_sold - c.qty_fulfilled as integer) as inventory_shortfall,
        round(case when c.quantity_sold - c.qty_fulfilled > 0 then (c.quantity_sold - c.qty_fulfilled) * coalesce(w.weighted_avg_cost, 0) else 0 end, 2) as fallback_cost_usd,
        round(c.cogs_usd + case when c.quantity_sold - c.qty_fulfilled > 0 then (c.quantity_sold - c.qty_fulfilled) * coalesce(w.weighted_avg_cost, 0) else 0 end, 2) as total_estimated_cogs,
        case when c.quantity_sold - c.qty_fulfilled = 0 then 'LIFO_FULL' when c.batches_consumed > 0 then 'LIFO_PARTIAL' else 'LIFO_NONE' end as costing_method
    from sale_cogs_calc c
    left join sku_weighted_avg w on c.sku = w.sku
)
select * from final order by sale_date, sale_id
EOF

cat > models/marts/inventory_turnover_metrics.sql << 'EOF'
{{ config(materialized='table') }}

with purchase_totals as (
    select sku, cast(sum(quantity) as integer) as total_purchased_qty, round(sum(total_cost_usd), 2) as total_purchased_cost_usd
    from {{ ref('purchase_costs_usd') }} group by sku
),
sale_totals as (
    select sku, cast(sum(quantity_sold) as integer) as total_sold_qty, round(sum(cogs_usd), 2) as total_cogs_usd, cast(sum(inventory_shortfall) as integer) as total_shortfall
    from {{ ref('sale_cogs') }} group by sku
),
remaining_inventory as (
    select coalesce(p.sku, s.sku) as sku,
        coalesce(p.total_purchased_qty, 0) - (coalesce(s.total_sold_qty, 0) - coalesce(s.total_shortfall, 0)) as remaining_qty
    from purchase_totals p full outer join sale_totals s on p.sku = s.sku
),
purchase_batches as (
    select sku, quantity, unit_cost_usd, purchase_date, purchase_id,
        sum(quantity) over (partition by sku order by purchase_date, purchase_id) as cum_qty
    from {{ ref('purchase_costs_usd') }}
),
remaining_cost_calc as (
    select r.sku, r.remaining_qty,
        coalesce(sum(case
            when r.remaining_qty <= 0 then 0
            when pb.cum_qty <= r.remaining_qty then pb.quantity * pb.unit_cost_usd
            when pb.cum_qty - pb.quantity < r.remaining_qty then (r.remaining_qty - (pb.cum_qty - pb.quantity)) * pb.unit_cost_usd
            else 0 end), 0) as remaining_cost
    from remaining_inventory r left join purchase_batches pb on r.sku = pb.sku
    group by r.sku, r.remaining_qty
),
fulfilled_totals as (
    select sku, sum(quantity_sold - inventory_shortfall) as total_fulfilled_qty
    from {{ ref('sale_cogs') }} group by sku
),
final as (
    select
        coalesce(p.sku, s.sku) as sku,
        coalesce(p.total_purchased_qty, 0) as total_purchased_qty,
        coalesce(p.total_purchased_cost_usd, 0.00) as total_purchased_cost_usd,
        coalesce(s.total_sold_qty, 0) as total_sold_qty,
        coalesce(s.total_cogs_usd, 0.00) as total_cogs_usd,
        cast(coalesce(ri.remaining_qty, 0) as integer) as remaining_inventory_qty,
        round(case when coalesce(ri.remaining_qty, 0) > 0 then coalesce(rc.remaining_cost, 0) else 0 end, 2) as remaining_inventory_cost_usd,
        round(case when coalesce(p.total_purchased_qty, 0) > 0 then p.total_purchased_cost_usd / p.total_purchased_qty else 0 end, 4) as weighted_avg_purchase_cost,
        round(case when coalesce(f.total_fulfilled_qty, 0) > 0 then s.total_cogs_usd / f.total_fulfilled_qty else 0 end, 4) as weighted_avg_sale_cost,
        round(case when coalesce(ri.remaining_qty, 0) > 0 and rc.remaining_cost > 0 then s.total_cogs_usd / (rc.remaining_cost / 2) else null end, 4) as inventory_turnover_ratio,
        cast(null as double) as gross_margin_pct
    from purchase_totals p
    full outer join sale_totals s on p.sku = s.sku
    left join remaining_inventory ri on coalesce(p.sku, s.sku) = ri.sku
    left join remaining_cost_calc rc on coalesce(p.sku, s.sku) = rc.sku
    left join fulfilled_totals f on coalesce(p.sku, s.sku) = f.sku
)
select * from final order by sku
EOF

# ===========================================================================
# STEP 6: Run dbt
# ===========================================================================
dbt deps
dbt run

echo "Solution complete!"
