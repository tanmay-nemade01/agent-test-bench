#!/bin/bash
set -e

# ===========================================================================
# STEP 1: Create DuckDB database with sample data
# ===========================================================================
mkdir -p /app/database
python3 << 'PYEOF'
import duckdb

conn = duckdb.connect('/app/database/retail.duckdb')

conn.execute("CREATE SCHEMA IF NOT EXISTS analytics")
conn.execute("CREATE SCHEMA IF NOT EXISTS customer")

# --- dim_date ---
conn.execute("""
CREATE TABLE IF NOT EXISTS analytics.dim_date (
    date_key INTEGER, full_date DATE, year INTEGER, month_number INTEGER, day_of_month INTEGER
)
""")
conn.execute("""
INSERT INTO analytics.dim_date VALUES
    (20240115, '2024-01-15', 2024, 1, 15),
    (20240120, '2024-01-20', 2024, 1, 20),
    (20240210, '2024-02-10', 2024, 2, 10),
    (20240215, '2024-02-15', 2024, 2, 15),
    (20240310, '2024-03-10', 2024, 3, 10),
    (20240315, '2024-03-15', 2024, 3, 15),
    (20240410, '2024-04-10', 2024, 4, 10),
    (20240520, '2024-05-20', 2024, 5, 20),
    (20240615, '2024-06-15', 2024, 6, 15),
    (20240720, '2024-07-20', 2024, 7, 20)
""")

# --- dim_customer ---
conn.execute("""
CREATE TABLE IF NOT EXISTS analytics.dim_customer (
    customer_key INTEGER, customer_id VARCHAR, segment_name VARCHAR, city VARCHAR, state VARCHAR, country VARCHAR
)
""")
conn.execute("""
INSERT INTO analytics.dim_customer VALUES
    (1, 'CUST-001', 'Premium',  'New York', 'NY', 'US'),
    (2, 'CUST-002', 'Standard', 'Chicago',  'IL', 'US'),
    (3, 'CUST-003', 'Premium',  'Houston',  'TX', 'US'),
    (4, 'CUST-004', 'Standard', 'Phoenix',  'AZ', 'US'),
    (5, 'CUST-005', 'Basic',    'Denver',   'CO', 'US')
""")

# --- customers ---
conn.execute("""
CREATE TABLE IF NOT EXISTS customer.customers (
    customer_id VARCHAR, acquisition_date DATE, customer_segment_snapshot VARCHAR, legacy_region_code VARCHAR
)
""")
conn.execute("""
INSERT INTO customer.customers VALUES
    ('CUST-001', '2024-01-01', 'Premium',  'NORTH'),
    ('CUST-002', '2024-01-10', 'Standard', 'SOUTH'),
    ('CUST-003', '2024-02-01', 'Premium',  'EAST'),
    ('CUST-004', '2024-02-15', 'Standard', 'WEST'),
    ('CUST-005', '2024-03-01', 'Basic',    'NORTH')
""")

# --- fact_sales ---
conn.execute("""
CREATE TABLE IF NOT EXISTS analytics.fact_sales (
    sale_key INTEGER, date_key INTEGER, customer_key INTEGER,
    total_amount DOUBLE, order_id VARCHAR, order_line_id VARCHAR
)
""")
conn.execute("""
INSERT INTO analytics.fact_sales VALUES
    ( 1, 20240115, 1, 150.00, 'O-001', 'OL-001'),
    ( 2, 20240210, 1, 200.00, 'O-002', 'OL-002'),
    ( 3, 20240210, 2, 100.00, 'O-003', 'OL-003'),
    ( 4, 20240315, 1, 175.00, 'O-004', 'OL-004'),
    ( 5, 20240315, 3, 250.00, 'O-005', 'OL-005'),
    ( 6, 20240410, 2, 120.00, 'O-006', 'OL-006'),
    ( 7, 20240410, 4, 300.00, 'O-007', 'OL-007'),
    ( 8, 20240520, 1, 180.00, 'O-008', 'OL-008'),
    ( 9, 20240520, 3, 220.00, 'O-009', 'OL-009'),
    (10, 20240615, 5,  50.00, 'O-010', 'OL-010'),
    (11, 20240720, 2,  90.00, 'O-011', 'OL-011'),
    (12, 20240120, 2,  80.00, 'O-012', 'OL-012')
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
      +materialized: view
EOF

cat <<EOF > packages.yml
packages: []
EOF

# ===========================================================================
# STEP 3: Source definitions and staging models
# ===========================================================================
mkdir -p models/staging models/intermediate models/marts

cat > models/staging/_sources.yml << 'EOF'
version: 2
sources:
  - name: analytics
    schema: analytics
    tables:
      - name: fact_sales
      - name: dim_customer
      - name: dim_date
  - name: customer
    schema: customer
    tables:
      - name: customers
EOF

cat > models/staging/stg_analytics__fact_sales.sql << 'EOF'
select sale_key, date_key, customer_key, total_amount, order_id, order_line_id
from {{ source('analytics', 'fact_sales') }}
EOF

cat > models/staging/stg_analytics__dim_customer.sql << 'EOF'
select customer_key, customer_id, segment_name, city, state, country
from {{ source('analytics', 'dim_customer') }}
EOF

cat > models/staging/stg_analytics__dim_date.sql << 'EOF'
select date_key, full_date, year, month_number, day_of_month
from {{ source('analytics', 'dim_date') }}
EOF

cat > models/staging/stg_customer__customers.sql << 'EOF'
select customer_id, acquisition_date, customer_segment_snapshot, legacy_region_code
from {{ source('customer', 'customers') }}
EOF

# ===========================================================================
# STEP 4: Intermediate models
# ===========================================================================

cat > models/intermediate/int_analytics__fact_sales.sql << 'EOF'
{{ config(materialized='view') }}
with sales as (
    select * from {{ ref('stg_analytics__fact_sales') }}
),
customers as (
    select * from {{ ref('stg_analytics__dim_customer') }}
),
dates as (
    select * from {{ ref('stg_analytics__dim_date') }}
)
select
    sales.order_id as transaction_id,
    customers.customer_id as customer_id,
    dates.full_date as transaction_date,
    sales.total_amount as amount,
    cast(null as varchar) as product_category
from sales
left join customers on sales.customer_key = customers.customer_key
left join dates on sales.date_key = dates.date_key
EOF

cat > models/intermediate/int_customer__customers.sql << 'EOF'
{{ config(materialized='view') }}
select
    customer_id,
    acquisition_date as signup_date,
    customer_segment_snapshot as customer_segment,
    legacy_region_code as region
from {{ ref('stg_customer__customers') }}
EOF

# ===========================================================================
# STEP 5: Mart models
# ===========================================================================

cat > models/marts/customer_cohorts.sql << 'INNEREOF'
{{ config(materialized='view') }}

with first_transaction as (
    select
        customer_id,
        min(transaction_date) as first_transaction_date,
        min(transaction_id) as first_txn_id
    from {{ ref('int_analytics__fact_sales') }}
    group by customer_id
),
first_transaction_details as (
    select
        t.customer_id,
        t.transaction_date as first_transaction_date,
        t.amount as first_transaction_amount
    from {{ ref('int_analytics__fact_sales') }} t
    inner join first_transaction ft
        on t.customer_id = ft.customer_id
        and t.transaction_date = ft.first_transaction_date
        and t.transaction_id = ft.first_txn_id
),
customer_info as (
    select customer_id, signup_date, region
    from {{ ref('int_customer__customers') }}
)
select
    ftd.customer_id,
    strftime(ftd.first_transaction_date, '%Y-%m') as cohort_month,
    ftd.first_transaction_date,
    ftd.first_transaction_amount,
    cast(case when ci.signup_date is null then 0
         else greatest(0, cast(ftd.first_transaction_date - ci.signup_date as integer))
    end as integer) as signup_to_first_purchase_days,
    case ci.region
        when 'NORTH' then 'Online'
        when 'SOUTH' then 'Retail'
        when 'EAST' then 'Partner'
        when 'WEST' then 'Direct'
        else 'Unknown'
    end as acquisition_channel
from first_transaction_details ftd
left join customer_info ci on ftd.customer_id = ci.customer_id
order by ftd.customer_id
INNEREOF

cat > models/marts/cohort_retention.sql << 'INNEREOF'
{{ config(materialized='view') }}

with customer_cohorts as (
    select
        customer_id,
        strftime(min(transaction_date), '%Y-%m') as cohort_month,
        min(transaction_date) as first_txn_date
    from {{ ref('int_analytics__fact_sales') }}
    group by customer_id
),
monthly_activity as (
    select distinct
        t.customer_id, cc.cohort_month, cc.first_txn_date,
        strftime(t.transaction_date, '%Y-%m') as activity_month,
        cast((extract(year from t.transaction_date) - extract(year from cc.first_txn_date)) * 12 +
             (extract(month from t.transaction_date) - extract(month from cc.first_txn_date)) as integer) as period_number
    from {{ ref('int_analytics__fact_sales') }} t
    inner join customer_cohorts cc on t.customer_id = cc.customer_id
),
cohort_sizes as (
    select cohort_month, count(distinct customer_id) as cohort_size
    from customer_cohorts group by cohort_month
),
period_activity as (
    select cohort_month, period_number, activity_month as period_month,
           count(distinct customer_id) as active_customers
    from monthly_activity group by cohort_month, period_number, activity_month
),
retained_calc as (
    select m1.cohort_month, m1.period_number, count(distinct m1.customer_id) as retained_customers
    from monthly_activity m1
    inner join monthly_activity m2
        on m1.customer_id = m2.customer_id
        and m1.cohort_month = m2.cohort_month
        and m1.period_number = m2.period_number + 1
    group by m1.cohort_month, m1.period_number
),
revenue_by_period as (
    select cc.cohort_month,
        cast((extract(year from t.transaction_date) - extract(year from cc.first_txn_date)) * 12 +
             (extract(month from t.transaction_date) - extract(month from cc.first_txn_date)) as integer) as period_number,
        sum(t.amount) as period_revenue
    from {{ ref('int_analytics__fact_sales') }} t
    inner join customer_cohorts cc on t.customer_id = cc.customer_id
    group by cc.cohort_month, period_number
),
combined as (
    select
        pa.cohort_month,
        cast(pa.period_number as integer) as period_number,
        pa.period_month, cs.cohort_size, pa.active_customers,
        case when pa.period_number = 0 then pa.active_customers else coalesce(rc.retained_customers, 0) end as retained_customers,
        round(cast(pa.active_customers as double) / cast(cs.cohort_size as double), 4) as retention_rate,
        coalesce(r.period_revenue, 0) as period_revenue
    from period_activity pa
    inner join cohort_sizes cs on pa.cohort_month = cs.cohort_month
    left join retained_calc rc on pa.cohort_month = rc.cohort_month and pa.period_number = rc.period_number
    left join revenue_by_period r on pa.cohort_month = r.cohort_month and pa.period_number = r.period_number
),
final as (
    select cohort_month, period_number, period_month, cohort_size,
           active_customers, retained_customers, retention_rate, period_revenue,
           sum(period_revenue) over (partition by cohort_month order by period_number rows between unbounded preceding and current row) as cumulative_revenue
    from combined
)
select * from final order by cohort_month, period_number
INNEREOF

cat > models/marts/customer_clv.sql << 'INNEREOF'
{{ config(materialized='view') }}

{% set ref_date = var('reference_date', '2024-12-31') %}

with customer_stats as (
    select
        customer_id,
        strftime(min(transaction_date), '%Y-%m') as cohort_month,
        count(*) as total_transactions,
        sum(amount) as total_revenue,
        round(avg(amount), 2) as avg_transaction_value,
        min(transaction_date) as first_txn,
        max(transaction_date) as last_txn
    from {{ ref('int_analytics__fact_sales') }}
    group by customer_id
),
lifespan_calc as (
    select *,
        cast(greatest(1,
            (extract(year from last_txn) - extract(year from first_txn)) * 12 +
            (extract(month from last_txn) - extract(month from first_txn)) + 1
        ) as integer) as customer_lifespan_months,
        cast(
            (extract(year from cast('{{ ref_date }}' as date)) - extract(year from last_txn)) * 12 +
            (extract(month from cast('{{ ref_date }}' as date)) - extract(month from last_txn))
        as integer) as months_since_last_transaction
    from customer_stats
),
churn_factors as (
    select *,
        round(total_revenue / customer_lifespan_months, 2) as monthly_revenue_rate,
        cast(months_since_last_transaction as double) / 12.0 as base_score,
        case when total_transactions >= 10 then 0.7 when total_transactions >= 5 then 0.85 when total_transactions >= 2 then 1.0 else 1.3 end as frequency_factor,
        case when months_since_last_transaction <= 1 then 0.5 when months_since_last_transaction <= 3 then 0.8 when months_since_last_transaction <= 6 then 1.0 else 1.2 end as recency_factor
    from lifespan_calc
),
clv_calc as (
    select customer_id, cohort_month, total_transactions, total_revenue,
        avg_transaction_value, customer_lifespan_months, monthly_revenue_rate,
        months_since_last_transaction,
        round(least(1.0, base_score * frequency_factor * recency_factor), 2) as churn_probability
    from churn_factors
)
select
    customer_id, cohort_month, total_transactions, total_revenue,
    avg_transaction_value, customer_lifespan_months, monthly_revenue_rate,
    months_since_last_transaction, churn_probability,
    round(monthly_revenue_rate * 12 * (1 - churn_probability), 2) as predicted_clv_12m,
    case
        when monthly_revenue_rate * 12 * (1 - churn_probability) >= 1000 then 'Platinum'
        when monthly_revenue_rate * 12 * (1 - churn_probability) >= 500 then 'Gold'
        when monthly_revenue_rate * 12 * (1 - churn_probability) >= 100 then 'Silver'
        else 'Bronze'
    end as customer_tier
from clv_calc
order by customer_id
INNEREOF

# ===========================================================================
# STEP 6: Run dbt
# ===========================================================================
export DBT_PROFILES_DIR=/app/dbt_transforms
dbt deps
dbt run

echo "Solution complete!"
