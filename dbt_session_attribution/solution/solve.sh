#!/bin/bash
set -e

# ===========================================================================
# STEP 1: Create DuckDB database with sample data
# ===========================================================================
mkdir -p /app/database
python3 << 'PYEOF'
import duckdb
import datetime

conn = duckdb.connect('/app/database/retail.duckdb')

conn.execute("CREATE SCHEMA IF NOT EXISTS digital")
conn.execute("CREATE SCHEMA IF NOT EXISTS analytics")

# --- web_sessions ---
conn.execute("""
CREATE TABLE IF NOT EXISTS digital.web_sessions (
    session_id VARCHAR, visitor_id VARCHAR, customer_id VARCHAR, channel_id INTEGER,
    session_start TIMESTAMP, session_end TIMESTAMP, duration_seconds INTEGER,
    page_views INTEGER, landing_page VARCHAR, exit_page VARCHAR,
    referrer VARCHAR, utm_source VARCHAR, utm_medium VARCHAR, utm_campaign VARCHAR,
    device_type VARCHAR, browser VARCHAR, os VARCHAR, ip_address VARCHAR,
    country VARCHAR, is_converted BOOLEAN, order_id VARCHAR
)
""")

# Sessions for Customer C1: 3 sessions before conversion ORD-1
# S-001: Organic Search via google (4 days before conversion)
# S-002: Paid Social via facebook (2 days before conversion)
# S-003: Paid Search via cpc (0.5 days before), then converts
conn.execute("""
INSERT INTO digital.web_sessions VALUES
    ('S-001','V1','C1',1,'2024-01-10 10:00:00','2024-01-10 10:15:00',900,5,'/home','/products','google.com',NULL,NULL,NULL,'desktop','Chrome','Windows','1.1.1.1','US',false,NULL),
    ('S-002','V1','C1',2,'2024-01-12 14:00:00','2024-01-12 14:05:00',300,4,'/products','/cart','facebook.com','fb','paid_social','winter_sale','mobile','Safari','iOS','1.1.1.2','US',false,NULL),
    ('S-003','V1','C1',3,'2024-01-14 02:00:00','2024-01-14 02:10:00',600,6,'/promo','/checkout',NULL,'google','cpc','brand_search','desktop','Chrome','Windows','1.1.1.3','US',true,'ORD-1'),

    -- Sessions for Customer C2: 2 sessions before conversion ORD-2
    -- S-005: Organic Social via twitter (3 days before)
    -- S-006: Email channel (0.2 days before), then converts
    -- S-004: Bounce session (duration=3, should be filtered out)
    ('S-004','V2','C2',4,'2024-01-11 11:00:00','2024-01-11 11:00:03',3,1,'/home','/home','google.com',NULL,NULL,NULL,'desktop','Chrome','Windows','2.2.2.1','US',false,NULL),
    ('S-005','V2','C2',5,'2024-01-13 16:00:00','2024-01-13 16:08:00',480,4,'/blog','/products','twitter.com',NULL,NULL,NULL,'desktop','Firefox','Linux','2.2.2.2','US',false,NULL),
    ('S-006','V2','C2',6,'2024-01-16 08:00:00','2024-01-16 08:12:00',720,6,'/products','/checkout',NULL,NULL,'email','promo_jan','mobile','Safari','iOS','2.2.2.3','US',true,'ORD-2'),

    -- Sessions for Customer C3: 3 sessions, but one is bot traffic
    -- S-007: Bot session (filtered out)
    -- S-008: Organic Social via linkedin (2 days before)
    -- S-009: Direct channel (0.1 days before), then converts
    ('S-007','V3','C3',7,'2024-01-09 08:00:00','2024-01-09 08:03:00',180,3,'/home','/products','bing.com',NULL,NULL,NULL,'bot','BotBrowser','BotOS','3.3.3.1','US',false,NULL),
    ('S-008','V3','C3',8,'2024-01-12 10:00:00','2024-01-12 10:06:00',360,4,'/careers','/about','linkedin.com',NULL,NULL,NULL,'desktop','Firefox','Windows','3.3.3.2','US',false,NULL),
    ('S-009','V3','C3',9,'2024-01-14 12:00:00','2024-01-14 12:20:00',1200,10,'/home','/checkout',NULL,NULL,NULL,NULL,'desktop','Chrome','MacOS','3.3.3.3','US',true,'ORD-3'),

    -- Sessions for Customer C4: single session conversion (only 1 eligible session)
    ('S-010','V4','C4',10,'2024-01-15 09:00:00','2024-01-15 09:08:00',480,5,'/home','/checkout','google.com',NULL,NULL,NULL,'desktop','Chrome','Windows','4.4.4.1','US',true,'ORD-4'),

    -- Sessions for Customer C5: 4+ sessions with 3+ distinct channels (triggers cross-channel bonus)
    -- S-011: Organic Search via google (5 days before)
    -- S-012: Paid Social via facebook (3 days before)
    -- S-013: Email channel (1 day before)
    -- S-014: Direct, converts
    ('S-011','V5','C5',11,'2024-01-08 10:00:00','2024-01-08 10:12:00',720,6,'/home','/products','google.com',NULL,NULL,NULL,'desktop','Chrome','Windows','5.5.5.1','US',false,NULL),
    ('S-012','V5','C5',12,'2024-01-10 14:00:00','2024-01-10 14:08:00',480,4,'/ad-landing','/products','facebook.com','fb','paid_social','retarget','mobile','Safari','iOS','5.5.5.2','US',false,NULL),
    ('S-013','V5','C5',13,'2024-01-12 10:00:00','2024-01-12 10:05:00',300,3,'/promo','/cart',NULL,NULL,'email','weekly','desktop','Chrome','Windows','5.5.5.3','US',false,NULL),
    ('S-014','V5','C5',14,'2024-01-13 15:00:00','2024-01-13 15:10:00',600,5,'/home','/checkout',NULL,NULL,NULL,NULL,'desktop','Chrome','Windows','5.5.5.4','US',true,'ORD-5'),

    -- Extra session for Customer C1 that is too old (outside 14 day window) - should not be attributed
    ('S-015','V1','C1',15,'2023-12-20 10:00:00','2023-12-20 10:10:00',600,5,'/home','/about','bing.com',NULL,NULL,NULL,'desktop','Edge','Windows','1.1.1.4','US',false,NULL),

    -- Single page session (page_views=1) - should be filtered
    ('S-016','V2','C2',16,'2024-01-14 09:00:00','2024-01-14 09:03:00',180,1,'/home','/home','yahoo.com',NULL,NULL,NULL,'desktop','Chrome','Windows','2.2.2.4','US',false,NULL)
""")

# --- fact_sales ---
conn.execute("""
CREATE TABLE IF NOT EXISTS analytics.fact_sales (
    sale_key INTEGER, date_key INTEGER, time_key INTEGER, customer_key INTEGER,
    product_key INTEGER, employee_key INTEGER, channel_key INTEGER, geography_key INTEGER,
    order_id VARCHAR, order_line_id VARCHAR, quantity INTEGER,
    unit_price DOUBLE, discount_amount DOUBLE, tax_amount DOUBLE,
    total_amount DOUBLE, cost_amount DOUBLE, profit_amount DOUBLE
)
""")
conn.execute("""
INSERT INTO analytics.fact_sales VALUES
    (1, 20240114, 1, 1, 1, 1, 1, 1, 'ORD-1', 'OL-1', 2, 250.00, 0, 25.00, 500.00, 200.00, 300.00),
    (2, 20240116, 1, 2, 2, 1, 2, 1, 'ORD-2', 'OL-2', 1, 300.00, 0, 15.00, 300.00, 150.00, 150.00),
    (3, 20240114, 1, 3, 3, 1, 3, 1, 'ORD-3', 'OL-3', 3, 250.00, 0, 37.50, 750.00, 400.00, 350.00),
    (4, 20240115, 1, 4, 4, 1, 4, 1, 'ORD-4', 'OL-4', 1, 200.00, 0, 10.00, 200.00, 100.00, 100.00),
    (5, 20240113, 1, 5, 5, 1, 5, 1, 'ORD-5', 'OL-5', 4, 250.00, 0, 50.00, 1000.00, 500.00, 500.00)
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
# STEP 3: Source definitions and staging models
# ===========================================================================
mkdir -p models/staging models/intermediate models/marts

cat > models/staging/_sources.yml << 'EOF'
version: 2
sources:
  - name: digital
    schema: digital
    tables:
      - name: web_sessions
  - name: analytics
    schema: analytics
    tables:
      - name: fact_sales
EOF

cat > models/staging/stg_digital__web_sessions.sql << 'EOF'
select
    session_id, visitor_id, customer_id, channel_id,
    session_start, session_end, duration_seconds, page_views,
    landing_page, exit_page, referrer,
    utm_source, utm_medium, utm_campaign,
    device_type, browser, os, ip_address, country,
    is_converted, order_id
from {{ source('digital', 'web_sessions') }}
EOF

cat > models/staging/stg_analytics__fact_sales.sql << 'EOF'
select
    sale_key, date_key, time_key, customer_key, product_key,
    employee_key, channel_key, geography_key,
    order_id, order_line_id, quantity,
    unit_price, discount_amount, tax_amount,
    total_amount, cost_amount, profit_amount
from {{ source('analytics', 'fact_sales') }}
EOF

# ===========================================================================
# STEP 4: Mart model – attribution_report
# ===========================================================================

cat > models/marts/attribution_report.sql << 'SQLEOF'
{{ config(materialized='table') }}

with quality_sessions as (
    select *
    from {{ ref('stg_digital__web_sessions') }}
    where duration_seconds >= 5
      and page_views >= 2
      and lower(device_type) != 'bot'
),

channeled as (
    select *,
        case
            when lower(utm_medium) in ('cpc', 'ppc') then 'Paid Search'
            when lower(utm_medium) = 'paid_social'
                 or (lower(coalesce(referrer, '')) like '%facebook%' and utm_source is not null and utm_source != '')
                 then 'Paid Social'
            when lower(coalesce(referrer, '')) like '%google%'
                 or lower(coalesce(referrer, '')) like '%bing%'
                 or lower(coalesce(referrer, '')) like '%yahoo%'
                 then 'Organic Search'
            when lower(coalesce(referrer, '')) like '%facebook%'
                 or lower(coalesce(referrer, '')) like '%twitter%'
                 or lower(coalesce(referrer, '')) like '%linkedin%'
                 or lower(coalesce(referrer, '')) like '%instagram%'
                 then 'Organic Social'
            when lower(coalesce(utm_medium, '')) = 'email'
                 or lower(coalesce(referrer, '')) like '%mail%'
                 then 'Email'
            when referrer is null or referrer = '' then 'Direct'
            else 'Referral'
        end as channel
    from quality_sessions
),

conversions as (
    select
        ws.session_id as converting_session_id,
        ws.customer_id,
        ws.session_start as conversion_time,
        ws.order_id,
        fs.total_amount as conversion_revenue
    from {{ ref('stg_digital__web_sessions') }} ws
    inner join {{ ref('stg_analytics__fact_sales') }} fs
        on ws.order_id = fs.order_id
    where ws.is_converted = true
      and ws.order_id is not null
),

touchpoints as (
    select
        c.converting_session_id,
        c.customer_id,
        c.conversion_time,
        c.order_id,
        c.conversion_revenue,
        ch.session_id,
        ch.session_start,
        ch.channel,
        cast((julianday(c.conversion_time) - julianday(ch.session_start)) as double) as days_before
    from conversions c
    inner join channeled ch
        on c.customer_id = ch.customer_id
    where ch.session_start < c.conversion_time
      and cast((julianday(c.conversion_time) - julianday(ch.session_start)) as double) <= 14.0
),

ranked as (
    select *,
        row_number() over (partition by order_id order by session_start asc) as pos,
        count(*) over (partition by order_id) as total_sessions
    from touchpoints
),

distinct_channels_per_conversion as (
    select order_id, count(distinct channel) as n_channels
    from ranked
    group by order_id
),

with_weights as (
    select
        r.*,
        dc.n_channels,
        case
            when r.total_sessions = 1 then 1.0
            when r.pos = 1 then 1.5
            when r.pos = r.total_sessions then 1.3
            else 1.0
        end as position_weight,
        exp(-1.0 * r.days_before / 7.0) as time_weight,
        case when dc.n_channels >= 3 then 1.1 else 1.0 end as channel_bonus
    from ranked r
    left join distinct_channels_per_conversion dc on r.order_id = dc.order_id
),

raw_weighted as (
    select *,
        position_weight * time_weight as raw_weight
    from with_weights
),

normalized as (
    select *,
        sum(raw_weight) over (partition by order_id) as total_raw_weight
    from raw_weighted
),

credited as (
    select
        channel,
        order_id,
        raw_weight / total_raw_weight as credit,
        (raw_weight / total_raw_weight) * conversion_revenue * channel_bonus as attributed_revenue
    from normalized
),

final as (
    select
        channel,
        round(sum(credit), 2) as total_conversions,
        round(sum(attributed_revenue), 2) as total_revenue
    from credited
    group by channel
    order by channel
)

select * from final
SQLEOF

# ===========================================================================
# STEP 5: Run dbt
# ===========================================================================
export DBT_PROFILES_DIR=/app/dbt_transforms
dbt deps
dbt run

echo "Solution complete!"
