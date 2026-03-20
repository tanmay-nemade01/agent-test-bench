import duckdb
import pytest


DB_PATH = "/app/database/retail.duckdb"


def get_conn():
    return duckdb.connect(DB_PATH, read_only=True)


# ── Table existence ──────────────────────────────────────────────────────

def test_customer_cohorts_table_exists():
    conn = get_conn()
    tables = [r[0] for r in conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='main'").fetchall()]
    assert "customer_cohorts" in tables, "customer_cohorts table/view is missing"


def test_cohort_retention_table_exists():
    conn = get_conn()
    tables = [r[0] for r in conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='main'").fetchall()]
    assert "cohort_retention" in tables, "cohort_retention table/view is missing"


def test_customer_clv_table_exists():
    conn = get_conn()
    tables = [r[0] for r in conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='main'").fetchall()]
    assert "customer_clv" in tables, "customer_clv table/view is missing"


# ── Schema validation ────────────────────────────────────────────────────

def test_customer_cohorts_schema():
    conn = get_conn()
    cols = {r[0] for r in conn.execute("SELECT column_name FROM information_schema.columns WHERE table_name='customer_cohorts' AND table_schema='main'").fetchall()}
    expected = {"customer_id", "cohort_month", "first_transaction_date", "first_transaction_amount",
                "signup_to_first_purchase_days", "acquisition_channel"}
    missing = expected - cols
    assert not missing, f"customer_cohorts missing columns: {missing}"


def test_cohort_retention_schema():
    conn = get_conn()
    cols = {r[0] for r in conn.execute("SELECT column_name FROM information_schema.columns WHERE table_name='cohort_retention' AND table_schema='main'").fetchall()}
    expected = {"cohort_month", "period_number", "period_month", "cohort_size",
                "active_customers", "retained_customers", "retention_rate",
                "period_revenue", "cumulative_revenue"}
    missing = expected - cols
    assert not missing, f"cohort_retention missing columns: {missing}"


def test_customer_clv_schema():
    conn = get_conn()
    cols = {r[0] for r in conn.execute("SELECT column_name FROM information_schema.columns WHERE table_name='customer_clv' AND table_schema='main'").fetchall()}
    expected = {"customer_id", "cohort_month", "total_transactions", "total_revenue",
                "avg_transaction_value", "customer_lifespan_months", "monthly_revenue_rate",
                "months_since_last_transaction", "churn_probability",
                "predicted_clv_12m", "customer_tier"}
    missing = expected - cols
    assert not missing, f"customer_clv missing columns: {missing}"


# ── Row counts ────────────────────────────────────────────────────────────

def test_customer_cohorts_has_rows():
    conn = get_conn()
    count = conn.execute("SELECT count(*) FROM main.customer_cohorts").fetchone()[0]
    assert count > 0, "customer_cohorts is empty"


def test_cohort_retention_has_rows():
    conn = get_conn()
    count = conn.execute("SELECT count(*) FROM main.cohort_retention").fetchone()[0]
    assert count > 0, "cohort_retention is empty"


def test_customer_clv_has_rows():
    conn = get_conn()
    count = conn.execute("SELECT count(*) FROM main.customer_clv").fetchone()[0]
    assert count > 0, "customer_clv is empty"


# ── Data quality ──────────────────────────────────────────────────────────

def test_customer_cohorts_no_null_customer_id():
    conn = get_conn()
    nulls = conn.execute("SELECT count(*) FROM main.customer_cohorts WHERE customer_id IS NULL").fetchone()[0]
    assert nulls == 0, "customer_cohorts has NULL customer_id values"


def test_customer_cohorts_valid_acquisition_channel():
    conn = get_conn()
    channels = {r[0] for r in conn.execute("SELECT DISTINCT acquisition_channel FROM main.customer_cohorts").fetchall()}
    valid = {"Online", "Retail", "Partner", "Direct", "Unknown"}
    invalid = channels - valid
    assert not invalid, f"Invalid acquisition_channel values: {invalid}"


def test_cohort_retention_rate_bounds():
    conn = get_conn()
    out_of_range = conn.execute(
        "SELECT count(*) FROM main.cohort_retention WHERE retention_rate < 0 OR retention_rate > 1.01"
    ).fetchone()[0]
    assert out_of_range == 0, "retention_rate values outside [0, 1] range"


def test_cohort_retention_period_number_non_negative():
    conn = get_conn()
    negatives = conn.execute("SELECT count(*) FROM main.cohort_retention WHERE period_number < 0").fetchone()[0]
    assert negatives == 0, "cohort_retention has negative period_number"


def test_cohort_retention_cumulative_revenue_non_decreasing():
    conn = get_conn()
    rows = conn.execute("""
        SELECT cohort_month, period_number, cumulative_revenue
        FROM main.cohort_retention ORDER BY cohort_month, period_number
    """).fetchall()
    prev = {}
    for cohort, period, cum_rev in rows:
        if cohort in prev:
            assert cum_rev >= prev[cohort] - 0.01, (
                f"Cumulative revenue decreased for {cohort} at period {period}: {cum_rev} < {prev[cohort]}"
            )
        prev[cohort] = cum_rev


def test_customer_clv_valid_tiers():
    conn = get_conn()
    tiers = {r[0] for r in conn.execute("SELECT DISTINCT customer_tier FROM main.customer_clv").fetchall()}
    valid = {"Platinum", "Gold", "Silver", "Bronze"}
    invalid = tiers - valid
    assert not invalid, f"Invalid customer_tier values: {invalid}"


def test_customer_clv_churn_probability_bounds():
    conn = get_conn()
    out = conn.execute(
        "SELECT count(*) FROM main.customer_clv WHERE churn_probability < 0 OR churn_probability > 1.01"
    ).fetchone()[0]
    assert out == 0, "churn_probability values outside [0, 1] range"


def test_customer_clv_positive_revenue():
    conn = get_conn()
    negatives = conn.execute("SELECT count(*) FROM main.customer_clv WHERE total_revenue < 0").fetchone()[0]
    assert negatives == 0, "customer_clv has negative total_revenue"


def test_customer_clv_positive_transactions():
    conn = get_conn()
    bad = conn.execute("SELECT count(*) FROM main.customer_clv WHERE total_transactions <= 0").fetchone()[0]
    assert bad == 0, "customer_clv has non-positive total_transactions"


def test_customer_cohorts_unique_customers():
    conn = get_conn()
    total = conn.execute("SELECT count(*) FROM main.customer_cohorts").fetchone()[0]
    distinct = conn.execute("SELECT count(DISTINCT customer_id) FROM main.customer_cohorts").fetchone()[0]
    assert total == distinct, "customer_cohorts has duplicate customer_id entries"


def test_cohort_retention_cohort_size_positive():
    conn = get_conn()
    bad = conn.execute("SELECT count(*) FROM main.cohort_retention WHERE cohort_size <= 0").fetchone()[0]
    assert bad == 0, "cohort_retention has non-positive cohort_size"
