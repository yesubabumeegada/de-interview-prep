---
title: "SCD Implementation - Real World"
topic: etl-concepts
subtopic: scd-implementation
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [etl, scd, production, case-study, dbt-snapshots, dimensional-modeling]
---

# SCD Implementation — Real World

## Case Study 1: Customer Dimension SCD2 with dbt Snapshots

### Problem

An e-commerce company's customer dimension was implemented as SCD Type 1 (overwrite). When the CMO asked "what was the conversion rate for customers who were in the Gold tier in Q3 2023?", the answer was impossible — all customers showed their current tier, not their Q3 2023 tier.

### Solution: Migrate to SCD Type 2 with dbt Snapshots

```sql
-- snapshots/customers_snapshot.sql
{% snapshot customers_snapshot %}

{{
    config(
        target_database='analytics',
        target_schema='snapshots',
        unique_key='customer_id',
        strategy='timestamp',
        updated_at='updated_at',
        invalidate_hard_deletes=True
    )
}}

SELECT
    customer_id,
    email,
    loyalty_tier,        -- Gold, Silver, Bronze
    lifetime_value_usd,
    country,
    signup_date,
    updated_at
FROM {{ source('oltp', 'customers') }}

{% endsnapshot %}
```

```bash
# Run snapshot to capture initial state
dbt snapshot

# Subsequent runs automatically detect changes and add new rows
dbt snapshot  # Run daily
```

### Point-in-Time Tier Analysis

```sql
-- "What was the conversion rate for Gold tier customers in Q3 2023?"
WITH gold_in_q3 AS (
    SELECT DISTINCT customer_id
    FROM snapshots.customers_snapshot
    WHERE loyalty_tier = 'Gold'
      AND dbt_valid_from <= '2023-09-30'
      AND (dbt_valid_to > '2023-07-01' OR dbt_valid_to IS NULL)
),
orders_in_q3 AS (
    SELECT
        o.customer_id,
        COUNT(*) AS order_count
    FROM orders o
    WHERE o.created_at BETWEEN '2023-07-01' AND '2023-09-30'
    GROUP BY 1
)
SELECT
    COUNT(DISTINCT g.customer_id)    AS gold_customers_in_q3,
    COUNT(DISTINCT o.customer_id)    AS gold_customers_who_ordered,
    ROUND(100.0 * COUNT(DISTINCT o.customer_id) / COUNT(DISTINCT g.customer_id), 2) AS conversion_rate_pct
FROM gold_in_q3 g
LEFT JOIN orders_in_q3 o ON g.customer_id = o.customer_id;
```

---

## Case Study 2: Product Catalog SCD Implementation

### Problem

A retailer had 500,000 SKUs. Products moved between categories frequently (seasonal reclassification, brand changes). Category-level revenue attribution was wrong because the category a product was in today was being applied to historical orders.

### Solution: Product Dimension SCD Type 2

```sql
-- Full SCD Type 2 implementation for product catalog
CREATE TABLE dim_product (
    product_sk      UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    product_id      TEXT NOT NULL,
    product_name    TEXT NOT NULL,
    category        TEXT NOT NULL,
    subcategory     TEXT,
    brand           TEXT,
    unit_cost_usd   DECIMAL(10, 2),
    effective_at    TIMESTAMPTZ NOT NULL,
    expired_at      TIMESTAMPTZ,
    is_current      BOOLEAN NOT NULL DEFAULT TRUE,
    row_hash        TEXT NOT NULL,  -- For change detection
    UNIQUE (product_id, effective_at)
);

-- Airflow task: nightly SCD2 update
def update_product_scd2(ds: str, engine):
    """
    Apply SCD Type 2 changes for the product catalog.
    """
    # 1. Load staging
    staging_df = pd.read_sql(
        "SELECT * FROM raw.products",
        source_engine
    )

    # 2. Compute hash for change detection
    hash_cols = ["product_name", "category", "subcategory", "brand", "unit_cost_usd"]
    staging_df["row_hash"] = staging_df[hash_cols].apply(
        lambda row: hashlib.md5("|".join(str(v) for v in row).encode()).hexdigest(),
        axis=1
    )

    # 3. Identify changed records
    current_sql = "SELECT product_id, row_hash FROM dim_product WHERE is_current = TRUE"
    current_df  = pd.read_sql(current_sql, engine)

    merged = staging_df.merge(current_df, on="product_id", suffixes=("_src", "_tgt"), how="left")
    changed = merged[
        merged["row_hash_tgt"].isna() |  # New products
        (merged["row_hash_src"] != merged["row_hash_tgt"])  # Changed products
    ]

    if changed.empty:
        print(f"No product changes detected for {ds}")
        return 0

    # 4. Close old records
    changed_ids = tuple(changed["product_id"].tolist())
    with engine.begin() as conn:
        conn.execute(sa.text(f"""
            UPDATE dim_product
            SET is_current = FALSE, expired_at = NOW()
            WHERE product_id IN {changed_ids}
              AND is_current = TRUE
        """))

    # 5. Insert new versions
    new_versions = changed.rename(columns={"row_hash_src": "row_hash"})[
        ["product_id", "product_name", "category", "subcategory", "brand", "unit_cost_usd", "row_hash"]
    ].copy()
    new_versions["effective_at"] = datetime.utcnow()
    new_versions["expired_at"]   = None
    new_versions["is_current"]   = True

    new_versions.to_sql("dim_product", engine, if_exists="append", index=False)
    print(f"Updated {len(new_versions)} product records")
    return len(new_versions)
```

### Category Revenue Attribution with SCD2

```sql
-- Revenue by product category at time of order (historically accurate)
SELECT
    DATE_TRUNC('month', o.created_at) AS month,
    p.category                         AS category_at_order_time,
    SUM(oi.line_total_usd)             AS revenue_usd,
    COUNT(DISTINCT o.order_id)         AS order_count
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
JOIN dim_product p
    ON oi.product_id = p.product_id
   AND o.created_at  BETWEEN p.effective_at AND COALESCE(p.expired_at, NOW())
WHERE o.created_at >= '2023-01-01'
GROUP BY 1, 2
ORDER BY 1, 3 DESC;
```

---

## Case Study 3: Employee Dimension for HR Analytics

### Problem

HR needed to track all job changes for compliance reporting: "Show all employees who were in the 'Engineering' department at any point in 2023, their original department, and the department they moved to."

### SCD Type 2 Employee Dimension

```sql
CREATE TABLE dim_employee (
    employee_sk     BIGSERIAL PRIMARY KEY,
    employee_id     TEXT NOT NULL,
    full_name       TEXT NOT NULL,
    department      TEXT NOT NULL,
    job_title       TEXT NOT NULL,
    manager_id      TEXT,
    salary_band     TEXT,
    location        TEXT,
    effective_at    DATE NOT NULL,
    expired_at      DATE,
    is_current      BOOLEAN NOT NULL DEFAULT TRUE
);

-- Compliance query: employees who were in Engineering in 2023
SELECT
    e.employee_id,
    e.full_name,
    e.department AS department_in_2023,
    e.job_title  AS title_in_2023,
    e.effective_at,
    e.expired_at,
    CASE WHEN e.is_current THEN 'Current' ELSE 'Changed' END AS status
FROM dim_employee e
WHERE e.department = 'Engineering'
  AND e.effective_at <= '2023-12-31'
  AND (e.expired_at IS NULL OR e.expired_at >= '2023-01-01')
ORDER BY e.employee_id, e.effective_at;

-- Department transitions: who moved out of Engineering?
SELECT
    emp.employee_id,
    emp.full_name,
    emp.department   AS old_department,
    emp.expired_at   AS moved_on,
    next_ver.department AS new_department
FROM dim_employee emp
JOIN dim_employee next_ver
    ON emp.employee_id = next_ver.employee_id
   AND next_ver.effective_at = emp.expired_at  -- Contiguous versions
WHERE emp.department = 'Engineering'
  AND emp.is_current = FALSE
  AND emp.expired_at >= '2023-01-01';
```

---

## Operational Considerations

### SCD Type 2 Performance Tuning

```sql
-- Essential indexes for SCD2 performance
CREATE INDEX idx_dim_product_natural_key ON dim_product (product_id);
CREATE INDEX idx_dim_product_current     ON dim_product (product_id, is_current) WHERE is_current = TRUE;
CREATE INDEX idx_dim_product_dates       ON dim_product (product_id, effective_at, expired_at);

-- Partial index for current records (most common query pattern)
-- Covers: WHERE product_id = ? AND is_current = TRUE
-- Typically 1/N th the size of the full index (N = avg versions per product)
```

### SCD2 Data Volume Estimation

```python
def estimate_scd2_growth(
    natural_key_count: int,
    avg_changes_per_year: float,
    years_of_history: int,
    row_size_bytes: int = 500
) -> dict:
    """
    Estimate Type 2 table size given change frequency.
    """
    total_versions = natural_key_count * (1 + avg_changes_per_year * years_of_history)
    total_bytes    = total_versions * row_size_bytes

    return {
        "natural_keys":      natural_key_count,
        "total_versions":    int(total_versions),
        "avg_versions_per_key": 1 + avg_changes_per_year * years_of_history,
        "storage_gb":        total_bytes / 1e9,
    }

# Example: 1M customers, avg 0.5 address changes/year, 3 years history
estimate = estimate_scd2_growth(1_000_000, 0.5, 3, 200)
# {'natural_keys': 1000000, 'total_versions': 2500000, 'storage_gb': 0.5}
```

---

## Interview Tips

> **Tip 1:** The dbt snapshot story is a common real-world answer: "We had Type 1, analytics couldn't answer historical questions, we migrated to dbt snapshots which automated SCD Type 2." It shows dbt expertise and business awareness.

> **Tip 2:** Always justify SCD Type 2 with a specific business question that requires it. "What was the customer's tier at time of purchase?" is a concrete example that resonates with interviewers.

> **Tip 3:** Partial indexes on `is_current = TRUE` are a critical performance optimization for large SCD2 tables. Without them, queries for current records scan all historical versions.

> **Tip 4:** The contiguous-version join pattern (`next_ver.effective_at = emp.expired_at`) enables "who moved from X to Y" queries — a powerful SCD2 capability that pure Type 1 tables can't support.

> **Tip 5:** Data volume estimation for SCD2 demonstrates planning maturity. A dimension that changes frequently (e.g., stock price = essentially Type 2 at tick frequency) becomes unmanageable — that's where Iceberg time travel is a better fit.
