---
title: "SCD Implementation - Scenario Questions"
topic: etl-concepts
subtopic: scd-implementation
content_type: scenario_question
tags: [etl, scd, dimensional-modeling, interview, scenarios]
---

# Scenario Questions — SCD Implementation

<article data-difficulty="junior">

## 🟢 Junior: Choose the Right SCD Type

**Scenario:** You're designing a `dim_customer` table. Customers can change their: (A) email address, (B) shipping address, (C) loyalty tier (Bronze/Silver/Gold). The analytics team needs to: track current email for communications, analyze purchase patterns by loyalty tier at time of purchase, and report on address changes for fraud detection. Which SCD type do you choose for each attribute, and why?

<details>
<summary>💡 Hint</summary>
Think about each attribute separately. Does the business need to know what the value was at the time of a specific event (like a purchase)? Or just the current value?
</details>

<details>
<summary>✅ Solution</summary>

**Email → SCD Type 1 (Overwrite)**

For communications, only the current email matters. Historical emails aren't needed for analytics — the customer wants mail at their current address.

```sql
-- Type 1: just update in place
UPDATE dim_customer SET email = :new_email WHERE customer_id = :cid;
```

**Loyalty Tier → SCD Type 2 (Add New Row)**

Purchase pattern analysis by tier requires knowing what tier the customer was in AT THE TIME OF PURCHASE. Current tier would misattribute purchases.

```sql
-- Type 2 table structure
CREATE TABLE dim_customer_tier_history (
    customer_sk  BIGSERIAL PRIMARY KEY,
    customer_id  TEXT NOT NULL,
    loyalty_tier TEXT NOT NULL,   -- Bronze, Silver, Gold
    effective_at TIMESTAMPTZ NOT NULL,
    expired_at   TIMESTAMPTZ,
    is_current   BOOLEAN DEFAULT TRUE
);

-- Query: Revenue by loyalty tier at time of purchase
SELECT
    c.loyalty_tier,
    SUM(o.total_usd) AS revenue_usd
FROM orders o
JOIN dim_customer_tier_history c
    ON o.customer_id = c.customer_id
   AND o.created_at BETWEEN c.effective_at AND COALESCE(c.expired_at, NOW())
GROUP BY 1;
```

**Shipping Address → SCD Type 3 (Previous Value Column) OR Type 2**

For fraud detection, knowing the immediately previous address is most valuable. If only one level of history is needed, Type 3 is simpler:

```sql
-- Type 3: current + previous address
CREATE TABLE dim_customer_address (
    customer_id      TEXT PRIMARY KEY,
    current_address  TEXT,
    current_city     TEXT,
    prev_address     TEXT,
    prev_city        TEXT,
    address_changed_at TIMESTAMPTZ
);

-- Fraud query: customers who changed address right before a large order
SELECT o.customer_id, o.total_usd, a.address_changed_at
FROM orders o
JOIN dim_customer_address a ON o.customer_id = a.customer_id
WHERE a.address_changed_at >= o.created_at - INTERVAL '7 days'
  AND o.total_usd > 500;
```

If more than one previous address is needed for fraud detection, use Type 2 instead.

**Summary:**

| Attribute | SCD Type | Reason |
|---|---|---|
| email | Type 1 | Only current value needed |
| loyalty_tier | Type 2 | Point-in-time accuracy required for analytics |
| shipping_address | Type 3 or 2 | Previous value for fraud; full history if deeper analysis needed |

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Implement SCD Type 2 in Python

**Scenario:** You have a `staging_products` table with the latest product catalog data (product_id, name, category, price_usd). Your `dim_product` table is SCD Type 2 with columns: product_sk, product_id, name, category, price_usd, effective_at, expired_at, is_current. Write the Python/SQL logic to apply daily SCD Type 2 updates — handling new products, changed products, and deleted (discontinued) products.

<details>
<summary>💡 Hint</summary>
Break the problem into three cases: (1) New products not in dim_product at all, (2) Existing products where an attribute changed, (3) Products in dim_product that no longer exist in staging (discontinued). Handle them in the right order to avoid conflicts.
</details>

<details>
<summary>✅ Solution</summary>

```python
import pandas as pd
import sqlalchemy as sa
import hashlib
from datetime import datetime

def apply_scd2_products(staging_engine, target_engine, run_time: datetime = None):
    """
    Daily SCD Type 2 update for product dimension.
    Handles: new products, changed attributes, discontinued products.
    """
    if run_time is None:
        run_time = datetime.utcnow()

    # Step 1: Load staging and current dimension
    staging_df = pd.read_sql(
        "SELECT product_id, name, category, price_usd FROM staging_products",
        staging_engine
    )
    current_df = pd.read_sql(
        "SELECT product_id, name, category, price_usd, product_sk FROM dim_product WHERE is_current = TRUE",
        target_engine
    )

    # Step 2: Compute row hash for change detection
    def make_hash(row):
        return hashlib.md5(f"{row['name']}|{row['category']}|{row['price_usd']}".encode()).hexdigest()

    staging_df["hash"] = staging_df.apply(make_hash, axis=1)
    current_df["hash"] = current_df.apply(make_hash, axis=1)

    # Step 3: Classify records
    merged = staging_df.merge(
        current_df[["product_id", "hash", "product_sk"]],
        on="product_id",
        how="outer",
        suffixes=("_src", "_tgt")
    )

    # New products: in staging but not in dim
    new_products = merged[merged["hash_tgt"].isna()]

    # Changed products: in both, but hash differs
    changed_products = merged[
        merged["hash_tgt"].notna() &
        (merged["hash_src"] != merged["hash_tgt"])
    ]

    # Discontinued products: in dim but not in staging
    discontinued_ids = set(current_df["product_id"]) - set(staging_df["product_id"])

    print(f"New: {len(new_products)}, Changed: {len(changed_products)}, Discontinued: {len(discontinued_ids)}")

    with target_engine.begin() as conn:
        # Step 4: Close changed records
        if not changed_products.empty:
            changed_ids = tuple(changed_products["product_id"].tolist())
            conn.execute(sa.text(f"""
                UPDATE dim_product
                SET is_current = FALSE, expired_at = :t
                WHERE product_id IN {changed_ids} AND is_current = TRUE
            """), {"t": run_time})

        # Step 5: Close discontinued records
        if discontinued_ids:
            disc_ids = tuple(discontinued_ids)
            conn.execute(sa.text(f"""
                UPDATE dim_product
                SET is_current = FALSE, expired_at = :t,
                    discontin_at = :t   -- Optional: mark as discontinued
                WHERE product_id IN {disc_ids} AND is_current = TRUE
            """), {"t": run_time})

        # Step 6: Insert new versions (new + changed)
        to_insert = pd.concat([
            new_products[["product_id", "name", "category", "price_usd"]],
            changed_products[["product_id", "name", "category", "price_usd"]],
        ])

        if not to_insert.empty:
            to_insert["effective_at"] = run_time
            to_insert["expired_at"]   = None
            to_insert["is_current"]   = True

            to_insert.to_sql("dim_product", conn, if_exists="append", index=False)

    return {
        "new": len(new_products),
        "changed": len(changed_products),
        "discontinued": len(discontinued_ids),
    }
```

**Test your implementation:**

```python
def test_scd2_new_product():
    # Add a new product to staging; verify it appears in dim with is_current=True
    pass

def test_scd2_price_change():
    # Change a product's price; verify old row has is_current=False and expired_at set,
    # and new row has is_current=True with the new price
    pass

def test_scd2_discontinued():
    # Remove a product from staging; verify dim row has is_current=False
    pass

def test_scd2_idempotent():
    # Run the function twice with the same staging data; verify no duplicates
    pass
```

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Retroactive SCD Type 2 Correction with Bi-Temporal Model

**Scenario:** Your company uses SCD Type 2 for the product dimension. In January, Product X was classified as "Electronics." In March, the merchandising team realizes it should have been "Appliances" since launch (November of the previous year). They want to: correct all historical reports to show "Appliances" for this product since November, but also preserve an audit trail of what our system believed at the time. Additionally, quarterly revenue reports that have already been published showed "Electronics" — you need to be able to regenerate them showing what the system believed during Q1 and Q2. How do you design for this?

<details>
<summary>💡 Hint</summary>
Standard SCD Type 2 can't handle this because retroactive corrections will overwrite what the system "believed" at the time. You need bi-temporal modeling: valid time (when the fact was true in reality) AND transaction time (when we recorded it).
</details>

<details>
<summary>✅ Solution</summary>

**The problem with standard SCD2:**

When you correct the category in the SCD2 table, you overwrite the history. Re-running the Q1 report after correction would show "Appliances" — but the published Q1 report showed "Electronics." You've lost the ability to reproduce the published report.

**Bi-temporal solution:**

```sql
CREATE TABLE dim_product_bitemporal (
    product_sk      UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    product_id      TEXT NOT NULL,
    product_name    TEXT NOT NULL,
    category        TEXT NOT NULL,

    -- Valid time: when was this true in reality?
    valid_from      DATE NOT NULL,
    valid_to        DATE,           -- NULL = currently valid in reality

    -- Transaction time: when did our system record this?
    txn_from        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    txn_to          TIMESTAMPTZ,    -- NULL = currently recorded in system

    UNIQUE (product_id, valid_from, txn_from)
);
```

**Initial data (November - before we discovered the error):**

```sql
-- November: Product X is launched as "Electronics" (incorrect categorization)
INSERT INTO dim_product_bitemporal
    (product_id, product_name, category, valid_from, valid_to, txn_from, txn_to)
VALUES
    ('PROD-X', 'Product X', 'Electronics', '2023-11-01', NULL, NOW(), NULL);

-- Q1 report runs in February: uses current system knowledge
-- Query: "electronics revenue in Q1" → correctly includes Product X
```

**March: Retroactive correction**

```sql
BEGIN;

-- Step 1: Close the current "Electronics" record in transaction time
-- (We're saying: our system now no longer believes this is the current truth)
UPDATE dim_product_bitemporal
SET txn_to = NOW()
WHERE product_id = 'PROD-X'
  AND txn_to IS NULL;

-- Step 2: Insert the corrected record with the corrected VALID time
-- Product X was ALWAYS Appliances since Nov 1 (retroactive correction)
INSERT INTO dim_product_bitemporal
    (product_id, product_name, category, valid_from, valid_to, txn_from, txn_to)
VALUES
    ('PROD-X', 'Product X', 'Appliances', '2023-11-01', NULL, NOW(), NULL);

COMMIT;
```

**Result:**

```sql
-- Current state: What category is Product X RIGHT NOW?
SELECT category FROM dim_product_bitemporal
WHERE product_id = 'PROD-X'
  AND txn_to IS NULL     -- Currently believed
  AND valid_to IS NULL;  -- Currently valid
-- Returns: "Appliances"

-- Historical audit: What did our SYSTEM believe during Q1 2024?
-- (For reproducing the published Q1 report)
SELECT category FROM dim_product_bitemporal
WHERE product_id = 'PROD-X'
  AND txn_from  <= '2024-03-31'   -- System believed this as of Q1
  AND (txn_to IS NULL OR txn_to > '2024-03-31')
  AND valid_from <= '2024-01-01'
  AND (valid_to IS NULL OR valid_to > '2024-01-01');
-- Returns: "Electronics" (what the system believed in Q1)

-- Corrected historical view: What ACTUALLY happened since Nov 1?
SELECT category FROM dim_product_bitemporal
WHERE product_id = 'PROD-X'
  AND txn_to IS NULL     -- Current belief
  AND valid_from <= '2023-11-15'
  AND (valid_to IS NULL OR valid_to > '2023-11-15');
-- Returns: "Appliances" (corrected historical truth)
```

**Revenue report versioning:**

```python
def generate_revenue_report(
    quarter_start: date, quarter_end: date,
    as_of_system_date: date = None  # None = current beliefs
) -> pd.DataFrame:
    """
    Generate revenue report.
    - Without as_of_system_date: uses current corrected data
    - With as_of_system_date: reproduces what the system believed on that date
      (for re-generating published reports)
    """
    if as_of_system_date:
        txn_filter = f"AND p.txn_from <= '{as_of_system_date}' AND (p.txn_to IS NULL OR p.txn_to > '{as_of_system_date}')"
    else:
        txn_filter = "AND p.txn_to IS NULL"  # Current system beliefs

    sql = f"""
        SELECT
            p.category,
            SUM(oi.line_total_usd) AS revenue_usd
        FROM orders o
        JOIN order_items oi ON o.order_id = oi.order_id
        JOIN dim_product_bitemporal p
            ON oi.product_id = p.product_id
           AND DATE(o.created_at) BETWEEN p.valid_from AND COALESCE(p.valid_to, '9999-12-31')
           {txn_filter}
        WHERE DATE(o.created_at) BETWEEN '{quarter_start}' AND '{quarter_end}'
        GROUP BY 1
    """
    return pd.read_sql(sql, engine)

# Current corrected Q1 report (shows Appliances for Product X in Q1)
current_q1 = generate_revenue_report(date(2024, 1, 1), date(2024, 3, 31))

# Reproduced published Q1 report (shows Electronics — what system believed in Q1)
published_q1 = generate_revenue_report(
    date(2024, 1, 1), date(2024, 3, 31),
    as_of_system_date=date(2024, 2, 15)  # During Q1, before correction
)
```

**The three stakeholder answers this design provides:**
1. **"What's the current category?"** → Current valid time + current transaction time
2. **"What was the category historically (corrected)?"** → Point-in-time valid time + current transaction time
3. **"What did our system show in the published Q1 report?"** → Q1 valid time + Q1 transaction time

</details>

</article>

---

## ⚡ Quick-fire Q&A

**Q: What is a Slowly Changing Dimension (SCD) and why does it matter?**
A: An SCD is a dimension table that tracks changes to attributes over time. It matters because business entities (customers, products) change, and analytics often need to reflect the attribute values that were true at the time of a historical transaction, not just current values.

**Q: What is the difference between SCD Type 1 and Type 2?**
A: SCD Type 1 overwrites the old value with the new one — no history is kept, simple to implement. SCD Type 2 adds a new row for each change with effective dates and a current flag — full history is preserved, enabling point-in-time analysis.

**Q: How does SCD Type 2 work mechanically?**
A: When a change is detected in a source record, the current row in the dimension table has its end_date set to the change date and is_current set to false. A new row is inserted with the new attribute values, start_date set to the change date, end_date set to NULL, and is_current set to true.

**Q: What columns are typically added to support SCD Type 2?**
A: A surrogate key (unique per row version), effective_start_date, effective_end_date (NULL for the current record), and is_current (boolean flag). The natural/business key remains the same across all versions of the same entity.

**Q: How do you query SCD Type 2 for point-in-time analysis?**
A: Join the fact table to the dimension table on the natural key AND filter where the fact's event_date is between the dimension's effective_start_date and effective_end_date, ensuring the attribute values used are those that were true at the time of the event.

**Q: What is SCD Type 3 and when would you use it?**
A: SCD Type 3 adds extra columns to store the previous value alongside the current value (e.g., current_region, previous_region). It supports limited history (typically one change) and is used when only the most recent prior state is relevant, avoiding the row proliferation of Type 2.

**Q: How do you implement SCD Type 2 efficiently at scale in a cloud warehouse?**
A: Use MERGE (UPSERT) statements to detect changes in the incoming dataset compared to the current dimension rows, close changed rows in one pass, and insert new versions in a second pass. In Spark, use Delta Lake's MERGE INTO for transactional SCD Type 2 at scale.

**Q: What is SCD Type 6 (hybrid)?**
A: SCD Type 6 combines Types 1, 2, and 3 — it keeps full history like Type 2, stores the current value in all historical rows like Type 1 (denormalized for easy querying), and adds previous value columns like Type 3. It simplifies reporting but increases storage.

---

## 💼 Interview Tips

- Interviewers almost always ask about SCD Type 2 specifically — be ready to write or explain the MERGE SQL pattern from memory, including how you close old rows and insert new ones atomically.
- Demonstrate you understand the point-in-time query pattern, not just the load pattern — showing you know how downstream analysts consume SCD Type 2 data proves end-to-end understanding.
- Mention surrogate keys explicitly — using only natural keys for SCD Type 2 leads to fact table join ambiguity, and knowing this detail signals production experience.
- Discuss performance implications: SCD Type 2 grows dimension tables with every change, so full table scans become expensive — partitioning by is_current or using columnar storage helps.
- For senior roles, connect SCD implementation to the orchestration layer: how do incremental loads feed the MERGE, and how do you ensure the dimension state is consistent with the fact table?
- Avoid confusing SCD types under pressure — enumerate Type 1 (overwrite), Type 2 (new row + history), Type 3 (add column) clearly before discussing which to recommend for a given scenario.
