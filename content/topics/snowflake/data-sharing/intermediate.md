---
title: "Data Sharing - Intermediate"
topic: snowflake
subtopic: data-sharing
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [snowflake, data-sharing, secure-views, cross-region, multi-tenant, listings]
---

# Snowflake Data Sharing — Intermediate

## Multi-Tenant Data Sharing

Share different data subsets with different consumers from one table:

```sql
-- Single table with data for multiple partners:
-- production.orders has: partner_id, order_id, amount, ...

-- Create a secure view PER PARTNER (row-filtered):
CREATE SECURE VIEW shared.partner_acme_orders AS
    SELECT order_id, amount, order_date, product_category
    FROM production.gold.orders
    WHERE partner_id = 'ACME';

CREATE SECURE VIEW shared.partner_globex_orders AS
    SELECT order_id, amount, order_date, product_category
    FROM production.gold.orders
    WHERE partner_id = 'GLOBEX';

-- Share each view with the appropriate account:
CREATE SHARE acme_share;
GRANT USAGE ON DATABASE production TO SHARE acme_share;
GRANT USAGE ON SCHEMA production.shared TO SHARE acme_share;
GRANT SELECT ON VIEW shared.partner_acme_orders TO SHARE acme_share;
ALTER SHARE acme_share ADD ACCOUNTS = 'acme_snowflake_account';

CREATE SHARE globex_share;
GRANT USAGE ON DATABASE production TO SHARE globex_share;
GRANT USAGE ON SCHEMA production.shared TO SHARE globex_share;
GRANT SELECT ON VIEW shared.partner_globex_orders TO SHARE globex_share;
ALTER SHARE globex_share ADD ACCOUNTS = 'globex_snowflake_account';

-- Each partner only sees THEIR data (enforced by secure view filter)
-- ACME can't see Globex data, and vice versa!
```

### Dynamic Multi-Tenant (Scalable Approach)

```sql
-- For 50+ partners: don't create 50 views!
-- Use a SINGLE secure view with CURRENT_ACCOUNT() filtering:

CREATE SECURE VIEW shared.my_orders AS
    SELECT order_id, amount, order_date, product_category
    FROM production.gold.orders
    WHERE partner_snowflake_account = CURRENT_ACCOUNT();
-- CURRENT_ACCOUNT() returns the querying consumer's account identifier
-- Each consumer automatically sees only their own data!

-- One share serves ALL partners:
CREATE SHARE universal_partner_share;
GRANT SELECT ON VIEW shared.my_orders TO SHARE universal_partner_share;
ALTER SHARE universal_partner_share ADD ACCOUNTS = 'acme', 'globex', 'initech', ...;
-- All partners use the same share, but each sees different data!
```

---

## Cross-Region and Cross-Cloud Sharing

```sql
-- By default, sharing works within the same Snowflake region
-- Cross-region: requires data replication

-- Step 1: Enable replication for your database
ALTER DATABASE production ENABLE REPLICATION TO ACCOUNTS 
    org_name.account_in_eu_west_1,
    org_name.account_in_ap_southeast_1;

-- Step 2: Create replica in consumer's region
-- (Snowflake handles the replication automatically)
CREATE DATABASE production_replica AS REPLICA OF org_name.account_us_east_1.production;

-- Step 3: Share from the replica (in consumer's region)
-- Now consumer queries from local storage (no cross-region data transfer per query!)

-- COST:
-- Replication: data transfer + storage in remote region
-- Once replicated: consumer queries are local (fast, no transfer per query)
-- Good for: large datasets queried frequently from remote regions
-- Alternative: consumer pays cross-region query costs per query (cheaper for infrequent access)

-- Cross-cloud (AWS Snowflake → Azure Snowflake):
-- Same mechanism: replicate database across clouds
-- Then share locally within the target cloud's region
```

---

## Private Listings (Snowflake Marketplace)

```sql
-- Private Listing: share with specific accounts via Marketplace UI
-- (vs direct SHARE which requires knowing the exact account identifier)

-- Benefits of Listings over direct Shares:
-- 1. Discoverability: consumers find you in Marketplace search
-- 2. Self-service: consumers click "Get" instead of you adding their account manually
-- 3. Terms & Conditions: attach legal terms to the listing
-- 4. Metrics: see who accessed your data (usage analytics)
-- 5. Monetization: charge for data access (paid listings)

-- Creating a Private Listing (UI-based):
-- Provider Studio → New Listing → Private → Select shared objects → 
-- Set title/description → Add authorized consumers → Publish

-- Consumer experience:
-- Marketplace → Private Listings → "Partner Analytics" → "Get" → 
-- Creates database automatically → query immediately!
```

---

## Sharing Secure UDFs

```sql
-- Share custom functions (not just data):

CREATE SECURE FUNCTION shared.calculate_ltv(
    total_orders NUMBER, avg_order_value DECIMAL, months_active NUMBER
)
RETURNS DECIMAL
AS
$$
    total_orders * avg_order_value * (months_active / 12.0) * 1.2  -- Proprietary formula!
$$;

-- Add to share:
GRANT USAGE ON FUNCTION shared.calculate_ltv(NUMBER, DECIMAL, NUMBER) TO SHARE analytics_share;

-- Consumer uses your function on THEIR data:
SELECT 
    customer_id,
    shared_db.shared.calculate_ltv(order_count, avg_order_value, months_since_signup) AS predicted_ltv
FROM my_local_data.customers;
-- They can USE your formula but can't SEE it (SECURE keyword)!
-- Great for: proprietary scoring models, pricing algorithms
```

---

## Share Management and Governance

```sql
-- Monitor who's accessing your shared data:
SELECT *
FROM SNOWFLAKE.ACCOUNT_USAGE.DATA_TRANSFER_HISTORY
WHERE DIRECTION = 'OUTBOUND'  -- Data flowing to consumers
  AND START_TIME >= DATEADD('day', -30, CURRENT_TIMESTAMP());

-- View all active shares and their consumers:
SHOW SHARES;
DESCRIBE SHARE analytics_share;  -- Shows: accounts, granted objects

-- Revoke access (immediately stops consumer from querying):
ALTER SHARE analytics_share REMOVE ACCOUNTS = 'former_partner_account';
-- Instant: consumer loses access immediately (next query fails)

-- Audit: consumer query activity
-- Consumers' queries appear in THEIR account's query history
-- Provider can't directly see consumer queries (privacy)
-- But: SNOWFLAKE.ACCOUNT_USAGE.DATA_TRANSFER_HISTORY shows data egress
```

---

## Sharing with Data Clean Rooms

```sql
-- Data Clean Room: two parties analyze combined data without exposing raw records

-- Pattern: secure UDF + shared view (neither party sees the other's raw data)

-- Provider creates analysis function:
CREATE SECURE FUNCTION shared.overlap_analysis(consumer_email VARCHAR)
RETURNS TABLE (segment VARCHAR, overlap_pct DECIMAL)
AS
$$
    SELECT 
        p.customer_segment AS segment,
        COUNT(CASE WHEN p.email = consumer_email THEN 1 END) * 100.0 / COUNT(*) AS overlap_pct
    FROM production.customers p
    GROUP BY p.customer_segment
$$;

-- Consumer calls the function with their data:
SELECT segment, overlap_pct
FROM TABLE(shared_db.shared.overlap_analysis(c.email))
JOIN my_db.customers c;
-- Result: overlap percentages (aggregate) — no raw emails exposed to either party!

-- This enables:
-- Audience overlap analysis (media companies)
-- Customer matching (without sharing PII)
-- Collaborative analytics (competitive intelligence)
```

---

## Interview Tips

> **Tip 1:** "How do you share different data with different partners from one table?" — Secure views with row-level filtering. Option A: one view per partner (WHERE partner_id = 'X'). Option B: single dynamic view using CURRENT_ACCOUNT() (scales to 100+ partners). Each partner only sees their data through the secure view filter.

> **Tip 2:** "How does cross-region sharing work?" — Within same region: instant (metadata pointer, zero data copy). Cross-region: requires database replication to the consumer's region first. Replication has transfer + storage costs, but subsequent queries are local (fast). Use cross-region replication for frequently-accessed large datasets; accept per-query transfer costs for infrequent access.

> **Tip 3:** "Data Sharing vs data clean rooms?" — Regular sharing: consumer sees the actual rows (filtered, but visible). Clean rooms: neither party sees the other's raw data — only aggregated/computed results. Implement with: secure UDFs (proprietary logic hidden) + secure views (raw data hidden). Use cases: audience overlap, customer matching without PII exposure.
