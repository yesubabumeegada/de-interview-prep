---
title: "Data Sharing - Real-World Production Examples"
topic: snowflake
subtopic: data-sharing
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [snowflake, data-sharing, production, patterns, marketplace]
---

# Snowflake Data Sharing — Real-World Production Examples

## Pattern 1: B2B Partner Data Exchange

```sql
-- E-commerce company shares order data with 20 brand partners
-- Each partner only sees their own brand's orders

-- Dynamic secure view (scales to any number of partners):
CREATE SECURE VIEW shared.partner_orders AS
    SELECT 
        order_id, order_date, quantity, unit_price, 
        customer_region, product_sku, product_name
        -- EXCLUDED: customer_id, customer_email, internal_cost, margin
    FROM production.gold.orders o
    JOIN production.dim.partner_accounts pa 
        ON o.brand_id = pa.brand_id 
        AND pa.snowflake_account = CURRENT_ACCOUNT();
-- Each partner's Snowflake account automatically maps to their brand_id
-- Partner "Nike" only sees Nike orders; Partner "Adidas" only sees Adidas orders

-- Single share for all partners:
CREATE SHARE brand_partner_share;
GRANT SELECT ON VIEW shared.partner_orders TO SHARE brand_partner_share;
ALTER SHARE brand_partner_share ADD ACCOUNTS = 'nike_account', 'adidas_account', ...; 

-- Partners consume:
CREATE DATABASE supplier_data FROM SHARE provider.brand_partner_share;
SELECT * FROM supplier_data.shared.partner_orders WHERE order_date >= '2024-01-01';
-- Nike sees only Nike orders. Adidas sees only Adidas orders. Automatic!
```

---

## Pattern 2: Internal Data Mesh (Cross-Department)

```sql
-- Large org: 5 departments, each with own Snowflake account
-- Share data products between departments (internal marketplace)

-- FINANCE DEPARTMENT (provider):
CREATE SHARE finance_metrics_share COMMENT = 'Financial KPIs for business units';
CREATE SECURE VIEW finance.shared.revenue_metrics AS
    SELECT business_unit, month, revenue, costs, margin_pct
    FROM finance.gold.monthly_metrics;
GRANT SELECT ON VIEW finance.shared.revenue_metrics TO SHARE finance_metrics_share;
ALTER SHARE finance_metrics_share ADD ACCOUNTS = 'sales_dept', 'marketing_dept', 'exec_dept';

-- SALES DEPARTMENT (provider):
CREATE SHARE sales_pipeline_share COMMENT = 'Sales pipeline for forecasting';
CREATE SECURE VIEW sales.shared.pipeline AS
    SELECT opportunity_id, stage, amount, close_date, probability
    FROM sales.gold.opportunities WHERE stage != 'Lost';  -- Don't share lost deals
GRANT SELECT ON VIEW sales.shared.pipeline TO SHARE sales_pipeline_share;
ALTER SHARE sales_pipeline_share ADD ACCOUNTS = 'finance_dept', 'exec_dept';

-- MARKETING DEPARTMENT (consumer of both):
CREATE DATABASE finance_data FROM SHARE finance_dept.finance_metrics_share;
CREATE DATABASE sales_data FROM SHARE sales_dept.sales_pipeline_share;

-- Marketing can now analyze:
SELECT f.revenue, s.pipeline_amount
FROM finance_data.shared.revenue_metrics f
JOIN sales_data.shared.pipeline s ON f.business_unit = s.business_unit;
-- Cross-department analytics without ETL!
```

---

## Pattern 3: Data Product Monetization

```sql
-- Weather data company sells forecast data via Marketplace

-- Product setup:
-- Tier 1: Free (last 7 days, 2 cities, hourly)
CREATE SECURE VIEW marketplace.weather_free AS
    SELECT city, observation_time, temperature, humidity, conditions
    FROM weather.observations
    WHERE observation_time >= DATEADD('day', -7, CURRENT_DATE())
      AND city IN ('New York', 'Los Angeles');

-- Tier 2: Standard ($500/month — all US cities, 90 days, hourly)
CREATE SECURE VIEW marketplace.weather_standard AS
    SELECT city, state, observation_time, temperature, humidity, 
           wind_speed, precipitation, conditions, forecast_24h
    FROM weather.observations
    WHERE observation_time >= DATEADD('day', -90, CURRENT_DATE())
      AND country = 'US';

-- Tier 3: Enterprise ($2000/month — global, 5 years, minute-level)  
CREATE SECURE VIEW marketplace.weather_enterprise AS
    SELECT * FROM weather.observations
    WHERE observation_time >= DATEADD('year', -5, CURRENT_DATE());

-- Publish to Marketplace:
-- Free tier: public listing (anyone can get)
-- Standard: private listing ($500/month auto-billing via Snowflake)
-- Enterprise: private listing ($2000/month, requires contract)
```

---

## Pattern 4: Automated Share Management

```python
# Manage 50+ shares programmatically (API-based)

import snowflake.connector
from datetime import datetime

class ShareManager:
    """Automate share lifecycle: create, maintain, audit, expire."""
    
    def __init__(self, conn):
        self.conn = conn
    
    def onboard_partner(self, partner_name: str, partner_account: str, 
                       brand_id: str, contract_end: datetime):
        """Create share and configure access for new partner."""
        
        # Register partner mapping
        self.conn.cursor().execute(f"""
            INSERT INTO governance.partner_registry 
            (partner_name, snowflake_account, brand_id, contract_end, active)
            VALUES ('{partner_name}', '{partner_account}', '{brand_id}', 
                    '{contract_end}', TRUE)
        """)
        
        # Add to universal share (secure view uses CURRENT_ACCOUNT for filtering)
        self.conn.cursor().execute(f"""
            ALTER SHARE brand_partner_share ADD ACCOUNTS = '{partner_account}'
        """)
        
        # Update partner → brand mapping table (used by secure view)
        self.conn.cursor().execute(f"""
            INSERT INTO production.dim.partner_accounts 
            (partner_name, snowflake_account, brand_id)
            VALUES ('{partner_name}', '{partner_account}', '{brand_id}')
        """)
        
        return f"Partner {partner_name} onboarded to share"
    
    def offboard_partner(self, partner_account: str):
        """Revoke access when contract expires."""
        self.conn.cursor().execute(f"""
            ALTER SHARE brand_partner_share REMOVE ACCOUNTS = '{partner_account}'
        """)
        self.conn.cursor().execute(f"""
            UPDATE governance.partner_registry SET active = FALSE 
            WHERE snowflake_account = '{partner_account}'
        """)
    
    def audit_shares(self) -> dict:
        """Monthly audit of all active shares."""
        result = self.conn.cursor().execute("""
            SELECT share_name, 
                   ARRAY_AGG(granted_to) AS consumers,
                   COUNT(*) AS object_count
            FROM SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_SHARES
            GROUP BY share_name
        """).fetchall()
        return result

# Automated: offboard partners whose contracts expired
# (Run as scheduled task daily)
```

---

## Pattern 5: Cross-Cloud Data Sharing

```sql
-- Challenge: Provider on AWS (us-east-1), Consumer on Azure (westeurope)
-- Solution: Replicate to Azure region, then share locally

-- Step 1: Enable cross-cloud replication
ALTER DATABASE production ENABLE REPLICATION TO ACCOUNTS
    org_name.azure_westeurope_account;

-- Step 2: Create replica in Azure region
-- (On the Azure account):
CREATE DATABASE production_eu AS REPLICA OF org_name.aws_us_east_1.production;

-- Step 3: Auto-refresh replica (keep in sync)
ALTER DATABASE production_eu REFRESH;
-- Schedule: every 15 minutes (or configure auto-refresh)

-- Step 4: Share from the Azure replica to EU consumers
CREATE SHARE eu_analytics_share;
GRANT SELECT ON VIEW production_eu.shared.eu_metrics TO SHARE eu_analytics_share;
ALTER SHARE eu_analytics_share ADD ACCOUNTS = 'eu_partner_account';

-- EU consumer queries locally (no cross-cloud transfer per query!):
-- Latency: same as local query
-- Cost: replication transfer + storage (one-time per refresh, not per query)

-- COST ANALYSIS:
-- 100 GB replicated to EU: $0.02/GB transfer = $2 per refresh
-- Daily refresh: $2 × 1 = $2/day
-- vs per-query cross-cloud: depends on query scan size
-- If consumer queries 10 GB/day across cloud: $0.02 × 10 = $0.20/day (cheaper!)
-- If consumer queries 500 GB/day: $10/day (replication is cheaper!)
-- Break-even: ~100 GB queried/day → above that, replication wins
```

---

## Interview Tips

> **Tip 1:** "How do you share data with 50 partners while keeping data isolated?" — Single secure view with `CURRENT_ACCOUNT()` filtering + mapping table. One share for all partners (add accounts as needed). Each partner sees only their data. Onboard/offboard via API automation. Scales to hundreds of partners without creating hundreds of views.

> **Tip 2:** "How do you sell data on Snowflake Marketplace?" — Create tiered offerings (free sample → paid full dataset). Use secure views to control what each tier sees. Publish as Marketplace listing (public or private). Snowflake handles billing and metering. Consumer clicks "Get" → instant access. Revenue collected by Snowflake → paid to you.

> **Tip 3:** "Data sharing across AWS and Azure?" — Cross-cloud requires database replication to the target cloud. Replicate the database to an account in the consumer's cloud/region. Share locally from the replica. Consumer queries are local (fast, no per-query transfer). Replication has ongoing cost (transfer + storage), but eliminates per-query cross-cloud charges. Use when consumers query frequently.
