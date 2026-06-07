---
title: "Data Sharing - Scenario Questions"
topic: snowflake
subtopic: data-sharing
content_type: scenario_question
tags: [snowflake, data-sharing, interview, scenarios]
---

# Scenario Questions — Data Sharing

<article data-difficulty="junior">

## 🟢 Junior: Creating a Basic Share

**Scenario:** Share the `gold.daily_revenue` table with a partner company (account: `partner_xyz`). They should see revenue data but NOT customer-level details.

<details>
<summary>💡 Hint</summary>
Create a secure view that exposes only aggregate data (no customer details). Share the view (not the underlying table). Use a secure view to hide the query logic.
</details>

<details>
<summary>✅ Solution</summary>

```sql
-- Step 1: Create secure view (controls what partner sees)
CREATE SECURE VIEW production.shared.partner_revenue AS
    SELECT revenue_date, region, total_orders, total_revenue
    -- EXCLUDED: customer_id, customer_name, order-level details
    FROM production.gold.daily_revenue;

-- Step 2: Create share
CREATE SHARE partner_analytics_share
    COMMENT = 'Daily revenue metrics for Partner XYZ';

-- Step 3: Grant objects to share
GRANT USAGE ON DATABASE production TO SHARE partner_analytics_share;
GRANT USAGE ON SCHEMA production.shared TO SHARE partner_analytics_share;
GRANT SELECT ON VIEW production.shared.partner_revenue TO SHARE partner_analytics_share;

-- Step 4: Add partner account
ALTER SHARE partner_analytics_share ADD ACCOUNTS = 'partner_xyz';

-- PARTNER SIDE (they run this):
CREATE DATABASE revenue_from_provider FROM SHARE your_account.partner_analytics_share;
SELECT * FROM revenue_from_provider.shared.partner_revenue WHERE revenue_date >= '2024-01-01';
```

**Key Points:**
- Share a SECURE VIEW (not the base table) to control visible columns
- SECURE keyword: partner can't see view definition (SHOW CREATE VIEW blocked)
- Partner gets: read-only access to the view's output (aggregate revenue)
- Partner can't see: customer details, view logic, underlying tables
- Zero data copy: partner reads directly from your storage
- Always current: when you update gold.daily_revenue, partner sees changes immediately

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Multi-Tenant Sharing

**Scenario:** You have 30 brand partners, each needing to see only THEIR orders from your platform. Design a scalable sharing solution (not 30 separate views).

<details>
<summary>💡 Hint</summary>
Use CURRENT_ACCOUNT() in a single secure view. Map each partner's Snowflake account to their brand_id. One share serves all partners with automatic row-level filtering.
</details>

<details>
<summary>✅ Solution</summary>

```sql
-- Mapping table: which Snowflake account belongs to which brand
CREATE TABLE production.config.partner_mapping (
    partner_name VARCHAR,
    snowflake_account VARCHAR,
    brand_id VARCHAR
);
INSERT INTO production.config.partner_mapping VALUES
    ('Nike', 'NIKE_ACCOUNT_123', 'BRAND_NIKE'),
    ('Adidas', 'ADIDAS_ACCOUNT_456', 'BRAND_ADIDAS'),
    -- ... 30 partners
    ('Puma', 'PUMA_ACCOUNT_789', 'BRAND_PUMA');

-- ONE secure view serves ALL 30 partners:
CREATE SECURE VIEW production.shared.my_brand_orders AS
    SELECT o.order_id, o.order_date, o.quantity, o.revenue, o.product_name, o.region
    FROM production.gold.orders o
    JOIN production.config.partner_mapping pm 
        ON o.brand_id = pm.brand_id
    WHERE pm.snowflake_account = CURRENT_ACCOUNT();
-- CURRENT_ACCOUNT() = the querying partner's account identifier
-- Nike queries → sees Nike orders. Adidas queries → sees Adidas orders. Automatic!

-- ONE share for all partners:
CREATE SHARE universal_brand_share;
GRANT USAGE ON DATABASE production TO SHARE universal_brand_share;
GRANT USAGE ON SCHEMA production.shared TO SHARE universal_brand_share;
GRANT SELECT ON VIEW production.shared.my_brand_orders TO SHARE universal_brand_share;

-- Add all 30 partner accounts:
ALTER SHARE universal_brand_share ADD ACCOUNTS = 
    'NIKE_ACCOUNT_123', 'ADIDAS_ACCOUNT_456', 'PUMA_ACCOUNT_789'; -- ... all 30

-- ONBOARD new partner: just add to mapping table + add account to share!
INSERT INTO production.config.partner_mapping VALUES ('NewBrand', 'NEW_ACCOUNT', 'BRAND_NEW');
ALTER SHARE universal_brand_share ADD ACCOUNTS = 'NEW_ACCOUNT';
-- Done! No new view needed. The secure view automatically filters for them.
```

**Key Points:**
- ONE secure view + ONE share serves ALL 30 partners (scalable!)
- CURRENT_ACCOUNT() provides automatic row-level filtering per consumer
- Adding a new partner: 1 INSERT + 1 ALTER SHARE (2 commands!)
- No per-partner views: eliminates 30× view management
- Security: secure view hides filtering logic; partners can't see other brands
- Performance: Snowflake optimizes the JOIN at query time (efficient)

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Data Product Platform

**Scenario:** Design a data monetization platform: sell weather data on Snowflake Marketplace with 3 tiers (Free/Standard/$2K Enterprise), track usage per consumer, auto-expire trial accounts, and handle cross-region delivery.

<details>
<summary>💡 Hint</summary>
Tiered secure views (each tier sees different data scope). Marketplace listings per tier. Usage tracking via ACCOUNT_USAGE. Trial expiration via automated tasks. Cross-region via database replication.
</details>

<details>
<summary>✅ Solution</summary>

```sql
-- TIERED DATA PRODUCTS:

-- FREE tier: 7 days, 5 cities
CREATE SECURE VIEW marketplace.weather_free AS
    SELECT city, observation_time, temperature, conditions
    FROM weather.observations
    WHERE observation_time >= DATEADD('day', -7, CURRENT_DATE())
      AND city IN ('New York', 'London', 'Tokyo', 'Sydney', 'Berlin');

-- STANDARD tier ($500/mo): 90 days, all US/EU cities, forecasts
CREATE SECURE VIEW marketplace.weather_standard AS
    SELECT city, country, observation_time, temperature, humidity,
           wind_speed, precipitation, conditions, forecast_24h, forecast_7d
    FROM weather.observations
    WHERE observation_time >= DATEADD('day', -90, CURRENT_DATE())
      AND country IN ('US', 'GB', 'DE', 'FR', 'JP', 'AU');

-- ENTERPRISE tier ($2000/mo): 5 years, global, minute-level, custom
CREATE SECURE VIEW marketplace.weather_enterprise AS
    SELECT * FROM weather.observations  -- Everything!
    WHERE observation_time >= DATEADD('year', -5, CURRENT_DATE());

-- MARKETPLACE LISTINGS (configured in UI → Provider Studio):
-- Listing 1: "Weather Data - Free Sample" (public, free, points to weather_free)
-- Listing 2: "Weather Data - Standard" (private, $500/mo, points to weather_standard)
-- Listing 3: "Weather Data - Enterprise" (private, $2000/mo, requires contract)

-- USAGE TRACKING:
CREATE TASK monetization.track_usage
    WAREHOUSE = 'ADMIN_XS'
    SCHEDULE = 'USING CRON 0 * * * * UTC'
AS
    INSERT INTO monetization.consumer_usage (consumer_account, tier, queries, timestamp)
    SELECT 
        target_account,
        CASE 
            WHEN listing_name LIKE '%Enterprise%' THEN 'enterprise'
            WHEN listing_name LIKE '%Standard%' THEN 'standard'
            ELSE 'free'
        END AS tier,
        COUNT(*) AS queries,
        CURRENT_TIMESTAMP()
    FROM SNOWFLAKE.ACCOUNT_USAGE.DATA_TRANSFER_HISTORY
    WHERE START_TIME >= DATEADD('hour', -1, CURRENT_TIMESTAMP())
    GROUP BY target_account, tier;

-- TRIAL EXPIRATION:
CREATE TASK monetization.expire_trials
    WAREHOUSE = 'ADMIN_XS'
    SCHEDULE = 'USING CRON 0 0 * * * UTC'
AS
BEGIN
    -- Find trial accounts older than 14 days
    FOR trial IN (
        SELECT consumer_account FROM monetization.trials
        WHERE start_date < DATEADD('day', -14, CURRENT_DATE()) AND active = TRUE
    ) DO
        -- Revoke access
        EXECUTE IMMEDIATE 'ALTER SHARE weather_free_share REMOVE ACCOUNTS = ''' || 
            trial.consumer_account || '''';
        UPDATE monetization.trials SET active = FALSE 
        WHERE consumer_account = trial.consumer_account;
    END FOR;
END;

-- CROSS-REGION DELIVERY:
-- Replicate weather database to regions where consumers exist:
ALTER DATABASE weather ENABLE REPLICATION TO ACCOUNTS
    org_name.eu_west_account, org_name.ap_southeast_account;
-- Create replicas in those regions:
-- EU consumers query EU replica (low latency, no cross-region transfer per query)
```

**Key Points:**
- Three tiers via secure views (same data, different access scopes)
- Snowflake Marketplace handles billing ($500/$2000 subscriptions)
- Usage tracking: DATA_TRANSFER_HISTORY shows consumer activity
- Trial expiration: automated task revokes access after 14 days
- Cross-region: database replication for consumers in other regions
- Revenue tracking: monitor subscriptions + usage for business reporting
- Zero data management for consumers (they just "Get" the listing)

</details>

</article>
