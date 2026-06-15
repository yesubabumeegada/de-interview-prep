---
title: "Snowflake Data Clean Rooms Scenario Questions"
description: "Interview scenarios covering clean room vs. data sharing decisions, clean room design for retail advertising, and architecting privacy-preserving audience measurement systems"
content_type: scenario_question
topic: snowflake
subtopic: data-clean-rooms
tags: [snowflake, data-clean-rooms, privacy, advertising, audience-measurement, GDPR, native-app, differential-privacy, scenarios]
---

<article data-difficulty="junior">

## Scenario: Clean Room vs. Regular Data Sharing

You are a junior data engineer at a retail company. Your marketing team has two upcoming data collaboration requests:

**Request A**: Your logistics vendor wants to access your product catalog (product IDs, names, weights, dimensions, SKUs) to improve their shipping cost calculations.

**Request B**: Your advertising agency wants to combine your customer purchase data with their ad exposure data to measure whether last month's digital campaign drove sales.

Your manager asks you: "Should we use Snowflake Data Sharing or a Data Clean Room for each of these? What's the difference?"

<details>
<summary>✅ Solution</summary>

**The Key Distinction**

Snowflake Data Sharing and Data Clean Rooms solve different problems:

| Feature | Snowflake Data Sharing | Data Clean Room |
|---------|----------------------|-----------------|
| Consumer sees raw data | ✅ Yes (read-only) | ❌ No — aggregates only |
| Consumer runs arbitrary queries | ✅ Yes | ❌ No — pre-approved templates only |
| Privacy preserving | Only if data is non-sensitive | ✅ By design |
| PII protection | No built-in protection | ✅ Core feature |
| Use case | Reference data, non-sensitive collaboration | Sensitive data, PII, competitive data |

**Request A: Product Catalog → Use Regular Data Sharing**

The product catalog (product IDs, names, dimensions, SKUs) contains **no personal data**. It is reference data that the vendor legitimately needs to see in full to do their job.

```sql
-- Create a share for the logistics vendor
CREATE SHARE logistics_vendor_share;

-- Grant access to the product catalog database/schema/table
GRANT USAGE ON DATABASE products_db TO SHARE logistics_vendor_share;
GRANT USAGE ON SCHEMA products_db.public TO SHARE logistics_vendor_share;
GRANT SELECT ON TABLE products_db.public.product_catalog TO SHARE logistics_vendor_share;

-- Add the vendor's Snowflake account
ALTER SHARE logistics_vendor_share
  ADD ACCOUNTS = logistics_partner_account_locator;
```

Why not a clean room?
- No privacy risk — product dimensions are not PII
- The vendor needs to see actual SKUs and weights to compute shipping costs; aggregates would be useless
- Clean rooms add unnecessary complexity and cost

**Request B: Customer Purchase Data + Ad Exposure → Use a Data Clean Room**

The marketing campaign data involves:
- Your data: customer emails, purchase history, amounts → **PII** protected by GDPR/CCPA
- Agency data: which emails/users were shown which ads → **behavioral data**

You cannot share raw purchase records with the advertising agency because:
1. It exposes customer PII to a third party (GDPR requires legal basis and data minimization)
2. Competitively, your customer list is valuable — you don't want the agency to have it
3. The agency doesn't need individual records — they need aggregate metrics (conversion rate, ROAS)

```sql
-- The agency sets up a clean room (or uses one from a platform like Google/Meta)
-- You link your hashed purchase data as the consumer
-- You run an approved analysis template: "how many users who saw ads also purchased?"

-- Result you receive (aggregate only):
-- CAMPAIGN_A: 2.1M impressions, 89,400 matched customers, 3.2% conversion rate
-- CAMPAIGN_B: 1.4M impressions, 52,100 matched customers, 4.8% conversion rate

-- The agency never sees your customer list
-- You never see who specifically was shown which ad
```

**Summary Answer for Your Manager**:

Use **Data Sharing** for Request A because the product catalog is non-sensitive reference data that the vendor needs to see in full. Use a **Data Clean Room** for Request B because it involves PII and sensitive customer data — the agency only needs aggregate campaign metrics, not individual customer records. A clean room gives them the insights they need while protecting your customers' privacy and your competitive data.

</details>

</article>

<article data-difficulty="mid">

## Scenario: Design a Clean Room for Retail Ad Campaign Overlap

You work at a retail media company. Two of your clients — a grocery chain (Retailer X) and a consumer packaged goods company (Brand Y, e.g., a cereal brand) — want to collaborate:

- **Retailer X** has: loyalty card purchase history, customer emails, store visit data
- **Brand Y** has: digital ad exposure data (which hashed emails saw their cereal ads on social media, display, and video), and their own DTC website visitor data

Their joint goal: Understand how Brand Y's digital advertising influenced in-store and online purchases of their cereal products at Retailer X.

You need to design and configure a Snowflake Data Clean Room that enables this analysis while meeting these constraints:
- Neither party can see the other's raw customer records
- Minimum cohort size of 50 for all results
- Only aggregate metrics can leave the clean room
- Must be GDPR and CCPA compliant
- Brand Y should be able to run 3 types of analysis: reach overlap, conversion attribution, and frequency analysis

**Question**: Walk through the design, data preparation, and configuration of this clean room.

<details>
<summary>✅ Solution</summary>

## Step 1: Choose the Clean Room Architecture

Since Retailer X has the purchase data (the ground truth for conversions), they should be the **Provider** — they control what data is accessible and what queries are allowed.

Brand Y is the **Consumer** — they install the clean room in their Snowflake account and bring their ad exposure data.

```
PROVIDER: Retailer X (Snowflake Account: retailer_x_account)
  → Shares: loyalty purchase data (hashed)
  → Defines: allowed query templates, minimum thresholds

CONSUMER: Brand Y (Snowflake Account: brand_y_account)
  → Brings: ad exposure data (hashed)
  → Runs: approved analyses only
```

## Step 2: Data Preparation (Both Sides)

**Retailer X data preparation**:

```sql
-- Retailer X: prepare loyalty purchase data for clean room
-- Must hash all PII before sharing
CREATE OR REPLACE TABLE loyalty_purchases_cleanroom AS
SELECT
  -- Hash email with consistent normalization
  SHA2(LOWER(TRIM(loyalty_email)), 256) AS hashed_email,
  -- Remove exact dates (use week buckets to reduce re-identification risk)
  DATE_TRUNC('week', purchase_date) AS purchase_week,
  product_category,
  brand_name,
  -- Bucket amounts (avoid exact revenue which could identify individuals)
  CASE
    WHEN total_spend < 5 THEN '0-5'
    WHEN total_spend < 20 THEN '5-20'
    WHEN total_spend < 50 THEN '20-50'
    ELSE '50+'
  END AS spend_bucket,
  store_type  -- 'online', 'in-store'
FROM loyalty_transactions
WHERE purchase_date >= DATEADD('day', -90, CURRENT_DATE())
  AND loyalty_email IS NOT NULL
  AND gdpr_consent = TRUE  -- GDPR consent gate
  AND ccpa_opted_out = FALSE;  -- CCPA opt-out gate

-- Cluster by hashed_email for join performance
ALTER TABLE loyalty_purchases_cleanroom CLUSTER BY (hashed_email);
```

**Brand Y data preparation**:

```sql
-- Brand Y: prepare ad exposure data
-- Critical: must use SAME hashing approach as Retailer X
CREATE OR REPLACE TABLE ad_exposures_cleanroom AS
SELECT
  SHA2(LOWER(TRIM(matched_email)), 256) AS hashed_email,
  campaign_id,
  ad_channel,        -- 'social', 'display', 'video', 'search'
  exposure_count,    -- number of times shown this ad
  first_exposure_ts,
  last_exposure_ts
FROM ad_exposure_data
WHERE matched_email IS NOT NULL
  AND consent_to_match = TRUE;   -- user consented to email matching by the ad platform
```

## Step 3: Clean Room Configuration (Provider Side)

```sql
-- On Retailer X's Snowflake account
SET cleanroom_name = 'brand_y_grocery_cleanroom';

CALL samooha_provider_app.CORE.CREATE_CLEANROOM($cleanroom_name);

-- Add Brand Y as the consumer
CALL samooha_provider_app.CORE.ADD_CONSUMER(
  $cleanroom_name,
  'BRAND_Y_ACCOUNT_LOCATOR'
);

-- Link Retailer X's purchase data (what Brand Y's analyses can join against)
CALL samooha_provider_app.CORE.LINK_DATASETS(
  $cleanroom_name,
  ['retailer_x_cleanroom_db.shared.loyalty_purchases_cleanroom']
);

-- Set minimum threshold
CALL samooha_provider_app.CORE.SET_DEFAULT_MIN_THRESHOLD($cleanroom_name, 50);
```

## Step 4: Define the Three Analysis Templates

**Template 1: Reach Overlap**
```sql
CALL samooha_provider_app.CORE.ADD_ANALYSIS_TEMPLATE(
  $cleanroom_name,
  'reach_overlap',
  $$
  SELECT
    'Audience Reach Summary' AS analysis_type,
    COUNT(DISTINCT r.hashed_email) AS retailer_x_loyalty_base,
    COUNT(DISTINCT c.hashed_email) AS brand_y_exposed_audience,
    COUNT(DISTINCT CASE WHEN r.hashed_email IS NOT NULL AND c.hashed_email IS NOT NULL
                        THEN r.hashed_email END) AS matched_audience,
    ROUND(
      COUNT(DISTINCT CASE WHEN r.hashed_email IS NOT NULL AND c.hashed_email IS NOT NULL
                          THEN r.hashed_email END) /
      COUNT(DISTINCT c.hashed_email) * 100, 2
    ) AS reach_pct_of_exposed
  FROM samooha_provider_app.SHARED_SCHEMA.loyalty_purchases_cleanroom r
  FULL OUTER JOIN {{ consumer_table }} c ON r.hashed_email = c.hashed_email
  WHERE c.campaign_id = '{{ campaign_id }}'
  HAVING matched_audience >= 50
  $$
);
```

**Template 2: Conversion Attribution**
```sql
CALL samooha_provider_app.CORE.ADD_ANALYSIS_TEMPLATE(
  $cleanroom_name,
  'conversion_attribution',
  $$
  SELECT
    c.ad_channel,
    c.campaign_id,
    COUNT(DISTINCT c.hashed_email) AS exposed_users,
    COUNT(DISTINCT CASE
      WHEN r.hashed_email IS NOT NULL
        AND r.brand_name = 'Brand Y Cereal'
        AND r.purchase_week >= DATE_TRUNC('week', c.first_exposure_ts::DATE)
        AND r.purchase_week <= DATE_TRUNC('week', DATEADD('day', 14, c.last_exposure_ts::DATE))
      THEN c.hashed_email END) AS converters_in_14d_window,
    ROUND(
      COUNT(DISTINCT CASE WHEN r.hashed_email IS NOT NULL ... THEN c.hashed_email END) /
      NULLIF(COUNT(DISTINCT c.hashed_email), 0) * 100, 3
    ) AS conversion_rate_pct
  FROM {{ consumer_table }} c
  LEFT JOIN samooha_provider_app.SHARED_SCHEMA.loyalty_purchases_cleanroom r
    ON c.hashed_email = r.hashed_email
  WHERE c.campaign_id = '{{ campaign_id }}'
    AND c.ad_channel IN ({{ channels }})
  GROUP BY c.ad_channel, c.campaign_id
  HAVING COUNT(DISTINCT c.hashed_email) >= 50
  ORDER BY conversion_rate_pct DESC
  $$
);
```

**Template 3: Frequency Analysis**
```sql
CALL samooha_provider_app.CORE.ADD_ANALYSIS_TEMPLATE(
  $cleanroom_name,
  'frequency_analysis',
  $$
  SELECT
    CASE
      WHEN c.exposure_count = 1 THEN '1 exposure'
      WHEN c.exposure_count BETWEEN 2 AND 3 THEN '2-3 exposures'
      WHEN c.exposure_count BETWEEN 4 AND 7 THEN '4-7 exposures'
      WHEN c.exposure_count >= 8 THEN '8+ exposures'
    END AS frequency_bucket,
    COUNT(DISTINCT c.hashed_email) AS users_in_bucket,
    COUNT(DISTINCT CASE WHEN r.brand_name = 'Brand Y Cereal'
                        THEN c.hashed_email END) AS purchasers_in_bucket,
    ROUND(
      COUNT(DISTINCT CASE WHEN r.brand_name = 'Brand Y Cereal' THEN c.hashed_email END) /
      COUNT(DISTINCT c.hashed_email) * 100, 2
    ) AS purchase_rate_pct
  FROM {{ consumer_table }} c
  LEFT JOIN samooha_provider_app.SHARED_SCHEMA.loyalty_purchases_cleanroom r
    ON c.hashed_email = r.hashed_email
  WHERE c.campaign_id = '{{ campaign_id }}'
  GROUP BY frequency_bucket
  HAVING COUNT(DISTINCT c.hashed_email) >= 50
  ORDER BY MIN(c.exposure_count)
  $$
);
```

## Step 5: Consumer Setup and Running Analyses

```sql
-- On Brand Y's account: install the clean room and link their ad exposure data
CALL brand_y_grocery_cleanroom.CORE.LINK_CONSUMER_DATASET(
  'brand_y_analytics.marketing.ad_exposures_cleanroom',
  'brand_y_exposures'
);

-- Grant analyst role to the marketing data scientist
GRANT ROLE brand_y_grocery_cleanroom.CORE.ANALYST TO USER marketing_data_scientist;

-- Run analyses
CALL brand_y_grocery_cleanroom.CORE.RUN_ANALYSIS(
  'reach_overlap',
  OBJECT_CONSTRUCT('consumer_table', 'brand_y_exposures', 'campaign_id', 'Q4_CEREAL_CAMPAIGN')
);

CALL brand_y_grocery_cleanroom.CORE.RUN_ANALYSIS(
  'conversion_attribution',
  OBJECT_CONSTRUCT(
    'consumer_table', 'brand_y_exposures',
    'campaign_id', 'Q4_CEREAL_CAMPAIGN',
    'channels', ARRAY_CONSTRUCT('social', 'display', 'video')
  )
);
```

## GDPR/CCPA Compliance Checklist

- [x] Only consented users included in both datasets (gated at data prep)
- [x] Hashed PII — no raw emails in clean room
- [x] Minimum threshold 50 — no individual-level output
- [x] Approved query templates only — no ad-hoc SQL by Brand Y
- [x] Audit log of all analysis runs maintained
- [x] Right to erasure: propagated by re-running the hashed export that excludes deleted users
- [x] Data Processing Agreement (DPA) between Retailer X and Brand Y covers the clean room usage

</details>

</article>

<article data-difficulty="senior">

## Scenario: Architect a Privacy-Preserving Audience Measurement System

You are the lead data architect at a large media measurement company. Three major broadcasters (A, B, C) and five major advertisers want to move to a new audience measurement system after the deprecation of third-party cookies and changes to cross-app tracking on mobile (Apple ATT).

The goal: measure unduplicated reach and frequency of ad campaigns across all three broadcaster platforms, and measure conversion lift (did people who saw ads on these broadcasters subsequently purchase the advertiser's products?).

**Constraints and requirements**:
- Broadcasters will NOT share individual viewer data with advertisers or with each other
- Advertisers will NOT share their customer purchase lists with broadcasters or each other
- GDPR and CCPA compliance required — only consented users can be included
- Scale: 200M+ hashed profiles across all broadcasters; 50M+ consumer purchase records per advertiser
- Must support cross-device matching (a user may watch on TV, tablet, and mobile)
- Latency: campaign reports must be available within 24 hours of campaign end
- Auditors must be able to verify that privacy controls are working
- The system must eventually be monetized as a SaaS product (advertiser subscription)

**Question**: Design the complete architecture for this system. Cover the data model, clean room topology, privacy mechanisms, performance approach, and monetization model.

<details>
<summary>✅ Solution</summary>

## Architecture Overview

This is a **multi-party consortium clean room** — the most complex type. No single party can be both provider and operator. A **neutral operator** (our measurement company) is required.

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                    MEASUREMENT COMPANY (Neutral Operator)                     │
│                    Snowflake Account: measurement_co_account                  │
│                                                                               │
│    ┌──────────────────────────────────────────────────────────────────────┐  │
│    │                  CONSORTIUM CLEAN ROOM (Native App)                   │  │
│    │                                                                        │  │
│    │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐               │  │
│    │  │ Broadcaster A │  │ Broadcaster B │  │ Broadcaster C │               │  │
│    │  │ ad_exposures  │  │ ad_exposures  │  │ ad_exposures  │               │  │
│    │  │ (hashed IDs)  │  │ (hashed IDs)  │  │ (hashed IDs)  │               │  │
│    │  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘               │  │
│    │         └─────────────────┼─────────────────┘                        │  │
│    │                           │ identity resolution (hashed spine)        │  │
│    │                    ┌──────▼───────┐                                   │  │
│    │                    │ Identity     │                                   │  │
│    │                    │ Resolution   │  ← neutral spine, no raw data     │  │
│    │                    │ Layer        │                                   │  │
│    │                    └──────┬───────┘                                   │  │
│    │                           │                                           │  │
│    │  ┌────────────────────────▼──────────────────────────────────────┐  │  │
│    │  │                   Analysis Engine                              │  │  │
│    │  │  - Unduplicated reach                                          │  │  │
│    │  │  - Cross-broadcaster frequency                                 │  │  │
│    │  │  - Conversion lift (joined to advertiser data)                 │  │  │
│    │  └────────────────────────────────────────────────────────────────┘  │  │
│    └──────────────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────────────┘
                ▲ each broadcaster shares data via Snowflake Secure Share
                ▼ each advertiser installs a consumer clean room
```

## Data Model

### The Identity Spine

The hardest part of cross-broadcaster measurement is identity. A user watching on Broadcaster A's app is identified by a different ID than on Broadcaster B. We need a **privacy-safe identity graph**.

```sql
-- Identity spine maintained by measurement company
-- Maps different platform IDs to a common hashed spine ID
-- The raw mapping is NEVER shared; only the hashed spine IDs are used for analysis

CREATE TABLE identity_spine (
  spine_id              VARCHAR(64),  -- Our internal hashed ID (never exposed externally)
  hashed_email          VARCHAR(64),  -- SHA-256 of normalized email
  hashed_phone          VARCHAR(64),  -- SHA-256 of normalized phone (E.164)
  hashed_broadcast_id_a VARCHAR(64),  -- SHA-256 of Broadcaster A's viewer ID
  hashed_broadcast_id_b VARCHAR(64),  -- SHA-256 of Broadcaster B's viewer ID
  hashed_broadcast_id_c VARCHAR(64),  -- SHA-256 of Broadcaster C's viewer ID
  device_count          INTEGER,      -- number of linked devices
  hh_id                 VARCHAR(64),  -- household ID (for CTV measurement)
  consent_status        VARCHAR(20),  -- 'CONSENTED', 'OPT_OUT', 'UNKNOWN'
  last_updated          TIMESTAMP
);
-- This spine is the core IP of the measurement company
-- It never leaves the measurement company's environment
```

### Broadcaster Data (Shared into Clean Room)

```sql
-- Each broadcaster shares a view of their ad exposure data
-- Using their broadcaster-specific IDs (already hashed)

-- Broadcaster A's share
CREATE SECURE VIEW broadcaster_a_exposures_share AS
SELECT
  SHA2(CONCAT(viewer_id, 'BCAST_A_SALT'), 256) AS hashed_viewer_id,
  campaign_id,
  creative_id,
  exposure_ts,
  device_type,     -- 'CTV', 'mobile', 'desktop', 'tablet'
  program_id,      -- which show/content the ad appeared in
  exposure_duration_sec,
  -- NO: viewer name, address, demographics (separate data deal for demographics)
FROM viewer_ad_exposures
WHERE consent_to_measurement = TRUE;
```

### Identity Resolution Without Raw Data

```sql
-- Resolution function in the clean room: maps hashed broadcaster IDs to spine IDs
-- Uses the identity spine to join across broadcasters
-- Key: the spine is only accessible to the measurement company's operators, not broadcasters or advertisers

CREATE SECURE FUNCTION resolve_to_spine(
  hashed_broadcaster_id VARCHAR,
  broadcaster_code VARCHAR
) RETURNS VARCHAR
AS $$
  SELECT spine_id
  FROM identity_spine
  WHERE (broadcaster_code = 'A' AND hashed_broadcast_id_a = hashed_broadcaster_id)
     OR (broadcaster_code = 'B' AND hashed_broadcast_id_b = hashed_broadcaster_id)
     OR (broadcaster_code = 'C' AND hashed_broadcast_id_c = hashed_broadcaster_id)
  LIMIT 1
$$;
-- This function is accessible only within the clean room app — not to broadcasters or advertisers
```

## Privacy Mechanisms

### Layer 1: Hashing at Source
Each broadcaster hashes their own viewer IDs with their own salt before sharing. The measurement company maps to the spine via their identity graph.

### Layer 2: Minimum Thresholds with Tiering

```sql
-- Tiered thresholds based on sensitivity of the report
FUNCTION apply_threshold(cohort_size INTEGER, report_type VARCHAR) RETURNS BOOLEAN AS $$
  CASE report_type
    WHEN 'REACH_TOTAL' THEN cohort_size >= 1000     -- broad metric, low threshold
    WHEN 'REACH_BY_SHOW' THEN cohort_size >= 500
    WHEN 'REACH_BY_DEMO' THEN cohort_size >= 250    -- demographic cuts need higher threshold
    WHEN 'CONVERSION_LIFT' THEN cohort_size >= 100
    WHEN 'FREQUENCY_DIST' THEN cohort_size >= 1000
    ELSE cohort_size >= 1000
  END
$$;
```

### Layer 3: Differential Privacy for Fine-Grained Reports

```sql
-- For advertiser-requested demographic breakdowns, apply DP noise
-- Global sensitivity = 1 (one person can change count by 1)
-- Epsilon = 2.0 (moderate privacy; high accuracy on large cohorts)
SELECT
  age_bracket,
  gender,
  geo_region,
  dp_laplace_noise(COUNT(DISTINCT spine_id)::FLOAT, 1.0, 2.0) AS dp_unique_reach,
  dp_laplace_noise(AVG(exposure_count)::FLOAT, 5.0, 2.0) AS dp_avg_frequency
FROM cross_broadcaster_exposures cbx
JOIN identity_spine s ON cbx.spine_id = s.spine_id
GROUP BY age_bracket, gender, geo_region
HAVING COUNT(DISTINCT spine_id) >= 250;
```

## Performance Architecture

At 200M+ profiles, performance requires careful design:

```sql
-- Clustering strategy for the main exposure fact table
CREATE TABLE cross_broadcaster_exposures (
  spine_id      VARCHAR(64),
  campaign_id   VARCHAR(50),
  broadcaster   VARCHAR(5),
  exposure_date DATE,
  device_type   VARCHAR(20),
  program_id    VARCHAR(50)
)
CLUSTER BY (campaign_id, exposure_date);  -- most analyses filter by campaign + date

-- Materialized view: pre-compute spine-level reach per campaign
-- Refreshed nightly; enables sub-second reporting
CREATE DYNAMIC TABLE campaign_reach_materialized
  TARGET_LAG = '1 hour'
  WAREHOUSE = measurement_wh
AS
SELECT
  campaign_id,
  COUNT(DISTINCT spine_id) AS total_unique_reach,
  COUNT(DISTINCT CASE WHEN broadcaster = 'A' THEN spine_id END) AS reach_broadcaster_a,
  COUNT(DISTINCT CASE WHEN broadcaster = 'B' THEN spine_id END) AS reach_broadcaster_b,
  COUNT(DISTINCT CASE WHEN broadcaster = 'C' THEN spine_id END) AS reach_broadcaster_c,
  COUNT(DISTINCT CASE WHEN broadcaster IN ('A', 'B') THEN spine_id END) AS reach_ab_only,
  -- ... all combinations up to 7 (2^3 - 1 non-empty subsets)
  COUNT(DISTINCT CASE WHEN broadcaster = 'A' AND broadcaster = 'B' AND broadcaster = 'C'
                      THEN spine_id END) AS reach_all_three
FROM cross_broadcaster_exposures
GROUP BY campaign_id;
-- 24-hour latency requirement met: Dynamic Table refreshes hourly
```

## Monetization Model

As a SaaS product:

```sql
-- Tiered subscription model
-- Tier 1 ($50K/year): Single-broadcaster reach + frequency only
-- Tier 2 ($150K/year): Cross-broadcaster unduplicated reach + frequency
-- Tier 3 ($300K/year): Full: cross-broadcaster + conversion lift + demographic breakdowns

-- Snowflake Marketplace listing with per-query pricing for overages
CALL measurement_marketplace_app.CORE.PUBLISH_LISTING(
  OBJECT_CONSTRUCT(
    'listing_name', 'Cross-Broadcaster Audience Measurement',
    'pricing_model', 'SUBSCRIPTION_WITH_OVERAGE',
    'base_price_annual_usd', 150000,    -- Tier 2 base
    'included_campaigns_per_year', 50,
    'overage_price_per_campaign', 3500,
    'trial_campaigns', 2
  )
);
```

## Audit and Compliance Architecture

```sql
-- Immutable audit log (append-only table)
CREATE TABLE measurement_audit_log (
  log_id            VARCHAR(36) DEFAULT UUID_STRING(),
  event_type        VARCHAR(50),     -- 'BROADCASTER_DATA_INGESTED', 'ANALYSIS_RUN', 'RESULT_DELIVERED'
  actor_account     VARCHAR(100),    -- who triggered this event
  campaign_id       VARCHAR(50),
  data_subjects_count INTEGER,       -- how many spine IDs processed
  thresholds_applied VARIANT,        -- which thresholds were enforced
  dp_epsilon_used   FLOAT,
  result_rows       INTEGER,
  event_ts          TIMESTAMP,
  audit_hash        VARCHAR(64)      -- SHA-256 of previous row (chain of trust)
) CHANGE_TRACKING = FALSE;           -- prevent updates to audit records

-- Regular external audit export (for regulators, IAB MRC accreditation)
COPY INTO @audit_export_stage/monthly_audit.csv
FROM (
  SELECT *
  FROM measurement_audit_log
  WHERE DATE_TRUNC('month', event_ts) = DATE_TRUNC('month', DATEADD('month', -1, CURRENT_DATE()))
)
FILE_FORMAT = (TYPE = CSV COMPRESSION = GZIP);
-- Provided to IAB MRC auditors for annual accreditation renewal
```

## Key Design Decisions Explained

**Why a neutral operator model?**
Broadcasters will not share data if a competitor broadcaster can see it. Advertisers won't share purchase data with broadcasters. The neutral operator (measurement company) holds the identity spine and operates the clean room. Neither broadcasters nor advertisers can access the spine directly.

**Why Dynamic Tables for the materialized reach metrics?**
Counting distinct spine IDs across 200M profiles for every report request would be computationally expensive and wouldn't meet the 24-hour latency requirement. Dynamic Tables pre-compute the reach rollups and refresh hourly, enabling sub-second report delivery.

**Why differential privacy in addition to minimum thresholds?**
Minimum thresholds alone can be defeated by differencing attacks, particularly in demographic breakdown queries. With 200M profiles, an adversary with knowledge of specific demographics could infer individual-level data. DP provides a formal guarantee that prevents this, which is required for IAB MRC accreditation.

**How to handle cross-device attribution at CTV scale?**
Connected TV is a household device — multiple family members share it. The identity spine tracks household IDs (`hh_id`) in addition to individual spine IDs. Campaign reach is reported at both the individual level (for 1:1 addressable campaigns) and the household level (for CTV campaigns where household is the unit of measurement).

</details>

</article>
