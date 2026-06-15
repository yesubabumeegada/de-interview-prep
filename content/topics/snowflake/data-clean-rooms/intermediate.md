---
title: "Snowflake Data Clean Room Setup: Roles, Policies, and Query Templates"
description: "Setting up Snowflake Data Clean Rooms, configuring provider/consumer/analyst roles, JINJA templates for policy enforcement, and allowed query patterns"
content_type: study_material
topic: snowflake
subtopic: data-clean-rooms
layer: intermediate
difficulty_level: mid-level
tags: [snowflake, data-clean-rooms, native-app, JINJA, privacy-policy, provider, consumer, analyst, query-templates, setup]
---

# Snowflake Data Clean Room Setup: Roles, Policies, and Query Templates

## Clean Room Architecture in Snowflake

Snowflake's Data Clean Room solution (previously called Samooha) is built on the Native App Framework. The architecture has two sides: the **provider side** where the clean room app is created and published, and the **consumer side** where it's installed and run.

```
PROVIDER SIDE                              CONSUMER SIDE
─────────────────────────────────          ─────────────────────────────────
1. Create clean room app                   4. Install clean room from listing
2. Share provider data (secure share)      5. Link consumer data
3. Publish to Marketplace or direct        6. Run allowed analyses
   share to consumer account               7. View aggregate results only
```

---

## Setting Up a Clean Room: Provider Perspective

### Step 1: Install the Clean Room Provider App

The clean room framework itself is a Native App available from the Snowflake Marketplace.

```sql
-- As provider: install the Snowflake Data Clean Room provider app
-- (typically done via Snowflake Marketplace UI, or:)
CREATE APPLICATION samooha_provider_app
  FROM APPLICATION PACKAGE samooha_pkg
  USING VERSION v1;
```

### Step 2: Create and Configure a Clean Room

```sql
-- Initialize a new clean room
CALL samooha_provider_app.CORE.CREATE_CLEANROOM($cleanroom_name);

-- Example:
SET cleanroom_name = 'retail_advertising_cleanroom';
CALL samooha_provider_app.CORE.CREATE_CLEANROOM($cleanroom_name);

-- Add the consumer (by Snowflake account locator or org.account format)
CALL samooha_provider_app.CORE.ADD_CONSUMER(
  $cleanroom_name,
  'CONSUMER_ACCOUNT_LOCATOR'
);
```

### Step 3: Link Provider Data

The provider links their data to the clean room. This data is shared via a secure data share into the consumer's clean room app — the consumer never sees the raw rows.

```sql
-- Link a table as provider data (the consumer can join against this)
CALL samooha_provider_app.CORE.LINK_DATASETS(
  $cleanroom_name,
  ['ad_impressions_db.public.impressions_hashed']
);

-- The table must have PII already hashed:
-- impressions_hashed schema:
-- hashed_email VARCHAR(64)   -- SHA-256 of normalized email
-- ad_id        VARCHAR(50)
-- impression_ts TIMESTAMP
-- campaign_id  VARCHAR(50)
-- platform     VARCHAR(30)
```

### Step 4: Define Allowed Analyses (JINJA Templates)

This is the key privacy enforcement mechanism. The provider defines SQL templates using JINJA syntax. Consumers can only run these templates — no ad-hoc SQL.

```sql
-- Register a JINJA template for overlap analysis
CALL samooha_provider_app.CORE.ADD_ANALYSIS_TEMPLATE(
  $cleanroom_name,
  'overlap_count',
  $$
  SELECT
    p.campaign_id,
    COUNT(DISTINCT p.hashed_email) AS total_impressions,
    COUNT(DISTINCT c.hashed_email) AS reached_users,
    COUNT(DISTINCT CASE WHEN cvt.hashed_email IS NOT NULL THEN p.hashed_email END) AS converters,
    ROUND(
      COUNT(DISTINCT CASE WHEN cvt.hashed_email IS NOT NULL THEN p.hashed_email END) /
      NULLIF(COUNT(DISTINCT p.hashed_email), 0) * 100,
      2
    ) AS conversion_rate_pct
  FROM samooha_provider_app.SHARED_SCHEMA.impressions_hashed p
  INNER JOIN {{ consumer_table }} c
    ON p.hashed_email = c.{{ consumer_join_col }}
  LEFT JOIN {{ consumer_table }} cvt
    ON p.hashed_email = cvt.{{ consumer_join_col }}
    AND cvt.{{ consumer_event_col }} = 'PURCHASE'
  WHERE p.campaign_id IN ({{ campaign_ids }})
  GROUP BY p.campaign_id
  HAVING COUNT(DISTINCT p.hashed_email) >= 25   -- minimum threshold enforcement
  $$
);
```

### JINJA Template Variables Explained

| Variable | Type | Description |
|----------|------|-------------|
| `{{ consumer_table }}` | Table reference | The consumer's data table (injected safely) |
| `{{ consumer_join_col }}` | Column name | Which column to join on (hashed email, phone, etc.) |
| `{{ consumer_event_col }}` | Column name | Column containing event type (PURCHASE, CLICK, etc.) |
| `{{ campaign_ids }}` | List | Filter for specific campaigns |

JINJA prevents SQL injection and ensures consumers can only parameterize the template — not rewrite the query logic.

---

## Setting Up the Consumer Side

### Step 1: Install the Consumer Clean Room App

```sql
-- Consumer installs the clean room (sent by provider via direct listing or marketplace)
-- This happens via UI or:
CREATE APPLICATION retail_advertising_cleanroom
  FROM APPLICATION PACKAGE retail_advertising_cleanroom_pkg
  USING VERSION v1;
```

### Step 2: Link Consumer Data

```sql
-- Consumer links their own data to the clean room
CALL retail_advertising_cleanroom.CORE.LINK_CONSUMER_DATASET(
  'my_purchase_data_db.analytics.customer_events_hashed',
  'consumer_purchases'   -- alias used in templates
);

-- Consumer purchase events table must have hashed email for joining:
-- hashed_email  VARCHAR(64)
-- event_type    VARCHAR(50)   -- 'PURCHASE', 'ADD_TO_CART', etc.
-- event_ts      TIMESTAMP
-- product_id    VARCHAR(50)
-- revenue       FLOAT
```

### Step 3: Run an Allowed Analysis

Consumers call the templates with their parameters. They cannot write arbitrary SQL.

```sql
-- Run the overlap analysis template defined by the provider
CALL retail_advertising_cleanroom.CORE.RUN_ANALYSIS(
  'overlap_count',
  OBJECT_CONSTRUCT(
    'consumer_table', 'consumer_purchases',
    'consumer_join_col', 'hashed_email',
    'consumer_event_col', 'event_type',
    'campaign_ids', ARRAY_CONSTRUCT('CAMP_001', 'CAMP_002', 'CAMP_003')
  )
);
```

**Output** (aggregate only — no individual-level data):
```
CAMPAIGN_ID | TOTAL_IMPRESSIONS | REACHED_USERS | CONVERTERS | CONVERSION_RATE_PCT
CAMP_001    | 145,230           | 89,450        | 3,244      | 3.63
CAMP_002    | 78,900            | 52,100        | 987        | 1.89
CAMP_003    | 210,000           | 131,500       | 8,750      | 6.65
```

---

## Roles and Permissions in a Clean Room

Snowflake clean rooms use a three-layer permission model:

### Provider Roles

```sql
-- Provider admin: full control over the clean room configuration
GRANT ROLE samooha_provider_app.CORE.ADMIN TO USER provider_admin;

-- Provider viewer: can see clean room status and usage metrics
GRANT ROLE samooha_provider_app.CORE.VIEWER TO USER provider_analyst;
```

### Consumer Roles

```sql
-- Consumer admin: can configure consumer data links and manage analysts
GRANT ROLE retail_advertising_cleanroom.CORE.ADMIN TO USER consumer_admin;

-- Analyst: can run approved analyses only; cannot reconfigure the clean room
GRANT ROLE retail_advertising_cleanroom.CORE.ANALYST TO USER data_scientist_1;
GRANT ROLE retail_advertising_cleanroom.CORE.ANALYST TO USER data_scientist_2;
```

### Analyst Capabilities

| Action | ANALYST Role | ADMIN Role |
|--------|-------------|-----------|
| Run analysis templates | ✅ | ✅ |
| View aggregate results | ✅ | ✅ |
| Link consumer datasets | ❌ | ✅ |
| View raw provider data | ❌ | ❌ |
| Add/remove analysis templates | ❌ | ❌ (provider only) |
| View query logs/audit | ❌ | ✅ |

---

## Allowed Query Patterns

The provider controls exactly what kinds of analyses are permitted. Common patterns:

### Pattern 1: Reach and Overlap

```sql
-- Template: How many users are in both datasets?
SELECT
  COUNT(DISTINCT p.hashed_email) AS provider_universe,
  COUNT(DISTINCT c.hashed_email) AS consumer_universe,
  COUNT(DISTINCT p.hashed_email) AS overlap_count,
  ROUND(overlap_count / provider_universe * 100, 2) AS overlap_pct
FROM provider_impressions p
INNER JOIN {{ consumer_table }} c ON p.hashed_email = c.hashed_email
HAVING overlap_count >= {{ min_threshold | default(25) }}
```

### Pattern 2: Conversion Attribution

```sql
-- Template: Of users who saw ads, how many converted?
SELECT
  p.campaign_id,
  p.ad_format,
  COUNT(DISTINCT p.hashed_email) AS impressions_count,
  COUNT(DISTINCT CASE WHEN c.event_type = 'PURCHASE' THEN p.hashed_email END) AS converters,
  SUM(CASE WHEN c.event_type = 'PURCHASE' THEN c.revenue END) AS attributed_revenue
FROM provider_impressions p
LEFT JOIN {{ consumer_table }} c
  ON p.hashed_email = c.hashed_email
  AND c.event_ts BETWEEN p.impression_ts AND DATEADD('day', 7, p.impression_ts)
WHERE p.campaign_id = '{{ campaign_id }}'
GROUP BY p.campaign_id, p.ad_format
HAVING COUNT(DISTINCT p.hashed_email) >= 25
```

### Pattern 3: Audience Segmentation (Aggregate Only)

```sql
-- Template: What are the demographic breakdowns of converters?
-- Note: consumer cannot drill into individual segment members
SELECT
  c.age_bracket,
  c.gender,
  c.region,
  COUNT(DISTINCT p.hashed_email) AS users_reached,
  COUNT(DISTINCT CASE WHEN c.event_type = 'PURCHASE' THEN p.hashed_email END) AS converters
FROM provider_impressions p
INNER JOIN {{ consumer_table }} c ON p.hashed_email = c.hashed_email
GROUP BY c.age_bracket, c.gender, c.region
HAVING COUNT(DISTINCT p.hashed_email) >= 100   -- higher threshold for segment analysis
```

### What Query Patterns Are Blocked

The clean room framework prevents:
- `SELECT * FROM provider_data` — row-level export of provider data
- `SELECT hashed_email FROM ...` — exporting join keys
- `COPY INTO @stage FROM ...` — exfiltrating results to external stage
- Queries without aggregation that could expose individual records
- `UNION` or subquery patterns designed to bypass minimum thresholds

---

## Minimum Threshold Enforcement

Minimum thresholds prevent re-identification via small-cohort attacks. The provider sets these:

```sql
-- Set organization-wide minimum threshold
CALL samooha_provider_app.CORE.SET_DEFAULT_MIN_THRESHOLD(
  $cleanroom_name,
  25   -- suppress results for cohorts smaller than 25 users
);

-- Set template-specific threshold (can be higher)
-- The HAVING clause in the template itself enforces this
-- For demographic segmentation, use a higher threshold (100+)
```

### Why This Matters

Without minimum thresholds:
1. Consumer runs: "Show me converters where age=35, gender=M, zip=10001, product=iPad"
2. If result = 1, consumer now knows a specific individual converted
3. Combined with other data, this can re-identify the person

With threshold of 25: Any cohort < 25 is suppressed → re-identification prevented.

---

## Data Preparation Best Practices

### Normalizing and Hashing PII for Joining

Both sides must hash identifiers the same way for joins to work:

```python
import hashlib
import re

def normalize_and_hash_email(email: str) -> str:
    """Normalize email and hash with SHA-256 for clean room joining."""
    # Normalize: lowercase, strip whitespace
    normalized = email.strip().lower()
    # Remove dots from Gmail addresses (john.doe@ == johndoe@)
    local, domain = normalized.split('@', 1)
    if domain == 'gmail.com':
        local = local.replace('.', '')
    normalized = f"{local}@{domain}"
    # Hash
    return hashlib.sha256(normalized.encode('utf-8')).hexdigest()

def normalize_and_hash_phone(phone: str) -> str:
    """Normalize phone number to E.164 and hash."""
    # Strip non-numeric characters
    digits_only = re.sub(r'\D', '', phone)
    # Assume US numbers; add country code if missing
    if len(digits_only) == 10:
        digits_only = '1' + digits_only
    return hashlib.sha256(digits_only.encode('utf-8')).hexdigest()
```

```sql
-- Snowflake-side hashing for provider data preparation
CREATE OR REPLACE TABLE ad_impressions_hashed AS
SELECT
  SHA2(LOWER(TRIM(email)), 256) AS hashed_email,
  SHA2(REGEXP_REPLACE(phone, '[^0-9]', ''), 256) AS hashed_phone,
  ad_id,
  campaign_id,
  impression_ts,
  ad_format
FROM ad_impressions_raw
WHERE email IS NOT NULL OR phone IS NOT NULL;

-- Create an index on the hash columns for join performance
-- (In Snowflake: clustering key)
ALTER TABLE ad_impressions_hashed
  CLUSTER BY (hashed_email);
```

---

## Monitoring and Auditing Clean Room Usage

```sql
-- Who ran what analyses and when (from the provider's perspective)
SELECT
  QUERY_START_TIME,
  USER_NAME,
  QUERY_TEXT,
  EXECUTION_STATUS,
  ROWS_PRODUCED
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE QUERY_TEXT ILIKE '%RUN_ANALYSIS%'
  AND QUERY_TEXT ILIKE '%cleanroom%'
ORDER BY QUERY_START_TIME DESC
LIMIT 100;

-- Track analysis runs via clean room metadata
CALL samooha_provider_app.CORE.GET_ANALYSIS_HISTORY($cleanroom_name);
```

---

## Common Intermediate Interview Questions

**Q: How does the JINJA template system enforce privacy in Snowflake clean rooms?**

JINJA templates allow parameterization (table names, column names, filter values) but prevent consumers from rewriting the underlying SQL logic. The provider writes the query structure — including aggregation, minimum thresholds, and join conditions. Consumers fill in approved parameters (like which campaign to analyze), but cannot change the SELECT clause to return row-level data or bypass the HAVING threshold.

**Q: What is the difference between the provider, consumer, and analyst roles?**

The provider creates the clean room and defines what analyses are allowed. They control which data is shared and set privacy policies (minimum thresholds, allowed query patterns). The consumer installs the clean room in their Snowflake account and links their own data. The analyst is a consumer-side user who runs analyses — they can run approved templates and see aggregate results, but cannot reconfigure the clean room or see raw data.

**Q: Why must PII be hashed before entering a clean room, and does Snowflake do this automatically?**

PII must be hashed so that neither party can extract raw personal data from the shared environment. The clean room can join on hashed identifiers without either side seeing the actual emails or phone numbers. Snowflake does NOT hash automatically — both parties must hash their data **before** loading it into the clean room. They must also agree on a consistent normalization and hashing approach (same SHA-256 implementation, same normalization rules for email) or joins won't work.
