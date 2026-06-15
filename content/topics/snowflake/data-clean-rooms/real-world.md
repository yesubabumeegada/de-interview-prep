---
title: "Snowflake Data Clean Rooms in Production: Advertising, Healthcare, and Financial Use Cases"
description: "Real-world clean room implementations for advertising measurement, healthcare collaboration, financial data sharing, and replacing cookie-based tracking"
content_type: study_material
topic: snowflake
subtopic: data-clean-rooms
layer: real-world
difficulty_level: senior
tags: [snowflake, data-clean-rooms, advertising, healthcare, financial, cookie-deprecation, production, collaboration, privacy]
---

# Snowflake Data Clean Rooms in Production: Advertising, Healthcare, and Financial Use Cases

## Real-World Scenario 1: Advertising Measurement

### The Business Context

A national grocery chain (Retailer) runs advertising campaigns across multiple digital channels. They want to measure:
- Which ad campaigns drove in-store and online purchases?
- What is the true incremental lift from advertising vs. organic purchases?
- How can they optimize budget allocation across channels?

**The problem**: To answer these questions, they need to join their purchase data with ad impression data from Google, Meta, and a connected TV platform. But they can't share their purchase records with these platforms, and the platforms won't share raw user data with the retailer.

**Solution**: Three separate clean rooms, one with each advertising partner.

### Implementation: Google Ads Clean Room

```sql
-- PROVIDER: Google (sets up the clean room with their impression data)
-- Provider shares: hashed Google user IDs matched to hashed emails

-- CONSUMER (Retailer side): Link purchase data
-- Retailer maps their loyalty customer emails to hashed format
CREATE OR REPLACE TABLE loyalty_purchases_hashed AS
SELECT
  SHA2(LOWER(TRIM(loyalty_email)), 256) AS hashed_email,
  transaction_date,
  store_id,
  product_category,
  total_spend,
  is_online
FROM loyalty_transactions
WHERE transaction_date >= DATEADD('day', -90, CURRENT_DATE())
  AND loyalty_email IS NOT NULL;

-- Run the overlap analysis template (provided by Google)
CALL google_ads_cleanroom.CORE.RUN_ANALYSIS(
  'campaign_conversion_attribution',
  OBJECT_CONSTRUCT(
    'consumer_table', 'loyalty_purchases_hashed',
    'consumer_join_col', 'hashed_email',
    'consumer_event_date_col', 'transaction_date',
    'attribution_window_days', 7,
    'campaign_ids', ARRAY_CONSTRUCT('CAMP_HOLIDAY_2024', 'CAMP_LOYALTY_Q4')
  )
);
```

**Results**:
```
CAMPAIGN           | REACH    | MATCHED_PURCHASERS | CONV_RATE | AVG_BASKET_SIZE
CAMP_HOLIDAY_2024  | 2.1M     | 187,450            | 8.93%     | $87.42
CAMP_LOYALTY_Q4    | 890K     | 124,800            | 14.02%    | $112.30
```

### Cross-Platform Deduplication

A key problem in advertising: the same person may see ads on Google, Meta, AND connected TV. Without a clean room, you count them as three separate converters. With a multi-platform clean room approach:

```sql
-- After running analyses against each platform's clean room,
-- aggregate results to understand cross-platform reach and deduplication

-- Results from each platform (aggregate, no individual data)
-- Platform A: 187,450 converters
-- Platform B: 143,200 converters  
-- Platform C: 89,700 converters
-- Question: How many unique converters across ALL platforms?

-- This requires a multi-party clean room where all three platforms participate
-- Some vendors (LiveRamp, Habu) offer this as a managed service on top of Snowflake
-- The triple overlap is mathematically estimated using set theory bounds
-- since exact triple overlap requires all three parties in one environment

-- Estimated unique total converters (inclusion-exclusion lower bound):
-- At least: max(187450, 143200, 89700) = 187,450
-- At most: 187,450 + 143,200 + 89,700 = 420,350
-- True value determined by multi-party clean room
```

### Incremental Lift Measurement

The most sophisticated advertising question: "Would these customers have bought anyway, even without seeing my ads?" This requires a **holdout test** design within the clean room:

```sql
-- Provider template: Compare converter rates for exposed vs. unexposed users
-- Provider randomly withholds 10% of eligible users from ad delivery
-- Clean room compares: did exposed users convert at higher rate than unexposed?

-- Template for lift measurement
SELECT
  exposure_group,        -- 'EXPOSED' or 'HOLDOUT'
  COUNT(DISTINCT p.hashed_email) AS group_size,
  COUNT(DISTINCT CASE WHEN c.event_type = 'PURCHASE' THEN p.hashed_email END) AS converters,
  ROUND(
    COUNT(DISTINCT CASE WHEN c.event_type = 'PURCHASE' THEN p.hashed_email END) /
    COUNT(DISTINCT p.hashed_email) * 100, 3
  ) AS conversion_rate_pct,
  ROUND(
    (MAX(CASE WHEN exposure_group = 'EXPOSED'
           THEN COUNT(DISTINCT CASE WHEN c.event_type = 'PURCHASE' THEN p.hashed_email END)::FLOAT /
                COUNT(DISTINCT p.hashed_email) END) OVER () -
     MAX(CASE WHEN exposure_group = 'HOLDOUT'
           THEN COUNT(DISTINCT CASE WHEN c.event_type = 'PURCHASE' THEN p.hashed_email END)::FLOAT /
                COUNT(DISTINCT p.hashed_email) END) OVER ()) * 100, 3
  ) AS incremental_lift_pp
FROM provider_user_pool p   -- includes both exposed and holdout users
LEFT JOIN {{ consumer_table }} c ON p.hashed_email = c.hashed_email
GROUP BY exposure_group
HAVING COUNT(DISTINCT p.hashed_email) >= 10000;  -- higher threshold for lift tests
```

---

## Real-World Scenario 2: Healthcare Data Sharing

### The Business Context

A pharmaceutical company (Pharma) wants to understand treatment outcomes for a drug they manufacture. They need to:
- Identify patients on their drug who switched to a competitor
- Understand adherence patterns and outcomes
- Collaborate with a health insurance payer who has claims data

**The constraint**: PHI (Protected Health Information) cannot be shared between Pharma and the Payer without patient consent (HIPAA). But aggregate outcomes analysis is permissible under HIPAA's research exceptions if PII is not exposed.

### HIPAA-Compliant Clean Room Architecture

```sql
-- PROVIDER: Health Insurance Payer
-- Payer has: claims data, diagnoses, treatment history, outcomes

-- Data must be de-identified to HIPAA Safe Harbor standard before entry
-- 18 identifiers must be removed or generalized:

CREATE TABLE payer_claims_deidentified AS
SELECT
  -- Hashed patient ID (one-way; payer keeps the mapping, pharma never gets it)
  SHA2(CONCAT(member_id, 'SALT_PAYER_2024'), 256) AS hashed_patient_id,

  -- Dates generalized to year only (removing specific dates per HIPAA Safe Harbor)
  YEAR(admission_date) AS admission_year,
  YEAR(discharge_date) AS discharge_year,

  -- Age in 5-year brackets (not exact age per Safe Harbor)
  FLOOR(age / 5) * 5 AS age_bracket_start,

  -- Geography at ZIP-3 (3-digit prefix, not full ZIP)
  LEFT(patient_zip, 3) AS zip3,

  -- Clinical data preserved (non-identifying)
  diagnosis_code,       -- ICD-10 codes
  drug_ndc,            -- NDC drug code
  therapy_duration_days,
  readmission_90day,
  medication_adherence_pct
FROM member_claims
WHERE drug_ndc LIKE '12345%'  -- Pharma's drug NDC prefix
  AND admission_year >= 2022;

-- CONSUMER: Pharma company
-- Pharma has: prescription fill data, patient-reported outcomes (from surveys)
-- Their data is also hashed with the same salt scheme agreed with the payer

CALL payer_pharma_cleanroom.CORE.RUN_ANALYSIS(
  'treatment_adherence_outcomes',
  OBJECT_CONSTRUCT(
    'consumer_table', 'pharma_prescription_fills_hashed',
    'consumer_join_col', 'hashed_patient_id',
    'diagnosis_code_filter', 'E11%',   -- Type 2 Diabetes ICD-10 prefix
    'min_therapy_days', 180
  )
);
```

**Aggregate results (what Pharma actually receives)**:
```
AGE_BRACKET | ZIP3 | DIAGNOSIS | ADHERENCE_BUCKET | PATIENT_COUNT | AVG_OUTCOMES_SCORE | READMIT_RATE
35-40       | 100  | E11.9     | 80-100%          | 847           | 7.3                | 4.2%
35-40       | 100  | E11.9     | 60-80%           | 312           | 6.1                | 8.9%
35-40       | 100  | E11.9     | <60%             | 156           | 4.8                | 15.3%
```

Pharma learns: adherence correlates strongly with outcomes. No individual patient data is exposed.

### Real-World Challenges in Healthcare Clean Rooms

**Challenge 1: Matching rates**

Patient matching in healthcare is notoriously difficult. Hospitals use different patient ID systems. The solution is fuzzy matching strategies:

```sql
-- Multi-identifier matching with confidence scoring
SELECT
  p.hashed_member_id,
  c.hashed_rx_id,
  CASE
    WHEN p.hashed_member_id = c.hashed_member_id THEN 1.0        -- exact member ID match
    WHEN p.hashed_ssn_last4 = c.hashed_ssn_last4
         AND p.hashed_dob = c.hashed_dob THEN 0.95               -- SSN+DOB match
    WHEN p.hashed_name = c.hashed_name
         AND p.hashed_dob = c.hashed_dob
         AND p.zip3 = c.zip3 THEN 0.85                           -- name+DOB+ZIP match
    ELSE 0.0
  END AS match_confidence
FROM payer_claims p
JOIN pharma_rx c ON match_confidence >= 0.85
```

**Challenge 2: Small cell suppression vs. clinical significance**

A clean room threshold of 25 might suppress critical findings for rare conditions. Healthcare researchers sometimes need to see cohorts as small as 5. Solution: Implement tiered suppression with IRB-level approval for lower thresholds.

---

## Real-World Scenario 3: Financial Services Data Collaboration

### The Business Context

A consortium of banks wants to improve fraud detection by sharing patterns — not transactions. A fraudster who commits credit card fraud at Bank A often tries the same pattern at Bank B. If banks could share patterns, they could detect fraud earlier.

**The constraint**: Banks cannot share customer transaction data with each other (privacy laws, competitive concerns).

**Solution**: A fraud consortium clean room where banks share **fraud signals** (aggregate patterns) not individual transactions.

### Consortium Clean Room Architecture

```sql
-- OPERATOR: A neutral third party (or Snowflake Marketplace)
-- Maintains the consortium clean room that all banks participate in

-- Each bank submits hashed fraud signals (not raw transactions)
-- These are feature vectors derived from transactions, not transactions themselves

-- Bank A's contribution:
CREATE TABLE fraud_signals_bank_a AS
SELECT
  SHA2(CONCAT(card_number, 'BANK_A_SALT'), 256) AS hashed_card,
  SHA2(merchant_id, 256) AS hashed_merchant,
  transaction_hour_of_day,
  transaction_day_of_week,
  amount_bucket,         -- '0-50', '50-200', '200-1000', '1000+'
  geo_distance_from_home, -- miles from cardholder's home ZIP
  is_confirmed_fraud,    -- 1 or 0
  fraud_category         -- 'card_not_present', 'account_takeover', etc.
FROM transactions_last_90_days
WHERE is_confirmed_fraud = 1 OR RANDOM() < 0.01;  -- fraud + 1% sample of legit
-- (Only confirmed fraud + small sample shared — minimizes data exposure)

-- Operator clean room aggregates patterns across all banks
-- Without any individual transaction being exposed

SELECT
  fraud_category,
  hashed_merchant,   -- are certain merchants fraud hotspots?
  transaction_hour_of_day,
  amount_bucket,
  COUNT(*) AS fraud_count,
  AVG(geo_distance_from_home) AS avg_distance
FROM consortium_fraud_signals  -- union of all bank contributions
GROUP BY 1, 2, 3, 4
HAVING COUNT(*) >= 10   -- minimum threshold
ORDER BY fraud_count DESC;
```

### Real-Time Fraud Signal Sharing

For fraud, latency matters. An advanced pattern uses Snowflake Streams and Tasks to near-real-time share fraud signals:

```sql
-- Stream on confirmed fraud table
CREATE STREAM new_fraud_stream ON TABLE confirmed_fraud_events;

-- Task that pushes new fraud signals to consortium clean room every 5 minutes
CREATE TASK push_fraud_signals_task
  WAREHOUSE = fraud_signals_wh
  SCHEDULE = '5 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('new_fraud_stream')
AS
  INSERT INTO consortium_clean_room.shared_schema.fraud_signals
  SELECT
    SHA2(CONCAT(card_number, $bank_salt), 256) AS hashed_card,
    SHA2(merchant_id, 256) AS hashed_merchant,
    EXTRACT(HOUR FROM transaction_ts) AS hour_of_day,
    amount_bucket,
    fraud_category,
    CURRENT_TIMESTAMP() AS signal_ts
  FROM new_fraud_stream
  WHERE METADATA$ACTION = 'INSERT';
```

---

## Real-World Scenario 4: Replacing Cookie-Based Tracking

### The Problem

A digital publisher (news website) and an advertiser (car manufacturer) previously used cookie-based retargeting:
1. User visits car manufacturer's website → cookie dropped
2. User visits the news website → cookie read → ad for the car served
3. User buys a car → cookie track confirms conversion

**After cookie deprecation**: Step 2 fails — the publisher's site can't read the advertiser's cookie.

### Clean Room Solution: First-Party Data Matching

```sql
-- PUBLISHER (Provider): They have logged-in user data
-- Users who consented to targeted advertising (GDPR consent managed)
CREATE TABLE publisher_consented_users AS
SELECT
  SHA2(LOWER(TRIM(registered_email)), 256) AS hashed_email,
  user_age_bracket,
  user_interest_categories,  -- derived from article reading patterns
  device_type,
  last_seen_ts
FROM registered_users
WHERE advertising_consent = TRUE  -- GDPR consent gate
  AND last_seen_ts >= DATEADD('day', -30, CURRENT_DATE());

-- ADVERTISER (Consumer): CRM data of people who expressed interest
CREATE TABLE car_website_visitors_hashed AS
SELECT
  SHA2(LOWER(TRIM(email)), 256) AS hashed_email,
  model_interest,    -- which car model they viewed
  intent_score,      -- derived from pages viewed, time on site
  crm_segment        -- existing customer, conquest, etc.
FROM website_leads
WHERE email IS NOT NULL
  AND consent_digital_advertising = TRUE;

-- Clean room analysis: Which publisher users are in the advertiser's CRM?
-- Publisher shows targeted ads to these users WITHOUT sharing the list
CALL publisher_cleanroom.CORE.RUN_ANALYSIS(
  'audience_overlap_and_segmentation',
  OBJECT_CONSTRUCT(
    'consumer_table', 'car_website_visitors_hashed',
    'consumer_join_col', 'hashed_email',
    'segment_col', 'crm_segment',
    'model_interest_filter', 'SUV'
  )
);
```

**Result**: Publisher knows to serve car ads to their users who match the advertiser's CRM — no cookie needed.

### Consent Management Integration

```sql
-- Critical: Only use data from users who consented
-- Clean room must enforce consent at the data layer, not just policy

-- Publisher consent-gated view (accessed by clean room app)
CREATE SECURE VIEW consented_publisher_users AS
SELECT
  hashed_email,
  interest_categories,
  age_bracket
FROM publisher_user_hashed
WHERE advertising_consent = TRUE
  AND consent_given_ts >= DATEADD('year', -1, CURRENT_DATE())  -- annual reconsent
  AND consent_withdrawn_ts IS NULL;   -- not revoked
-- SECURE VIEW ensures clean room app cannot bypass consent filter

-- When a user withdraws consent:
UPDATE publisher_users
SET consent_withdrawn_ts = CURRENT_TIMESTAMP()
WHERE user_id = :user_id;
-- They immediately drop out of the SECURE VIEW and future clean room analyses
```

---

## Measuring Clean Room ROI

For practitioners defending the investment in clean room infrastructure:

```sql
-- Attribution improvement: Compare pre/post clean room measurement accuracy
-- (Hypothetical: tracks whether clean room reduced over/under attribution)

WITH pre_cleanroom_attribution AS (
  SELECT campaign_id, estimated_conversions, attributed_revenue
  FROM legacy_mta_results
  WHERE measurement_period = 'PRE_CLEANROOM'
),
post_cleanroom_attribution AS (
  SELECT campaign_id, verified_conversions, verified_revenue
  FROM cleanroom_attribution_results
  WHERE measurement_period = 'POST_CLEANROOM'
),
comparison AS (
  SELECT
    pre.campaign_id,
    pre.estimated_conversions AS pre_conv,
    post.verified_conversions AS post_conv,
    ABS(pre.estimated_conversions - post.verified_conversions) / post.verified_conversions AS error_rate,
    post.verified_revenue - pre.attributed_revenue AS revenue_delta
  FROM pre_cleanroom_attribution pre
  JOIN post_cleanroom_attribution post ON pre.campaign_id = post.campaign_id
)
SELECT
  AVG(error_rate) AS avg_measurement_error_reduction,
  SUM(revenue_delta) AS total_attributed_revenue_uplift,
  COUNT_IF(error_rate > 0.2) AS campaigns_with_high_pre_error
FROM comparison;
```

---

## Lessons Learned from Production Clean Rooms

### 1. Hash Coordination Is Harder Than Expected

The most common failure in a new clean room deployment: Provider and consumer hash emails differently, resulting in 0% match rate.

**Checklist for successful hash coordination**:
- [ ] Case normalization (lowercase before hashing)
- [ ] Whitespace trimming
- [ ] Handling of email variants (gmail dots, plus aliases)
- [ ] Phone number E.164 normalization before hashing
- [ ] Agreed hash algorithm (SHA-256 vs. SHA-512 vs. MD5)
- [ ] Same salt (or no salt — salting prevents cross-provider matching)

### 2. Match Rates Are Always Lower Than Expected

A typical first-party email match rate between two companies is 30–60%. Reasons:
- Different users registered with different email addresses on each platform
- Some emails are business vs. personal
- Data recency (older emails may have churned)

Design analyses to be useful at lower match rates. Don't promise insights that require 80%+ match.

### 3. Minimum Thresholds Kill Niche Analysis

A retail clean room with a threshold of 25 cannot tell an advertiser whether their campaign worked for customers in rural areas (too small a cohort). Work with the provider to establish tiered thresholds for different use cases.

### 4. GDPR Right to Erasure Must Be Propagated

When a user exercises their right to erasure, the clean room must be updated:

```sql
-- When a user requests deletion (GDPR Article 17):
-- Delete from source → clean room data must be refreshed

-- 1. Mark user as deleted in source system
UPDATE users SET deleted_ts = CURRENT_TIMESTAMP() WHERE email = :email;

-- 2. Re-run the hashed export to clean room (excludes deleted users)
-- If using a Snowflake Share, the consumer sees the updated view immediately
-- For exported files/tables, must re-export and overwrite

-- 3. Log the erasure for audit trail
INSERT INTO gdpr_erasure_log (hashed_email, request_ts, fulfilled_ts)
VALUES (SHA2(LOWER(:email), 256), :request_ts, CURRENT_TIMESTAMP());
```

### 5. Governance Documentation Is Part of the Product

Regulators and legal teams will ask:
- What data is in the clean room?
- Who can run what analyses?
- How are minimum thresholds set?
- What is the audit trail?

Build documentation and audit reports as a feature, not an afterthought. The `cleanroom_audit_log` table from the senior deep-dive section should be queryable by legal and compliance teams.
