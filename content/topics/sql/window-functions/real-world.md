---
title: "SQL Window Functions - Real-World Production Examples"
topic: sql
subtopic: window-functions
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [sql, window-functions, production, scd, deduplication, etl]
---

# SQL Window Functions — Real-World Production Examples

## Example 1: SCD Type 2 — Identifying Current Records

In a Slowly Changing Dimension table, use window functions to identify the current version of each record:

```sql
-- Identify current and historical records in an SCD Type 2 table
WITH versioned_customers AS (
    SELECT 
        customer_id,
        name,
        address,
        effective_date,
        expiration_date,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id 
            ORDER BY effective_date DESC
        ) AS version_rank,
        LEAD(effective_date) OVER (
            PARTITION BY customer_id 
            ORDER BY effective_date
        ) AS next_version_date
    FROM dim_customer
)
SELECT 
    customer_id,
    name,
    address,
    effective_date,
    COALESCE(next_version_date, '9999-12-31'::date) AS expiration_date,
    CASE WHEN version_rank = 1 THEN TRUE ELSE FALSE END AS is_current
FROM versioned_customers;
```

## Example 2: Data Quality — Detecting Duplicates in Pipeline

```sql
-- Production deduplication for event stream ingestion
-- Keep the first occurrence per (user_id, event_type, 5-minute window)
WITH deduped_events AS (
    SELECT 
        event_id,
        user_id,
        event_type,
        event_timestamp,
        payload,
        ROW_NUMBER() OVER (
            PARTITION BY 
                user_id, 
                event_type,
                DATE_TRUNC('minute', event_timestamp) -- 1-min windows
            ORDER BY event_timestamp ASC, event_id ASC  -- Deterministic tie-breaking
        ) AS occurrence_num
    FROM raw_events
    WHERE event_date = CURRENT_DATE  -- Partition pruning
)
INSERT INTO cleaned_events
SELECT event_id, user_id, event_type, event_timestamp, payload
FROM deduped_events
WHERE occurrence_num = 1;
```

## Example 3: Revenue Attribution — First/Last Touch

```sql
-- Marketing attribution: first touch and last touch before conversion
WITH touchpoints AS (
    SELECT 
        conversion_id,
        user_id,
        channel,
        touch_timestamp,
        FIRST_VALUE(channel) OVER (
            PARTITION BY conversion_id 
            ORDER BY touch_timestamp ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS first_touch_channel,
        LAST_VALUE(channel) OVER (
            PARTITION BY conversion_id 
            ORDER BY touch_timestamp ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS last_touch_channel,
        ROW_NUMBER() OVER (
            PARTITION BY conversion_id 
            ORDER BY touch_timestamp ASC
        ) AS touch_order,
        COUNT(*) OVER (
            PARTITION BY conversion_id
        ) AS total_touches
    FROM marketing_touchpoints
)
SELECT 
    conversion_id,
    first_touch_channel,
    last_touch_channel,
    total_touches,
    -- Linear attribution: equal credit to each touchpoint
    conversion_value / total_touches AS linear_attribution_value
FROM touchpoints
WHERE touch_order = 1;  -- One row per conversion
```

## Example 4: Real-Time Alerting — Anomaly Detection

```sql
-- Detect metric anomalies: flag values > 3 standard deviations from rolling mean
WITH metrics_with_stats AS (
    SELECT 
        metric_name,
        recorded_at,
        value,
        AVG(value) OVER (
            PARTITION BY metric_name 
            ORDER BY recorded_at
            ROWS BETWEEN 100 PRECEDING AND 1 PRECEDING  -- Exclude current row
        ) AS rolling_mean,
        STDDEV(value) OVER (
            PARTITION BY metric_name 
            ORDER BY recorded_at
            ROWS BETWEEN 100 PRECEDING AND 1 PRECEDING
        ) AS rolling_stddev
    FROM system_metrics
    WHERE recorded_at >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
)
SELECT 
    metric_name,
    recorded_at,
    value,
    rolling_mean,
    rolling_stddev,
    CASE 
        WHEN ABS(value - rolling_mean) > 3 * rolling_stddev THEN 'CRITICAL'
        WHEN ABS(value - rolling_mean) > 2 * rolling_stddev THEN 'WARNING'
        ELSE 'NORMAL'
    END AS alert_level
FROM metrics_with_stats
WHERE rolling_stddev > 0  -- Avoid division by zero
  AND ABS(value - rolling_mean) > 2 * rolling_stddev;
```

## Example 5: ETL — Incremental Load with Change Detection

```sql
-- Detect actual changes vs duplicates in CDC stream
WITH source_with_change_detection AS (
    SELECT 
        record_id,
        col_a,
        col_b,
        col_c,
        updated_at,
        -- Create hash of business columns for change comparison
        MD5(CONCAT_WS('|', col_a, col_b, col_c)) AS row_hash,
        LAG(MD5(CONCAT_WS('|', col_a, col_b, col_c))) OVER (
            PARTITION BY record_id 
            ORDER BY updated_at
        ) AS prev_row_hash
    FROM staging_table
)
INSERT INTO target_table
SELECT record_id, col_a, col_b, col_c, updated_at
FROM source_with_change_detection
WHERE row_hash != COALESCE(prev_row_hash, '')  -- Only true changes
ORDER BY record_id, updated_at;
```

## Example 6: Data Pipeline — Gap Filling (Forward Fill)

```sql
-- Forward-fill missing sensor readings (carry forward last known value)
WITH all_timestamps AS (
    SELECT 
        sensor_id,
        ts,
        reading,
        -- Count non-null values up to current row to create groups
        COUNT(reading) OVER (
            PARTITION BY sensor_id 
            ORDER BY ts
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS fill_group
    FROM sensor_data_with_gaps
)
SELECT 
    sensor_id,
    ts,
    -- Use FIRST_VALUE within each fill_group to carry forward
    FIRST_VALUE(reading) OVER (
        PARTITION BY sensor_id, fill_group 
        ORDER BY ts
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS filled_reading
FROM all_timestamps
ORDER BY sensor_id, ts;
```

## Production Considerations

### 1. Memory and Performance at Scale

```sql
-- For billion-row tables, limit the partition size:
-- BAD: unbounded partition (may try to load millions of rows)
SUM(amount) OVER (PARTITION BY customer_id ORDER BY date)

-- BETTER: Bound the frame
SUM(amount) OVER (
    PARTITION BY customer_id 
    ORDER BY date
    ROWS BETWEEN 365 PRECEDING AND CURRENT ROW  -- Only last year
)
```

### 2. Null Handling

```sql
-- Window functions handle NULLs differently:
-- COUNT ignores NULLs
-- SUM ignores NULLs (but returns NULL if ALL values are NULL)
-- ROW_NUMBER() still assigns numbers to NULL rows
-- LAG/LEAD return NULL if the offset row doesn't exist

-- Always handle NULLs explicitly:
COALESCE(LAG(value) OVER (...), 0) AS prev_value_or_zero
```

### 3. Determinism

```sql
-- NON-DETERMINISTIC (dangerous in production):
ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC)
-- If two people have the same salary, row_number assignment is random!

-- DETERMINISTIC (always produces the same result):
ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC, employee_id ASC)
-- Adding a unique tie-breaker ensures reproducible results
```

## Interview Tip 💡

> In production DE interviews, show that you think about **edge cases**: What happens with NULLs? What if the partition is massive? Is the result deterministic? How does it behave during backfills? These considerations separate senior candidates from mid-level ones.
