---
title: "Time Travel - Real-World Production Examples"
topic: snowflake
subtopic: time-travel
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [snowflake, time-travel, production, recovery, patterns, auditing]
---

# Snowflake Time Travel — Real-World Production Examples

## Pattern 1: Automated Recovery Runbook

```sql
-- PRODUCTION INCIDENT: Bad UPDATE corrupted orders table
-- Runbook for data recovery team:

-- STEP 1: Identify the bad query
SELECT query_id, user_name, query_text, start_time, rows_affected
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE query_text ILIKE '%UPDATE%production.orders%'
  AND start_time >= DATEADD('hour', -6, CURRENT_TIMESTAMP())
  AND rows_affected > 10000  -- Suspiciously large update
ORDER BY start_time DESC;

-- STEP 2: Verify data before the bad query is correct
SELECT COUNT(*), AVG(amount), MIN(amount), MAX(amount)
FROM production.orders BEFORE (STATEMENT => 'bad_query_id');
-- Compare with expected values

-- STEP 3: Clone at the correct point
CREATE TABLE production.orders_recovery
CLONE production.orders BEFORE (STATEMENT => 'bad_query_id');

-- STEP 4: Validate recovery
SELECT COUNT(*) FROM production.orders_recovery;
SELECT COUNT(*) FROM production.orders BEFORE (STATEMENT => 'bad_query_id');
-- Counts should match

-- STEP 5: Swap (instant, atomic)
ALTER TABLE production.orders SWAP WITH production.orders_recovery;
-- Production is now restored!

-- STEP 6: Clean up and document
DROP TABLE production.orders_recovery;  -- This is the corrupted version now
-- Log the incident in ops tracking system

-- TOTAL RECOVERY TIME: 5-10 minutes (vs hours/days with traditional backup/restore)
```

---

## Pattern 2: Monthly Compliance Snapshots

```sql
-- Regulatory requirement: quarterly financial snapshots for auditors
-- Must show EXACTLY what data looked like at quarter-end close

-- Automated snapshot creation (scheduled at end of each quarter):
CREATE OR REPLACE PROCEDURE compliance.create_quarterly_snapshot()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    quarter_end TIMESTAMP;
    snapshot_name VARCHAR;
BEGIN
    quarter_end := DATE_TRUNC('quarter', CURRENT_DATE()) - INTERVAL '1 second';
    snapshot_name := 'COMPLIANCE_' || TO_CHAR(quarter_end, 'YYYYMMDD');
    
    EXECUTE IMMEDIATE 'CREATE SCHEMA IF NOT EXISTS compliance.' || snapshot_name;
    
    EXECUTE IMMEDIATE 
        'CREATE TABLE compliance.' || snapshot_name || '.orders ' ||
        'CLONE production.orders AT (TIMESTAMP => ''' || quarter_end || ''')';
    
    EXECUTE IMMEDIATE 
        'CREATE TABLE compliance.' || snapshot_name || '.customers ' ||
        'CLONE production.customers AT (TIMESTAMP => ''' || quarter_end || ''')';
    
    EXECUTE IMMEDIATE 
        'CREATE TABLE compliance.' || snapshot_name || '.transactions ' ||
        'CLONE production.financial_transactions AT (TIMESTAMP => ''' || quarter_end || ''')';
    
    RETURN 'Snapshot created: compliance.' || snapshot_name;
END;
$$;

-- Schedule: run on first day of each quarter
-- Result: compliance.COMPLIANCE_20240331.orders, .customers, .transactions
-- These clones are PERMANENT (don't expire like Time Travel)
-- Auditors can query them anytime: SELECT * FROM compliance.COMPLIANCE_20240331.orders;
```

---

## Pattern 3: ETL Rollback Automation

```sql
-- Automated rollback for ETL pipelines that produce bad data

CREATE OR REPLACE PROCEDURE etl.run_with_rollback(
    target_table VARCHAR, transform_query VARCHAR, quality_threshold FLOAT
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    pre_run_timestamp TIMESTAMP;
    row_count_before NUMBER;
    row_count_after NUMBER;
    quality_score FLOAT;
BEGIN
    -- Record state before transformation
    pre_run_timestamp := CURRENT_TIMESTAMP();
    EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || target_table INTO row_count_before;
    
    -- Execute the transformation
    EXECUTE IMMEDIATE transform_query;
    
    -- Quality check: did the result make sense?
    EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || target_table INTO row_count_after;
    
    -- Simple quality check: row count shouldn't drop by >50% or increase by >200%
    quality_score := row_count_after::FLOAT / NULLIF(row_count_before, 0);
    
    IF (quality_score < 0.5 OR quality_score > 2.0) THEN
        -- BAD! Rollback using Time Travel
        EXECUTE IMMEDIATE 
            'CREATE OR REPLACE TABLE ' || target_table || 
            ' CLONE ' || target_table || 
            ' AT (TIMESTAMP => ''' || pre_run_timestamp || ''')';
        
        RETURN 'ROLLBACK: Quality check failed (ratio: ' || quality_score || '). Table restored.';
    END IF;
    
    RETURN 'SUCCESS: ' || row_count_after || ' rows (quality ratio: ' || quality_score || ')';
END;
$$;

-- Usage:
CALL etl.run_with_rollback(
    'production.silver.orders',
    'INSERT OVERWRITE INTO production.silver.orders SELECT ... FROM raw.orders ...',
    0.5
);
-- If the transform produces weird results → auto-rollback to pre-run state!
```

---

## Pattern 4: Change Data Audit Trail

```sql
-- Build a complete audit trail using Time Travel + scheduled snapshots

-- Daily audit: what changed in critical tables?
CREATE OR REPLACE TASK audit.daily_change_report
    WAREHOUSE = 'AUDIT_WH_XS'
    SCHEDULE = 'USING CRON 0 7 * * * UTC'  -- 7 AM daily
AS
BEGIN
    -- Capture changes in orders table (last 24 hours)
    INSERT INTO audit.change_log (table_name, change_date, inserts, updates, deletes)
    SELECT 
        'production.orders',
        CURRENT_DATE() - 1,
        (SELECT COUNT(*) FROM production.orders 
         EXCEPT SELECT COUNT(*) FROM production.orders AT (OFFSET => -86400)),
        0, 0;  -- Simplified; full version compares row-by-row
    
    -- Better approach using CHANGES clause:
    INSERT INTO audit.change_log (table_name, change_date, action, row_count)
    SELECT 
        'production.orders',
        CURRENT_DATE() - 1,
        METADATA$ACTION,
        COUNT(*)
    FROM production.orders
    CHANGES (INFORMATION => DEFAULT)
    AT (TIMESTAMP => DATEADD('day', -1, CURRENT_TIMESTAMP()))
    GROUP BY METADATA$ACTION;
END;

-- Audit query: "Show me all changes to customer 12345 over the last month"
-- (Requires querying at multiple historical points)
CREATE OR REPLACE PROCEDURE audit.customer_change_history(cust_id NUMBER, days_back NUMBER)
RETURNS TABLE (check_date DATE, name VARCHAR, email VARCHAR, region VARCHAR)
LANGUAGE SQL
AS
$$
DECLARE
    result RESULTSET;
BEGIN
    -- Sample at daily intervals for the last N days
    result := (
        SELECT 
            check_date,
            name, email, region
        FROM (
            SELECT CURRENT_DATE() - SEQ4() AS check_date
            FROM TABLE(GENERATOR(ROWCOUNT => days_back))
        ) dates,
        LATERAL (
            SELECT name, email, region
            FROM production.customers 
            AT (TIMESTAMP => dates.check_date::TIMESTAMP_LTZ)
            WHERE customer_id = cust_id
        )
    );
    RETURN TABLE(result);
END;
$$;
```

---

## Pattern 5: Development Database Refresh

```sql
-- Nightly: refresh development database with fresh production data

-- Scheduled task (runs at midnight):
CREATE OR REPLACE TASK ops.refresh_dev_db
    WAREHOUSE = 'OPS_WH_XS'
    SCHEDULE = 'USING CRON 0 0 * * * UTC'
AS
BEGIN
    -- Drop old dev database
    DROP DATABASE IF EXISTS dev_environment;
    
    -- Clone production (instant, zero-copy!)
    CREATE DATABASE dev_environment CLONE production;
    
    -- Grant access to dev team
    GRANT USAGE ON DATABASE dev_environment TO ROLE developers;
    GRANT USAGE ON ALL SCHEMAS IN DATABASE dev_environment TO ROLE developers;
    GRANT SELECT ON ALL TABLES IN DATABASE dev_environment TO ROLE developers;
    
    -- Mask PII in dev (don't expose real customer data to developers!)
    UPDATE dev_environment.production.customers
    SET email = 'dev_' || customer_id || '@test.com',
        phone = '555-000-' || LPAD(customer_id % 10000, 4, '0'),
        name = 'Customer ' || customer_id;
    -- PII masked, but data relationships preserved for testing
END;

-- Result: developers get fresh data every morning
-- Storage: minimal (clone shares partitions with production)
-- Only the masked PII columns create new storage (UPDATE creates new partitions)
-- Total extra storage: ~5% of customers table size (only PII columns changed)
```

---

## Interview Tips

> **Tip 1:** "How do you automate recovery with Time Travel?" — Stored procedure that: records pre-run timestamp, executes transformation, validates output (quality checks), and auto-rolls back via CLONE AT timestamp if quality fails. Production recovery time: 5-10 minutes (vs hours with traditional backups). Key: quality checks IMMEDIATELY after transformation.

> **Tip 2:** "How do you handle compliance snapshots?" — Schedule quarterly CLONE operations at quarter-end timestamps. Clones are permanent (don't expire like Time Travel). Auditors query the snapshot anytime: `SELECT * FROM compliance.Q1_2024.transactions`. Zero-copy until queried → minimal storage cost. This replaces expensive manual export/backup processes.

> **Tip 3:** "Development database strategy?" — Nightly CLONE of production (instant, zero-copy). Mask PII after clone (UPDATE email/phone). Grant to dev team. Result: fresh production-like data every morning with zero storage cost (until dev makes changes). Developers work on realistic data without PII exposure.
