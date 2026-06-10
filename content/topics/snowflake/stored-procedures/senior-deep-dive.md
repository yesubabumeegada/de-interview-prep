---
title: "Stored Procedures - Senior Deep Dive"
topic: snowflake
subtopic: stored-procedures
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [snowflake, stored-procedures, production, framework, metadata-driven]
---

# Snowflake Stored Procedures — Senior-Level Deep Dive

## Metadata-Driven ETL Framework

```sql
-- Configuration table drives all ETL (add new sources = add a row, not new code!)
CREATE TABLE etl.pipeline_config (
    pipeline_id NUMBER AUTOINCREMENT,
    source_schema VARCHAR, source_table VARCHAR,
    target_schema VARCHAR, target_table VARCHAR,
    primary_key VARCHAR, load_type VARCHAR, -- 'incremental' or 'full'
    stream_name VARCHAR, active BOOLEAN DEFAULT TRUE
);

-- Generic pipeline executor:
CREATE OR REPLACE PROCEDURE etl.run_pipeline(pipeline_id_param NUMBER)
RETURNS VARCHAR LANGUAGE SQL AS
$$
DECLARE
    config RECORD;
    result VARCHAR;
BEGIN
    SELECT * INTO config FROM etl.pipeline_config WHERE pipeline_id = pipeline_id_param;
    
    IF (config.load_type = 'incremental' AND SYSTEM$STREAM_HAS_DATA(config.stream_name)) THEN
        EXECUTE IMMEDIATE 
            'MERGE INTO ' || config.target_schema || '.' || config.target_table || ' t ' ||
            'USING (SELECT * FROM ' || config.stream_name || ' WHERE METADATA$ACTION=''INSERT'' ' ||
            'QUALIFY ROW_NUMBER() OVER (PARTITION BY ' || config.primary_key || ' ORDER BY _loaded_at DESC)=1) s ' ||
            'ON t.' || config.primary_key || ' = s.' || config.primary_key || ' ' ||
            'WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *';
        result := 'INCREMENTAL: ' || SQLROWCOUNT || ' rows merged';
    ELSEIF (config.load_type = 'full') THEN
        EXECUTE IMMEDIATE 'INSERT OVERWRITE INTO ' || config.target_schema || '.' || config.target_table ||
            ' SELECT * FROM ' || config.source_schema || '.' || config.source_table;
        result := 'FULL: ' || SQLROWCOUNT || ' rows loaded';
    ELSE
        result := 'SKIPPED: no data or unknown load_type';
    END IF;
    
    INSERT INTO etl.run_log (pipeline_id, result, run_time) VALUES (pipeline_id_param, result, CURRENT_TIMESTAMP());
    RETURN result;
END;
$$;

-- Run all active pipelines:
CREATE OR REPLACE PROCEDURE etl.run_all_pipelines()
RETURNS VARCHAR LANGUAGE SQL AS
$$
DECLARE
    pipeline_cursor CURSOR FOR SELECT pipeline_id FROM etl.pipeline_config WHERE active = TRUE;
    results VARCHAR DEFAULT '';
BEGIN
    FOR p IN pipeline_cursor DO
        results := results || CALL etl.run_pipeline(p.pipeline_id) || '\n';
    END FOR;
    RETURN results;
END;
$$;

-- Add new source: just insert a config row!
INSERT INTO etl.pipeline_config (source_schema, source_table, target_schema, target_table, primary_key, load_type, stream_name)
VALUES ('raw', 'new_source', 'silver', 'new_source', 'id', 'incremental', 'new_source_stream');
-- No new code needed — the framework handles it!
```

---

## Advanced Error Handling and Retry

```sql
CREATE OR REPLACE PROCEDURE etl.resilient_load(table_name VARCHAR, max_retries NUMBER DEFAULT 3)
RETURNS VARCHAR LANGUAGE SQL AS
$$
DECLARE
    attempt NUMBER DEFAULT 0;
    success BOOLEAN DEFAULT FALSE;
    last_error VARCHAR;
BEGIN
    WHILE (attempt < max_retries AND NOT success) DO
        attempt := attempt + 1;
        BEGIN
            EXECUTE IMMEDIATE 'CALL etl.run_pipeline_for_table(''' || table_name || ''')';
            success := TRUE;
        EXCEPTION
            WHEN OTHER THEN
                last_error := SQLERRM;
                INSERT INTO etl.retry_log (table_name, attempt, error, retry_time)
                VALUES (table_name, attempt, last_error, CURRENT_TIMESTAMP());
                -- Wait before retry (exponential backoff simulation):
                CALL SYSTEM$WAIT(attempt * 10);  -- 10s, 20s, 30s
        END;
    END WHILE;
    
    IF (NOT success) THEN
        CALL etl.send_alert('Pipeline FAILED after ' || max_retries || ' retries: ' || table_name || ' - ' || last_error);
        RETURN 'FAILED after ' || max_retries || ' attempts: ' || last_error;
    END IF;
    RETURN 'SUCCESS on attempt ' || attempt;
END;
$$;
```

---

## Stored Procedure Security

```sql
-- Principle of least privilege:
-- Procedure OWNER has full access; callers only need EXECUTE

-- Create with OWNER's RIGHTS (procedure uses owner's elevated access):
CREATE PROCEDURE admin.create_user_schema(user_name VARCHAR)
RETURNS VARCHAR LANGUAGE SQL
EXECUTE AS OWNER  -- Uses admin role's privileges!
AS $$
BEGIN
    EXECUTE IMMEDIATE 'CREATE SCHEMA IF NOT EXISTS sandbox.' || user_name;
    EXECUTE IMMEDIATE 'GRANT ALL ON SCHEMA sandbox.' || user_name || ' TO ROLE ' || user_name || '_role';
    RETURN 'Schema created: sandbox.' || user_name;
END;
$$;

-- Grant EXECUTE (not the underlying permissions!):
GRANT USAGE ON PROCEDURE admin.create_user_schema(VARCHAR) TO ROLE user_management;
-- Users with user_management role can create schemas via this procedure
-- They can't CREATE SCHEMA directly (no DDL privilege) — only through the procedure!
-- The procedure temporarily elevates their access (controlled, auditable)
```

---

## Interview Tips

> **Tip 1:** "How do you build a scalable ETL framework with stored procedures?" — Metadata-driven: configuration table defines sources, targets, keys, and load types. Generic executor procedure reads config and runs the appropriate operation (MERGE for incremental, INSERT OVERWRITE for full). Adding a new source = adding a config row, not writing new code. Scales to 100+ tables with one set of procedures.

> **Tip 2:** "How do you implement retry logic?" — Loop with attempt counter: try operation → on failure: log error, increment counter, wait (backoff) → retry until max_retries. If all retries fail: alert team, log final error, return failure status. The Task framework uses SUSPEND_TASK_AFTER_NUM_FAILURES similarly.

> **Tip 3:** "Owner's rights vs caller's rights?" — Owner's rights (default): procedure executes with the OWNER's permissions regardless of who calls it. Used for: admin procedures needing elevated access. Caller's rights: executes with the CALLER's permissions. Used for: utility procedures that should respect the caller's data access level. Owner's rights = controlled privilege escalation.

## ⚡ Cheat Sheet

**Snowflake architecture layers**
```
Cloud Services:   metadata, optimizer, access control, query planning
Virtual Warehouse: compute (T-shirt sizes: XS to 6XL); auto-suspend + auto-resume
Storage:          columnar Parquet on S3/Blob/GCS; billed separately from compute
```

**Virtual warehouse management**
```sql
CREATE WAREHOUSE analytics_wh WITH WAREHOUSE_SIZE='MEDIUM'
  AUTO_SUSPEND=60 AUTO_RESUME=TRUE MAX_CLUSTER_COUNT=3 MIN_CLUSTER_COUNT=1
  SCALING_POLICY='ECONOMY';  -- or STANDARD
ALTER WAREHOUSE analytics_wh SUSPEND;
ALTER WAREHOUSE analytics_wh SET WAREHOUSE_SIZE='LARGE';
```

**Time travel**
```sql
SELECT * FROM orders AT (OFFSET => -60*60);                          -- 1 hour ago
SELECT * FROM orders AT (TIMESTAMP => '2024-01-15 08:00:00'::TIMESTAMP);
SELECT * FROM orders BEFORE (STATEMENT => '8e5d0ca9-005e-44e6-b858-a8f5b37c5726');
-- Restore from time travel
CREATE TABLE orders_restored CLONE orders AT (OFFSET => -3600);
-- Default retention: 1 day (standard), up to 90 days (enterprise)
```

**Streams and Tasks**
```sql
-- Stream: CDC on a table
CREATE STREAM orders_stream ON TABLE orders;
SELECT * FROM orders_stream;  -- METADATA$ACTION, METADATA$ISUPDATE, METADATA$ROW_ID

-- Task: scheduled or triggered compute
CREATE TASK process_orders
  WAREHOUSE = 'etl_wh'
  SCHEDULE = '5 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('orders_stream')
AS
  INSERT INTO gold.orders SELECT * FROM orders_stream WHERE METADATA$ACTION = 'INSERT';

ALTER TASK process_orders RESUME;
```

**Dynamic Tables**
```sql
CREATE DYNAMIC TABLE gold.orders_summary
  TARGET_LAG = '5 minutes'
  WAREHOUSE = etl_wh
AS
  SELECT region, SUM(amount) AS total FROM silver.orders GROUP BY region;
-- Snowflake automatically refreshes when source changes; no task/stream needed
```

**Snowpipe (continuous ingestion)**
```sql
CREATE PIPE orders_pipe AUTO_INGEST=TRUE AS
  COPY INTO orders FROM @orders_stage FILE_FORMAT=(TYPE='CSV');
-- S3 event notification → SQS → Snowpipe auto-triggers COPY on new files
-- Latency: ~1 minute; cost: per-file compute credits
```

**Data sharing**
```sql
CREATE SHARE sales_share;
GRANT USAGE ON DATABASE prod TO SHARE sales_share;
GRANT SELECT ON TABLE prod.gold.orders TO SHARE sales_share;
ALTER SHARE sales_share ADD ACCOUNTS = partner_account_id;
-- Consumer sees a read-only database — no data copy, no egress charges
```

**Stored procedures (JavaScript/Python/Snowflake Scripting)**
```sql
CREATE OR REPLACE PROCEDURE load_and_validate(p_date STRING)
RETURNS STRING LANGUAGE PYTHON RUNTIME_VERSION='3.10'
PACKAGES=('snowflake-snowpark-python') HANDLER='run'
AS $$
def run(session, p_date):
    df = session.table("staging.orders").filter(f"order_date = '{p_date}'")
    if df.count() == 0:
        return f"No data for {p_date}"
    df.write.save_as_table("gold.orders", mode="append")
    return f"Loaded {df.count()} rows"
$$;
```

**External tables**
```sql
CREATE EXTERNAL TABLE ext_orders (
    order_id NUMBER AS (VALUE:c1::NUMBER),
    amount   FLOAT  AS (VALUE:c3::FLOAT)
) WITH LOCATION=@orders_stage FILE_FORMAT=(TYPE='PARQUET')
AUTO_REFRESH=TRUE;
-- Reads directly from S3; no data copy to Snowflake storage
```

**Materialized views**
```sql
CREATE MATERIALIZED VIEW mv_orders_by_region AS
  SELECT region, SUM(amount) AS total FROM orders GROUP BY region;
-- Auto-incremental refresh by Snowflake when base table changes
-- Best for: complex aggregations queried frequently; available in Enterprise+
```

**Key interview points**
- Micro-partitions: 50-500 MB compressed Parquet; automatic clustering per load order
- Cluster keys: explicit clustering on high-cardinality columns (date, customer_id)
- Query profile: check for partition pruning, spillage to disk, heavy operators
- Zero-copy clone: CREATE TABLE dev_orders CLONE gold.orders — instant, no storage cost
- Fail-safe: 7-day recovery window after time travel expires (Snowflake internal only)
