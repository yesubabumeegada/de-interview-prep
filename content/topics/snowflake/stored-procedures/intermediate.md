---
title: "Stored Procedures - Intermediate"
topic: snowflake
subtopic: stored-procedures
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [snowflake, stored-procedures, dynamic-sql, cursors, transactions]
---

# Snowflake Stored Procedures — Intermediate

## Dynamic SQL (EXECUTE IMMEDIATE)

```sql
CREATE OR REPLACE PROCEDURE etl.generic_incremental_load(
    source_schema VARCHAR, source_table VARCHAR, 
    target_schema VARCHAR, target_table VARCHAR,
    pk_column VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    sql_text VARCHAR;
    rows_merged NUMBER;
BEGIN
    sql_text := 'MERGE INTO ' || target_schema || '.' || target_table || ' t ' ||
        'USING ' || source_schema || '.' || source_table || '_stream s ' ||
        'ON t.' || pk_column || ' = s.' || pk_column || ' ' ||
        'WHEN MATCHED AND s.METADATA$ACTION = ''INSERT'' THEN UPDATE SET * ' ||
        'WHEN NOT MATCHED AND s.METADATA$ACTION = ''INSERT'' THEN INSERT *';
    
    EXECUTE IMMEDIATE sql_text;
    rows_merged := SQLROWCOUNT;
    
    INSERT INTO etl.load_log (source, target, rows_processed, load_time)
    VALUES (source_schema || '.' || source_table, target_schema || '.' || target_table, 
            rows_merged, CURRENT_TIMESTAMP());
    
    RETURN 'Merged ' || rows_merged || ' rows into ' || target_table;
END;
$$;

-- Reusable across many tables:
CALL etl.generic_incremental_load('raw', 'orders', 'silver', 'orders', 'order_id');
CALL etl.generic_incremental_load('raw', 'customers', 'silver', 'customers', 'customer_id');
```

---

## RESULTSET and Cursors

```sql
CREATE OR REPLACE PROCEDURE etl.process_all_streams()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    stream_cursor CURSOR FOR
        SELECT stream_name, table_name
        FROM etl.stream_registry
        WHERE active = TRUE;
    stream_name VARCHAR;
    table_name VARCHAR;
    total_processed NUMBER DEFAULT 0;
BEGIN
    OPEN stream_cursor;
    
    FOR record IN stream_cursor DO
        stream_name := record.stream_name;
        table_name := record.table_name;
        
        IF (SYSTEM$STREAM_HAS_DATA(stream_name)) THEN
            EXECUTE IMMEDIATE 
                'INSERT INTO silver.' || table_name || 
                ' SELECT * FROM ' || stream_name || 
                ' WHERE METADATA$ACTION = ''INSERT''';
            total_processed := total_processed + SQLROWCOUNT;
        END IF;
    END FOR;
    
    CLOSE stream_cursor;
    RETURN 'Processed ' || total_processed || ' total rows across all streams';
END;
$$;
```

---

## Transaction Control

```sql
CREATE OR REPLACE PROCEDURE etl.atomic_pipeline_step()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    -- All operations in one transaction (all succeed or all fail):
    BEGIN TRANSACTION;
    
    -- Step 1: Delete stale data
    DELETE FROM silver.orders WHERE order_date < DATEADD('year', -2, CURRENT_DATE());
    
    -- Step 2: Insert new data
    INSERT INTO silver.orders
    SELECT * FROM orders_stream WHERE METADATA$ACTION = 'INSERT';
    
    -- Step 3: Update metrics
    UPDATE gold.table_metadata
    SET last_refresh = CURRENT_TIMESTAMP(), row_count = (SELECT COUNT(*) FROM silver.orders)
    WHERE table_name = 'silver.orders';
    
    COMMIT;
    RETURN 'SUCCESS: transaction committed';
    
EXCEPTION
    WHEN OTHER THEN
        ROLLBACK;
        INSERT INTO etl.error_log VALUES ('atomic_pipeline_step', SQLERRM, CURRENT_TIMESTAMP());
        RETURN 'ROLLED BACK: ' || SQLERRM;
END;
$$;
```

---

## Python Stored Procedures (Snowpark)

```sql
CREATE OR REPLACE PROCEDURE ml.score_customers()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python', 'pandas', 'scikit-learn')
HANDLER = 'main'
AS
$$
import pandas as pd
from sklearn.cluster import KMeans

def main(session):
    # Read data into pandas
    df = session.table("gold.customer_features").to_pandas()
    
    # Run K-means clustering
    features = df[['total_orders', 'total_spend', 'days_since_last_order']]
    kmeans = KMeans(n_clusters=4, random_state=42)
    df['segment'] = kmeans.fit_predict(features)
    
    # Write results back
    session.create_dataframe(df).write.mode("overwrite").save_as_table("gold.customer_segments")
    
    return f"SUCCESS: {len(df)} customers scored into 4 segments"
$$;

CALL ml.score_customers();
```

---

## Interview Tips

> **Tip 1:** "When do you use dynamic SQL (EXECUTE IMMEDIATE)?" — For reusable procedures that operate on different tables (generic ETL loaders, admin utilities). Build SQL strings from parameters, then execute. Essential for: table-name parameters, conditional DDL, and metadata-driven pipelines.

> **Tip 2:** "How do you ensure atomicity in stored procedures?" — Use explicit BEGIN TRANSACTION / COMMIT / ROLLBACK. In the EXCEPTION block: ROLLBACK to undo partial work. This ensures: either ALL steps succeed or NONE persist (critical for multi-table ETL where partial state is dangerous).

> **Tip 3:** "Python vs SQL stored procedures?" — SQL: for DML operations (INSERT, MERGE, UPDATE) — fastest, native. Python: for ML (scikit-learn, pandas), complex algorithms, external API calls. Use SQL for 90% of ETL procedures; Python for the 10% requiring ML or complex computation.
