---
title: "Azure Data Factory — Scenarios"
topic: azure
subtopic: azure-data-factory
content_type: study_material
difficulty_level: mid-level
layer: scenarios
tags: [azure, adf, scenarios, interview, pipeline-design]
---

# Azure Data Factory — Interview Scenarios

## Scenario 1: Design a Daily ETL for 50 Tables from On-Prem SQL Server

**Question:** You need to ingest 50 tables daily from an on-premises SQL Server into ADLS Gen2. Each table has an `updated_at` column. Some tables are small (10K rows), some are large (100M rows). Design the ADF pipeline.

**Answer:**

```
Architecture: metadata-driven pipeline with SHIR

Infrastructure setup:
  1. Install Self-Hosted IR on on-prem Windows server (8 CPU, 16GB RAM)
  2. Register SHIR with ADF — creates secure tunnel via Azure Service Bus
  3. Create Linked Services:
     - OnPremSQL (SHIR-based): server, db, credentials from Key Vault
     - ADLSG2: managed identity auth to storage account
  4. Create control table in Azure SQL DB:

CREATE TABLE etl_config (
    table_name      VARCHAR(100) PRIMARY KEY,
    source_schema   VARCHAR(50)  DEFAULT 'dbo',
    sink_path       VARCHAR(200),
    watermark_col   VARCHAR(50),
    is_active       BIT          DEFAULT 1,
    last_loaded_at  DATETIME2    DEFAULT '2000-01-01'
);
-- Insert 50 rows, one per table

Pipeline design:

Pipeline: master_daily_etl (Schedule Trigger — daily 1 AM)
  Activity 1: LookupConfig
    - Query: SELECT * FROM etl_config WHERE is_active = 1
    - First row only: No (returns all 50 rows)

  Activity 2: ForEach (settings: sequential=false, batchCount=10)
    -- 10 tables copy in parallel to saturate SHIR (4-node SHIR handles 10 parallel)
    Inside ForEach → Execute Pipeline: child_copy_table
      Parameters:
        tableName:    @{item().table_name}
        schema:       @{item().source_schema}
        sinkPath:     @{item().sink_path}
        watermarkCol: @{item().watermark_col}
        lastLoadedAt: @{item().last_loaded_at}

Pipeline: child_copy_table (parameters: tableName, schema, sinkPath, watermarkCol, lastLoadedAt)
  Activity 1: CopyActivity
    Source: "SELECT * FROM @{schema}.@{tableName} WHERE @{watermarkCol} > '@{lastLoadedAt}'"
    Sink: ADLS path = @{sinkPath}/run_date=@{formatDateTime(pipeline().TriggerTime,'yyyy-MM-dd')}/
    Format: Parquet + Snappy
    parallelCopies: 4 (for large tables, adjustable per table via config)
    Retry: 3 times, 60s interval

  Activity 2 (on success): UpdateWatermark
    Stored Procedure: UPDATE etl_config SET last_loaded_at = GETUTCDATE() WHERE table_name = @tableName

  Activity 3 (on failure): LogError
    Stored Procedure: INSERT INTO etl_error_log VALUES (@tableName, @errorMsg, GETUTCDATE())

Result:
  50 tables processed with 10 concurrent copies
  New tables: add 1 row to etl_config — zero pipeline changes
  SHIR handles 10 concurrent connections fine on 8-CPU server
  Runtime estimate: 50 tables / 10 parallel = 5 "waves" × ~5 min/table = ~25 min total
```

---

## Scenario 2: ADF Pipeline Is Failing on 3rd Day of Month, Never Fails Otherwise

**Question:** A tumbling window pipeline loads monthly sales data. It fails every 3rd day of the month but succeeds other days. How do you investigate?

**Answer:**

```
Clue: monthly pattern, not daily. Likely cause: large volume on 3rd day (end-of-previous-month data settled).

Step 1: Check ADF Monitor
  Pipeline Runs → filter to failing runs
  Activity Runs → find which activity fails
  Error message: "SQL timeout after 3600 seconds" or "OOM in Data Flow"

Step 2: If Copy Activity timeout:
  Source SQL takes too long → 3rd day has 30-day month worth of records
  Fix options:
    a) Increase timeout from 3600s to 7200s in Copy Activity policy
    b) Enable partition range: split the large table copy into 10 parallel ranges
       partitionOption: DynamicRange
       partitionColumnName: order_id
       partitionUpperBound: @{activity('LookupMaxId').output.firstRow.max_id}
       partitionLowerBound: 1
       parallelCopies: 10

Step 3: If Data Flow OOM:
  Data Flow cluster running out of memory on large monthly data
  Fix: increase cluster size (General Compute → Memory Optimized) for this pipeline
  Or: split the Data Flow into two runs (first half of month, second half)

Step 4: If Synapse load timeout:
  PolyBase/COPY INTO taking too long for monthly batch
  Fix: pre-stage data in multiple small Parquet files
  COPY INTO: Azure Synapse has 60-min default timeout → increase via SET QUERY_TIMEOUT

Step 5: Verify fix with historical backfill
  Tumbling Window: right-click the 3rd-day failed windows → Rerun
  Monitor: confirm they now succeed

Root cause pattern: monthly summary creates 30× the volume of a typical daily run
General rule: design pipelines for worst-case volume (month-end, year-end), not average
```

---

## Scenario 3: Real-Time File Arrival Trigger

**Question:** Files land in an ADLS Gen2 container from 50 partner systems throughout the day. Each file must trigger processing within 5 minutes of arrival. Design the pipeline.

**Answer:**

```
Solution: Event-based trigger (Storage Event Trigger)

Setup:
  1. Create Storage Event Trigger in ADF:
     - Storage account: partners-raw
     - Container: /inbound/
     - Blob path begins with: (empty — all blobs)
     - Event: BlobCreated
     - Blob path ends with: .parquet (filter non-data files)
     
  2. Behind the scenes: ADF creates Azure Event Grid subscription
     Event Grid receives BlobCreated event from Storage → delivers to ADF trigger
     Latency: typically 30–90 seconds from file creation to pipeline start
  
  3. Pipeline receives trigger variables:
     @triggerBody().folderPath   → "inbound/partner_A/2024-01-15/"
     @triggerBody().fileName     → "orders_2024_01_15_123456.parquet"
  
  4. Pipeline: process_partner_file
     Activity 1: DerivePartner
       Set Variable: partner_name = first segment of folderPath
       @{split(triggerBody().folderPath, '/')[1]}  -- extracts 'partner_A'
     
     Activity 2: LookupPartnerConfig
       Query: SELECT * FROM partner_config WHERE partner_name = @{variables('partner_name')}
     
     Activity 3: Copy/Transform based on partner config
       Each partner has different schema → config-driven schema mapping
     
     Activity 4: MoveToProcessed
       Copy file to /processed/{partner}/{date}/
       Delete from /inbound/

Concurrency:
  50 files arriving simultaneously → 50 parallel pipeline runs
  Set pipeline concurrency = 50 (default is 10, increase it)
  SHIR: if on-prem sources are involved, size SHIR for peak concurrent connections

Failure handling:
  Failed pipeline: file stays in /inbound/ for manual replay
  Dead letter: after N failures, move to /failed/ container
  Alert: Azure Monitor alert on pipeline failure count > 5 in 1 hour
  
Latency:
  BlobCreated event → ADF trigger → pipeline start: ~1-2 minutes
  Pipeline execution (copy + transform): ~2-3 minutes
  Total end-to-end: ~3-5 minutes ✓ (meets 5-minute SLA)
```

---

## Interview Tips

> **Tip 1:** "How do you design for idempotency in ADF pipelines?" — For Copy Activity: write to a path that includes run date/window, not current timestamp. Use mode "overwrite" on the sink — re-running writes the same data to the same path. For UPSERT targets (SQL, Delta): ensure Copy uses UPSERT/MERGE mode, not INSERT. For Data Flows: use "Truncate Table" or partition overwrite (`replaceWhere`). Key: a pipeline run that executes twice on the same window should produce the same result as running once.

> **Tip 2:** "What's the max scale of ADF?" — Single factory limits: 5,000 pipeline runs per 24 hours (soft limit, can increase), 100 concurrent pipeline runs per factory (default), 40 activities per pipeline (logical), no hard limit on number of pipelines. For enterprise: use separate ADF factories per environment (dev/stage/prod), not per team. If hitting 100 concurrent runs: request limit increase from Microsoft or batch operations in ForEach with batchCount to stay under limit.

> **Tip 3:** "How would you migrate from an on-premises SSIS-based ETL to ADF?" — Four options: (1) Lift-and-shift: deploy Azure-SSIS IR, run existing .dtsx packages unchanged (fastest migration, no Azure-native optimization). (2) Re-platform: recreate SSIS packages as ADF Copy + Mapping Data Flow (more work, better cost and performance). (3) Hybrid: move simple packages to ADF, keep complex SSIS on Azure-SSIS IR while modernizing. (4) Replace with Databricks for heavy transformation packages. In practice: audit all SSIS packages, categorize by complexity, apply Pareto — 80% of packages are simple copies that can become ADF Copy Activities.
