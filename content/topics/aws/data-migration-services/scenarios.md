---
title: "AWS Data Migration Services Scenarios"
description: "Hands-on migration scenarios using AWS DMS, SCT, and Snow family"
content_type: scenario_question
topic: aws-services
subtopic: data-migration-services
tags: [aws, dms, data-migration, snowball, sct, oracle, redshift]
---

# AWS Data Migration Services Scenarios

<article data-difficulty="junior">

## Scenario: Choosing the Right Migration Tool

Your team needs to migrate 10TB of CSV files stored on an on-premises NAS server to Amazon S3. A colleague suggests using AWS DMS. Is DMS the right tool? What would you use instead?

<details>
<summary>✅ Solution</summary>

DMS is designed for database-to-database migrations, not file transfers. For this use case:

**Correct tool: AWS DataSync**
- Designed specifically for file and object storage migration
- Agent installed on-prem connects to NAS (NFS/SMB)
- Transfers to S3, EFS, or FSx
- Built-in data integrity verification (checksums)
- Scheduling and bandwidth throttling
- Up to 10x faster than open-source tools

**When to use Snow family instead:**
- If 10TB needs to arrive in <1 week and network is slow (<100Mbps), Snowball is faster
- At 100Mbps sustained: 10TB takes ~10 days via DataSync
- Snowball: 80TB capacity, ~1 week turnaround (ship + ingest)
- At 10TB with decent network: DataSync wins on simplicity and cost

**Setup steps:**
1. Deploy DataSync agent (VM) on-prem
2. Create NAS location (source) + S3 location (destination)
3. Create DataSync task with filter rules and schedule
4. Monitor transfer progress in CloudWatch
5. Validate: DataSync task verification report

</details>

</article>

<article data-difficulty="mid">

## Scenario: Zero-Downtime MySQL to Redshift Migration

Your company runs an e-commerce platform on MySQL (RDS). You need to migrate 3 years of historical data (500GB, 80 tables) to Amazon Redshift for analytics, with zero downtime to the production MySQL database. New transactions must continue flowing to Redshift after migration.

<details>
<summary>✅ Solution</summary>

**Architecture: DMS Full Load + CDC**

**Phase 1: Preparation**
- Create Redshift target schema (use SCT or manual DDL)
- Choose Redshift distribution keys based on join patterns (e.g., `customer_id` for orders table)
- Create DMS replication instance (r5.xlarge for 500GB, Multi-AZ)
- Enable MySQL binary logging (`binlog_format=ROW`, `binlog_row_image=FULL`)

**Phase 2: Full Load**
```
DMS Task Settings:
- Task type: Migrate existing data and replicate ongoing changes
- Target table prep: Do nothing (pre-create tables)
- LOB settings: Limited LOB (64KB) unless BLOBs present
- Parallel load: Segment by PK ranges (8 segments per large table)
- Stop task after: Never (keep CDC running)
```

**Phase 3: CDC (ongoing replication)**
- DMS reads MySQL binlog continuously
- Latency target: <30s replication lag
- Monitor: `CDCLatencySource` + `CDCLatencyTarget` CloudWatch metrics

**Phase 4: Cutover**
1. Confirm CDC lag < 5 seconds
2. Switch analytics queries to Redshift (read traffic only — no cutover needed for MySQL writes)
3. Keep DMS task running for ongoing sync

**Validation:**
- Row count comparison: `SELECT COUNT(*) FROM orders` on both systems
- Revenue spot check: sum of order_total for last 30 days
- DMS task validation report (enable `enableValidation: true`)

**Cost estimate:** ~$200/month for DMS replication instance + data transfer

</details>

</article>

<article data-difficulty="senior">

## Scenario: Oracle Data Warehouse to Snowflake Migration

You're leading a migration of a 50TB Oracle data warehouse (12c) with 200+ tables, 80 stored procedures, 50 materialized views, and dozens of ETL jobs (Oracle Data Integrator) to Snowflake. The business requires: <4 hours downtime, full data validation, and existing BI reports working on Day 1.

<details>
<summary>✅ Solution</summary>

**Migration Strategy: Phased with Parallel Run**

**Phase 1: Assessment (Weeks 1-2)**
- Run AWS SCT on Oracle schema → Snowflake target
- Review SCT assessment report:
  - Tables: ~95% auto-converted
  - Stored procedures: ~50% auto-converted (PL/SQL → JavaScript UDFs or Snowpark)
  - Materialized views → Snowflake dynamic tables or dbt models
  - ODI jobs → Snowflake Tasks + Streams or migrate to dbt/Airflow
- Prioritize manual conversion backlog by business criticality

**Phase 2: Schema & ETL Migration (Weeks 3-8)**
- Convert Oracle DDL → Snowflake (NUMBER → FLOAT/NUMBER, DATE → TIMESTAMP_NTZ)
- Rewrite stored procedures as Snowpark Python or JavaScript UDFs
- Rebuild ETL in dbt (preferred) or Airflow DAGs
- Rebuild BI reports pointing to Snowflake (Tableau/Power BI connection swap)

**Phase 3: Data Migration (Weeks 9-11)**
- 50TB strategy: Oracle EXPDP → S3 via AWS Direct Connect, then Snowflake COPY INTO
  - DMS not recommended at 50TB Oracle scale (use native Oracle export)
  - Parallel: export 10 schemas simultaneously overnight
  - Snowflake COPY INTO with PURGE=TRUE, 8 virtual warehouse size for load
- Incremental sync: Oracle GoldenGate or AWS DMS CDC for delta during parallel run

**Phase 4: Parallel Run (Weeks 12-13)**
- Both Oracle and Snowflake receive writes (dual-write from ETL layer)
- Run validation suite daily:
  - Row counts per table
  - Aggregate checksums (SUM, COUNT DISTINCT) for key metrics
  - Revenue reconciliation report
- BI team validates all reports on Snowflake copy

**Phase 5: Cutover (<4 hours)**
1. T-2h: Final SCT/GoldenGate sync, drain replication lag to 0
2. T-0: Stop Oracle ETL writes
3. T+15min: Final validation row count check
4. T+30min: Switch all ETL to write to Snowflake only
5. T+1h: Switch all BI connections to Snowflake
6. T+4h: Oracle DB goes read-only, migration complete

**Rollback plan:** Oracle remains read-only for 2 weeks; if critical issue found, re-point BI to Oracle within 30 minutes

**Key risks & mitigations:**
- Oracle DATE stores time (Snowflake DATE does not) → migrate to TIMESTAMP_NTZ
- Number precision: Oracle NUMBER(38) → Snowflake FLOAT loses precision for exact decimal → use NUMBER(38,10)
- Sequence gaps after migration → AUTOINCREMENT in Snowflake resets; use MAX(id)+1 seed
- Partition-wise queries in Oracle → Snowflake clustering keys (not identical, needs query tuning)

</details>

</article>
