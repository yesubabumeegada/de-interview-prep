---
title: "Trade-off Analysis — Scenarios"
topic: system-design
subtopic: trade-off-analysis
content_type: study_material
difficulty_level: mid-level
layer: scenarios
tags: [system-design, trade-offs, interview, scenarios, decisions]
---

# Trade-off Analysis — Interview Scenarios

## Scenario 1: Kafka vs Database Polling for CDC

**Question:** Your team needs to sync order changes from PostgreSQL to Snowflake within 5 minutes. Two engineers disagree: one wants to use Debezium + Kafka CDC, the other wants to poll PostgreSQL every 5 minutes using a watermark. Analyze both options and make a recommendation.

**Answer:**

```
Option A: Debezium + Kafka (Log-based CDC)
  How it works: Debezium reads PostgreSQL WAL → Kafka topic → Kafka Connect → Snowflake
  Pros:
    - Captures DELETEs (watermark polling misses hard deletes)
    - Low latency: < 1 second from transaction commit to Kafka
    - Zero load on PostgreSQL (reads WAL, doesn't query tables)
    - Captures all changes (even updates with same updated_at timestamp)
  Cons:
    - Complex setup: Debezium connector, Kafka, replication slot configuration
    - Requires PostgreSQL WAL level = logical (must enable on source DB)
    - Operational overhead: Kafka cluster to manage
    - Schema changes require connector reconfiguration
  Cost estimate: MSK ~$300/month + Kafka Connect EC2 ~$100/month = ~$400/month

Option B: Watermark polling (SELECT WHERE updated_at > last_run)
  How it works: Airflow DAG every 5 min, JDBC reads new/updated rows, COPY to Snowflake
  Pros:
    - Simple: SQL + Airflow = 2 tools team already knows
    - Zero new infrastructure
    - Easy to debug ("run the SQL, see the data")
    - Schema changes handled automatically (SELECT * picks up new columns)
  Cons:
    - Misses hard DELETEs (no updated_at on deleted rows)
    - Requires updated_at column on all tables
    - Adds query load to source PostgreSQL (run every 5 minutes)
    - Edge case: UPDATE with same updated_at (same second) may be missed
  Cost: $0 (uses existing Airflow + Snowflake)

Recommendation:
  Choose Option B (watermark polling) if:
    - Hard deletes are rare or handled via soft deletes (is_deleted = true)
    - Source PostgreSQL has updated_at on all relevant tables
    - Source DB can absorb 5-minute polling queries
    - Team lacks Kafka expertise
  
  Choose Option A (Debezium) if:
    - Hard deletes must propagate (financial adjustments, GDPR right-to-erasure)
    - Source DB is under heavy load (polling would add strain)
    - Latency < 1 minute is needed (5-minute batch polling misses this)
    - Team has or is willing to invest in Kafka expertise
  
  For this scenario (5 min SLO, team debating):
  Start with Option B (watermark). Validate 95% of cases work.
  Add soft delete handling (is_deleted flag + watermark on deleted_at).
  Migrate to Debezium only when: deletes become a real problem OR
  latency requirement drops to < 1 minute.
```

---

## Scenario 2: Snowflake vs ClickHouse for Real-Time Analytics

**Question:** The business wants a dashboard that answers "revenue by product category in the last 15 minutes" in under 2 seconds. Current setup: Snowflake with 1B rows in `orders_fact`. Evaluate Snowflake vs ClickHouse for this use case.

**Answer:**

```
Snowflake for this use case:
  Current: Snowflake XS-M warehouse, query on 1B rows
  Real-time (last 15 min) filter: even with clustering key on order_timestamp,
    Snowflake must wake up (auto-resume: 1-2 sec), then scan micro-partitions
    Total: 3-8 seconds per dashboard load
  Optimization attempts:
    - Cluster by (TO_TIMESTAMP(order_timestamp)) → micro-partition pruning
    - Materialized view for last-15-min window → but can't be ON COMMIT (too expensive)
    - Result cache: only helps for identical repeat queries
  Verdict: Snowflake can get to 2-3 seconds, not < 2 seconds for real-time queries

ClickHouse for this use case:
  ClickHouse: columnar OLAP database optimized for real-time aggregation
  MergeTree table on 1B rows, partitioned by toDate(order_timestamp)
  Query: SELECT category, SUM(amount) WHERE order_timestamp >= NOW() - INTERVAL 15 MINUTE
  Typical performance: 100-500ms for this query type on 1B rows
  Streaming ingestion: Kafka → ClickHouse (native Kafka engine)
  Latency: data visible in ClickHouse within 1-2 seconds of Kafka publish

Trade-off comparison:
  | Dimension          | Snowflake     | ClickHouse         |
  |--------------------|---------------|--------------------|
  | Query latency      | 3-8 seconds   | 100-500ms ✓        |
  | Setup complexity   | Already have  | New infra to manage|
  | Ops overhead       | None (SaaS)   | Medium (self-host)  |
  | Cost               | $0 incremental| +$500-1000/month   |
  | SQL compatibility  | Full ANSI SQL | Mostly ANSI SQL    |
  | Real-time ingestion| Batch COPY    | Native Kafka engine|
  | BI tool support    | All tools     | Grafana, Superset  |

Recommendation:
  If latency < 2 seconds is a hard requirement: ClickHouse
    - Add ClickHouse alongside Snowflake (dual write from Kafka)
    - Dashboard queries ClickHouse for real-time; Snowflake for historical
  If 3-5 seconds is acceptable: stay with Snowflake
    - Add Snowflake cluster key (order_timestamp) + separate XS warehouse
      with AUTO_RESUME=TRUE for dashboard queries
    - Eliminate auto-resume latency with a scheduled "keep-warm" ping

Decision: real-time < 2 seconds = ClickHouse. Near-real-time 3-5 seconds = Snowflake with tuning.
```

---

## Scenario 3: Monorepo vs Separate Repos for Data Platform

**Question:** Your data platform has: dbt models, Airflow DAGs, Python ingestion scripts, Spark jobs, and Terraform infra-as-code. Should you put everything in one repository or separate repositories?

**Answer:**

```
Monorepo (everything in one repo):
  Structure:
    /dbt              ← dbt models
    /airflow          ← DAG definitions
    /ingestion        ← Python ingestion
    /spark            ← Spark jobs
    /infrastructure   ← Terraform
    /tests            ← integration tests

  Pros:
    - Atomic changes: dbt model + Airflow DAG + Spark job updated in one PR
    - Unified CI/CD: one pipeline, one test suite, one linting config
    - Easy cross-project search and refactoring
    - Single PR review covers full feature
    - Consistent tooling: same pre-commit hooks, same Python version
  
  Cons:
    - CI/CD slow: every PR triggers all tests (even unrelated changes)
    - Access control: all contributors see all code (not always OK)
    - Merge conflicts: many engineers in same repo → more conflicts

Separate repos:
  dbt-models, airflow-dags, ingestion-scripts, spark-jobs, infra

  Pros:
    - Faster CI/CD (only tests relevant to changed code run)
    - Independent deployments (deploy dbt without affecting Airflow)
    - Clearer ownership and access control per team
  
  Cons:
    - Cross-repo changes require multiple PRs (coordination overhead)
    - Hard to find "where is the code for X" (new engineers struggle)
    - Duplicated configuration (Python deps, linting in every repo)
    - Version coordination: dbt model v2 requires Airflow DAG change too → sync across repos

Recommendation:
  Small team (< 5 DE): monorepo strongly recommended
    Rationale: coordination overhead > CI speed benefit; team learns the full system
  
  Large team (> 10 DE, separate domains): modular repos with a shared infra repo
    Keep: dbt + Airflow in one repo (they're tightly coupled)
    Separate: Spark jobs (different runtime, different team), Terraform (infra team)
  
  Always: use path-based CI filtering in monorepo (only run dbt tests when dbt/ changed)
    GitHub Actions: on: push: paths: ['dbt/**']
    This gives monorepo benefits without slow CI penalty
```
