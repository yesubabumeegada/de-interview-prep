---
title: "Lambda & Kappa Architecture — Real World"
topic: system-design
subtopic: lambda-kappa-architecture
content_type: study_material
difficulty_level: mid-level
layer: real-world
tags: [system-design, lambda-architecture, kappa-architecture, production, migration]
---

# Lambda & Kappa Architecture — Real World

## Pattern 1: Migrating Lambda to Lakehouse

**Situation:** A company has a Lambda Architecture that's 4 years old: Hadoop batch + Kafka Streams. Two separate codebases. Every feature change requires double implementation. Plan to migrate to Lakehouse.

```
Migration strategy: strangle the batch layer incrementally

Phase 1 (Month 1-2): Add Delta Lake as new serving layer
  - New Spark Structured Streaming job writes to Delta (same logic as Kafka Streams)
  - Delta table exposed alongside existing serving layer
  - A/B comparison: validate Delta results match existing batch views
  - Zero risk: old system still runs

Phase 2 (Month 3-4): Migrate batch jobs one by one
  - Re-implement batch jobs as Spark SQL writing to Delta
  - Each migrated job: validate output matches Hadoop job output
  - After validation: switch BI reports to Delta output, retire Hadoop job
  - Do this for 1-2 most important tables first

Phase 3 (Month 5-6): Eliminate speed layer
  - Streaming job (Kafka Streams) replaced by Spark Structured Streaming → Delta
  - No more separate "speed layer" — Delta handles both with time-travel
  - Retire old serving layer merge code

Phase 4 (Month 7): Decommission Hadoop
  - All jobs migrated to Spark + Delta
  - Hadoop cluster shut down
  - Savings: significant infrastructure cost reduction

Key principle: never big bang. Migrate table by table. Run old and new in parallel.
```

---

## Pattern 2: Kappa in Practice — Fraud Detection

```python
# Kappa architecture for real-time fraud detection

# Single source of truth: Kafka topic 'transactions.raw'
# Retention: 90 days (long enough for model retraining)

# Real-time job (production):
fraud_stream = (
    spark.readStream
        .format("kafka")
        .option("subscribe", "transactions.raw")
        .option("startingOffsets", "latest")
        .load()
    .transform(score_transaction)       # ML model scoring
    .writeStream
        .format("delta")
        .option("checkpointLocation", "s3://checkpoints/fraud_v3")
        .start("s3://delta/fraud_scores_v3")
)

# When model is updated (reprocessing):
def deploy_new_model(model_version: str):
    # Step 1: Start new job reading from beginning
    new_job = start_streaming_job(
        source_offsets="earliest",
        model_version=model_version,
        output_table=f"fraud_scores_{model_version}",
        checkpoint=f"s3://checkpoints/fraud_{model_version}"
    )
    
    # Step 2: Monitor catch-up progress
    while get_consumer_lag(f"fraud_consumer_{model_version}") > 10000:
        time.sleep(60)
        print(f"Still catching up: lag = {get_consumer_lag(...)}")
    
    # Step 3: Switch serving layer
    db.execute(f"""
        UPDATE model_serving_config
        SET active_output_table = 'fraud_scores_{model_version}',
            updated_at = NOW()
    """)
    
    # Step 4: Stop old job
    stop_old_job(current_version)
    print(f"Switched to model {model_version}")
```

---

## Architecture Decision Matrix

| Scenario | Recommended Architecture | Why |
|---|---|---|
| Batch-only analytics, >4 hr latency OK | Pure batch (dbt + Snowflake) | Simpler, cheaper, fully SQL |
| Real-time + historical analytics | Kappa (Spark + Delta Lake) | One codebase, unified storage |
| ML training + real-time inference | Lambda (batch for training, streaming for inference) | Inherently different requirements |
| Legacy Hadoop + new streaming needs | Lambda (transitional) | Can't migrate overnight |
| Regulatory, exact historical reports | Lambda or Lakehouse with Delta time-travel | Audit trail required |
| Simple streaming, no history needed | Kappa with short retention | No need for long Kafka retention |

---

## Interview Tips

> **Tip 1:** "What are the operational challenges of Lambda Architecture?" — Two separate systems to operate, monitor, and maintain. Separate alerting setups. Two code paths to keep in sync — any business logic change must be implemented twice (once in batch SQL/Spark, once in streaming Flink/Kafka Streams). Integration testing is harder: you need to test both paths and the merge logic. Separate skill sets: team members who know Hadoop batch may not know streaming. These operational costs are why Kappa/Lakehouse became more popular.

> **Tip 2:** "How do you handle a bug fix that affects historical data in Kappa?" — Deploy a new version of the streaming job that reads from Kafka offset 0 (beginning of the log). It processes all historical events with the corrected logic and writes to a new output table. When it catches up to real-time, switch the serving layer to the new output. This assumes: (1) Kafka retains events long enough, (2) the corrected logic is idempotent, (3) the new output table is validated before switching.

> **Tip 3:** "How would you explain Lambda vs Kappa to a non-technical stakeholder?" — Lambda: "We have two systems — one that's very accurate but takes hours, and one that's fast but approximate. We show you the combination." Kappa: "We have one system that stores everything and processes it as a stream. When we fix a bug or update a calculation, we reprocess the history automatically — like rewinding and replaying a recording with the new logic." Most stakeholders prefer Kappa's simplicity: one number, one system, same result for historical and real-time.
