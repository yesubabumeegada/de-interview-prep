---
title: "Schema Management - Real World"
topic: pyspark
subtopic: schema-management
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [pyspark, schema, production, incidents, schema-drift, timestamp, best-practices]
---

# Schema Management — Real World

## Production Schema Incidents

These are real categories of incidents that happen in production PySpark pipelines. Understanding them is essential for designing resilient systems.

---

### Incident 1: Inference Reading 10 Files Instead of 1

**What happened:** A pipeline used `inferSchema=True` on an S3 prefix containing 500 Parquet files. Spark sampled only the first few files for schema inference. Those files had a sparse `promo_code` column that only appeared in 3% of records. In the sample files it had no non-null values, so Spark inferred `StringType`. Later, a partition containing numeric promo codes (e.g., `"12345"` stored as integer in a different upstream system) caused type conflicts, silently converting thousands of records to null.

**How it was found:** An anomaly alert fired on a KPI dashboard showing a sudden 12% drop in attributed revenue. Tracing back through the lineage revealed the null promo codes.

**Fix:**
```python
# Define explicit schema based on agreed-upon data contract
PROMO_SCHEMA = StructType([
    StructField("order_id", LongType(), nullable=False),
    StructField("promo_code", StringType(), nullable=True),  # explicitly STRING, always
    StructField("discount_amount", DecimalType(10, 2), nullable=True),
])

df = spark.read.schema(PROMO_SCHEMA).parquet("s3://bucket/promos/")
```

---

### Incident 2: Schema Drift from Column Reordering (Position-Based Parquet Reader)

**What happened:** An upstream team reordered columns in their Parquet export (swapped `city` and `state`). The consuming pipeline read Parquet by **position** (not by name) because a legacy schema was applied using `select()` positionally:

```python
# Fragile — assumes column order matches schema order
df = spark.read.parquet(path).toDF("user_id", "email", "city", "state")
```

After the reorder, `city` and `state` values were silently swapped. This corrupted 3 weeks of data before being caught by a data quality check.

**Fix:**
```python
# Read Parquet by column name — Parquet's native format supports name-based reads
schema = StructType([
    StructField("user_id", LongType()),
    StructField("email", StringType()),
    StructField("city", StringType()),
    StructField("state", StringType()),
])
df = spark.read.schema(schema).parquet(path)
# Spark matches columns by NAME from the schema, not by position
```

**Lesson:** Never use `.toDF()` to rename columns on an existing dataset with positional mapping. Use `.withColumnRenamed()` or an explicit schema with name matching.

---

### Incident 3: `mergeSchema=True` Silently Accepting Bad Data

**What happened:** A team ran `mergeSchema=True` in their append job. An upstream bug introduced a `revenue` column with type `StringType` instead of the existing `DoubleType`. Delta merged the schemas by promoting `revenue` to `StringType` (the wider type). Downstream aggregations failed with `AnalysisException: cannot resolve 'revenue' due to data type mismatch`.

**Fix:**
```python
# Always validate BEFORE writing, even with mergeSchema
def safe_append_with_merge(df, table_path, expected_schema):
    # Fail on type changes even if names are the same
    for field in expected_schema.fields:
        if field.name in df.columns:
            actual_type = df.schema[field.name].dataType
            if actual_type != field.dataType:
                raise ValueError(
                    f"Type change detected for '{field.name}': "
                    f"expected {field.dataType}, got {actual_type}. "
                    f"Refusing to mergeSchema."
                )
    
    df.write.format("delta").mode("append") \
        .option("mergeSchema", "true") \
        .save(table_path)
```

**Lesson:** `mergeSchema=True` is not a substitute for schema validation. It only handles additive changes safely — type changes can still corrupt your table.

---

### Incident 4: Timestamp Timezone Mismatch (Spark 3.4+)

**What happened:** After upgrading from Spark 3.2 to 3.4, timestamp columns that were previously stored as `TimestampType` (UTC-anchored, timezone-aware) started behaving differently. The team's data was written in Spark 3.2 with wall-clock timestamps in US/Eastern, but `TimestampType` stored them as UTC. In Spark 3.4, `TimestampNTZType` (No Time Zone) was introduced, and some reading paths defaulted to it, causing 5-hour offsets in reporting.

**TimestampType vs TimestampNTZType:**

| Type | Behavior | Use case |
|------|----------|----------|
| `TimestampType` | UTC-anchored; adjusts to session timezone | Events (things that happen at a specific instant) |
| `TimestampNTZType` | No timezone; wall-clock reading | Dates/times where timezone is irrelevant (e.g., business hours) |

```python
from pyspark.sql.types import TimestampNTZType

# Explicit: use TimestampType for events, TimestampNTZType for wall-clock times
schema = StructType([
    StructField("event_at", TimestampType(), nullable=True),        # UTC event time
    StructField("scheduled_at", TimestampNTZType(), nullable=True), # wall-clock time
])

# Set session timezone explicitly to avoid surprises
spark.conf.set("spark.sql.session.timeZone", "UTC")
```

---

## Schema Management Best Practices Checklist

Use this checklist when designing or reviewing a PySpark pipeline:

### Design Time
- [ ] All schemas are defined explicitly as `StructType` objects (not inferred)
- [ ] Schemas are version-controlled alongside pipeline code
- [ ] `DecimalType(p, s)` is used for monetary/financial columns (not `DoubleType`)
- [ ] `LongType` is used for IDs (not `IntegerType`, which overflows at ~2.1B)
- [ ] Nested JSON columns have defined `StructType` schemas for `from_json`
- [ ] Timestamp timezone strategy is documented (UTC everywhere or explicit NTZ)

### Before Each Write
- [ ] Schema validation runs before every write to a production table
- [ ] Type changes are rejected even if `mergeSchema=True` is enabled
- [ ] New columns are flagged and reviewed before being silently merged
- [ ] A schema version or contract version is logged with each pipeline run

### Pipeline Operations
- [ ] Schema drift detection is implemented at the data ingestion layer
- [ ] Dead-letter queue or rejected-records table handles schema-invalid records
- [ ] Schema comparison between expected and actual is part of data quality tests
- [ ] `FAILFAST` mode is used for Avro/Protobuf deserialization in streaming

### Upgrades and Migrations
- [ ] `spark.sql.session.timeZone` is set explicitly in all jobs
- [ ] `TimestampType` vs `TimestampNTZType` behavior is validated after Spark version upgrades
- [ ] Column renames are handled with `withColumnRenamed`, not positional tricks
- [ ] Breaking schema changes trigger a new table/topic, not an in-place modification

---

## Key Takeaways

- Schema inference is a development convenience, not a production practice.
- Column reordering in upstream Parquet files is a silent killer — always use name-based schema matching.
- `mergeSchema=True` prevents write failures but can still accept semantically wrong types.
- `TimestampType` and `TimestampNTZType` are not interchangeable — test after Spark upgrades.
- A schema validation function before every write is worth more than any post-hoc data quality check.
