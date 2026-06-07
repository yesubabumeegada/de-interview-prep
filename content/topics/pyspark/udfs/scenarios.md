---
title: "PySpark UDFs - Interview Scenarios"
topic: pyspark
subtopic: udfs
content_type: study_material
difficulty_level: mid-level
layer: real-world
tags: [pyspark, udf, interview-scenarios, pandas-udf, optimization, native-functions]
---

# PySpark UDFs — Interview Scenarios

## Junior Scenario: Write a Simple UDF

**Question:** "Write a UDF that takes a full name string (e.g., 'John Michael Smith') and returns the initials (e.g., 'JMS'). Register it for both SQL and DataFrame use."

### Solution

```python
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName("UDF_Scenario").getOrCreate()

# Define the UDF logic
def get_initials(full_name):
    """Extract initials from a full name."""
    if full_name is None:
        return None
    parts = full_name.strip().split()
    if not parts:
        return None
    return ''.join(part[0].upper() for part in parts if part)

# Register for SQL use
spark.udf.register("get_initials", get_initials, StringType())

# Create DataFrame-compatible version
get_initials_udf = F.udf(get_initials, StringType())

# Test data
test_df = spark.createDataFrame([
    ("John Michael Smith",),
    ("Jane Doe",),
    ("Madonna",),
    (None,),
    ("",),
], ["full_name"])

# DataFrame API usage
result_df = test_df.withColumn("initials", get_initials_udf(F.col("full_name")))
result_df.show()
# +------------------+--------+
# |         full_name|initials|
# +------------------+--------+
# |John Michael Smith|     JMS|
# |          Jane Doe|      JD|
# |           Madonna|       M|
# |              null|    null|
# |                  |    null|
# +------------------+--------+

# SQL usage
test_df.createOrReplaceTempView("names")
result_sql = spark.sql("SELECT full_name, get_initials(full_name) AS initials FROM names")
result_sql.show()
```

**Expected Answer Points:**
- Handle None explicitly (first check)
- Handle empty string edge case
- Declare return type (StringType)
- Register for both SQL and DataFrame use
- Show awareness that this COULD be done with native functions (split + transform + concat)

### Bonus: Native Alternative

```python
# Without UDF — pure Spark functions
native_result = (test_df
    .withColumn("parts", F.split(F.trim(F.col("full_name")), "\\s+"))
    .withColumn("initials",
        F.when(F.col("full_name").isNull() | (F.trim(F.col("full_name")) == ""), None)
         .otherwise(
             F.concat_ws("", F.transform(F.col("parts"), lambda x: F.upper(F.substring(x, 1, 1))))
         ))
    .drop("parts"))
```

---

## Mid-Level Scenario: Convert Python UDF to Pandas UDF

**Question:** "This Python UDF processes 200 million rows and takes 90 minutes. Convert it to a Pandas UDF and explain the expected performance improvement."

### The Slow Python UDF

```python
import re
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

log_schema = StructType([
    StructField("ip", StringType()),
    StructField("path", StringType()),
    StructField("response_time_ms", DoubleType()),
])

@F.udf(log_schema)
def parse_log_line(line):
    """Parse Apache log line — processes ONE row at a time."""
    if line is None:
        return None
    
    pattern = r'(\d+\.\d+\.\d+\.\d+).*?"(?:GET|POST)\s+(\S+).*?(\d+\.\d+)$'
    match = re.match(pattern, line)
    
    if match:
        return {
            "ip": match.group(1),
            "path": match.group(2),
            "response_time_ms": float(match.group(3)),
        }
    return {"ip": None, "path": None, "response_time_ms": None}

# Takes 90 minutes for 200M rows
result = logs_df.withColumn("parsed", parse_log_line(F.col("raw_log")))
```

### The Fast Pandas UDF

```python
import pandas as pd
import numpy as np

@F.pandas_udf(log_schema)
def parse_log_line_vectorized(lines: pd.Series) -> pd.DataFrame:
    """Vectorized log parsing using pandas string methods."""
    
    # Extract IP addresses (vectorized regex)
    ips = lines.str.extract(r'^(\d+\.\d+\.\d+\.\d+)', expand=False)
    
    # Extract paths (vectorized regex)
    paths = lines.str.extract(r'"(?:GET|POST)\s+(\S+)', expand=False)
    
    # Extract response times (vectorized regex + type conversion)
    response_times = lines.str.extract(r'(\d+\.\d+)$', expand=False).astype(float)
    
    return pd.DataFrame({
        "ip": ips,
        "path": paths,
        "response_time_ms": response_times,
    })

# Takes ~12 minutes for 200M rows (7.5x speedup)
result = logs_df.withColumn("parsed", parse_log_line_vectorized(F.col("raw_log")))
```

### Even Better: Native Functions

```python
# Takes ~4 minutes for 200M rows (22x speedup from original)
result = (logs_df
    .withColumn("ip", F.regexp_extract("raw_log", r"^(\d+\.\d+\.\d+\.\d+)", 1))
    .withColumn("path", F.regexp_extract("raw_log", r'"(?:GET|POST)\s+(\S+)', 1))
    .withColumn("response_time_ms",
        F.regexp_extract("raw_log", r"(\d+\.\d+)$", 1).cast("double"))
)
```

### Performance Comparison

| Approach | Duration (200M rows) | Speedup | Why Faster |
|----------|---------------------|---------|-----------|
| Python UDF | 90 min | 1x | Row-by-row serialization + Python regex |
| Pandas UDF | 12 min | 7.5x | Arrow batches + vectorized pandas regex |
| Native functions | 4 min | 22x | JVM-native, code-gen, predicate pushdown |

**Expected Answer Points:**
- Pandas UDF uses `pd.Series.str.extract()` instead of `re.match()` per row
- Arrow batch transfer eliminates per-row serialization
- numpy/pandas operations are C-optimized under the hood
- Further improvement: native `regexp_extract` runs in JVM with codegen
- Mention that native is preferred but Pandas UDF is acceptable when logic is too complex

---

## Senior Scenario: Eliminate UDF with Native Functions

**Question:** "This pipeline has 5 UDFs that process 2 billion rows daily. Each UDF adds 15-30 minutes to the job. Your task: eliminate as many UDFs as possible using native Spark functions. Which ones can be eliminated, and which ones truly need UDFs?"

### The UDFs to Analyze

```python
# UDF 1: Email validation — CAN ELIMINATE
@F.udf(BooleanType())
def is_valid_email(email):
    if not email:
        return False
    pattern = r'^[\w\.-]+@[\w\.-]+\.\w+$'
    return bool(re.match(pattern, email))

# Native replacement:
valid_email = F.col("email").rlike(r'^[\w\.\-]+@[\w\.\-]+\.\w+$')

# UDF 2: JSON path extraction — CAN ELIMINATE
@F.udf(StringType())
def extract_nested_field(json_str, path):
    import json
    try:
        data = json.loads(json_str)
        for key in path.split("."):
            data = data[key]
        return str(data)
    except:
        return None

# Native replacement:
value = F.get_json_object(F.col("json_col"), "$.path.to.field")

# UDF 3: Custom hash for PII — CAN ELIMINATE
@F.udf(StringType())
def hash_pii(value):
    import hashlib
    if not value:
        return None
    return hashlib.sha256(value.encode()).hexdigest()

# Native replacement:
hashed = F.sha2(F.col("pii_field"), 256)

# UDF 4: Complex scoring with external lookup — NEEDS UDF (Pandas)
@F.udf(DoubleType())
def calculate_fraud_score(amount, merchant_cat, hour, country, history_json):
    """Complex multi-factor scoring with parsed history."""
    import json
    history = json.loads(history_json) if history_json else {}
    
    score = 0.0
    avg_amount = history.get("avg_amount", amount)
    if amount > avg_amount * 5:
        score += 40
    if hour >= 0 and hour < 5:
        score += 15
    if country != history.get("home_country"):
        score += 25
    # ... 15 more rules
    return min(score, 100.0)

# Pandas UDF replacement (can't fully eliminate but can vectorize):
@F.pandas_udf(DoubleType())
def calculate_fraud_score_fast(
    amount: pd.Series, merchant_cat: pd.Series,
    hour: pd.Series, country: pd.Series, 
    avg_amount: pd.Series, home_country: pd.Series
) -> pd.Series:
    score = pd.Series(0.0, index=amount.index)
    score += np.where(amount > avg_amount * 5, 40, 0)
    score += np.where((hour >= 0) & (hour < 5), 15, 0)
    score += np.where(country != home_country, 25, 0)
    return score.clip(upper=100.0)

# UDF 5: Custom NLP tokenization — NEEDS UDF (Iterator Pandas)
@F.udf(ArrayType(StringType()))
def tokenize_text(text):
    import nltk
    return nltk.word_tokenize(text) if text else []

# Iterator Pandas UDF (load nltk data once):
@F.pandas_udf(ArrayType(StringType()))
def tokenize_fast(batch_iter: Iterator[pd.Series]) -> Iterator[pd.Series]:
    import nltk
    nltk.download('punkt', quiet=True)
    for batch in batch_iter:
        yield batch.apply(lambda t: nltk.word_tokenize(t) if t else [])
```

### Decision Summary

| UDF | Eliminable? | Replacement | Time Saved |
|-----|------------|-------------|-----------|
| Email validation | Yes | `rlike()` | 25 min |
| JSON extraction | Yes | `get_json_object()` | 20 min |
| PII hashing | Yes | `sha2()` | 15 min |
| Fraud scoring | Partial | Pandas UDF + pre-extract fields | 20 min |
| NLP tokenization | No | Iterator Pandas UDF | 10 min |

**Total savings: ~90 min/day from 2.5 hours of UDF overhead**

**Expected Answer Points:**
- Regex, JSON, and hash operations all have native equivalents
- Complex scoring can be partially nativized by extracting JSON fields first
- NLP/ML operations genuinely need UDFs — use Iterator pattern for efficiency
- Pre-extracting nested JSON fields before the UDF reduces per-row work
- Quantify the impact: native functions are 10-20x faster per operation

---

## Interview Tips

> **Tip 1:** "For simple UDF questions, always handle nulls and show you know the native alternative." — "Write the UDF correctly with null handling, then mention 'In production, I'd use the native function X which is 10-20x faster because it runs in the JVM with code generation.' This shows you can write UDFs but prefer to avoid them."

> **Tip 2:** "For UDF conversion, explain the Arrow mechanism." — "Pandas UDFs use Apache Arrow to transfer data in columnar batches of 10K rows. Instead of serializing one row at a time, we get a pandas Series backed by a numpy array. The pandas string methods (str.extract, str.contains) are implemented in C, not Python, so they're much faster than Python regex per-row. The combined effect: Arrow transfer + vectorized C operations = 5-50x speedup."

> **Tip 3:** "For UDF elimination, show systematic analysis." — "I categorize each UDF: (1) Has a direct native equivalent (regex → rlike, JSON → get_json_object, hash → sha2), (2) Can be expressed with combinations of native functions (when/otherwise, array functions), (3) Needs vectorization (convert to Pandas UDF), (4) Truly needs row-level Python (external APIs, complex libraries). Most UDFs I encounter in production fall into categories 1 or 2 — they're written by people who didn't know the native function existed."
