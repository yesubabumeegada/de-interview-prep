---
title: "Pig - Intermediate"
topic: hadoop
subtopic: pig
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [hadoop, pig, udf, cogroup, join-optimization, stream, schema]
---

# Pig — Intermediate

## UDFs in Python and Java

User Defined Functions extend Pig Latin with custom logic.

### Python UDF

```python
# clean_text.py
import re

@outputSchema('cleaned:chararray')
def clean_text(text):
    """Remove special characters and lowercase"""
    if text is None:
        return ''
    return re.sub(r'[^a-z0-9\s]', '', text.lower()).strip()

@outputSchema('is_valid_email:boolean')
def is_valid_email(email):
    if email is None:
        return False
    pattern = r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$'
    return bool(re.match(pattern, email))
```

```pig
-- Use Python UDF in Pig script
REGISTER 'clean_text.py' USING jython AS text_utils;

customers = LOAD '/data/raw/customers' 
            USING PigStorage(',')
            AS (id:int, name:chararray, email:chararray);

cleaned_customers = FOREACH customers GENERATE
  id,
  text_utils.clean_text(name) AS clean_name,
  text_utils.is_valid_email(email) AS email_valid,
  email;

valid_customers = FILTER cleaned_customers BY email_valid == true;
STORE valid_customers INTO '/data/output/valid_customers';
```

### Java UDF

```java
// ToUpperCaseUDF.java
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import java.io.IOException;

public class ToUpperCaseUDF extends EvalFunc<String> {
    @Override
    public String exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0 || input.get(0) == null) {
            return null;
        }
        return ((String) input.get(0)).toUpperCase();
    }
}
```

```bash
# Compile and use
javac -cp $(hadoop classpath):$(pig classpath) ToUpperCaseUDF.java
jar cf myudfs.jar ToUpperCaseUDF.class
```

```pig
REGISTER 'myudfs.jar';
orders = LOAD '/data/raw/orders' USING PigStorage(',')
         AS (id:int, status:chararray);

upper_status = FOREACH orders GENERATE id, com.company.ToUpperCaseUDF(status) AS status_upper;
```

## COGROUP for Advanced Joins

`COGROUP` is more powerful than `JOIN` — it groups two (or more) datasets by a common key and preserves ALL tuples from each, including those without matches (like a full outer join).

```pig
-- Datasets
orders = LOAD '/data/raw/orders' USING PigStorage(',')
         AS (order_id:int, customer_id:int, amount:double);

returns = LOAD '/data/raw/returns' USING PigStorage(',')
          AS (return_id:int, customer_id:int, return_amount:double);

-- COGROUP: group both datasets by customer_id
orders_and_returns = COGROUP orders BY customer_id, returns BY customer_id;
-- Result: {group: int, orders: bag, returns: bag}

-- Customers with both orders AND returns
both = FILTER orders_and_returns BY 
       COUNT(orders) > 0 AND COUNT(returns) > 0;

-- Compute net spend per customer
net_spend = FOREACH both GENERATE
  group AS customer_id,
  SUM(orders.amount) AS total_orders,
  SUM(returns.return_amount) AS total_returns,
  SUM(orders.amount) - SUM(returns.return_amount) AS net_spend;

STORE net_spend INTO '/data/output/net_spend';
```

### COGROUP vs JOIN

| Aspect | JOIN | COGROUP |
|--------|------|---------|
| Result | Flat tuples | Bags of matched records |
| NULL handling | Inner: drops non-matches; Outer: nulls | Always includes bags (empty if no match) |
| Aggregation | Post-join | Can aggregate directly on bags |
| Use case | Lookup/enrichment | Complex multi-dataset aggregations |

## Replicated Joins (Map-Side Joins)

For joins where one dataset is small enough to fit in memory:

```pig
-- Small lookup table
product_categories = LOAD '/data/raw/product_categories'
                     USING PigStorage(',')
                     AS (product_id:int, category:chararray);
-- Assume this is small (< 1 GB)

-- Large fact table
order_items = LOAD '/data/raw/order_items'
              USING PigStorage(',')
              AS (item_id:int, order_id:int, product_id:int, quantity:int);

-- Replicated join: product_categories is broadcast to all mappers
enriched_items = JOIN order_items BY product_id, product_categories BY product_id USING 'replicated';
-- 'replicated' = map-side join, no reduce phase needed
-- Result: much faster for small-on-large joins

STORE enriched_items INTO '/data/output/enriched_items';
```

## STREAM Operator

`STREAM` pipes data through any Unix command or script during execution:

```pig
-- Stream data through a Python cleaning script
orders = LOAD '/data/raw/orders_messy' USING PigStorage(',');

-- Define the external script
DEFINE cleaner `python3 clean_orders.py`
  SHIP ('clean_orders.py');

-- Apply the stream transformation
cleaned = STREAM orders THROUGH cleaner AS (
  order_id:int, customer_id:int, amount:double, status:chararray
);

STORE cleaned INTO '/data/output/orders_cleaned';
```

## SAMPLE

```pig
-- Take a 1% random sample for analysis or testing
orders = LOAD '/data/raw/orders' USING PigStorage(',')
         AS (order_id:int, customer_id:int, amount:double);

sample_orders = SAMPLE orders 0.01;  -- 1% sample
DUMP sample_orders;
```

## SPLIT for Routing

```pig
-- Route data to different outputs based on conditions
orders = LOAD '/data/raw/orders' USING PigStorage(',')
         AS (order_id:int, amount:double, status:chararray);

SPLIT orders INTO
  large_orders IF amount >= 1000.0,
  medium_orders IF (amount >= 100.0 AND amount < 1000.0),
  small_orders IF amount < 100.0;

STORE large_orders INTO '/data/output/large_orders';
STORE medium_orders INTO '/data/output/medium_orders';
STORE small_orders INTO '/data/output/small_orders';
```

## Multi-Query Optimization

By default, Pig processes each `STORE` statement in a separate MapReduce job. Multi-query execution consolidates multiple outputs into fewer jobs:

```pig
-- Without optimization: 2 separate MR jobs
orders = LOAD '/data/raw/orders' USING PigStorage(',')
         AS (order_id:int, customer_id:int, amount:double, status:chararray);

completed = FILTER orders BY status == 'completed';
pending = FILTER orders BY status == 'pending';

-- These would run as 2 MR jobs without multi-query
STORE completed INTO '/data/output/completed';
STORE pending INTO '/data/output/pending';
```

```bash
# Enable multi-query (default in Pig, disable for debugging)
pig -optimizer_off MultiQueryOptimizer myscript.pig

# Explicitly disable for troubleshooting
pig -Dpig.optimizer.rules.disabled=MultiQueryOptimizer myscript.pig
```

## PigStorage vs Other Storage Functions

| StorageFunc | Format | Use case |
|-------------|--------|----------|
| `PigStorage(',')` | CSV/TSV | Simple delimited text |
| `TextLoader` | Raw text lines | Log files, unstructured |
| `JsonLoader` | JSON | API response data |
| `AvroStorage` | Avro | Schema-enforced binary |
| `OrcStorage` | ORC | Columnar, analytics |
| `HBaseStorage` | HBase rows | Real-time lookup tables |

```pig
-- Load JSON
events = LOAD '/data/raw/events' USING JsonLoader(
  'event_id:chararray, user_id:int, event_type:chararray, timestamp:long'
);

-- Load Avro
records = LOAD '/data/raw/records' USING AvroStorage();

-- Store as ORC
STORE results INTO '/data/output/results' USING OrcStorage();
```

## Schema Definition

```pig
-- Explicit schema in LOAD
orders = LOAD '/data/raw/orders' USING PigStorage(',')
         AS (
           order_id:int,
           customer_id:int,
           amount:double,
           items:bag{t:(product_id:int, quantity:int)},
           metadata:map[]
         );

-- Schema for nested structures
user_events = LOAD '/data/raw/user_events' USING JsonLoader(
  'user_id:int, events:bag{e:(event_type:chararray, ts:long, props:map[])}'
);

-- Access nested fields
event_types = FOREACH user_events GENERATE
  user_id,
  FLATTEN(events) AS (event_type, ts, props);
```

## Useful Built-in Functions

```pig
-- String functions
UPPER(str), LOWER(str), TRIM(str), LTRIM(str), RTRIM(str)
SUBSTRING(str, start, end)
INDEXOF(str, 'search', startFrom)
REPLACE(str, 'old', 'new')
CONCAT(str1, str2)
REGEX_EXTRACT(str, 'pattern', matchIndex)
TOKENIZE(str, ' ')  -- splits into bag of words

-- Math functions
ABS(x), CEIL(x), FLOOR(x), ROUND(x)
LOG(x), EXP(x), SQRT(x)
MAX(bag.field), MIN(bag.field), SUM(bag.field), AVG(bag.field), COUNT(bag)

-- Type conversion
(int)myfield, (double)myfield
ToString(datetime), ToDate('2024-01-15', 'yyyy-MM-dd')
```

## Interview Tips

> **Tip 1:** The key difference between `JOIN` and `COGROUP` is what the result looks like. `JOIN` produces flat tuples; `COGROUP` produces bags. Use `COGROUP` when you need to aggregate across multiple relations for the same key, like comparing a customer's orders vs. returns.

> **Tip 2:** Replicated joins (`USING 'replicated'`) are a major performance optimization in Pig. They require the smaller dataset to fit in the distributed cache (typically under 1 GB). The small dataset is distributed to all mapper nodes, eliminating the reduce phase entirely.

> **Tip 3:** Python UDFs use Jython (Python 2) by default in older Pig versions. For Python 3 compatibility, use the streaming UDF pattern with `STREAM` instead. This is a common gotcha in production environments.

> **Tip 4:** Multi-query optimization is enabled by default and important for efficiency. When you `STORE` multiple derived relations from the same base load, Pig can process them in a single scan. Disabling it for debugging is useful when you need to isolate which stage produced wrong output.

> **Tip 5:** Schema is optional in Pig, but highly recommended in production. Without schema, all fields are `bytearray` and you lose type safety. Always define schemas in `LOAD` statements — it improves performance (type optimization) and makes scripts more maintainable.
