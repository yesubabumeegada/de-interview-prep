---
title: "Snowflake Cortex AI - Fundamentals"
topic: snowflake
subtopic: cortex-ai
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [snowflake, cortex, ai, llm, machine-learning, sql-ai, sentiment, summarize]
---

# Snowflake Cortex AI — Fundamentals

## 🎯 Analogy

Snowflake Cortex is like having a team of AI specialists built into your SQL console. Need sentiment analysis? Call `SENTIMENT()`. Need to summarize 10,000 support tickets? Call `SUMMARIZE()`. Need to classify products? Call `CLASSIFY_TEXT()`. No Python, no API keys, no model hosting — just SQL functions on your existing data.

---

## What Is Snowflake Cortex?

Snowflake Cortex is Snowflake's native AI/ML platform — a suite of SQL-accessible AI functions that run on Snowflake's compute without moving your data to external services.

**Key advantage:** Your data never leaves Snowflake. No sending to OpenAI, no API rate limits, no egress costs.

```
Traditional approach:
  Snowflake → export data → OpenAI API → parse results → import back

Cortex approach:
  SELECT CORTEX.SENTIMENT(review_text) FROM product_reviews;  -- done!
```

---

## Cortex LLM Functions

Call large language models directly from SQL:

### COMPLETE — Open-Ended Generation

```sql
-- Call an LLM for any text generation task
SELECT SNOWFLAKE.CORTEX.COMPLETE(
    'mistral-7b',           -- model: mistral-7b, llama3-8b, llama3-70b, snowflake-arctic, etc.
    'Summarize this in 2 sentences: ' || product_description
) AS summary
FROM products
WHERE category = 'Electronics'
LIMIT 10;
```

### SUMMARIZE — Built-In Summarization

```sql
-- Summarize long text (optimized for this task)
SELECT
    ticket_id,
    SNOWFLAKE.CORTEX.SUMMARIZE(ticket_body) AS summary
FROM support_tickets
WHERE LENGTH(ticket_body) > 500;
```

### SENTIMENT — Classify Sentiment

```sql
-- Returns: positive, negative, neutral (-1 to 1 score)
SELECT
    review_id,
    product_id,
    review_text,
    SNOWFLAKE.CORTEX.SENTIMENT(review_text) AS sentiment_score
FROM product_reviews;
-- sentiment_score: 0.87 = strongly positive, -0.43 = somewhat negative
```

### TRANSLATE — Language Translation

```sql
-- Translate to English from any language (auto-detected)
SELECT
    review_id,
    original_language,
    SNOWFLAKE.CORTEX.TRANSLATE(review_text, 'en') AS english_text
FROM international_reviews
WHERE original_language != 'en';
```

### EXTRACT_ANSWER — Question Answering

```sql
-- Extract a specific answer from a document
SELECT
    doc_id,
    SNOWFLAKE.CORTEX.EXTRACT_ANSWER(
        document_text,
        'What is the cancellation policy?'
    ) AS cancellation_policy
FROM legal_documents;
```

### CLASSIFY_TEXT — Multi-Class Classification

```sql
-- Classify text into one of several categories
SELECT
    ticket_id,
    ticket_body,
    SNOWFLAKE.CORTEX.CLASSIFY_TEXT(
        ticket_body,
        ['billing', 'technical_issue', 'account_access', 'feature_request', 'other']
    ):label::STRING AS category
FROM support_tickets;
```

---

## Available Models

| Model | Best For | Size |
|-------|---------|------|
| `snowflake-arctic` | Instruction-following, enterprise tasks | Large |
| `llama3.1-70b` | Complex reasoning, analysis | Large |
| `llama3.1-8b` | Fast generation, simple tasks | Small |
| `mistral-7b` | Code, structured output | Small |
| `mistral-large2` | Multi-language, nuanced tasks | Large |
| `reka-flash` | Multimodal (text + image) | Medium |

---

## Cortex ML Functions

Built-in ML models for structured tabular data — no Python or data science expertise needed:

### FORECAST — Time Series Prediction

```sql
-- Train and predict in one SQL call
SELECT *
FROM TABLE(
    SNOWFLAKE.ML.FORECAST(
        INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'daily_sales_view'),
        TIMESTAMP_COLNAME => 'sale_date',
        TARGET_COLNAME => 'total_revenue',
        SERIES_COLNAME => 'region',     -- optional: forecast per region
        CONFIG_OBJECT => {'prediction_interval': 0.95}
    )
);
-- Returns: forecast_timestamp, forecast_value, lower_bound, upper_bound
```

### ANOMALY_DETECTION — Find Outliers

```sql
SELECT *
FROM TABLE(
    SNOWFLAKE.ML.ANOMALY_DETECTION(
        INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'daily_errors_view'),
        TIMESTAMP_COLNAME => 'error_date',
        TARGET_COLNAME => 'error_count',
        LABEL_COLNAME => 'is_anomaly'   -- optional: provide known anomalies for supervised mode
    )
);
-- Returns: is_anomaly (boolean), percentile, distance from expected
```

---

## Cost: Tokens

Cortex LLM functions charge by **tokens** (not warehouse credits):

| Function | Cost | Notes |
|----------|------|-------|
| `COMPLETE` (small models) | ~$0.10/M tokens | mistral-7b, llama3-8b |
| `COMPLETE` (large models) | ~$0.80/M tokens | llama3-70b |
| `SENTIMENT`, `SUMMARIZE`, `TRANSLATE` | ~$0.15/M tokens | Optimized task functions |
| `FORECAST`, `ANOMALY_DETECTION` | Credits (serverless) | Tabular ML functions |

```sql
-- Monitor Cortex token usage
SELECT function_name, SUM(token_credits) AS total_tokens, SUM(token_credits) * 0.0001 AS est_cost_usd
FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_USAGE_HISTORY
WHERE start_time >= DATEADD('day', -30, CURRENT_TIMESTAMP())
GROUP BY function_name
ORDER BY total_tokens DESC;
```

---

## Try It Yourself

```sql
-- Quick test of Cortex LLM functions
SELECT
    'Great product, very fast shipping!' AS review,
    SNOWFLAKE.CORTEX.SENTIMENT('Great product, very fast shipping!') AS sentiment,
    SNOWFLAKE.CORTEX.COMPLETE('mistral-7b', 'Categorize this review as Positive/Negative/Neutral: Great product, very fast shipping!') AS category;

-- Try summarize on a longer text
SELECT SNOWFLAKE.CORTEX.SUMMARIZE(
    'Snowflake is a cloud-based data warehousing platform that separates compute and storage. ' ||
    'It was founded in 2012 and went public in 2020 in one of the largest software IPOs. ' ||
    'Snowflake runs on AWS, Azure, and GCP and offers a SQL interface for data analytics. ' ||
    'Recently, Snowflake has expanded into AI with Cortex, Snowpark, and ML capabilities.'
) AS summary;
```

---

## Interview Tips

> **Tip 1:** "What is Snowflake Cortex?" — "Snowflake's native AI/ML platform — SQL functions that call LLMs and ML models directly on your data without exporting it. Covers LLM tasks (SENTIMENT, SUMMARIZE, TRANSLATE, COMPLETE, CLASSIFY_TEXT) and tabular ML (FORECAST, ANOMALY_DETECTION). The key value: zero data movement, built-in access control, billed via tokens."

> **Tip 2:** "How is Cortex COMPLETE different from calling OpenAI?" — "Cortex runs inside your Snowflake account — data doesn't leave your security perimeter. Access control is Snowflake RBAC. No API key management. Simpler billing (Snowflake invoice). Trade-off: fewer model options than OpenAI, and you can't fine-tune (as of 2024). For RAG with private data, Cortex Search is the Snowflake-native option."

> **Tip 3:** "When would you use SENTIMENT vs CLASSIFY_TEXT?" — "SENTIMENT returns a continuous score (-1 to 1) for positive/negative polarity — good for ranking or thresholds. CLASSIFY_TEXT lets you define custom categories (billing, technical, feature_request) — good for multi-class routing. Use SENTIMENT for 'how positive is this review?' and CLASSIFY_TEXT for 'what department should handle this ticket?'"
