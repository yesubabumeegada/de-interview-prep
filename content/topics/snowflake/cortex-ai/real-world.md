---
title: "Snowflake Cortex AI - Real-World Examples"
topic: snowflake
subtopic: cortex-ai
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [snowflake, cortex, production, rag, document-ai, support-automation, nlp]
---

# Snowflake Cortex AI — Real-World Production Examples

## Production Pattern: AI-Powered Support Ticket Routing

A SaaS company routes 10,000+ daily support tickets using Cortex — reducing manual triage from 4 hours to under 2 minutes:

```sql
-- Daily batch: classify, extract, and route all new tickets
CREATE OR REPLACE TASK triage_support_tickets
    WAREHOUSE = etl_wh
    SCHEDULE = 'USING CRON */5 * * * * UTC'  -- every 5 minutes
AS
INSERT INTO ticket_classifications (ticket_id, category, urgency, summary, assigned_team, classified_at)
SELECT
    ticket_id,
    analysis:category::STRING AS category,
    analysis:urgency::STRING AS urgency,
    analysis:summary::STRING AS summary,
    CASE analysis:category::STRING
        WHEN 'billing'    THEN 'finance-support'
        WHEN 'technical'  THEN 'engineering-support'
        WHEN 'account'    THEN 'account-management'
        ELSE 'general-support'
    END AS assigned_team,
    CURRENT_TIMESTAMP() AS classified_at
FROM (
    SELECT
        t.ticket_id,
        TRY_PARSE_JSON(
            SNOWFLAKE.CORTEX.COMPLETE(
                'mistral-7b',
                CONCAT(
                    'Analyze this support ticket. Return ONLY valid JSON: ',
                    '{"category": "<billing|technical|account|other>", ',
                    '"urgency": "<high|medium|low>", ',
                    '"summary": "<one sentence summary>"}. ',
                    'Ticket: ', LEFT(t.body, 2000)  -- cap at 2000 chars to control tokens
                )
            )
        ) AS analysis
    FROM support_tickets t
    WHERE t.created_at >= DATEADD('minute', -5, CURRENT_TIMESTAMP())
      AND t.ticket_id NOT IN (SELECT ticket_id FROM ticket_classifications)
);

ALTER TASK triage_support_tickets RESUME;

-- Quality monitoring: how often does parsing fail?
SELECT
    DATE_TRUNC('hour', classified_at) AS hour,
    COUNT(*) AS total,
    SUM(CASE WHEN category IS NULL THEN 1 ELSE 0 END) AS parse_failures,
    ROUND(100.0 * SUM(CASE WHEN category IS NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS failure_rate
FROM ticket_classifications
WHERE classified_at >= DATEADD('day', -1, CURRENT_TIMESTAMP())
GROUP BY 1
ORDER BY 1;
```

**Results:** 97.3% successful JSON parse rate. Mistral-7b chosen over larger models for cost (10x cheaper) with minimal quality loss on this structured classification task.

---

## Production Pattern: Invoice Processing with Document AI

A finance team processes 3,000+ invoices monthly — previously took 3 FTE data entry staff:

```sql
-- Stage contains PDF invoices uploaded by vendors
-- Directory table tracks files in the stage

-- Step 1: Create a custom Document AI model trained on your invoice format
-- (Done once in Snowflake UI with 20+ labeled examples)

-- Step 2: Batch extract all new invoices
CREATE TABLE invoices_extracted AS
SELECT
    f.RELATIVE_PATH AS file_name,
    f.LAST_MODIFIED AS uploaded_at,
    invoice_extractor!PREDICT(
        GET_PRESIGNED_URL('@invoice_stage', f.RELATIVE_PATH),
        1  -- first page
    ) AS raw_extraction
FROM DIRECTORY(@invoice_stage) f
WHERE f.RELATIVE_PATH LIKE '%.pdf'
  AND f.LAST_MODIFIED >= DATEADD('hour', -24, CURRENT_TIMESTAMP());

-- Step 3: Parse and load into structured table
INSERT INTO accounts_payable.invoices
SELECT
    UUID_STRING() AS invoice_id,
    file_name,
    raw_extraction:invoice_number::STRING AS invoice_number,
    raw_extraction:vendor_name::STRING AS vendor_name,
    raw_extraction:invoice_date::DATE AS invoice_date,
    raw_extraction:due_date::DATE AS due_date,
    raw_extraction:total_amount::FLOAT AS total_amount,
    raw_extraction:currency::STRING AS currency,
    raw_extraction:line_items AS line_items_json,
    CURRENT_TIMESTAMP() AS processed_at,
    'PENDING_REVIEW' AS status
FROM invoices_extracted
WHERE raw_extraction:invoice_number IS NOT NULL;  -- skip failed extractions

-- Step 4: Flag unusual invoices for human review
UPDATE accounts_payable.invoices
SET status = 'NEEDS_REVIEW', review_reason = 'AMOUNT_ANOMALY'
WHERE total_amount > 50000
   OR vendor_name NOT IN (SELECT vendor_name FROM approved_vendors)
   OR due_date < CURRENT_DATE();
```

**Accuracy:** 94% field extraction accuracy on standard invoice formats after training on 150 labeled examples. Human review required only for ~8% of invoices.

---

## Production Pattern: Product Review Intelligence

An e-commerce platform analyzes 50,000 new reviews daily using Cortex:

```sql
-- Batch job: enrich reviews with AI insights
INSERT INTO reviews_enriched
SELECT
    r.review_id,
    r.product_id,
    r.review_text,
    r.rating,
    -- Sentiment score (continuous -1 to 1)
    SNOWFLAKE.CORTEX.SENTIMENT(r.review_text) AS sentiment_score,
    -- Extract specific topics mentioned
    SNOWFLAKE.CORTEX.CLASSIFY_TEXT(
        r.review_text,
        ['shipping_speed', 'product_quality', 'customer_service', 'pricing', 'packaging', 'other']
    ):label::STRING AS primary_topic,
    -- Extract actionable issues for product team
    SNOWFLAKE.CORTEX.COMPLETE(
        'mistral-7b',
        CONCAT(
            'From this review, extract ONE specific product improvement suggestion. ',
            'Return ONLY the suggestion in 15 words or less, or "none" if no suggestion. ',
            'Review: ', LEFT(r.review_text, 500)
        )
    ) AS improvement_suggestion,
    CURRENT_TIMESTAMP() AS enriched_at
FROM product_reviews r
WHERE r.created_at >= DATEADD('hour', -1, CURRENT_TIMESTAMP())
  AND r.review_id NOT IN (SELECT review_id FROM reviews_enriched);

-- Product team dashboard query
SELECT
    p.product_name,
    COUNT(*) AS review_count,
    AVG(r.sentiment_score) AS avg_sentiment,
    AVG(r.rating) AS avg_rating,
    COUNT(DISTINCT CASE WHEN r.primary_topic = 'shipping_speed' THEN r.review_id END) AS shipping_mentions,
    COUNT(DISTINCT CASE WHEN r.primary_topic = 'product_quality' THEN r.review_id END) AS quality_mentions
FROM reviews_enriched r
JOIN products p USING (product_id)
WHERE r.enriched_at >= DATEADD('day', -7, CURRENT_TIMESTAMP())
GROUP BY p.product_name
ORDER BY avg_sentiment ASC  -- worst reviewed products first
LIMIT 20;
```

---

## Production Pattern: Internal Knowledge Base Chatbot (RAG)

An internal IT helpdesk chatbot built entirely in Snowflake:

```
Slack message → AWS Lambda → Snowflake REST API → Cortex Search + COMPLETE → response
```

```python
# Lambda handler calling Snowflake Cortex
import snowflake.connector
import json

def handler(event, context):
    question = event["user_question"]

    conn = snowflake.connector.connect(**snowflake_creds)
    cursor = conn.cursor()

    # Step 1: Retrieve relevant docs from Cortex Search
    cursor.execute(f"""
        SELECT PARSE_JSON(
            SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
                'it_kb_search',
                '{{"query": "{question}", "columns": ["chunk_text", "doc_title"], "limit": 3}}'
            )
        ):results AS results
    """)
    search_results = cursor.fetchone()[0]
    context_chunks = "\n\n".join([r["chunk_text"] for r in search_results])

    # Step 2: Generate answer with context
    cursor.execute(f"""
        SELECT SNOWFLAKE.CORTEX.COMPLETE(
            'snowflake-arctic',
            'You are an IT helpdesk assistant. Answer using ONLY the provided context. ' ||
            'If unsure, say to contact IT directly.\n\n' ||
            'Context:\n{context_chunks}\n\n' ||
            'Question: {question}'
        ) AS answer
    """)
    answer = cursor.fetchone()[0]
    conn.close()

    return {"answer": answer, "sources": [r["doc_title"] for r in search_results]}
```

**Cost:** ~2,000 tokens per question. At $0.0008/1K tokens for Arctic, 500 questions/day = ~$0.80/day. Eliminated 40% of helpdesk ticket volume.

---

## Monitoring Cortex in Production

```sql
-- Daily Cortex usage summary
SELECT
    DATE_TRUNC('day', start_time) AS day,
    function_name,
    COUNT(*) AS call_count,
    SUM(total_tokens) AS total_tokens,
    ROUND(SUM(total_tokens) / 1e6 * 0.80, 4) AS est_cost_usd
FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_USAGE_HISTORY
WHERE start_time >= DATEADD('day', -30, CURRENT_TIMESTAMP())
GROUP BY 1, 2
ORDER BY 1 DESC, total_tokens DESC;

-- Alert if token usage exceeds daily threshold
SELECT
    function_name,
    SUM(total_tokens) AS tokens_today
FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_USAGE_HISTORY
WHERE start_time >= DATE_TRUNC('day', CURRENT_TIMESTAMP())
HAVING SUM(total_tokens) > 10000000  -- 10M tokens = ~$8 for mistral-7b
ORDER BY tokens_today DESC;
```
