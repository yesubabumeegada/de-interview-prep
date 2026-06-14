---
title: "Snowflake Cortex AI - Scenario Questions"
topic: snowflake
subtopic: cortex-ai
content_type: scenario_question
tags: [snowflake, cortex, ai, scenarios, interview]
---

# Scenario Questions — Snowflake Cortex AI

<article data-difficulty="junior">

## 🟢 Junior: Analyze Product Review Sentiment

**Scenario:** Your e-commerce company has a `product_reviews` table with columns `review_id`, `product_id`, `review_text`, and `rating`. The product team wants to see which products have the most negative reviews this week. Use Cortex to add sentiment scores and build the report.

<details>
<summary>✅ Solution</summary>

```sql
-- Step 1: Add sentiment scores to recent reviews
CREATE OR REPLACE TABLE product_reviews_scored AS
SELECT
    review_id,
    product_id,
    review_text,
    rating,
    SNOWFLAKE.CORTEX.SENTIMENT(review_text) AS sentiment_score
FROM product_reviews
WHERE created_at >= DATEADD('week', -1, CURRENT_TIMESTAMP());

-- Step 2: Product-level sentiment report
SELECT
    p.product_name,
    COUNT(r.review_id) AS review_count,
    AVG(r.rating) AS avg_star_rating,
    ROUND(AVG(r.sentiment_score), 3) AS avg_sentiment,
    -- Bucket into Positive / Neutral / Negative
    SUM(CASE WHEN r.sentiment_score > 0.2 THEN 1 ELSE 0 END) AS positive_reviews,
    SUM(CASE WHEN r.sentiment_score BETWEEN -0.2 AND 0.2 THEN 1 ELSE 0 END) AS neutral_reviews,
    SUM(CASE WHEN r.sentiment_score < -0.2 THEN 1 ELSE 0 END) AS negative_reviews
FROM product_reviews_scored r
JOIN products p ON r.product_id = p.product_id
GROUP BY p.product_name
HAVING COUNT(r.review_id) >= 5   -- minimum 5 reviews for statistical significance
ORDER BY avg_sentiment ASC        -- most negative first
LIMIT 20;

-- Step 3: Sample the worst reviews for qualitative context
SELECT review_text, sentiment_score, rating
FROM product_reviews_scored
WHERE product_id = (
    SELECT product_id FROM product_reviews_scored
    GROUP BY product_id ORDER BY AVG(sentiment_score) ASC LIMIT 1
)
ORDER BY sentiment_score ASC
LIMIT 5;
```

**Key decisions:**
- Store sentiment scores in a separate table rather than computing on-the-fly — tokens cost money, cache the results
- Filter to only new reviews each run (incremental pattern) — don't re-score all historical reviews
- HAVING COUNT >= 5 prevents products with 1 review from dominating the worst list

</details>
</article>

---

<article data-difficulty="mid">

## 🟡 Mid-Level: Build a Semantic Document Search on Internal Wikis

**Scenario:** Your company has 10,000 internal wiki pages in Snowflake (`wiki_pages` table: `page_id`, `title`, `content`, `department`). Build a searchable knowledge base that allows employees to ask questions in natural language and get relevant page recommendations.

<details>
<summary>✅ Solution</summary>

```sql
-- Step 1: Create Cortex Search Service on wiki content
CREATE CORTEX SEARCH SERVICE wiki_search
    ON content
    ATTRIBUTES department, title     -- filterable metadata
    WAREHOUSE = search_wh
    TARGET_LAG = '15 minutes'        -- index new/updated pages within 15 min
    AS (
        SELECT
            page_id,
            title,
            department,
            content,
            last_updated
        FROM wiki_pages
        WHERE is_published = TRUE
    );

-- Step 2: Search function (used by application)
-- Search with optional department filter
SELECT
    PARSE_JSON(
        SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
            'wiki_search',
            OBJECT_CONSTRUCT(
                'query', 'how do I request access to Snowflake',
                'columns', ARRAY_CONSTRUCT('page_id', 'title', 'department'),
                'filter', OBJECT_CONSTRUCT('@eq', OBJECT_CONSTRUCT('department', 'IT')),
                'limit', 5
            )::STRING
        )
    ):results AS search_results;

-- Step 3: (Optional) Generate an answer using RAG
WITH search_results AS (
    SELECT f.value:title::STRING AS title, f.value:chunk_text::STRING AS content
    FROM TABLE(FLATTEN(
        input => PARSE_JSON(
            SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
                'wiki_search',
                '{"query": "how do I request Snowflake access", "columns": ["title", "content"], "limit": 3}'
            )
        ):results
    )) f
),
context_text AS (
    SELECT LISTAGG('Title: ' || title || '\n' || content, '\n\n---\n\n') AS full_context
    FROM search_results
)
SELECT SNOWFLAKE.CORTEX.COMPLETE(
    'llama3.1-8b',
    'Answer the question using only the provided wiki content. Be concise.\n\n' ||
    'Wiki content:\n' || full_context || '\n\nQuestion: how do I request Snowflake access?'
) AS answer
FROM context_text;
```

**Why Cortex Search over manual embeddings:** Auto-handles chunking, embedding refresh, and hybrid ranking. For 10,000 pages, setup time is minutes vs. days for a manual approach.

</details>
</article>

---

<article data-difficulty="senior">

## 🔴 Senior: Design a Cost-Controlled AI Enrichment Pipeline

**Scenario:** The product team wants to run AI enrichment on 200 million historical records (product descriptions), but you have a Cortex budget of $5,000/month. Each COMPLETE call costs ~$0.0008 for 1,000 tokens. The enrichment involves: (1) category classification, (2) keyword extraction, (3) quality score. Design a pipeline that maximizes coverage while staying within budget.

<details>
<summary>✅ Solution</summary>

**Budget math first:**
```
$5,000/month ÷ $0.80/1M tokens = 6.25B tokens/month
Average record: 300 input tokens + 100 output tokens = 400 tokens
6.25B ÷ 400 = ~15.6M records per month at $0.80/1M (llama3.1-70b)
Switch to mistral-7b ($0.08/1M): 6.25B ÷ 400 = 156M records — enough for all!
```

**Architecture:**

```sql
-- Step 1: Tiered approach — not all records need expensive models
-- Use fast/cheap mistral-7b for most records
-- Only route ambiguous classifications to the expensive model

-- Step 2: Combine all 3 tasks into ONE COMPLETE call (saves 2/3 of token cost)
CREATE OR REPLACE PROCEDURE enrich_products_batch(batch_size INT)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
AS $$
from snowflake.snowpark import Session

def run(session: Session, batch_size: int) -> str:
    # Get unenriched batch
    batch = session.sql(f"""
        SELECT product_id, LEFT(description, 400) AS description
        FROM products
        WHERE enriched_at IS NULL
        LIMIT {batch_size}
    """).collect()

    if not batch:
        return "No records to enrich"

    # Build combined prompt (3 tasks in 1 call = 3x token savings)
    results = []
    for row in batch:
        result = session.sql(f"""
            SELECT TRY_PARSE_JSON(SNOWFLAKE.CORTEX.COMPLETE(
                'mistral-7b',
                'Analyze this product description. Return ONLY JSON: ' ||
                '{{"category": "<electronics|clothing|food|home|other>", ' ||
                '"keywords": ["kw1", "kw2", "kw3"], ' ||
                '"quality_score": <0-10>}}. ' ||
                'Description: {row["DESCRIPTION"].replace("'", "''")}'
            )) AS result
        """).collect()[0]["RESULT"]
        results.append((row["PRODUCT_ID"], result))

    # Batch update
    for product_id, result in results:
        if result:
            session.sql(f"""
                UPDATE products SET
                    ai_category = '{result.get("category", "other")}',
                    ai_keywords = PARSE_JSON('{result.get("keywords", [])}'),
                    ai_quality_score = {result.get("quality_score", 0)},
                    enriched_at = CURRENT_TIMESTAMP()
                WHERE product_id = '{product_id}'
            """).collect()

    return f"Enriched {len(results)} records"
$$;

-- Step 3: Schedule daily batches that respect budget
CREATE TASK enrich_products_daily
    WAREHOUSE = etl_wh
    SCHEDULE = 'USING CRON 0 2 * * * UTC'  -- 2am daily
AS
CALL enrich_products_batch(500000);  -- 500K records/day at ~400 tokens = 200M tokens = $16/day

ALTER TASK enrich_products_daily RESUME;

-- Step 4: Monitor budget consumption
SELECT
    DATE_TRUNC('day', start_time) AS day,
    SUM(total_tokens) AS daily_tokens,
    ROUND(SUM(total_tokens) / 1e6 * 0.08, 2) AS est_daily_cost_usd  -- mistral-7b rate
FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_USAGE_HISTORY
WHERE start_time >= DATEADD('month', -1, CURRENT_TIMESTAMP())
GROUP BY 1 ORDER BY 1;
```

**Total cost:** 200M records × 400 tokens = 80B tokens / 1M × $0.08 = $6,400 for the full historical backlog. Running over 2 months stays within budget. After backfill, only delta (new products) runs daily at ~$5/day.

</details>
</article>
