---
title: "Snowflake Cortex AI - Intermediate"
topic: snowflake
subtopic: cortex-ai
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [snowflake, cortex, search, vector, rag, document-ai, analyst, embeddings]
---

# Snowflake Cortex AI — Intermediate Concepts

## Cortex Search: Semantic & Hybrid Search

Cortex Search enables vector-based semantic search on unstructured text — the foundation for RAG (Retrieval-Augmented Generation) pipelines inside Snowflake:

```sql
-- 1. Create a Cortex Search Service on a text column
CREATE CORTEX SEARCH SERVICE product_search
    ON product_description
    WAREHOUSE = search_wh
    TARGET_LAG = '1 minute'   -- how quickly new data is indexed
    AS (
        SELECT
            product_id,
            product_name,
            product_description,
            category,
            price
        FROM products
        WHERE is_active = TRUE
    );

-- 2. Search using SQL (semantic similarity — not just keyword match)
SELECT PARSE_JSON(
    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        'product_search',
        '{
            "query": "waterproof hiking boots for cold weather",
            "columns": ["product_id", "product_name", "category"],
            "limit": 5
        }'
    )
) AS results;
```

**Hybrid search** (keyword + semantic):

```sql
-- Add a filter (keyword match) + semantic ranking
SELECT PARSE_JSON(
    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        'product_search',
        '{
            "query": "comfortable running shoe",
            "columns": ["product_name", "price", "product_description"],
            "filter": {"@eq": {"category": "footwear"}},
            "limit": 10
        }'
    )
) AS results;
```

---

## Building a RAG Pipeline with Cortex

RAG = Retrieve relevant documents → Augment the prompt → Generate answer using LLM:

```sql
-- Full RAG pipeline in SQL
WITH user_question AS (
    SELECT 'What is your return policy for electronics?' AS question
),
retrieved_docs AS (
    -- Step 1: Retrieve semantically similar policy documents
    SELECT f.value:chunk_text::STRING AS doc_chunk
    FROM user_question,
    LATERAL FLATTEN(
        input => PARSE_JSON(
            SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
                'policy_search',
                '{
                    "query": "' || question || '",
                    "columns": ["chunk_text"],
                    "limit": 3
                }'
            )
        ):results
    ) f
),
context AS (
    SELECT LISTAGG(doc_chunk, '\n\n---\n\n') AS all_context FROM retrieved_docs
)
-- Step 2: Generate answer using LLM with context
SELECT
    SNOWFLAKE.CORTEX.COMPLETE(
        'llama3.1-70b',
        CONCAT(
            'Answer the question based ONLY on the provided context. ',
            'If the answer is not in the context, say "I don''t know".\n\n',
            'Context:\n', all_context,
            '\n\nQuestion: ', (SELECT question FROM user_question)
        )
    ) AS answer
FROM context;
```

---

## Cortex Analyst: Natural Language to SQL

Cortex Analyst translates natural language questions into SQL queries — enabling non-technical users to query data:

```sql
-- Step 1: Create a semantic model (YAML describing your data)
-- Saved as a file in a Snowflake stage @semantic_models/revenue_model.yaml

-- Step 2: Call Cortex Analyst via REST API or Python
-- (Available via Streamlit in Snowflake or via REST)

-- Via Python (Snowpark session):
import requests, json

response = requests.post(
    f"https://{account}.snowflakecomputing.com/api/v2/cortex/analyst/message",
    headers={"Authorization": f"Snowflake Token={session.connection.rest.token}"},
    json={
        "messages": [{"role": "user", "content": [{"type": "text",
                       "text": "Show me top 10 customers by revenue in Q1 2024"}]}],
        "semantic_model_file": "@semantic_models/revenue_model.yaml"
    }
)

result = response.json()
generated_sql = result["message"]["content"][0]["statement"]
print(generated_sql)
# Output: SELECT customer_id, SUM(amount) AS revenue
#         FROM fact_orders
#         WHERE order_date BETWEEN '2024-01-01' AND '2024-03-31'
#         GROUP BY customer_id ORDER BY revenue DESC LIMIT 10
```

---

## Document AI: Extract from PDFs and Images

Cortex Document AI extracts structured information from unstructured documents (invoices, contracts, receipts):

```sql
-- 1. Create a Document AI model (build a question set for extraction)
CREATE DOCUMENT AI MODEL invoice_extractor
    BASE_MODEL = 'pixtral-large';  -- multimodal model

-- 2. Run extraction on staged PDFs
SELECT
    f.RELATIVE_PATH AS filename,
    invoice_extractor!PREDICT(
        GET_PRESIGNED_URL('@invoice_stage', f.RELATIVE_PATH),
        1  -- page number (1 = first page, or ALL_PAGES)
    ) AS extracted
FROM DIRECTORY(@invoice_stage) f
WHERE f.RELATIVE_PATH LIKE '%.pdf';

-- 3. Parse the extracted JSON
SELECT
    filename,
    extracted:invoice_number::STRING AS invoice_number,
    extracted:vendor_name::STRING AS vendor,
    extracted:total_amount::FLOAT AS total,
    extracted:due_date::DATE AS due_date,
    extracted:line_items AS line_items
FROM invoices_extracted;
```

---

## Vector Embeddings

Generate vector embeddings directly in SQL for custom semantic search or clustering:

```sql
-- Generate embeddings for product descriptions
SELECT
    product_id,
    product_name,
    SNOWFLAKE.CORTEX.EMBED_TEXT_768(
        'e5-base-v2',           -- embedding model
        product_description
    ) AS description_embedding  -- returns a 768-dim VECTOR type
FROM products;

-- Store embeddings in a table with VECTOR column type
CREATE TABLE product_embeddings (
    product_id  VARCHAR,
    embedding   VECTOR(FLOAT, 768)
);

INSERT INTO product_embeddings
SELECT product_id, SNOWFLAKE.CORTEX.EMBED_TEXT_768('e5-base-v2', product_description)
FROM products;

-- Cosine similarity search (find most similar products)
SELECT
    b.product_id,
    VECTOR_COSINE_SIMILARITY(a.embedding, b.embedding) AS similarity
FROM product_embeddings a
CROSS JOIN product_embeddings b
WHERE a.product_id = 'PROD_001'
  AND b.product_id != 'PROD_001'
ORDER BY similarity DESC
LIMIT 5;
```

---

## Cortex Fine-Tuning (Custom Models)

Fine-tune a base model on your proprietary data:

```sql
-- Create a fine-tuning job
SELECT SNOWFLAKE.CORTEX.FINETUNE(
    'CREATE',
    'my_custom_classifier',     -- name for your fine-tuned model
    'mistral-7b',               -- base model
    'SELECT prompt, completion FROM training_data.labeled_tickets',   -- training data
    'SELECT prompt, completion FROM training_data.validation_tickets' -- validation data
);

-- Check status
SELECT SNOWFLAKE.CORTEX.FINETUNE('SHOW');

-- Use the fine-tuned model
SELECT SNOWFLAKE.CORTEX.COMPLETE(
    'my_custom_classifier',     -- your fine-tuned model
    ticket_body
) AS predicted_department
FROM support_tickets;
```

---

## Interview Tips

> **Tip 1:** "How would you build a RAG chatbot on internal documents using Snowflake?" — "Three components: (1) Cortex Search Service on the document table — handles chunking and embedding. (2) A search call to retrieve top-k relevant chunks for the user's question. (3) COMPLETE call with the retrieved context injected into the prompt. All in SQL or Python in Snowflake — no external vector DB needed."

> **Tip 2:** "What's the difference between Cortex Search and storing embeddings manually?" — "Cortex Search is fully managed — it handles chunking, embedding generation, index maintenance, and incremental updates automatically. Manual embeddings with `EMBED_TEXT_768` + VECTOR column give more control (custom chunking, custom models, custom similarity thresholds) but require more setup. Use Cortex Search for standard document search use cases; manual embeddings for custom similarity logic."

> **Tip 3:** "What is Cortex Analyst and who is it for?" — "Cortex Analyst is a natural language interface to SQL — business users ask questions in plain English and get correct SQL back. It uses a semantic model YAML that describes table relationships, column meanings, and common metrics. Best for self-serve analytics: stakeholders query dashboards without needing a DE to write SQL for every request."
