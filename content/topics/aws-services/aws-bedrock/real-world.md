---
title: "AWS Bedrock Real-World Applications for Data Engineers"
description: "Production use cases: AI-powered data quality, document processing pipelines, embedding-based data catalogs, and LLM-driven ETL transformation"
content_type: study_material
topic: aws-services
subtopic: aws-bedrock
layer: real-world
difficulty_level: mid-level
tags: [aws, bedrock, data-quality, document-processing, rag, data-catalog, etl, embeddings, production]
---

# AWS Bedrock Real-World Applications

## Use Case 1: AI-Powered Data Quality Checks

Traditional data quality rules are written manually — if a new type of anomaly emerges, you need to write a new rule. Bedrock enables flexible, natural-language-driven quality assessment.

### Problem

A financial data pipeline ingests transaction records from multiple sources. Anomalies include: amounts that are statistically unusual, descriptions that contain potential errors, currency mismatches, and merchant names that appear fraudulent. Writing deterministic rules to catch all of these is brittle.

### Solution: LLM-Based Anomaly Explanation

```python
import boto3
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf
import pandas as pd

bedrock = boto3.client("bedrock-runtime", region_name="us-east-1")
spark = SparkSession.builder.appName("bedrock-dq").getOrCreate()

def assess_transaction(row_json: str) -> str:
    """Call Bedrock to assess a single transaction for quality issues."""
    prompt = f"""You are a financial data quality analyst. Review this transaction record and identify any data quality issues. Be concise — output only: PASS, WARNING: <reason>, or FAIL: <reason>.

Transaction: {row_json}"""
    
    response = bedrock.converse(
        modelId="anthropic.claude-3-haiku-20240307-v1:0",
        messages=[{"role": "user", "content": [{"text": prompt}]}],
        inferenceConfig={"maxTokens": 100, "temperature": 0}
    )
    return response["output"]["message"]["content"][0]["text"].strip()

@pandas_udf("string")
def bedrock_quality_check(rows: pd.Series) -> pd.Series:
    results = []
    for row in rows:
        try:
            result = assess_transaction(row)
        except Exception as e:
            result = f"ERROR: {str(e)}"
        results.append(result)
    return pd.Series(results)

# Apply in Glue/Spark job
df = spark.read.parquet("s3://data-lake/transactions/raw/")
df_with_check = df.withColumn(
    "dq_assessment",
    bedrock_quality_check(df.to_json())  # serialize row to JSON string
)

# Separate clean vs. flagged records
clean = df_with_check.filter(df_with_check.dq_assessment.startswith("PASS"))
flagged = df_with_check.filter(~df_with_check.dq_assessment.startswith("PASS"))

flagged.write.mode("overwrite").parquet("s3://data-lake/transactions/quarantine/")
clean.drop("dq_assessment").write.mode("overwrite").parquet("s3://data-lake/transactions/validated/")
```

### Cost Consideration

At 100 tokens/record (input + output), Claude 3 Haiku costs $0.000025/record. For 10 million records: **$250/run**. This is viable for daily batch quality checks but not for real-time streaming.

---

## Use Case 2: Document Ingestion Pipeline (PDF → Structured Data)

Data engineers often need to extract structured data from unstructured documents: invoices, contracts, research reports, compliance filings.

### Architecture

```
S3 (PDF uploads)
    → EventBridge rule (s3:ObjectCreated)
    → Lambda: extract text with Amazon Textract
    → Lambda: send text + extraction schema to Bedrock
    → Validated JSON → DynamoDB or S3 (Parquet)
    → Glue Catalog registration
```

### Lambda: Textract → Bedrock Extraction

```python
import boto3
import json

textract = boto3.client("textract")
bedrock = boto3.client("bedrock-runtime", region_name="us-east-1")

EXTRACTION_SCHEMA = {
    "invoice_number": "string",
    "vendor_name": "string",
    "invoice_date": "YYYY-MM-DD",
    "line_items": [{"description": "string", "quantity": "number", "unit_price": "number", "total": "number"}],
    "subtotal": "number",
    "tax": "number",
    "total_amount": "number",
    "currency": "3-letter ISO code"
}

def lambda_handler(event, context):
    bucket = event["detail"]["bucket"]["name"]
    key = event["detail"]["object"]["key"]
    
    # Step 1: Extract text from PDF
    textract_response = textract.detect_document_text(
        Document={"S3Object": {"Bucket": bucket, "Name": key}}
    )
    raw_text = " ".join(
        block["Text"] for block in textract_response["Blocks"]
        if block["BlockType"] == "LINE"
    )
    
    # Step 2: Use Bedrock to extract structured data
    prompt = f"""Extract the following fields from this invoice text and return ONLY valid JSON matching the schema. If a field is not found, use null.

Schema: {json.dumps(EXTRACTION_SCHEMA, indent=2)}

Invoice text:
{raw_text[:8000]}  # Truncate to stay within context window

Return only the JSON object, no explanation."""

    response = bedrock.converse(
        modelId="anthropic.claude-3-haiku-20240307-v1:0",
        messages=[{"role": "user", "content": [{"text": prompt}]}],
        inferenceConfig={"maxTokens": 1000, "temperature": 0}
    )
    
    extracted_json = response["output"]["message"]["content"][0]["text"]
    
    # Step 3: Validate and store
    extracted = json.loads(extracted_json)
    extracted["source_document"] = f"s3://{bucket}/{key}"
    extracted["extraction_timestamp"] = context.aws_request_id
    
    # Write to S3 as structured JSON
    s3 = boto3.client("s3")
    output_key = key.replace("raw/", "structured/").replace(".pdf", ".json")
    s3.put_object(
        Bucket="processed-docs-bucket",
        Key=output_key,
        Body=json.dumps(extracted),
        ContentType="application/json"
    )
    
    return {"statusCode": 200, "extracted_fields": list(extracted.keys())}
```

---

## Use Case 3: Semantic Data Catalog with Bedrock + OpenSearch

Traditional data catalogs rely on exact-match search: you search for "customer" and get tables with "customer" in the name. Semantic search finds tables that *represent* customer data even if named differently (e.g., `acct_holders`, `subscriber_profiles`).

### Building the Catalog

```python
import boto3
import json
from opensearchpy import OpenSearch, RequestsHttpConnection
from aws_requests_auth.boto_utils import BotoAWSRequestsAuth

bedrock = boto3.client("bedrock-runtime", region_name="us-east-1")

def generate_table_embedding(table_metadata: dict) -> list:
    """Generate embedding for a Glue catalog table entry."""
    # Create a rich text representation
    columns_text = ", ".join(
        f"{col['Name']} ({col['Type']})" 
        for col in table_metadata.get("columns", [])
    )
    text = f"""
    Table: {table_metadata['table_name']}
    Database: {table_metadata['database']}
    Description: {table_metadata.get('description', 'No description')}
    Columns: {columns_text}
    Sample values: {table_metadata.get('sample_values', '')}
    """
    
    response = bedrock.invoke_model(
        modelId="amazon.titan-embed-text-v2:0",
        body=json.dumps({"inputText": text.strip(), "dimensions": 512, "normalize": True}),
        contentType="application/json",
        accept="application/json"
    )
    return json.loads(response["body"].read())["embedding"]

def search_catalog(query: str, os_client, top_k: int = 5) -> list:
    """Semantic search over the data catalog."""
    # Embed the user's query
    response = bedrock.invoke_model(
        modelId="amazon.titan-embed-text-v2:0",
        body=json.dumps({"inputText": query, "dimensions": 512, "normalize": True}),
        contentType="application/json",
        accept="application/json"
    )
    query_embedding = json.loads(response["body"].read())["embedding"]
    
    # kNN search in OpenSearch
    search_body = {
        "size": top_k,
        "query": {
            "knn": {
                "embedding": {"vector": query_embedding, "k": top_k}
            }
        },
        "_source": ["table_name", "database", "description", "columns"]
    }
    
    results = os_client.search(index="data-catalog", body=search_body)
    return [hit["_source"] for hit in results["hits"]["hits"]]

# Example usage
results = search_catalog("Where is customer contact information stored?", os_client)
# Returns: subscriber_profiles, acct_holders, crm_contacts — even without "customer" in name
```

---

## Use Case 4: LLM-Driven SQL Generation for Self-Service Analytics

Data engineers can build internal tools where analysts describe their question in plain English and get back validated SQL:

```python
def generate_sql(question: str, schema_context: str) -> str:
    prompt = f"""You are an expert data analyst. Generate a SQL query for the following question.
    
Database schema:
{schema_context}

Rules:
- Use only tables and columns that exist in the schema above
- Use Redshift SQL syntax
- Add comments explaining complex joins
- Return ONLY the SQL query, no explanation

Question: {question}"""

    response = bedrock.converse(
        modelId="anthropic.claude-3-sonnet-20240229-v1:0",  # Use Sonnet for complex SQL
        messages=[{"role": "user", "content": [{"text": prompt}]}],
        inferenceConfig={"maxTokens": 1000, "temperature": 0}
    )
    return response["output"]["message"]["content"][0]["text"]

# Example
schema = """
Tables:
- orders (order_id, customer_id, order_date, total_amount, status)
- customers (customer_id, name, email, region, signup_date)
- products (product_id, name, category, price)
- order_items (order_id, product_id, quantity, unit_price)
"""

sql = generate_sql(
    "Show me total revenue by product category for customers who signed up in 2023 and made at least 3 orders",
    schema
)
```

---

## Use Case 5: Automated Pipeline Documentation

Bedrock can auto-generate documentation from pipeline code, keeping docs in sync with implementation:

```python
def document_glue_job(job_script_path: str) -> str:
    with open(job_script_path) as f:
        code = f.read()
    
    prompt = f"""Analyze this AWS Glue PySpark job and generate structured documentation including:
1. Purpose (1-2 sentences)
2. Input sources (S3 paths, tables)
3. Transformations performed (bullet list)
4. Output destinations
5. Key configuration parameters
6. Dependencies and prerequisites
7. Estimated run time and resource requirements if determinable from the code

Code:
{code[:10000]}

Generate the documentation in Markdown format."""

    response = bedrock.converse(
        modelId="anthropic.claude-3-sonnet-20240229-v1:0",
        messages=[{"role": "user", "content": [{"text": prompt}]}],
        inferenceConfig={"maxTokens": 2000, "temperature": 0.1}
    )
    return response["output"]["message"]["content"][0]["text"]
```

---

## Key Takeaways

- AI-powered data quality using Bedrock (Haiku) costs ~$250 per 10M records — viable for daily batch, not real-time streaming
- The Textract → Bedrock pattern handles PDF/image document ingestion: Textract extracts text, Bedrock extracts structure
- Semantic data catalogs using Titan Embeddings + OpenSearch find relevant tables even when naming conventions differ from the search query
- SQL generation with Claude (Sonnet for complex queries) enables self-service analytics with schema-grounded prompts
- Bedrock can auto-generate pipeline documentation from code — useful for keeping runbooks in sync with implementation changes
