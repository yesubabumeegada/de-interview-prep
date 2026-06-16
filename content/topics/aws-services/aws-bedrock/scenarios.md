---
title: "AWS Bedrock Interview Scenarios"
description: "Scenario-based interview questions on AWS Bedrock for data engineers — foundation models, RAG pipelines, and production AI workloads"
content_type: scenario_question
topic: aws-services
subtopic: aws-bedrock
tags: [aws, bedrock, rag, knowledge-bases, embeddings, foundation-models, data-engineering, interview]
---

<article data-difficulty="junior">

## Scenario: Pick the Right Bedrock Model for a Data Pipeline Task

Your team wants to add AI capabilities to a data pipeline. The pipeline processes 5 million customer support tickets per day and needs to:
1. Classify each ticket into one of 12 predefined categories
2. Extract the product name, issue type, and urgency level from each ticket
3. Generate a 1-sentence summary for analyst review

Each ticket is 50–200 words. The budget for this feature is $500/month.

Which Bedrock model(s) would you choose and why? Estimate the cost.

<details>
<summary>✅ Solution</summary>

### Step 1: Estimate Token Volume

Average ticket: ~150 words ≈ 200 tokens input  
Output per ticket (classification + extraction + summary): ~100 tokens

5M tickets/day × 30 days = 150M tickets/month  
Input tokens: 150M × 200 = 30 billion tokens/month  
Output tokens: 150M × 100 = 15 billion tokens/month

### Step 2: Cost Comparison

| Model | Input cost | Output cost | Monthly total |
|-------|-----------|-------------|---------------|
| Claude 3 Haiku | $0.00025/1K | $0.00125/1K | 30B×$0.00025 + 15B×$0.00125 = $7,500 + $18,750 = **$26,250** |
| Titan Text Express | $0.0002/1K | $0.0006/1K | 30B×$0.0002 + 15B×$0.0006 = $6,000 + $9,000 = **$15,000** |

Both models far exceed the $500/month budget at full volume.

### Step 3: Make it Work Within Budget

**Option A: Process only high-priority tickets with Bedrock**

```python
# Rule-based pre-filter: only send escalated or VIP tickets to Bedrock
high_priority = tickets.filter(
    (tickets.is_vip == True) | 
    (tickets.response_count > 3) |
    (tickets.contains_keyword("urgent", "broken", "refund"))
)
# Assume 5% are high priority = 250K/day = 7.5M/month
# 7.5M × 300 tokens × $0.00025/1K = ~$562/month — close to budget
```

**Option B: Fine-tune a lighter model or use a smaller prompt**

Compress the prompt to 50 tokens, limit output to 50 tokens → 4× token reduction → fits budget

**Option C: Batch Inference (50% cost reduction)**

Use Bedrock Batch Inference for async processing:
- 150M tickets/month at batch prices ≈ 50% off real-time → ~$13,000/month
- Still over budget, but viable if budget can be raised

### Model Choice Recommendation

For this use case:
- **Classification + extraction**: Claude 3 Haiku or Titan Text Express — both are accurate enough for structured extraction tasks
- **Summaries**: Claude 3 Haiku produces better natural language summaries

Use **Titan Text Express** if cost is the primary constraint; switch to **Haiku** if quality matters more.

Always evaluate both models on 100–200 representative tickets before deciding — accuracy differences vary by domain.

</details>

</article>

<article data-difficulty="mid">

## Scenario: Design a RAG Pipeline for an Internal Data Catalog Q&A System

Your organization has 10,000 data tables across 5 business domains (Finance, Sales, Operations, HR, Marketing). Analysts frequently ask questions like:
- "Where is customer lifetime value stored?"
- "Which table has refund data?"
- "What's the grain of the daily_orders table?"

Currently, they email the data engineering team. You want to build a self-service Q&A system using AWS Bedrock Knowledge Bases.

Walk through the full architecture: ingestion, indexing, querying, and the key design decisions at each step.

<details>
<summary>✅ Solution</summary>

### Step 1: Prepare the Document Corpus

The knowledge base needs rich descriptions to answer questions accurately. Raw table schemas alone are insufficient.

**Document structure per table** (stored in S3 as Markdown):

```markdown
# Table: finance.daily_revenue

**Database:** finance  
**Schema:** revenue_analytics  
**Owner:** Finance Data Team  
**Refresh:** Daily at 2:00 AM UTC  
**Grain:** One row per business unit per day  

## Description
Aggregated daily revenue figures by business unit, product line, and region. 
Source: order management system transactions, processed through the revenue ETL pipeline.

## Columns
- `report_date` (DATE): The business date for the revenue figures
- `business_unit` (VARCHAR): BU identifier (e.g., EMEA, APAC, AMER)
- `product_line` (VARCHAR): Product category (e.g., Software, Services, Hardware)
- `gross_revenue` (DECIMAL): Total revenue before deductions
- `refunds` (DECIMAL): Refund amounts for the period
- `net_revenue` (DECIMAL): gross_revenue - refunds

## Common Use Cases
- Monthly and quarterly revenue reporting
- Business unit performance dashboards
- Commission calculations (do NOT use for payroll — see hr.commissions)

## Related Tables
- `finance.monthly_revenue`: Pre-aggregated monthly version
- `sales.daily_orders`: Source data before revenue recognition
```

Generate these automatically from Glue Data Catalog metadata + column comments.

### Step 2: Choose Chunking Strategy

**Hierarchical chunking** is best for table documentation:
- Parent chunk = full table document (for context)
- Child chunks = individual sections (Description, Columns, Use Cases)

This way, a query about "what columns does daily_revenue have" retrieves the Columns section specifically, not the whole document.

```python
# Bedrock KB configuration
vectorIngestionConfiguration={
    "chunkingConfiguration": {
        "chunkingStrategy": "HIERARCHICAL",
        "hierarchicalChunkingConfiguration": {
            "levelConfigurations": [
                {"maxTokens": 1500},  # parent: full table doc
                {"maxTokens": 300}    # child: section level
            ],
            "overlapTokens": 60
        }
    }
}
```

### Step 3: Metadata Filtering Schema

Add metadata fields to each document for filtered retrieval:

```python
# Metadata file alongside each document in S3 (same name + .metadata.json)
{
  "metadataAttributes": {
    "domain": "finance",
    "table_name": "daily_revenue",
    "has_pii": false,
    "data_freshness": "daily",
    "owner_team": "finance-data"
  }
}
```

Analysts in the Finance team should only see Finance domain tables:

```python
response = bedrock_agent_runtime.retrieve(
    knowledgeBaseId="KB_ID",
    retrievalQuery={"text": "Where is revenue data stored?"},
    retrievalConfiguration={
        "vectorSearchConfiguration": {
            "numberOfResults": 5,
            "filter": {"equals": {"key": "domain", "value": "finance"}}
        }
    }
)
```

### Step 4: Query + Generate Flow

```python
response = bedrock_agent_runtime.retrieve_and_generate(
    input={"text": "What's the grain of the daily_orders table and what does net_revenue mean?"},
    retrieveAndGenerateConfiguration={
        "type": "KNOWLEDGE_BASE",
        "knowledgeBaseConfiguration": {
            "knowledgeBaseId": "KB_ID",
            "modelArn": "arn:aws:bedrock:us-east-1::foundation-model/anthropic.claude-3-sonnet-20240229-v1:0",
            "retrievalConfiguration": {
                "vectorSearchConfiguration": {"numberOfResults": 5}
            },
            "generationConfiguration": {
                "promptTemplate": {
                    "textPromptTemplate": """You are a data catalog assistant. Answer the analyst's question based ONLY on the provided table documentation. If the information isn't in the context, say so — do not guess.

Context:
$search_results$

Question: $query$

Answer:"""
                }
            }
        }
    }
)
```

### Step 5: Keeping the Index Fresh

When table schemas change (columns added, grain changes, new tables created):

```python
# EventBridge rule: Glue Catalog table update → Lambda → KB sync
def lambda_handler(event, context):
    table = event["detail"]["tableName"]
    database = event["detail"]["databaseName"]
    
    # Re-generate documentation for changed table
    new_doc = generate_table_doc(database, table)
    
    # Upload to S3 (overwrites old doc)
    s3.put_object(
        Bucket="catalog-docs-bucket",
        Key=f"{database}/{table}.md",
        Body=new_doc
    )
    
    # Trigger KB re-ingestion
    bedrock_agent.start_ingestion_job(
        knowledgeBaseId="KB_ID",
        dataSourceId="DS_ID"
    )
```

### Key Design Decisions Summary

| Decision | Choice | Reason |
|----------|--------|--------|
| Chunking | Hierarchical | Table docs have natural section structure |
| Embedding | Titan Embeddings v2 (512-dim) | Good quality, 3× cheaper than 1536-dim |
| Generation model | Claude 3 Sonnet | Better at following "answer only from context" instructions |
| Metadata | Domain, table name, PII flag | Enables access control and targeted retrieval |
| Freshness | EventBridge-driven re-ingestion on schema changes | Keeps index in sync with Glue Catalog |

</details>

</article>

<article data-difficulty="senior">

## Scenario: Production Bedrock Pipeline Is Throttling — Diagnose and Fix

A production data pipeline calls Bedrock (Claude 3 Haiku) to classify 50,000 documents per hour during business hours. After launch, the pipeline starts dropping records with `ThrottlingException` errors. The on-call engineer escalates to you.

Walk through: how you diagnose the problem, your immediate mitigation, and long-term architecture changes.

<details>
<summary>✅ Solution</summary>

### Phase 1: Diagnose

**Check CloudWatch metrics for Bedrock:**

```python
import boto3

cw = boto3.client("cloudwatch")
response = cw.get_metric_statistics(
    Namespace="AWS/Bedrock",
    MetricName="InvocationThrottles",
    Dimensions=[
        {"Name": "ModelId", "Value": "anthropic.claude-3-haiku-20240307-v1:0"}
    ],
    StartTime=datetime.utcnow() - timedelta(hours=2),
    EndTime=datetime.utcnow(),
    Period=300,
    Statistics=["Sum"]
)
# Look for spikes in InvocationThrottles aligned with pipeline run schedule
```

Key Bedrock CloudWatch metrics:
- `InvocationThrottles`: How many requests were throttled
- `InvocationLatency`: P50/P99 latency (rising P99 = capacity pressure)
- `InvocationClientErrors`: 4xx errors including throttles

**Identify the throttle type:**
- `ThrottlingException` from Bedrock: You're hitting the **account-level RPM/TPM quota**
- `ModelNotReadyException`: Model capacity issue (rare)
- HTTP 429: Same as ThrottlingException

On-demand Bedrock has default service quotas:
- Claude 3 Haiku: 2,000 RPM and 100,000 TPM per region (these are soft limits, raiseable)

50,000 documents/hour = ~833 docs/minute. If each call is one document, that's 833 RPM — well under 2,000 RPM. But if the pipeline bursts all requests at the start of each hour (common with Airflow hourly DAGs), it could spike to 10,000+ RPM momentarily.

### Phase 2: Immediate Mitigation

**Add exponential backoff with jitter to all Bedrock calls:**

```python
import boto3
import time
import random
from botocore.exceptions import ClientError

bedrock = boto3.client("bedrock-runtime", region_name="us-east-1")

def invoke_with_retry(model_id, messages, max_retries=5):
    for attempt in range(max_retries):
        try:
            response = bedrock.converse(
                modelId=model_id,
                messages=messages,
                inferenceConfig={"maxTokens": 200, "temperature": 0}
            )
            return response["output"]["message"]["content"][0]["text"]
        
        except ClientError as e:
            if e.response["Error"]["Code"] == "ThrottlingException":
                if attempt == max_retries - 1:
                    raise
                # Exponential backoff with full jitter
                wait = (2 ** attempt) + random.uniform(0, 1)
                time.sleep(min(wait, 60))  # cap at 60 seconds
            else:
                raise
```

**Smooth the request rate with SQS:**

Replace direct Bedrock calls in the pipeline with an SQS queue + Lambda consumer:

```
Documents → SQS (rate-limited consumer)
SQS → Lambda (reserved concurrency = X)
Lambda → Bedrock (controlled RPM = X × avg_docs_per_invocation)
```

Set Lambda reserved concurrency to control the maximum Bedrock RPM:
- Target: 800 RPM (under the 2,000 limit with headroom)
- Lambda invocation duration: ~500ms per Bedrock call
- Lambda concurrency needed: 800 RPM / 120 invocations/minute/function = 7 concurrent Lambdas

### Phase 3: Long-Term Architecture Fix

**Option A: Request a Service Quota Increase**

```bash
aws service-quotas request-service-quota-increase \
  --service-code bedrock \
  --quota-code L-XXXX \  # quota code for Claude 3 Haiku RPM
  --desired-value 10000
```

This is the fastest fix for sustained high volume — usually approved within 1-2 business days.

**Option B: Multi-Region Distribution**

Spread requests across multiple regions using cross-region inference profiles:

```python
import random

CROSS_REGION_MODELS = [
    "us.anthropic.claude-3-haiku-20240307-v1:0",  # routes within US
    "eu.anthropic.claude-3-haiku-20240307-v1:0",  # routes within EU
]

clients = {
    "us": boto3.client("bedrock-runtime", region_name="us-east-1"),
    "eu": boto3.client("bedrock-runtime", region_name="eu-west-1")
}

def invoke_distributed(messages):
    region = random.choice(["us", "eu"])
    model = f"{region}.anthropic.claude-3-haiku-20240307-v1:0"
    return clients[region].converse(modelId=model, messages=messages)
```

**Option C: Provisioned Throughput**

For sustained 833 RPM sustained, calculate MUs needed:
- Haiku: 1 MU ≈ 250 input tokens/second
- At 50 tokens/doc average: 250 tokens/sec = 5 docs/sec = 300 RPM per MU
- Need: 833 / 300 ≈ 3 MUs

Cost: 3 MUs × $10/MU/hour × 730 hours/month = **$21,900/month**
vs. on-demand at 50,000 docs/hr × 8hrs/day × 22 days × 150 tokens × $0.00025/1K = **$495/month**

Provisioned throughput is NOT cost-effective here — on-demand with quota increase is better.

### Phase 4: Response Caching

If documents share similar patterns (e.g., templated purchase orders), cache Bedrock responses:

```python
import hashlib
import boto3
import json

dynamodb = boto3.resource("dynamodb")
cache = dynamodb.Table("bedrock-classification-cache")

def classify_with_cache(document_text: str, model_id: str) -> str:
    # Normalize whitespace before hashing
    normalized = " ".join(document_text.split())
    cache_key = hashlib.sha256(f"{model_id}:{normalized}".encode()).hexdigest()
    
    item = cache.get_item(Key={"pk": cache_key}).get("Item")
    if item:
        return item["classification"]
    
    result = invoke_with_retry(
        model_id,
        [{"role": "user", "content": [{"text": f"Classify: {document_text}"}]}]
    )
    
    cache.put_item(Item={
        "pk": cache_key,
        "classification": result,
        "ttl": int(time.time()) + 86400  # 24-hour TTL
    })
    return result
```

If 30% of documents are near-duplicates, caching reduces Bedrock calls by 30% → 583 RPM effective → below quota without any infrastructure changes.

### Summary: Decision Tree for Throttling

```
ThrottlingException in production
├── Is it a burst spike (hourly Airflow DAG)?
│   └── Fix: SQS queue + rate-limited Lambda consumer
├── Is it sustained high RPM?
│   ├── Can afford quota increase wait (1-2 days)?
│   │   └── Fix: Request service quota increase (free)
│   ├── Need immediate fix?
│   │   └── Fix: Multi-region distribution via cross-region inference profiles
│   └── Very high sustained volume with consistent latency requirements?
│       └── Fix: Provisioned throughput (only if volume justifies cost)
└── Are many documents similar/duplicate?
    └── Fix: DynamoDB response cache (30%+ hit rate = significant throttle reduction)
```

</details>

</article>
