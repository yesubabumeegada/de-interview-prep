---
title: "AWS Bedrock Intermediate: Agents, Knowledge Bases, and DE Pipelines"
description: "Bedrock Agents, Knowledge Bases for RAG, Guardrails, and integrating Bedrock into data engineering pipelines"
content_type: study_material
topic: aws-services
subtopic: aws-bedrock
layer: intermediate
difficulty_level: mid-level
tags: [aws, bedrock, bedrock-agents, knowledge-bases, rag, guardrails, embeddings, data-pipeline]
---

# AWS Bedrock Intermediate

## Bedrock Agents

Bedrock Agents allow you to build autonomous, multi-step AI workflows. An agent can reason about a task, decide which tools (Action Groups) to call, and orchestrate multiple steps to complete a goal — without you writing the orchestration logic.

### Architecture of a Bedrock Agent

```
User Request
    ↓
Bedrock Agent (ReAct loop — Reason + Act)
    ├── Calls Action Group 1 (Lambda function → query your database)
    ├── Calls Action Group 2 (Lambda function → write to S3)
    └── Returns synthesized response
```

### Action Groups

An Action Group is a Lambda function + an OpenAPI schema that describes what the function does. The agent decides when and how to call it based on the user's intent.

**Example — Data Pipeline Action Group:**

```json
// OpenAPI schema registered with the agent
{
  "openapi": "3.0.0",
  "paths": {
    "/get-pipeline-status": {
      "get": {
        "operationId": "getPipelineStatus",
        "description": "Returns the current status of a named data pipeline",
        "parameters": [
          {"name": "pipeline_name", "in": "query", "required": true, "schema": {"type": "string"}}
        ]
      }
    },
    "/trigger-pipeline": {
      "post": {
        "operationId": "triggerPipeline",
        "description": "Triggers a data pipeline run",
        "requestBody": {
          "content": {
            "application/json": {
              "schema": {
                "type": "object",
                "properties": {
                  "pipeline_name": {"type": "string"},
                  "parameters": {"type": "object"}
                }
              }
            }
          }
        }
      }
    }
  }
}
```

```python
# Lambda function implementing the action group
import json
import boto3

def lambda_handler(event, context):
    action = event.get("actionGroup")
    api_path = event.get("apiPath")
    parameters = event.get("parameters", [])
    
    if api_path == "/get-pipeline-status":
        pipeline_name = next(p["value"] for p in parameters if p["name"] == "pipeline_name")
        # Query Airflow/Step Functions/Glue for status
        status = get_status_from_airflow(pipeline_name)
        return {"response": {"status": status}}
    
    elif api_path == "/trigger-pipeline":
        body = json.loads(event.get("requestBody", {}).get("content", {}).get("application/json", {}).get("body", "{}"))
        result = trigger_glue_job(body["pipeline_name"], body.get("parameters", {}))
        return {"response": {"triggered": True, "run_id": result}}
```

### Agent Memory

Bedrock Agents support **session memory** — they can maintain conversation context across multiple turns in a session. For data engineering use cases (e.g., a chatbot that helps analysts query data), this means the agent can remember "we were talking about the sales pipeline" without re-stating it every turn.

---

## Knowledge Bases for RAG

Knowledge Bases (KB) is Bedrock's managed RAG implementation. It handles the full pipeline: document ingestion, chunking, embedding generation, vector storage, and retrieval — all without you managing the pieces individually.

### How Knowledge Bases Work

```
Your Documents (S3)
    ↓ [Ingestion job — Bedrock chunking + Titan Embeddings]
Vector Store (OpenSearch Serverless or Aurora pgvector or Pinecone)
    ↓ [At query time: embed query → vector search → retrieve top-K chunks]
Retrieved chunks + user query → Foundation Model → Answer
```

### Setting Up a Knowledge Base (boto3)

```python
import boto3

bedrock_agent = boto3.client("bedrock-agent", region_name="us-east-1")

# Create a Knowledge Base backed by OpenSearch Serverless
kb = bedrock_agent.create_knowledge_base(
    name="data-engineering-docs-kb",
    description="Internal DE runbooks, pipeline docs, and data dictionaries",
    roleArn="arn:aws:iam::123456789012:role/BedrockKBRole",
    knowledgeBaseConfiguration={
        "type": "VECTOR",
        "vectorKnowledgeBaseConfiguration": {
            "embeddingModelArn": "arn:aws:bedrock:us-east-1::foundation-model/amazon.titan-embed-text-v2:0"
        }
    },
    storageConfiguration={
        "type": "OPENSEARCH_SERVERLESS",
        "opensearchServerlessConfiguration": {
            "collectionArn": "arn:aws:aoss:us-east-1:123456789012:collection/my-collection",
            "vectorIndexName": "bedrock-kb-index",
            "fieldMapping": {
                "vectorField": "bedrock-knowledge-base-default-vector",
                "textField": "AMAZON_BEDROCK_TEXT_CHUNK",
                "metadataField": "AMAZON_BEDROCK_METADATA"
            }
        }
    }
)
```

### Adding a Data Source (S3 → KB)

```python
ds = bedrock_agent.create_data_source(
    knowledgeBaseId=kb["knowledgeBase"]["knowledgeBaseId"],
    name="de-docs-s3-source",
    dataSourceConfiguration={
        "type": "S3",
        "s3Configuration": {
            "bucketArn": "arn:aws:s3:::my-de-docs-bucket",
            "inclusionPrefixes": ["runbooks/", "data-dictionaries/"]
        }
    },
    vectorIngestionConfiguration={
        "chunkingConfiguration": {
            "chunkingStrategy": "FIXED_SIZE",
            "fixedSizeChunkingConfiguration": {
                "maxTokens": 512,
                "overlapPercentage": 20
            }
        }
    }
)

# Trigger ingestion
bedrock_agent.start_ingestion_job(
    knowledgeBaseId=kb["knowledgeBase"]["knowledgeBaseId"],
    dataSourceId=ds["dataSource"]["dataSourceId"]
)
```

### Querying the Knowledge Base

```python
bedrock_agent_runtime = boto3.client("bedrock-agent-runtime", region_name="us-east-1")

response = bedrock_agent_runtime.retrieve_and_generate(
    input={"text": "What is the SLA for the daily sales pipeline?"},
    retrieveAndGenerateConfiguration={
        "type": "KNOWLEDGE_BASE",
        "knowledgeBaseConfiguration": {
            "knowledgeBaseId": "ABCD1234",
            "modelArn": "arn:aws:bedrock:us-east-1::foundation-model/anthropic.claude-3-haiku-20240307-v1:0"
        }
    }
)

print(response["output"]["text"])
# "The daily sales pipeline has an SLA of 6:00 AM EST based on the runbook..."
print(response["citations"])  # Shows which source chunks were used
```

---

## Chunking Strategies for Data Engineering Documents

Choosing the right chunking strategy affects retrieval quality:

| Strategy | When to Use |
|----------|-------------|
| **Fixed size** (512 tokens, 20% overlap) | General documents, runbooks |
| **Semantic chunking** | Long-form documentation where topics shift mid-document |
| **Hierarchical chunking** | Structured docs (parent = section, child = paragraph) — better for "what does section X say" queries |
| **No chunking** | Short documents where splitting would lose context (data dictionary entries, schema definitions) |

---

## Bedrock Guardrails

Guardrails let you apply content filtering and safety controls to Bedrock model calls — both inputs and outputs:

```python
# Create a Guardrail
bedrock_client = boto3.client("bedrock", region_name="us-east-1")

guardrail = bedrock_client.create_guardrail(
    name="de-pipeline-guardrail",
    contentPolicyConfig={
        "filtersConfig": [
            {"type": "HATE", "inputStrength": "HIGH", "outputStrength": "HIGH"},
            {"type": "VIOLENCE", "inputStrength": "MEDIUM", "outputStrength": "HIGH"}
        ]
    },
    sensitiveInformationPolicyConfig={
        "piiEntitiesConfig": [
            {"type": "EMAIL", "action": "ANONYMIZE"},
            {"type": "AWS_ACCESS_KEY", "action": "BLOCK"},
            {"type": "CREDIT_DEBIT_CARD_NUMBER", "action": "BLOCK"}
        ]
    },
    blockedInputMessaging="This request cannot be processed.",
    blockedOutputsMessaging="The response has been filtered."
)
```

**Data engineering use case for Guardrails:** When building pipelines that process customer data through Bedrock (e.g., extracting entities from support tickets), Guardrails can automatically anonymize PII in both the input sent to the model and the output returned — ensuring customer data never leaves in a readable form.

---

## Integrating Bedrock into Data Pipelines

### Pattern 1: Lambda + Bedrock for Inline Enrichment

```python
# In a Glue job or Lambda — enrich records with AI-generated fields
def enrich_product_record(record):
    response = bedrock.invoke_model(
        modelId="anthropic.claude-3-haiku-20240307-v1:0",
        body=json.dumps({
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 200,
            "messages": [{
                "role": "user",
                "content": f"Classify this product description into one of [Electronics, Clothing, Food, Home]: {record['description']}"
            }]
        }),
        contentType="application/json",
        accept="application/json"
    )
    category = json.loads(response["body"].read())["content"][0]["text"].strip()
    return {**record, "ai_category": category}
```

### Pattern 2: S3 → Bedrock → S3 via Step Functions

For bulk document processing:

```
S3 (raw documents)
    → Lambda (split into chunks, fan out)
    → SQS (queue of chunks)
    → Lambda workers (call Bedrock per chunk)
    → S3 (enriched output)
    → Glue Catalog (register output dataset)
```

### Pattern 3: Embedding Pipeline for Semantic Search

```python
import boto3
import json
from opensearchpy import OpenSearch

bedrock = boto3.client("bedrock-runtime", region_name="us-east-1")
os_client = OpenSearch(hosts=[{"host": "your-opensearch-endpoint", "port": 443}])

def embed_and_index(doc_id, text):
    # Generate embedding
    response = bedrock.invoke_model(
        modelId="amazon.titan-embed-text-v2:0",
        body=json.dumps({"inputText": text, "dimensions": 512, "normalize": True}),
        contentType="application/json",
        accept="application/json"
    )
    embedding = json.loads(response["body"].read())["embedding"]
    
    # Index in OpenSearch
    os_client.index(
        index="data-catalog-embeddings",
        body={"doc_id": doc_id, "text": text, "embedding": embedding},
        id=doc_id
    )
```

---

## Key Takeaways

- Bedrock Agents combine LLM reasoning with Lambda-backed Action Groups to build autonomous workflows without custom orchestration code
- Knowledge Bases manage the full RAG pipeline (S3 → chunk → embed → OpenSearch → retrieve+generate) as a managed service
- Chunking strategy (fixed, semantic, hierarchical) significantly affects retrieval quality — match to document structure
- Guardrails apply PII anonymization and content filtering at the API layer, not in application code
- Three main DE pipeline patterns: inline enrichment (Lambda), bulk fan-out (Step Functions + SQS), and embedding pipelines for semantic search
