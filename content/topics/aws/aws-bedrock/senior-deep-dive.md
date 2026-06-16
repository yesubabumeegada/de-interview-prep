---
title: "AWS Bedrock Senior Deep Dive: Production Architecture and Advanced Patterns"
description: "Production Bedrock architectures, model evaluation, fine-tuning, cost control at scale, and Bedrock vs SageMaker tradeoffs"
content_type: study_material
topic: aws-services
subtopic: aws-bedrock
layer: senior-deep-dive
difficulty_level: senior
tags: [aws, bedrock, production, fine-tuning, model-evaluation, cost, sagemaker, rag, data-engineering]
---

# AWS Bedrock Senior Deep Dive

## Bedrock vs. SageMaker vs. Self-Hosted Models

For a senior DE choosing an AI inference platform, the decision has real architectural and cost consequences:

| Dimension | Bedrock | SageMaker Endpoints | Self-Hosted (EC2/EKS) |
|-----------|---------|--------------------|-----------------------|
| **Infrastructure** | None — fully managed | Manage endpoint scaling | Full VM/container management |
| **Model selection** | Curated set (Claude, Titan, Llama, etc.) | Any model (HuggingFace, custom) | Any model |
| **Cold start** | None (shared infrastructure) | Endpoint startup: 2-10 min | Manual scaling |
| **Latency** | Variable (shared), consistent with provisioned throughput | Consistent with dedicated endpoint | Lowest (co-located) |
| **Cost model** | Per token | Per endpoint-hour (running 24/7) | EC2/GPU on-demand or reserved |
| **Compliance** | AWS-managed data handling | Data stays in your VPC | Full control |
| **Fine-tuning** | Bedrock Custom Models (limited models) | Full fine-tuning on any model | Full control |

**When to choose Bedrock:**
- Variable/bursty workloads (pay only when you call)
- Need quick access to frontier models (Claude 3, Titan) without model management
- Team lacks MLOps expertise to manage SageMaker endpoints

**When to choose SageMaker:**
- Need to run a specific open-weight model not available on Bedrock
- Require very low latency with guaranteed throughput (dedicated endpoint)
- Have existing SageMaker infrastructure and MLOps pipelines

**When to choose self-hosted:**
- Very high volume where per-token costs exceed EC2 costs
- Regulatory requirement that model weights never leave your infrastructure
- Need specialized hardware (H100 GPUs) not available in SageMaker

---

## Provisioned Throughput: When and How

On-demand Bedrock throttles under load. For production pipelines:

### Model Units (MU) Calculation

```
Throughput goal: 1,000 input tokens/second sustained
Model: Claude 3 Haiku

1 Model Unit for Haiku = ~250 input tokens/second

Required MUs = 1,000 / 250 = 4 Model Units

Cost: 4 MUs × $10.00/MU/hour = $40/hour
Compare to on-demand: 1,000 tokens/sec × 3,600 sec/hr = 3.6M tokens/hr × $0.00025/1K = $0.90/hr

Provisioned throughput only makes sense if you're running at high, consistent utilization.
```

Provisioned throughput is purchased for 1 month or 6 months. It's appropriate for:
- Always-on document processing pipelines
- Real-time customer-facing applications with SLA requirements
- Workloads where throttling would cause downstream failures

---

## Bedrock Custom Models (Fine-Tuning)

Bedrock supports fine-tuning on selected base models (Titan Text, some Claude variants, Llama) using your own training data stored in S3.

### When to Fine-Tune

Fine-tuning is worth the cost and complexity only when:
1. The base model consistently fails at a well-defined task despite good prompting
2. You have 1,000+ high-quality labeled examples
3. The task is narrow and stable (model won't drift as requirements change)

For most DE use cases (classification, extraction, summarization), **prompt engineering and few-shot examples** outperform fine-tuning and are far cheaper to iterate.

### Fine-Tuning Process

```python
bedrock_client = boto3.client("bedrock", region_name="us-east-1")

# Training data format in S3: JSONL, one example per line
# {"prompt": "Classify: 'Server CPU at 95%'", "completion": "infrastructure_alert"}

job = bedrock_client.create_model_customization_job(
    jobName="de-alert-classifier-v1",
    customModelName="de-alert-classifier-titan-v1",
    roleArn="arn:aws:iam::123456789012:role/BedrockCustomizationRole",
    baseModelIdentifier="amazon.titan-text-express-v1",
    customizationType="FINE_TUNING",
    trainingDataConfig={
        "s3Uri": "s3://my-bucket/training-data/alert-examples.jsonl"
    },
    outputDataConfig={
        "s3Uri": "s3://my-bucket/fine-tuned-model-output/"
    },
    hyperParameters={
        "epochCount": "3",
        "batchSize": "8",
        "learningRate": "0.00005"
    }
)
```

### Custom Model Storage

Fine-tuned models are stored in Bedrock and only accessible in your account. They still require provisioned throughput (no on-demand option for custom models).

---

## Model Evaluation

Bedrock Model Evaluation lets you compare model outputs systematically before choosing which model to use in a pipeline.

### Automatic Evaluation

Bedrock can evaluate models on standard metrics using your dataset:

```python
eval_job = bedrock_client.create_evaluation_job(
    jobName="haiku-vs-sonnet-extraction",
    roleArn="arn:aws:iam::123456789012:role/BedrockEvalRole",
    evaluationConfig={
        "automated": {
            "datasetMetricConfigs": [
                {
                    "taskType": "Summarization",
                    "dataset": {
                        "name": "pipeline-doc-extraction",
                        "datasetLocation": {"s3Uri": "s3://my-bucket/eval-dataset/"}
                    },
                    "metricNames": ["BERTScore", "F1Score", "Rouge"]
                }
            ]
        }
    },
    inferenceConfig={
        "models": [
            {
                "bedrockModel": {
                    "modelIdentifier": "anthropic.claude-3-haiku-20240307-v1:0",
                    "inferenceParams": json.dumps({"max_tokens": 512})
                }
            },
            {
                "bedrockModel": {
                    "modelIdentifier": "anthropic.claude-3-sonnet-20240229-v1:0",
                    "inferenceParams": json.dumps({"max_tokens": 512})
                }
            }
        ]
    },
    outputDataConfig={"s3Uri": "s3://my-bucket/eval-results/"}
)
```

### Human Evaluation via Amazon SageMaker Ground Truth

For subjective tasks (data quality descriptions, anomaly explanations), combine Bedrock with SageMaker Ground Truth for human evaluation at scale.

---

## Production-Grade RAG Architecture

A production RAG pipeline over a data lake requires more than basic Knowledge Bases:

```
┌─────────────────────────────────────────────────────┐
│                  INGESTION PLANE                     │
│                                                      │
│  S3 (raw docs)                                       │
│      ↓ EventBridge (new object notification)         │
│  Lambda (pre-process: extract text from PDF/Excel)   │
│      ↓                                               │
│  Bedrock KB Ingestion Job (chunk + embed + index)    │
│      ↓                                               │
│  OpenSearch Serverless (vector index)                │
└─────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────┐
│                  QUERY PLANE                         │
│                                                      │
│  User Query                                          │
│      ↓                                               │
│  Bedrock Guardrails (input filtering)                │
│      ↓                                               │
│  Titan Embeddings (query → vector)                   │
│      ↓                                               │
│  OpenSearch kNN search (retrieve top-K chunks)       │
│      ↓ + metadata filter (date range, source type)   │
│  Re-ranking (optional: Cohere Rerank via Bedrock)    │
│      ↓                                               │
│  Claude (generate answer with retrieved context)     │
│      ↓                                               │
│  Bedrock Guardrails (output PII filtering)           │
│      ↓                                               │
│  Response + citations                                │
└─────────────────────────────────────────────────────┘
```

### Metadata Filtering for Precision

Pure semantic search returns the most semantically similar chunks, but often needs metadata constraints:

```python
response = bedrock_agent_runtime.retrieve(
    knowledgeBaseId="ABCD1234",
    retrievalQuery={"text": "What ETL jobs failed last week?"},
    retrievalConfiguration={
        "vectorSearchConfiguration": {
            "numberOfResults": 10,
            "filter": {
                "andAll": [
                    {"equals": {"key": "source_type", "value": "incident_report"}},
                    {"greaterThan": {"key": "created_date", "value": "2024-01-01"}}
                ]
            }
        }
    }
)
```

---

## Cost Control at Scale

### Token Budget Management

```python
# Estimate cost before processing large datasets
def estimate_bedrock_cost(text_samples, model="haiku", sample_size=100):
    import tiktoken
    enc = tiktoken.get_encoding("cl100k_base")
    
    prices = {
        "haiku": {"input": 0.00025, "output": 0.00125},
        "sonnet": {"input": 0.003, "output": 0.015}
    }
    
    # Sample to estimate tokens
    sample_tokens = sum(len(enc.encode(s)) for s in text_samples[:sample_size]) / sample_size
    total_input_tokens = sample_tokens * len(text_samples)
    total_output_tokens = total_input_tokens * 0.3  # assume output is 30% of input
    
    cost = (
        (total_input_tokens / 1000) * prices[model]["input"] +
        (total_output_tokens / 1000) * prices[model]["output"]
    )
    
    print(f"Estimated cost for {len(text_samples)} documents: ${cost:.2f}")
    return cost
```

### Caching with DynamoDB

For pipelines that call Bedrock with the same or similar prompts:

```python
import hashlib
import boto3
import json

dynamodb = boto3.resource("dynamodb")
cache_table = dynamodb.Table("bedrock-response-cache")

def invoke_with_cache(model_id, prompt, ttl_seconds=86400):
    cache_key = hashlib.sha256(f"{model_id}:{prompt}".encode()).hexdigest()
    
    # Check cache
    cached = cache_table.get_item(Key={"cache_key": cache_key}).get("Item")
    if cached:
        return cached["response"]
    
    # Call Bedrock
    response = bedrock.converse(
        modelId=model_id,
        messages=[{"role": "user", "content": [{"text": prompt}]}]
    )
    result = response["output"]["message"]["content"][0]["text"]
    
    # Store in cache with TTL
    import time
    cache_table.put_item(Item={
        "cache_key": cache_key,
        "response": result,
        "ttl": int(time.time()) + ttl_seconds
    })
    
    return result
```

### Batch Inference for Non-Real-Time Workloads

For batch document processing, Bedrock Batch Inference is significantly cheaper (up to 50% discount) vs. real-time:

```python
batch_job = bedrock_client.create_model_invocation_job(
    jobName="bulk-document-classification-2024-01",
    modelId="anthropic.claude-3-haiku-20240307-v1:0",
    roleArn="arn:aws:iam::123456789012:role/BedrockBatchRole",
    inputDataConfig={
        "s3InputDataConfig": {
            "s3Uri": "s3://my-bucket/batch-input/",
            "s3InputFormat": "JSONL"
        }
    },
    outputDataConfig={
        "s3OutputDataConfig": {"s3Uri": "s3://my-bucket/batch-output/"}
    }
)
# Batch jobs complete asynchronously — poll or use EventBridge for completion notification
```

---

## Key Takeaways

- Provisioned throughput is only cost-effective at sustained high utilization; otherwise on-demand is cheaper
- Fine-tuning requires 1,000+ quality examples and should only be used when prompt engineering fails — it's costly to iterate
- Production RAG adds metadata filtering, re-ranking, and PII guardrails on top of basic Knowledge Bases retrieve-and-generate
- Bedrock Batch Inference provides ~50% cost reduction for async, non-real-time document processing
- DynamoDB caching of Bedrock responses can dramatically cut costs for pipelines with repeated or similar prompts
- Model Evaluation (automated + human) should precede production model selection — cost and quality differ significantly across models for the same task
