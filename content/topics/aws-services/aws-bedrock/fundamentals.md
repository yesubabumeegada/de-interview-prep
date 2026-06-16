---
title: "AWS Bedrock Fundamentals for Data Engineers"
description: "Introduction to AWS Bedrock foundation models, APIs, and core concepts relevant to data engineering workloads"
content_type: study_material
topic: aws-services
subtopic: aws-bedrock
layer: fundamentals
difficulty_level: junior
tags: [aws, bedrock, llm, foundation-models, generative-ai, data-engineering]
---

# AWS Bedrock Fundamentals

## What Is AWS Bedrock?

AWS Bedrock is a fully managed service that provides access to high-performing foundation models (FMs) from Amazon and leading AI companies through a single API. It requires no infrastructure management — you call an API endpoint, pass your prompt, and receive a response.

For data engineers, Bedrock opens up a class of AI-augmented pipeline capabilities: automated data quality checks using LLMs, document ingestion and extraction, embedding generation for semantic search, and intelligent data transformation.

---

## Foundation Models Available on Bedrock

Bedrock provides access to models from multiple providers, all through a unified API:

### Amazon's Own Models

**Amazon Titan Text**
- General-purpose text generation and summarization
- Variants: Titan Text Lite (fast, cheap), Titan Text Express (balanced), Titan Text Premier (most capable)
- Good for: data transformation descriptions, automated metadata generation, SQL generation

**Amazon Titan Embeddings**
- Converts text into dense vector representations (embeddings)
- Output: 1536-dimensional vectors (v2)
- Critical for: building RAG (Retrieval-Augmented Generation) pipelines over data lake content

**Amazon Titan Multimodal Embeddings**
- Generates embeddings from both text and images
- Use case: embedding product images + descriptions for unified search

### Anthropic Models (Claude Family)
- Claude 3 Haiku: Fastest, cheapest — good for high-volume extraction tasks
- Claude 3 Sonnet: Balanced capability and cost
- Claude 3 Opus: Most capable — complex reasoning over documents
- Available via Bedrock with no separate Anthropic agreement needed

### Meta Llama Models
- Llama 3 8B, 70B: Open-weight models, available via Bedrock
- Good for: organizations that need open-weight model access through managed infrastructure

### Mistral AI Models
- Mistral 7B, Mixtral 8x7B: Strong performance at lower cost
- Good for: instruction following, summarization

### Stability AI
- Stable Diffusion XL: Image generation
- Less common in data engineering contexts

---

## Core Bedrock Concepts

### Model IDs

Each model has a specific ID used when calling the API:

```
anthropic.claude-3-sonnet-20240229-v1:0
anthropic.claude-3-haiku-20240307-v1:0
amazon.titan-embed-text-v2:0
meta.llama3-70b-instruct-v1:0
mistral.mistral-7b-instruct-v0:2
```

### Inference Profiles (Cross-Region)
Bedrock can route requests across regions for higher throughput and resilience. Cross-region inference profiles automatically use the best available region:

```
us.anthropic.claude-3-sonnet-20240229-v1:0   # routes within US regions
eu.anthropic.claude-3-haiku-20240307-v1:0    # routes within EU regions
```

### Provisioned Throughput vs. On-Demand

**On-Demand:** Pay per input/output token. No commitment. Subject to shared capacity limits (throttling under heavy load).

**Provisioned Throughput:** Purchase Model Units (MUs) in advance for guaranteed throughput. Required for production pipelines that need consistent latency and cannot tolerate throttling.

---

## Bedrock Pricing Model

Bedrock charges per token (not per API call):

| Model | Input (per 1K tokens) | Output (per 1K tokens) |
|-------|----------------------|------------------------|
| Claude 3 Haiku | $0.00025 | $0.00125 |
| Claude 3 Sonnet | $0.003 | $0.015 |
| Titan Text Express | $0.0002 | $0.0006 |
| Titan Embeddings v2 | $0.00002 | N/A |

**1 token ≈ 4 characters** (roughly 0.75 words). A 1,000-word document ≈ 1,333 tokens.

For data pipelines processing millions of documents, token costs add up fast. Always estimate token volume before choosing a model tier.

---

## Calling Bedrock: The InvokeModel API

The core Bedrock API is `InvokeModel`. The request/response format is model-specific (each model has its own JSON schema).

### Python with boto3

```python
import boto3
import json

bedrock = boto3.client("bedrock-runtime", region_name="us-east-1")

# Call Claude 3 Haiku
response = bedrock.invoke_model(
    modelId="anthropic.claude-3-haiku-20240307-v1:0",
    body=json.dumps({
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 1024,
        "messages": [
            {
                "role": "user",
                "content": "Classify this customer feedback as positive, negative, or neutral: 'Delivery was late but product quality was excellent.'"
            }
        ]
    }),
    contentType="application/json",
    accept="application/json"
)

result = json.loads(response["body"].read())
print(result["content"][0]["text"])
# Output: "Mixed — negative on delivery, positive on product quality"
```

### Calling the Embeddings Model

```python
response = bedrock.invoke_model(
    modelId="amazon.titan-embed-text-v2:0",
    body=json.dumps({
        "inputText": "customer churn prediction feature engineering",
        "dimensions": 512,        # reduce from 1536 for cost savings
        "normalize": True
    }),
    contentType="application/json",
    accept="application/json"
)

embedding = json.loads(response["body"].read())["embedding"]
print(len(embedding))  # 512
```

### Converse API (Unified Multi-Model Interface)

Bedrock's newer `converse` API normalizes the request format across all models — you don't need model-specific JSON schemas:

```python
response = bedrock.converse(
    modelId="anthropic.claude-3-haiku-20240307-v1:0",
    messages=[
        {"role": "user", "content": [{"text": "Summarize this data quality report in 3 bullet points."}]}
    ],
    inferenceConfig={"maxTokens": 512, "temperature": 0.1}
)

print(response["output"]["message"]["content"][0]["text"])
```

---

## IAM Permissions for Bedrock

Bedrock access requires explicit IAM permissions:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "bedrock:InvokeModel",
        "bedrock:InvokeModelWithResponseStream"
      ],
      "Resource": [
        "arn:aws:bedrock:us-east-1::foundation-model/anthropic.claude-3-haiku-20240307-v1:0"
      ]
    }
  ]
}
```

Model access must also be explicitly enabled per model in the Bedrock console (Model Access page) — models are not available by default.

---

## Key Takeaways

- Bedrock provides managed access to Claude, Titan, Llama, Mistral, and other models through a single AWS API
- No server management — pay per token consumed
- Two pricing modes: on-demand (per token, shared capacity) and provisioned throughput (guaranteed capacity)
- The `converse` API provides a unified interface across all models, avoiding model-specific JSON schemas
- IAM controls access at the model level; model access must be explicitly enabled in the console
- Titan Embeddings is the critical model for DE use cases involving semantic search and RAG over data lakes
