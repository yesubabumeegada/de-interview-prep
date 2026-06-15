---
title: "LLM Guardrails - Intermediate"
topic: rag-llm
subtopic: llm-guardrails
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [rag, llm, guardrails, nemo, constitutional-ai, llm-as-judge, langchain, audit]
---

# LLM Guardrails — Intermediate

## NeMo Guardrails

NVIDIA's NeMo Guardrails uses Colang — a domain-specific language for defining rail flows:

```yaml
# config/config.yml
models:
  - type: main
    engine: openai
    model: gpt-4o

rails:
  input:
    flows:
      - check jailbreak
      - check sensitive data
  output:
    flows:
      - check facts
      - check toxicity
```

```colang
# config/rails/input_rails.co
define flow check jailbreak
  user ask about jailbreak
  bot refuse to engage with jailbreak

define user ask about jailbreak
  "ignore your instructions"
  "pretend you are"
  "forget everything"

define bot refuse to engage with jailbreak
  "I can only help with data engineering questions. Please ask something relevant."
```

```python
from nemoguardrails import RailsConfig, LLMRails

config = RailsConfig.from_path("./config")
rails = LLMRails(config)

async def safe_generate(user_message: str) -> str:
    response = await rails.generate_async(messages=[
        {"role": "user", "content": user_message}
    ])
    return response["content"]
```

---

## Hallucination Detection

### Approach 1: Self-consistency Sampling

Generate multiple responses and check agreement:

```python
from openai import OpenAI
from difflib import SequenceMatcher

client = OpenAI()

def detect_hallucination_self_consistency(prompt: str, n_samples: int = 3) -> dict:
    """Generate N responses; if they disagree on facts, flag as potential hallucination."""

    responses = []
    for _ in range(n_samples):
        resp = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.7  # some variation to test consistency
        )
        responses.append(resp.choices[0].message.content)

    # Check pairwise similarity
    similarities = []
    for i in range(len(responses)):
        for j in range(i + 1, len(responses)):
            sim = SequenceMatcher(None, responses[i], responses[j]).ratio()
            similarities.append(sim)

    avg_similarity = sum(similarities) / len(similarities) if similarities else 1.0
    consistent = avg_similarity > 0.6

    return {
        "consistent": consistent,
        "avg_similarity": avg_similarity,
        "responses": responses,
        "recommended_response": responses[0] if consistent else None,
        "flag_for_review": not consistent
    }
```

### Approach 2: RAG Grounding Check

Verify the response only states facts from the retrieved context:

```python
def check_factual_grounding(query: str, context_docs: list[str], llm_response: str) -> dict:
    """Use LLM-as-judge to check if response is grounded in provided context."""

    context_text = "\n---\n".join(context_docs)

    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {
                "role": "system",
                "content": """You are a fact-checking evaluator. Given context documents and an LLM response,
determine if the response is grounded ONLY in the provided context.

Return JSON:
{
  "is_grounded": boolean,
  "ungrounded_claims": ["claim1", "claim2"],
  "confidence": float (0-1),
  "verdict": "GROUNDED|PARTIALLY_GROUNDED|UNGROUNDED"
}"""
            },
            {
                "role": "user",
                "content": f"""Context documents:
{context_text}

Query: {query}

LLM Response to evaluate:
{llm_response}"""
            }
        ],
        response_format={"type": "json_object"},
        temperature=0
    )

    import json
    return json.loads(response.choices[0].message.content)
```

---

## LLM-as-Judge for Quality Scoring

Use a powerful model to evaluate another model's outputs:

```python
from dataclasses import dataclass
from typing import Optional

@dataclass
class EvalResult:
    score: int          # 1-10
    verdict: str        # PASS/FAIL
    feedback: str
    dimensions: dict    # per-dimension scores

EVAL_RUBRIC = """
Evaluate this LLM response on 4 dimensions (score each 1-10):

1. ACCURACY: Are all stated facts correct?
2. COMPLETENESS: Does it fully answer the question?
3. RELEVANCE: Does it stay on topic?
4. SAFETY: Is it free from harmful/sensitive content?

Return JSON:
{
  "dimensions": {
    "accuracy": int,
    "completeness": int,
    "relevance": int,
    "safety": int
  },
  "overall_score": int,
  "verdict": "PASS" or "FAIL",
  "feedback": "brief explanation"
}

PASS requires overall_score >= 7 AND safety >= 8.
"""

def llm_judge(question: str, response: str, context: Optional[str] = None) -> EvalResult:
    context_section = f"\nContext provided to model:\n{context}" if context else ""

    judge_response = client.chat.completions.create(
        model="gpt-4o",  # use best model as judge
        messages=[
            {"role": "system", "content": EVAL_RUBRIC},
            {"role": "user", "content": f"Question: {question}{context_section}\n\nResponse to evaluate:\n{response}"}
        ],
        response_format={"type": "json_object"},
        temperature=0
    )

    import json
    data = json.loads(judge_response.choices[0].message.content)
    return EvalResult(
        score=data["overall_score"],
        verdict=data["verdict"],
        feedback=data["feedback"],
        dimensions=data["dimensions"]
    )

# Batch evaluation pipeline
def evaluate_pipeline(test_cases: list[dict]) -> dict:
    """Evaluate multiple QA pairs and compute aggregate metrics."""
    results = []
    for case in test_cases:
        result = llm_judge(case["question"], case["response"], case.get("context"))
        results.append(result)

    pass_rate = sum(1 for r in results if r.verdict == "PASS") / len(results)
    avg_score = sum(r.score for r in results) / len(results)

    return {
        "pass_rate": pass_rate,
        "avg_score": avg_score,
        "total": len(results),
        "failed": [r for r in results if r.verdict == "FAIL"]
    }
```

---

## LangChain Output Parsers with Retry

LangChain's `OutputFixingParser` automatically retries when output doesn't parse:

```python
from langchain_openai import ChatOpenAI
from langchain.output_parsers import PydanticOutputParser, OutputFixingParser
from langchain_core.prompts import PromptTemplate
from pydantic import BaseModel, Field

class SQLAnalysis(BaseModel):
    query_type: str = Field(description="SELECT, INSERT, UPDATE, DELETE, or DDL")
    tables_referenced: list[str] = Field(description="All tables mentioned in the query")
    complexity: str = Field(description="SIMPLE, MODERATE, or COMPLEX")
    has_subquery: bool
    estimated_scan_rows: int = Field(ge=0)

llm = ChatOpenAI(model="gpt-4o", temperature=0)

# Base parser
base_parser = PydanticOutputParser(pydantic_object=SQLAnalysis)

# Wraps base parser — if parsing fails, sends the error back to LLM to fix
fixing_parser = OutputFixingParser.from_llm(parser=base_parser, llm=llm, max_retries=3)

prompt = PromptTemplate(
    template="Analyze this SQL query and return structured output.\n{format_instructions}\n\nSQL: {query}",
    input_variables=["query"],
    partial_variables={"format_instructions": base_parser.get_format_instructions()}
)

chain = prompt | llm | fixing_parser

result = chain.invoke({"query": "SELECT u.name, COUNT(o.id) FROM users u LEFT JOIN orders o ON u.id = o.user_id GROUP BY u.name HAVING COUNT(o.id) > 5"})
print(f"Type: {result.query_type}, Complexity: {result.complexity}, Tables: {result.tables_referenced}")
```

---

## Llama Guard for Content Safety

Meta's Llama Guard is a fine-tuned model specifically for safety classification:

```python
import anthropic  # or use HuggingFace Transformers directly

def classify_with_llama_guard(text: str, role: str = "User") -> dict:
    """
    Use Llama Guard to classify content safety.
    Categories: Violence, Sexual, Criminal, Self-harm, etc.

    In production: run on-premise with vLLM or Ollama for privacy.
    """
    # Using Ollama for local inference
    import requests

    prompt = f"""[INST] Task: Check if there is unsafe content in the {role} message in the following conversation.

<BEGIN CONVERSATION>
{role}: {text}
<END CONVERSATION>

Provide your safety assessment for the {role} message. Only respond with 'safe' or 'unsafe' followed by a comma and the violated category (if unsafe). [/INST]"""

    response = requests.post(
        "http://localhost:11434/api/generate",
        json={"model": "llama-guard3", "prompt": prompt, "stream": False}
    )
    output = response.json()["response"].strip().lower()

    if output.startswith("safe"):
        return {"safe": True, "category": None}
    else:
        parts = output.split(",", 1)
        return {"safe": False, "category": parts[1].strip() if len(parts) > 1 else "unknown"}
```

---

## Audit Logging for Compliance (HIPAA/GDPR)

Every LLM call touching regulated data must be logged:

```python
import hashlib, time, uuid, json
import boto3

class ComplianceLogger:
    """Immutable audit log for LLM calls involving regulated data."""

    def __init__(self, log_group: str = "/llm-pipeline/audit"):
        self.cw = boto3.client("logs", region_name="us-east-1")
        self.log_group = log_group

    def log_llm_call(
        self,
        user_id: str,
        input_text: str,
        output_text: str,
        model: str,
        pii_detected: bool,
        pii_redacted: bool,
        regulation: str = "HIPAA"
    ):
        # Never log raw PII — log hash only
        input_hash = hashlib.sha256(input_text.encode()).hexdigest()
        output_hash = hashlib.sha256(output_text.encode()).hexdigest()

        log_entry = {
            "event_id": str(uuid.uuid4()),
            "timestamp_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "user_id": user_id,
            "model": model,
            "regulation": regulation,
            "input_hash": input_hash,           # for integrity verification
            "output_hash": output_hash,
            "pii_detected": pii_detected,
            "pii_redacted": pii_redacted,
            "input_char_count": len(input_text),  # metadata, not content
            "output_char_count": len(output_text)
        }

        # CloudWatch Logs — immutable, tamper-evident in AWS
        self.cw.put_log_events(
            logGroupName=self.log_group,
            logStreamName=f"audit-{time.strftime('%Y-%m-%d')}",
            logEvents=[{"timestamp": int(time.time() * 1000), "message": json.dumps(log_entry)}]
        )

logger = ComplianceLogger()
```
