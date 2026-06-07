---
title: "Fine-Tuning - Real-World Production Examples"
topic: rag-llm
subtopic: fine-tuning
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [rag, llm, fine-tuning, production, pipelines, deployment, sql-generation]
---

# Fine-Tuning LLMs — Real-World Production Examples

## Pattern 1: Fine-Tuning for Internal SQL Generation

Train a model to generate SQL queries specific to your data warehouse schema:

```python
import json
from openai import OpenAI

client = OpenAI()

# Step 1: Collect training data from query logs
def extract_training_pairs_from_logs(query_log_path: str) -> list[dict]:
    """Extract (natural_language_question, SQL) pairs from analyst query logs."""
    pairs = []
    
    # Source 1: Questions from Slack #data-questions + the SQL answer
    # Source 2: dbt model descriptions + their SQL
    # Source 3: Manually annotated examples from data team
    
    with open(query_log_path) as f:
        for entry in json.load(f):
            if entry.get("verified"):  # Only use verified correct queries
                pairs.append({
                    "messages": [
                        {"role": "system", "content": f"Generate SQL for our warehouse. Schema:\n{entry['relevant_schema']}"},
                        {"role": "user", "content": entry["natural_language_question"]},
                        {"role": "assistant", "content": entry["sql_query"]},
                    ]
                })
    
    return pairs

# Step 2: Augment with schema context
SCHEMA_CONTEXT = """
Tables:
- fact_orders (order_id, customer_id, product_id, amount, order_date, status)
- dim_customers (customer_id, name, segment, signup_date, country)
- dim_products (product_id, name, category, price, supplier_id)
- fact_events (event_id, user_id, event_type, event_timestamp, properties)
"""

# Step 3: Create high-quality training examples
training_data = [
    {"messages": [
        {"role": "system", "content": f"Generate SQL for our data warehouse.\n{SCHEMA_CONTEXT}"},
        {"role": "user", "content": "How many orders did we get last month?"},
        {"role": "assistant", "content": "SELECT COUNT(*) as order_count\nFROM fact_orders\nWHERE order_date >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')\n  AND order_date < DATE_TRUNC('month', CURRENT_DATE);"}
    ]},
    {"messages": [
        {"role": "system", "content": f"Generate SQL for our data warehouse.\n{SCHEMA_CONTEXT}"},
        {"role": "user", "content": "Top 10 customers by revenue this quarter"},
        {"role": "assistant", "content": "SELECT c.name, c.segment,\n       SUM(o.amount) as total_revenue,\n       COUNT(o.order_id) as order_count\nFROM fact_orders o\nJOIN dim_customers c ON o.customer_id = c.customer_id\nWHERE o.order_date >= DATE_TRUNC('quarter', CURRENT_DATE)\nGROUP BY c.name, c.segment\nORDER BY total_revenue DESC\nLIMIT 10;"}
    ]},
    # ... 300+ examples covering all common query patterns
]

# Step 4: Fine-tune
with open("sql_training.jsonl", "w") as f:
    for ex in training_data:
        f.write(json.dumps(ex) + "\n")

file = client.files.create(file=open("sql_training.jsonl", "rb"), purpose="fine-tune")
job = client.fine_tuning.jobs.create(
    training_file=file.id,
    model="gpt-4o-mini-2024-07-18",
    hyperparameters={"n_epochs": 3},
    suffix="sql-gen-v1",
)

# Step 5: Evaluate on held-out test queries
def evaluate_sql_model(model_name: str, test_cases: list[dict]) -> dict:
    correct = 0
    executable = 0
    
    for case in test_cases:
        response = client.chat.completions.create(
            model=model_name,
            messages=case["messages"][:-1],  # Without the assistant answer
            temperature=0,
        )
        generated_sql = response.choices[0].message.content
        
        # Check if SQL is executable
        if validate_sql_syntax(generated_sql):
            executable += 1
        
        # Check if results match expected
        if execute_and_compare(generated_sql, case["expected_sql"]):
            correct += 1
    
    return {"accuracy": correct / len(test_cases), "executable_rate": executable / len(test_cases)}
```

---

## Pattern 2: Fine-Tuning for PII Detection

```python
# Train a model to detect and classify PII in data pipeline outputs

# Training data from annotated samples:
pii_training = [
    {"messages": [
        {"role": "system", "content": "Detect PII in text. Return JSON with entities found."},
        {"role": "user", "content": "Customer John Smith (john.smith@gmail.com) called from 555-0123 about order #45123"},
        {"role": "assistant", "content": json.dumps({
            "pii_detected": True,
            "entities": [
                {"type": "PERSON_NAME", "value": "John Smith", "position": [9, 19]},
                {"type": "EMAIL", "value": "john.smith@gmail.com", "position": [21, 41]},
                {"type": "PHONE", "value": "555-0123", "position": [55, 63]},
            ],
            "risk_level": "high",
            "recommendation": "mask_all"
        })}
    ]},
    {"messages": [
        {"role": "system", "content": "Detect PII in text. Return JSON with entities found."},
        {"role": "user", "content": "The ETL job processed 1.2M records from the orders table in us-east-1"},
        {"role": "assistant", "content": json.dumps({
            "pii_detected": False,
            "entities": [],
            "risk_level": "none",
            "recommendation": "no_action"
        })}
    ]},
    # 500+ annotated examples covering all PII types
]

# After fine-tuning: model consistently detects PII without regex rules
# Handles edge cases regex misses: "my SSN is four five six seven eight nine oh one two"
```

---

## Pattern 3: Continuous Training Pipeline

```python
from datetime import datetime, timedelta
from dataclasses import dataclass

@dataclass
class TrainingRun:
    version: str
    model_name: str
    dataset_size: int
    eval_accuracy: float
    deployed: bool
    timestamp: datetime

class ProductionFineTuningPipeline:
    """End-to-end pipeline: data collection → training → evaluation → deployment."""
    
    def __init__(self, config):
        self.config = config
        self.client = OpenAI()
        self.training_history: list[TrainingRun] = []
    
    def weekly_training_cycle(self):
        """Triggered by Airflow every Monday."""
        
        # Step 1: Collect new training data
        new_examples = self.collect_new_examples()
        print(f"Collected {len(new_examples)} new examples from feedback")
        
        if len(new_examples) < 20:
            print("Not enough new data. Skipping training cycle.")
            return
        
        # Step 2: Merge with existing dataset (keep last 3000 examples)
        full_dataset = self.merge_and_deduplicate(new_examples)
        
        # Step 3: Quality checks
        quality = self.validate_dataset(full_dataset)
        if quality["issues"] > len(full_dataset) * 0.05:
            self.alert("Dataset has >5% quality issues. Manual review needed.")
            return
        
        # Step 4: Train new version
        version = f"v{datetime.now().strftime('%Y%m%d')}"
        model_name = self.train(full_dataset, version)
        
        # Step 5: Evaluate against current production
        new_score = self.evaluate(model_name)
        current_score = self.evaluate(self.config.current_model)
        
        print(f"New: {new_score:.3f} | Current: {current_score:.3f}")
        
        # Step 6: Deploy if improvement is significant
        if new_score > current_score + 0.02:
            self.deploy_canary(model_name, traffic_pct=0.1)
            self.training_history.append(TrainingRun(
                version=version, model_name=model_name,
                dataset_size=len(full_dataset), eval_accuracy=new_score,
                deployed=True, timestamp=datetime.now()
            ))
        else:
            print(f"New model not significantly better. Keeping current.")
    
    def collect_new_examples(self) -> list[dict]:
        """Collect training data from multiple sources."""
        examples = []
        
        # Source 1: User corrections (highest quality)
        corrections = self.db.query("""
            SELECT input, corrected_output 
            FROM user_corrections 
            WHERE created_at > :since AND confidence > 0.9
        """, {"since": datetime.now() - timedelta(days=7)})
        examples.extend(self.format_corrections(corrections))
        
        # Source 2: Highly-rated responses (thumbs up)
        good_responses = self.db.query("""
            SELECT input, output 
            FROM interactions 
            WHERE rating = 'positive' AND created_at > :since
        """, {"since": datetime.now() - timedelta(days=7)})
        examples.extend(self.format_positive(good_responses))
        
        return examples
    
    def deploy_canary(self, model_name: str, traffic_pct: float):
        """Gradually roll out new model."""
        self.config.set("canary_model", model_name)
        self.config.set("canary_traffic_pct", traffic_pct)
        
        # After 24h: check canary metrics
        # If positive: increase to 50%, then 100%
        # If negative: rollback to 0%
    
    def rollback(self):
        """Immediate rollback to previous model."""
        self.config.set("canary_traffic_pct", 0)
        self.alert("Model rolled back due to quality degradation.")
```

---

## Pattern 4: Cost Analysis — API vs Self-Hosted

```python
def compare_training_costs(
    num_examples: int,
    inference_queries_per_month: int,
    model_size: str = "7B",
) -> dict:
    """Compare OpenAI fine-tuning vs self-hosted LoRA."""
    
    # OpenAI fine-tuning
    openai_training_cost = num_examples * 200 * 3 * 0.000003  # tokens * epochs * $/token
    openai_inference_cost = inference_queries_per_month * 500 * 0.0000006  # tokens * $/token (2× base)
    openai_monthly = openai_inference_cost
    openai_total_year = openai_training_cost + openai_monthly * 12
    
    # Self-hosted (LoRA on AWS)
    gpu_hourly = 1.006  # g5.xlarge (A10G)
    training_hours = max(1, num_examples * 3 / 5000)  # ~5000 examples/hour
    self_hosted_training = training_hours * gpu_hourly
    
    # Inference: always-on GPU for serving
    self_hosted_monthly = gpu_hourly * 730  # 24/7
    self_hosted_total_year = self_hosted_training + self_hosted_monthly * 12
    
    # Break-even
    if openai_monthly > 0 and self_hosted_monthly > 0:
        breakeven_queries = self_hosted_monthly / (openai_inference_cost / inference_queries_per_month)
    else:
        breakeven_queries = 0
    
    return {
        "openai": {
            "training_cost": f"${openai_training_cost:.2f}",
            "monthly_inference": f"${openai_monthly:.2f}",
            "annual_total": f"${openai_total_year:.2f}",
        },
        "self_hosted": {
            "training_cost": f"${self_hosted_training:.2f}",
            "monthly_inference": f"${self_hosted_monthly:.2f}",
            "annual_total": f"${self_hosted_total_year:.2f}",
        },
        "recommendation": "openai" if openai_total_year < self_hosted_total_year else "self_hosted",
        "breakeven_queries_month": int(breakeven_queries),
    }

# Examples:
# 500 examples, 10K queries/month → OpenAI: $39/yr, Self-hosted: $8,824/yr → USE OPENAI
# 500 examples, 500K queries/month → OpenAI: $1,800/yr, Self-hosted: $8,824/yr → USE OPENAI
# 500 examples, 5M queries/month → OpenAI: $18,000/yr, Self-hosted: $8,824/yr → SELF-HOST
```

---

## Pattern 5: Model Versioning and Rollback

```python
class ModelVersionManager:
    """Track fine-tuned model versions with eval metrics and rollback capability."""
    
    def __init__(self, registry_db):
        self.registry = registry_db
    
    def register(self, model_name: str, metadata: dict):
        """Register a new model version."""
        self.registry.insert({
            "model_name": model_name,
            "version": metadata["version"],
            "training_date": datetime.now(),
            "dataset_size": metadata["dataset_size"],
            "eval_metrics": metadata["eval_metrics"],
            "status": "canary",  # canary → production → deprecated
        })
    
    def promote(self, model_name: str):
        """Promote canary to production."""
        # Deprecate current production
        self.registry.update(
            {"status": "production"}, 
            {"status": "deprecated", "deprecated_at": datetime.now()}
        )
        # Promote new model
        self.registry.update(
            {"model_name": model_name}, 
            {"status": "production", "promoted_at": datetime.now()}
        )
    
    def rollback(self):
        """Rollback to previous production model."""
        # Find the most recently deprecated model
        previous = self.registry.find_one(
            {"status": "deprecated"},
            sort=[("deprecated_at", -1)]
        )
        
        if previous:
            # Swap statuses
            current = self.registry.find_one({"status": "production"})
            self.registry.update({"model_name": current["model_name"]}, {"status": "failed"})
            self.registry.update({"model_name": previous["model_name"]}, {"status": "production"})
            return previous["model_name"]
        
        raise Exception("No previous model to rollback to")
    
    def get_production_model(self) -> str:
        """Get current production model name."""
        record = self.registry.find_one({"status": "production"})
        return record["model_name"] if record else self.default_model
```

---

## Interview Tips

> **Tip 1:** "How would you build a Text-to-SQL fine-tuning pipeline?" — Collect (question, SQL) pairs from: analyst query logs, dbt model docs, Slack data questions. Clean and verify each pair executes correctly. Include your actual schema in the system prompt. Fine-tune on 300-500 examples. Evaluate by running generated SQL and comparing results to expected outputs.

> **Tip 2:** "How do you handle model degradation over time?" — Monitor accuracy on a fixed eval set weekly. Collect new training data from user corrections and positive feedback. Retrain monthly with the full accumulated dataset. Use canary deployment (10% traffic) and auto-rollback if metrics drop. Never delete old training data.

> **Tip 3:** "OpenAI fine-tuning vs self-hosted?" — OpenAI: simple, cheap for <500K queries/month, no ops burden. Self-hosted: own the model, no per-query cost (good for >1M queries/month), full control over data privacy. Break-even is typically 500K-2M queries/month. Start with OpenAI, migrate to self-hosted when volume justifies it.
