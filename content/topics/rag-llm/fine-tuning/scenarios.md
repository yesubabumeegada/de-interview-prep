---
title: "Fine-Tuning - Scenario Questions"
topic: rag-llm
subtopic: fine-tuning
content_type: scenario_question
tags: [rag, llm, fine-tuning, interview, scenarios]
---

# Scenario Questions — Fine-Tuning

<article data-difficulty="junior">

## 🟢 Junior: When to Fine-Tune

**Scenario:** Your team has a RAG chatbot that answers well but inconsistently formats responses. Sometimes it returns JSON, sometimes prose, sometimes bullet points. The prompt says "return JSON" but it doesn't always comply. Should you fine-tune, or fix the prompt?

<details>
<summary>💡 Hint</summary>
Format consistency is one of the top use cases for fine-tuning. But first check if simpler solutions (response_format parameter, stricter prompt, few-shot examples) solve it.
</details>

<details>
<summary>✅ Solution</summary>

**Decision process:**

```python
# Step 1: Try response_format parameter (free, instant)
response = client.chat.completions.create(
    model="gpt-4o-mini",
    messages=[...],
    response_format={"type": "json_object"},  # Forces JSON output
)
# If this works → DONE, no fine-tuning needed

# Step 2: If response_format doesn't support your exact schema, try instructor
import instructor
client = instructor.from_openai(OpenAI())
response = client.chat.completions.create(
    model="gpt-4o-mini",
    response_model=YourPydanticSchema,
    messages=[...],
)
# If this works → DONE, no fine-tuning needed

# Step 3: If above approaches fail or are too slow (instructor retries), THEN fine-tune
# Fine-tuning teaches the model your exact output format natively
# After fine-tuning: model produces correct JSON first-try, no retries needed
```

**When fine-tuning IS the right answer:**
- You've tried `response_format` and instructor but still get 5-10% format failures
- Latency matters: fine-tuned model doesn't need retry logic (~30% faster)
- You need a very specific, complex schema that's hard to describe in a prompt
- Volume is high: 100K+ queries/day where even 5% failures = 5K bad responses

**Key Points:**
- Always try the cheaper solution first (prompt → response_format → instructor → fine-tune)
- Fine-tuning for format: only 50-100 examples needed
- After fine-tuning: format compliance typically goes from 90% to 99%+

</details>

</article>

<article data-difficulty="junior">

## 🟢 Junior: Preparing Training Data

**Scenario:** You want to fine-tune a model to classify data pipeline errors. You have 10,000 historical alerts with labels. How do you prepare this data for OpenAI fine-tuning?

<details>
<summary>💡 Hint</summary>
Convert to JSONL format with messages array (system, user, assistant). Split into train/validation. Check for quality issues.
</details>

<details>
<summary>✅ Solution</summary>

```python
import json
import random
from collections import Counter

# Raw data: list of (alert_text, category) tuples
raw_data = [
    ("ERROR: OOM in executor 5, heap 8GB exceeded", "memory"),
    ("WARN: Kafka consumer lag 5M on topic orders", "backpressure"),
    ("ERROR: Connection refused to postgres-prod:5432", "connectivity"),
    # ... 10,000 entries
]

# Step 1: Format for OpenAI
def format_for_finetuning(raw_data: list[tuple]) -> list[dict]:
    formatted = []
    for alert_text, category in raw_data:
        formatted.append({
            "messages": [
                {"role": "system", "content": "Classify data pipeline alerts into categories: memory, backpressure, connectivity, timeout, data_quality, permission, configuration, unknown"},
                {"role": "user", "content": f"Classify: {alert_text}"},
                {"role": "assistant", "content": category}
            ]
        })
    return formatted

# Step 2: Quality checks
def validate_data(data: list[dict]) -> dict:
    categories = [d["messages"][-1]["content"] for d in data]
    distribution = Counter(categories)
    
    issues = []
    # Check class balance
    min_count = min(distribution.values())
    max_count = max(distribution.values())
    if max_count > min_count * 10:
        issues.append(f"Severe class imbalance: {distribution}")
    
    # Check for duplicates
    texts = [d["messages"][1]["content"] for d in data]
    unique = len(set(texts))
    if unique < len(texts) * 0.95:
        issues.append(f"Duplicates: {len(texts) - unique} duplicate inputs")
    
    return {"total": len(data), "distribution": distribution, "issues": issues}

# Step 3: Split and save
all_data = format_for_finetuning(raw_data)
random.shuffle(all_data)

train_data = all_data[:8000]  # 80%
val_data = all_data[8000:]     # 20%

with open("train.jsonl", "w") as f:
    for item in train_data:
        f.write(json.dumps(item) + "\n")

with open("val.jsonl", "w") as f:
    for item in val_data:
        f.write(json.dumps(item) + "\n")

print(validate_data(all_data))
```

**Key Points:**
- JSONL format: one JSON object per line with messages array
- 80/20 train/val split (randomized to avoid temporal bias)
- Check class balance: severely imbalanced classes train poorly
- Remove duplicates: they bias the model toward over-represented examples
- System message should be consistent across all examples
- With 10K examples: use 2 epochs (more data = fewer epochs needed)

</details>

</article>

<article data-difficulty="junior">

## 🟢 Junior: Evaluating Fine-Tuning Results

**Scenario:** You fine-tuned a model for error classification and got training loss of 0.3 and validation loss of 0.35. The base model (without fine-tuning) gets 71% accuracy on your test set with few-shot prompting. How do you evaluate if fine-tuning was successful?

<details>
<summary>💡 Hint</summary>
Run the fine-tuned model on a held-out test set (separate from training AND validation). Compare accuracy, precision, recall, and format compliance against the base model.
</details>

<details>
<summary>✅ Solution</summary>

```python
from sklearn.metrics import classification_report
import json

def evaluate_model(model_name: str, test_cases: list[dict]) -> dict:
    """Evaluate model on held-out test set."""
    predictions = []
    ground_truth = []
    
    for case in test_cases:
        # Get model prediction
        response = client.chat.completions.create(
            model=model_name,
            messages=case["messages"][:-1],  # Exclude the answer
            temperature=0,
            max_tokens=20,
        )
        pred = response.choices[0].message.content.strip()
        expected = case["messages"][-1]["content"]
        
        predictions.append(pred)
        ground_truth.append(expected)
    
    # Compute metrics
    accuracy = sum(p == g for p, g in zip(predictions, ground_truth)) / len(test_cases)
    
    # Detailed classification report
    report = classification_report(ground_truth, predictions, output_dict=True)
    
    return {
        "accuracy": accuracy,
        "per_class": report,
        "invalid_outputs": sum(1 for p in predictions if p not in VALID_CATEGORIES),
    }

# Compare base vs fine-tuned
base_results = evaluate_model("gpt-4o-mini", test_cases)      # With few-shot prompt
ft_results = evaluate_model("ft:gpt-4o-mini:...", test_cases)  # Fine-tuned

print(f"Base model accuracy: {base_results['accuracy']:.1%}")   # 71%
print(f"Fine-tuned accuracy: {ft_results['accuracy']:.1%}")     # Expected: 88-95%
print(f"Invalid outputs (base): {base_results['invalid_outputs']}")  # Maybe 5-10
print(f"Invalid outputs (FT): {ft_results['invalid_outputs']}")      # Should be 0
```

**Key Points:**
- Test set must be separate from both training AND validation data
- Compare multiple metrics: accuracy, per-class F1, format compliance
- Fine-tuning typically improves classification accuracy by 15-25%
- Training loss 0.3, val loss 0.35: healthy (small gap = no severe overfitting)
- If val loss >> train loss (e.g., 0.3 vs 0.8): overfitting — reduce epochs
- Fine-tuned model should produce zero invalid/off-format outputs

</details>

</article>

<article data-difficulty="junior">

## 🟢 Junior: Cost Estimation

**Scenario:** Your manager asks: "How much will it cost to fine-tune GPT-4o-mini on 500 examples and serve 50K queries/month?" Provide a cost breakdown.

<details>
<summary>💡 Hint</summary>
Training cost = tokens × epochs × per-token-training-rate. Inference cost = queries × tokens × per-token-inference-rate (fine-tuned models cost 2× base).
</details>

<details>
<summary>✅ Solution</summary>

```python
# COST CALCULATION:

# Training (one-time):
examples = 500
avg_tokens_per_example = 200  # system + user + assistant messages
epochs = 3
training_tokens = examples * avg_tokens_per_example * epochs  # = 300,000 tokens
training_cost_per_1M = 3.00  # GPT-4o-mini fine-tuning: $3/1M training tokens
training_cost = (training_tokens / 1_000_000) * training_cost_per_1M
print(f"Training cost: ${training_cost:.2f}")  # $0.90

# Inference (monthly):
queries_per_month = 50_000
avg_input_tokens = 100   # query + system prompt
avg_output_tokens = 50   # classification label
input_cost_per_1M = 0.30   # Fine-tuned gpt-4o-mini input (base is $0.15)
output_cost_per_1M = 1.20  # Fine-tuned gpt-4o-mini output (base is $0.60)

monthly_input_cost = (queries_per_month * avg_input_tokens / 1_000_000) * input_cost_per_1M
monthly_output_cost = (queries_per_month * avg_output_tokens / 1_000_000) * output_cost_per_1M
monthly_inference_cost = monthly_input_cost + monthly_output_cost
print(f"Monthly inference: ${monthly_inference_cost:.2f}")  # ~$4.50

# TOTAL:
# One-time training: $0.90
# Monthly inference: $4.50
# Annual total: $0.90 + ($4.50 × 12) = $54.90

# Compare to base model with few-shot prompt (longer input):
# Base with 5 examples in prompt: 500 tokens input, 50 output
# Monthly: (50K × 500 / 1M × $0.15) + (50K × 50 / 1M × $0.60) = $5.25
# Fine-tuned SAVES money by eliminating few-shot examples from every prompt!
```

**Key Points:**
- Fine-tuning GPT-4o-mini is remarkably cheap: $1-5 for 500 examples
- Inference is 2× base price but prompts are shorter (no few-shot examples)
- Net effect: often cheaper than base + few-shot for high-volume use cases
- The expensive part isn't compute — it's the human time to create training data
- At 50K queries/month: fine-tuning pays for itself in week 1

</details>

</article>

<article data-difficulty="junior">

## 🟢 Junior: Basic OpenAI Fine-Tuning

**Scenario:** Walk through the complete process of fine-tuning GPT-4o-mini for a simple task: converting natural language date descriptions into ISO format dates. E.g., "last Tuesday" → "2024-03-05".

<details>
<summary>💡 Hint</summary>
Create JSONL training file → upload → create fine-tuning job → monitor → test the resulting model.
</details>

<details>
<summary>✅ Solution</summary>

```python
from openai import OpenAI
import json
import time

client = OpenAI()

# Step 1: Create training data
training_examples = [
    {"messages": [
        {"role": "system", "content": "Convert natural language dates to ISO format (YYYY-MM-DD). Today is 2024-03-08."},
        {"role": "user", "content": "last Tuesday"},
        {"role": "assistant", "content": "2024-03-05"}
    ]},
    {"messages": [
        {"role": "system", "content": "Convert natural language dates to ISO format (YYYY-MM-DD). Today is 2024-03-08."},
        {"role": "user", "content": "two weeks ago"},
        {"role": "assistant", "content": "2024-02-23"}
    ]},
    {"messages": [
        {"role": "system", "content": "Convert natural language dates to ISO format (YYYY-MM-DD). Today is 2024-03-08."},
        {"role": "user", "content": "next Friday"},
        {"role": "assistant", "content": "2024-03-15"}
    ]},
    # ... 100+ examples covering: relative dates, absolute dates, holidays, quarters, etc.
]

# Step 2: Save as JSONL
with open("dates_training.jsonl", "w") as f:
    for ex in training_examples:
        f.write(json.dumps(ex) + "\n")

# Step 3: Upload file
file = client.files.create(file=open("dates_training.jsonl", "rb"), purpose="fine-tune")
print(f"File uploaded: {file.id}")

# Step 4: Start fine-tuning
job = client.fine_tuning.jobs.create(
    training_file=file.id,
    model="gpt-4o-mini-2024-07-18",
    suffix="date-parser",
)
print(f"Job started: {job.id}")

# Step 5: Wait for completion
while True:
    job = client.fine_tuning.jobs.retrieve(job.id)
    print(f"Status: {job.status}")
    if job.status in ["succeeded", "failed", "cancelled"]:
        break
    time.sleep(30)

# Step 6: Use the fine-tuned model
if job.status == "succeeded":
    ft_model = job.fine_tuned_model
    
    response = client.chat.completions.create(
        model=ft_model,
        messages=[
            {"role": "system", "content": "Convert natural language dates to ISO format. Today is 2024-03-08."},
            {"role": "user", "content": "the Monday before last"}
        ],
        temperature=0,
    )
    print(response.choices[0].message.content)  # "2024-02-26"
```

**Key Points:**
- Entire process: ~10 minutes (5 min prep, 5 min training for 100 examples)
- Cost: ~$0.10 for 100 examples
- After fine-tuning: model consistently returns ISO dates without format issues
- The system prompt with "today's date" allows the model to handle relative dates
- Include edge cases in training: "end of quarter", "next business day", "fiscal year start"

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: LoRA Setup

**Scenario:** You need to fine-tune Llama 3.1 8B for internal code review comments generation. You have one A10G GPU (24GB). Set up LoRA fine-tuning with appropriate parameters.

<details>
<summary>💡 Hint</summary>
8B model needs ~16GB in float16, leaving insufficient memory for full fine-tuning on 24GB. Use LoRA (or QLoRA) to reduce trainable parameters and memory footprint.
</details>

<details>
<summary>✅ Solution</summary>

```python
from transformers import AutoModelForCausalLM, AutoTokenizer, TrainingArguments
from peft import LoraConfig, get_peft_model, TaskType
from trl import SFTTrainer
from datasets import load_dataset
import torch

# Load model (float16 fits in 24GB A10G)
model = AutoModelForCausalLM.from_pretrained(
    "meta-llama/Llama-3.1-8B-Instruct",
    torch_dtype=torch.float16,
    device_map="auto",
    attn_implementation="flash_attention_2",  # Faster + less memory
)

tokenizer = AutoTokenizer.from_pretrained("meta-llama/Llama-3.1-8B-Instruct")
tokenizer.pad_token = tokenizer.eos_token

# LoRA config — fits in remaining GPU memory
lora_config = LoraConfig(
    task_type=TaskType.CAUSAL_LM,
    r=16,                          # Rank 16: good balance of quality vs memory
    lora_alpha=32,                 # Alpha = 2 × r is common
    lora_dropout=0.05,             # Light regularization
    target_modules=[
        "q_proj", "k_proj", "v_proj", "o_proj",  # Attention layers
        "gate_proj", "up_proj", "down_proj",       # MLP layers
    ],
    bias="none",                   # Don't train bias terms (saves memory)
)

model = get_peft_model(model, lora_config)
model.print_trainable_parameters()
# trainable params: 13,107,200 (0.16%) || all params: 8,030,261,248

# Training arguments optimized for 24GB GPU
training_args = TrainingArguments(
    output_dir="./code-review-lora",
    num_train_epochs=3,
    per_device_train_batch_size=2,    # Small batch for memory
    gradient_accumulation_steps=8,    # Effective batch size: 16
    learning_rate=2e-4,
    warmup_ratio=0.1,
    fp16=True,                        # Half precision
    logging_steps=10,
    save_strategy="steps",
    save_steps=200,
    eval_strategy="steps",
    eval_steps=200,
    gradient_checkpointing=True,      # Trade compute for memory
    optim="adamw_torch_fused",        # Memory-efficient optimizer
    max_grad_norm=1.0,
)

# Dataset
dataset = load_dataset("json", data_files={"train": "train.jsonl", "test": "val.jsonl"})

trainer = SFTTrainer(
    model=model,
    train_dataset=dataset["train"],
    eval_dataset=dataset["test"],
    args=training_args,
    dataset_text_field="text",
    max_seq_length=2048,
)

trainer.train()
model.save_pretrained("./code-review-lora-adapter")
```

**Key Points:**
- 8B model in float16 = ~16GB; LoRA adds ~200MB; leaves ~8GB for activations
- gradient_checkpointing=True: trades 30% speed for 40% less memory
- Effective batch size = per_device_batch × gradient_accumulation = 2 × 8 = 16
- r=16 with all attention + MLP layers: good for moderate complexity tasks
- Training time: ~2 hours for 1000 examples on A10G
- If OOM: reduce per_device_batch_size to 1, increase gradient_accumulation to 16

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Dataset Curation from Production Logs

**Scenario:** You want to fine-tune a model for SQL generation. You have 100K historical queries from your data warehouse's query log. Most are messy (auto-generated, incomplete, or poorly written). Design a curation pipeline to extract 500 high-quality (question, SQL) training pairs.

<details>
<summary>💡 Hint</summary>
Filter for: human-written queries (not auto-generated), queries that completed successfully, moderate complexity, and diverse patterns. Use an LLM to generate natural language questions for the SQL.
</details>

<details>
<summary>✅ Solution</summary>

```python
import re
from collections import Counter

class SQLDatasetCurator:
    """Curate high-quality (NL question, SQL) pairs from query logs."""
    
    def curate(self, raw_queries: list[dict], target_size: int = 500) -> list[dict]:
        # Step 1: Filter out junk
        filtered = self.quality_filter(raw_queries)
        print(f"After quality filter: {len(filtered)} / {len(raw_queries)}")
        
        # Step 2: Deduplicate (similar queries)
        deduped = self.deduplicate(filtered)
        print(f"After dedup: {len(deduped)}")
        
        # Step 3: Ensure diversity (cover all patterns)
        diverse = self.ensure_diversity(deduped, target_size)
        print(f"After diversity selection: {len(diverse)}")
        
        # Step 4: Generate NL questions for each SQL
        pairs = self.generate_questions(diverse)
        
        # Step 5: Human review (sample 50 for spot-check)
        return pairs
    
    def quality_filter(self, queries: list[dict]) -> list[dict]:
        """Keep only high-quality human-written queries."""
        filtered = []
        for q in queries:
            sql = q["query_text"]
            
            # Skip auto-generated (ORM, BI tools)
            if "GENERATED" in sql or sql.count("_") > 20:
                continue
            
            # Skip too simple or too complex
            token_count = len(sql.split())
            if token_count < 10 or token_count > 500:
                continue
            
            # Must have completed successfully
            if q.get("status") != "SUCCESS":
                continue
            
            # Must touch real business tables (not system queries)
            if not any(t in sql.lower() for t in ["fact_", "dim_", "stg_"]):
                continue
            
            filtered.append(q)
        
        return filtered
    
    def ensure_diversity(self, queries: list[dict], target: int) -> list[dict]:
        """Ensure coverage of different SQL patterns."""
        # Categorize by pattern
        patterns = {"join": [], "aggregation": [], "window": [], "subquery": [], "simple": []}
        
        for q in queries:
            sql = q["query_text"].lower()
            if "over(" in sql or "window" in sql:
                patterns["window"].append(q)
            elif "join" in sql and "group by" in sql:
                patterns["join"].append(q)
            elif "group by" in sql:
                patterns["aggregation"].append(q)
            elif "select" in sql and "from" in sql and "where" in sql:
                patterns["subquery"].append(q) if "select" in sql[sql.index("from"):] else patterns["simple"].append(q)
            else:
                patterns["simple"].append(q)
        
        # Sample proportionally from each category
        selected = []
        per_category = target // len(patterns)
        for cat, qs in patterns.items():
            selected.extend(random.sample(qs, min(per_category, len(qs))))
        
        return selected[:target]
    
    def generate_questions(self, queries: list[dict]) -> list[dict]:
        """Use LLM to generate natural language questions for each SQL."""
        pairs = []
        for q in queries:
            response = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": f"What business question does this SQL answer? Write a natural, conversational question.\n\nSQL: {q['query_text']}\n\nQuestion:"}],
                temperature=0.3,
            )
            nl_question = response.choices[0].message.content.strip()
            pairs.append({
                "question": nl_question,
                "sql": q["query_text"],
                "tables_used": q.get("tables_accessed", []),
            })
        return pairs
```

**Key Points:**
- 100K raw → ~5K quality filtered → ~2K deduped → 500 diverse selected
- Quality filter removes: auto-generated, failed, too simple/complex queries
- Diversity ensures all SQL patterns are covered (not just SELECT * FROM)
- LLM-generated questions save weeks of manual annotation
- Always spot-check 50 pairs manually before training (catch LLM annotation errors)
- This pipeline is reusable: run monthly to collect fresh training data

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Handling Overfitting

**Scenario:** You fine-tuned on 200 examples for 5 epochs. Training loss dropped to 0.1 but validation loss is 0.8 (started at 1.2, dropped to 0.5 at epoch 2, then climbed). The model now memorizes training examples but performs poorly on new inputs. Fix it.

<details>
<summary>💡 Hint</summary>
Classic overfitting: model memorized training data. Solutions: reduce epochs, increase data, add regularization, or use early stopping.
</details>

<details>
<summary>✅ Solution</summary>

```python
# DIAGNOSIS:
# Epoch 1: train=0.8, val=0.9 (both learning)
# Epoch 2: train=0.5, val=0.5 (both improving - BEST CHECKPOINT)
# Epoch 3: train=0.3, val=0.6 (val starting to rise - overfitting begins)
# Epoch 4: train=0.2, val=0.7 (overfitting)
# Epoch 5: train=0.1, val=0.8 (severe overfitting)

# FIX 1: Reduce epochs (most effective)
job = client.fine_tuning.jobs.create(
    training_file=file.id,
    model="gpt-4o-mini-2024-07-18",
    hyperparameters={"n_epochs": 2},  # Stop at epoch 2 (best val loss)
)

# FIX 2: Get more training data
# 200 examples with 5 epochs = high overfitting risk
# 500+ examples with 2-3 epochs = much more robust
# Rule: examples × epochs should be 500-2000 total passes

# FIX 3: For Hugging Face LoRA — add regularization
lora_config = LoraConfig(
    r=8,                  # Lower rank = fewer parameters = less overfitting
    lora_dropout=0.1,     # Increased dropout (was 0.05)
    # ...
)

training_args = TrainingArguments(
    num_train_epochs=2,           # Fewer epochs
    weight_decay=0.01,            # L2 regularization
    load_best_model_at_end=True,  # Auto-select best checkpoint
    metric_for_best_model="eval_loss",
    greater_is_better=False,
    save_strategy="steps",
    eval_strategy="steps",
    eval_steps=50,
    save_steps=50,
)

# FIX 4: Data augmentation (if can't get more real data)
# Paraphrase existing inputs with an LLM to create more diverse examples
def augment_dataset(examples: list[dict], augmentation_factor: int = 3) -> list[dict]:
    augmented = list(examples)  # Keep originals
    for ex in examples:
        for _ in range(augmentation_factor - 1):
            # Paraphrase the input
            paraphrased = paraphrase(ex["messages"][1]["content"])
            new_ex = {
                "messages": [
                    ex["messages"][0],  # Same system prompt
                    {"role": "user", "content": paraphrased},
                    ex["messages"][2],  # Same expected output
                ]
            }
            augmented.append(new_ex)
    return augmented
```

**Key Points:**
- Overfitting = train loss << val loss (model memorizes rather than generalizes)
- First fix: reduce epochs (200 examples → max 2-3 epochs)
- Second fix: more data (aim for 500+ diverse examples)
- Third fix: regularization (dropout, weight decay, lower LoRA rank)
- Best practice: always use `load_best_model_at_end=True` to auto-select best checkpoint
- Prevention: plot train vs val loss during training — stop when val starts rising

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Model Comparison

**Scenario:** You've fine-tuned three variants: (A) 200 examples, 3 epochs; (B) 500 examples, 2 epochs; (C) 500 examples with LoRA r=32, 3 epochs. Design a systematic comparison to choose the best model.

<details>
<summary>💡 Hint</summary>
Use a fixed held-out test set (never seen during training). Measure accuracy, format compliance, latency, and cost. Use statistical tests to confirm differences are real.
</details>

<details>
<summary>✅ Solution</summary>

```python
import numpy as np
from scipy import stats

class ModelComparison:
    def __init__(self, test_cases: list[dict]):
        self.test_cases = test_cases  # 200+ held-out examples
    
    def compare_all(self, models: dict[str, str]) -> dict:
        """Run all models on test set and compare."""
        results = {}
        
        for name, model_id in models.items():
            results[name] = self.evaluate_model(model_id)
        
        # Statistical significance between best two
        sorted_models = sorted(results.items(), key=lambda x: x[1]["accuracy"], reverse=True)
        best = sorted_models[0]
        second = sorted_models[1]
        
        _, p_value = stats.proportions_ztest(
            [int(best[1]["accuracy"] * len(self.test_cases)), int(second[1]["accuracy"] * len(self.test_cases))],
            [len(self.test_cases), len(self.test_cases)]
        )
        
        return {
            "rankings": [(name, r["accuracy"]) for name, r in sorted_models],
            "winner": best[0],
            "p_value": p_value,
            "significant": p_value < 0.05,
            "detailed_results": results,
        }
    
    def evaluate_model(self, model_id: str) -> dict:
        correct = 0
        format_ok = 0
        latencies = []
        
        for case in self.test_cases:
            import time
            start = time.time()
            response = client.chat.completions.create(
                model=model_id,
                messages=case["messages"][:-1],
                temperature=0,
                max_tokens=100,
            )
            latencies.append((time.time() - start) * 1000)
            
            output = response.choices[0].message.content.strip()
            expected = case["messages"][-1]["content"]
            
            if output == expected:
                correct += 1
            if self.check_format(output):
                format_ok += 1
        
        return {
            "accuracy": correct / len(self.test_cases),
            "format_compliance": format_ok / len(self.test_cases),
            "latency_p50_ms": np.percentile(latencies, 50),
            "latency_p99_ms": np.percentile(latencies, 99),
        }

# Run comparison
comparison = ModelComparison(test_cases=held_out_test_set)
result = comparison.compare_all({
    "A_200ex_3ep": "ft:gpt-4o-mini:...:model-a",
    "B_500ex_2ep": "ft:gpt-4o-mini:...:model-b",
    "C_500ex_r32": "ft:gpt-4o-mini:...:model-c",
})

# Expected results:
# A: 82% accuracy (underfitting — too few examples)
# B: 91% accuracy (good balance)
# C: 89% accuracy (slightly overfit with high rank)
# Winner: B (statistically significant at p<0.05)
```

**Key Points:**
- Always compare on the SAME held-out test set (never used in training)
- 200+ test cases needed for reliable statistical comparison
- Check multiple dimensions: accuracy, format compliance, latency
- Use statistical significance testing — small accuracy differences may be noise
- Consider cost too: if B and C have similar accuracy, B is cheaper (no LoRA infra)
- Document comparison results for team decision-making

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: RLHF/DPO Pipeline

**Scenario:** Your fine-tuned SQL generator produces correct SQL 85% of the time, but users complain that the SQL style is unreadable (no formatting, confusing aliases, no comments). You want to train it to produce SQL that's both correct AND well-formatted/readable. Design a preference training pipeline.

<details>
<summary>💡 Hint</summary>
Use DPO with preference pairs: for each query, generate two SQL variants — one correct+readable (chosen) and one correct+messy (rejected). Train the model to prefer the readable style.
</details>

<details>
<summary>✅ Solution</summary>

```python
# Step 1: Generate preference pairs
def create_preference_data(questions: list[str], schema: str) -> list[dict]:
    """Generate (chosen, rejected) SQL pairs for DPO training."""
    pairs = []
    
    for question in questions:
        # Generate "chosen" (correct + well-formatted)
        chosen_response = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": f"""Write a SQL query for: {question}
Schema: {schema}
Requirements: Use clear aliases, add comments for complex logic, proper indentation, 
CTEs for readability. Follow our team's SQL style guide."""}],
            temperature=0,
        )
        
        # Generate "rejected" (correct but messy)
        rejected_response = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": f"""Write a SQL query for: {question}
Schema: {schema}
Write it as compact as possible, single line if possible, short aliases like t1/t2, no comments."""}],
            temperature=0,
        )
        
        pairs.append({
            "prompt": f"Generate SQL for: {question}\nSchema: {schema}",
            "chosen": chosen_response.choices[0].message.content,
            "rejected": rejected_response.choices[0].message.content,
        })
    
    return pairs

# Step 2: DPO Training
from trl import DPOTrainer, DPOConfig
from datasets import Dataset

preference_data = create_preference_data(questions, schema)
dataset = Dataset.from_list(preference_data)

dpo_config = DPOConfig(
    output_dir="./sql-dpo-model",
    beta=0.1,                    # How strongly to enforce preferences
    learning_rate=5e-7,          # Low LR for DPO
    num_train_epochs=1,          # Usually 1 epoch is enough for DPO
    per_device_train_batch_size=2,
    gradient_accumulation_steps=8,
    bf16=True,
)

# Start from SFT model (already fine-tuned for correctness)
trainer = DPOTrainer(
    model=sft_model,            # Your existing SQL fine-tuned model
    ref_model=sft_model_copy,   # Frozen copy as reference
    args=dpo_config,
    train_dataset=dataset,
    tokenizer=tokenizer,
)

trainer.train()
# Result: model produces correct SQL (from SFT) that's also readable (from DPO)
```

**Key Points:**
- DPO requires paired data: (prompt, chosen_response, rejected_response)
- Start from an SFT model (already correct), DPO adds style/preference
- beta=0.1: moderate preference enforcement (higher = stronger but may harm correctness)
- 500-1000 preference pairs typically sufficient for style alignment
- Evaluate both correctness (SQL executes correctly) AND readability (human review)
- DPO is much simpler than full RLHF: no reward model, no PPO instability

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Production Deployment with Canary

**Scenario:** You've trained a new model version that scores 5% better on your eval set. Deploy it to production serving 200K queries/day with zero risk of degrading user experience. Design the deployment strategy.

<details>
<summary>💡 Hint</summary>
Canary deployment: route 5-10% of traffic to new model, monitor quality metrics, gradually increase if positive, instant rollback if negative.
</details>

<details>
<summary>✅ Solution</summary>

```python
import hashlib
import time
from dataclasses import dataclass

@dataclass
class DeploymentConfig:
    production_model: str
    canary_model: str
    canary_pct: float
    min_canary_queries: int = 1000
    max_quality_drop: float = 0.02  # Rollback if quality drops >2%

class CanaryDeployment:
    """Progressive model deployment with automated rollback."""
    
    def __init__(self, config: DeploymentConfig):
        self.config = config
        self.metrics = {"production": [], "canary": []}
    
    def route_request(self, user_id: str) -> str:
        """Deterministic routing based on user_id."""
        h = int(hashlib.md5(user_id.encode()).hexdigest(), 16)
        if (h % 100) < (self.config.canary_pct * 100):
            return self.config.canary_model
        return self.config.production_model
    
    def record_quality(self, model_type: str, score: float):
        """Record quality score for monitoring."""
        self.metrics[model_type].append({"score": score, "time": time.time()})
    
    def check_canary_health(self) -> dict:
        """Evaluate if canary is performing well enough to promote."""
        canary_scores = [m["score"] for m in self.metrics["canary"]]
        prod_scores = [m["score"] for m in self.metrics["production"]]
        
        if len(canary_scores) < self.config.min_canary_queries:
            return {"status": "collecting", "samples": len(canary_scores)}
        
        canary_avg = sum(canary_scores[-1000:]) / 1000
        prod_avg = sum(prod_scores[-1000:]) / 1000
        
        if canary_avg < prod_avg - self.config.max_quality_drop:
            # Canary is WORSE — rollback!
            return {"status": "rollback", "canary": canary_avg, "prod": prod_avg}
        elif canary_avg >= prod_avg:
            # Canary is better or equal — safe to promote
            return {"status": "promote", "canary": canary_avg, "prod": prod_avg}
        else:
            # Canary is slightly worse but within tolerance — continue monitoring
            return {"status": "monitoring", "canary": canary_avg, "prod": prod_avg}

# DEPLOYMENT RUNBOOK:
# Day 1: Deploy canary at 5% traffic
# Day 2: Check health → if "promote", increase to 25%
# Day 3: Check health → if still "promote", increase to 50%
# Day 4: Check health → promote to 100%, deprecate old model
# ANY DAY: if "rollback" → immediately set canary_pct=0

# Automated version (Airflow DAG):
# hourly_check → if rollback: alert + set canary=0
#              → if promote AND days_at_current_pct >= 1: increase_pct()
#              → if monitoring: do nothing (wait)
```

**Key Points:**
- Start at 5% traffic: limits blast radius to 10K queries/day
- Deterministic routing (user_id hash): same user always gets same model (consistent UX)
- Automated health check: compares canary quality against production baseline
- Rollback threshold: 2% quality drop triggers immediate rollback (configurable)
- Gradual ramp: 5% → 25% → 50% → 100% over 4-7 days
- Keep old model warm for instant rollback during entire ramp-up period
- Log which model served each request for post-hoc analysis

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Distributed Training on Spot Instances

**Scenario:** You need to fine-tune a 70B parameter model on 50K examples. This requires 8× A100 GPUs for ~12 hours. On-demand cost: $250/hr × 12 = $3,000. Design a cost-optimized training setup using spot instances with checkpointing for fault tolerance.

<details>
<summary>💡 Hint</summary>
Spot instances are 60-70% cheaper but can be terminated with 2-minute warning. Checkpoint frequently (every 100 steps) and auto-resume from last checkpoint. Use DeepSpeed ZeRO-3 for model parallelism.
</details>

<details>
<summary>✅ Solution</summary>

```python
# INFRASTRUCTURE:
# 8× A100 80GB (p4d.24xlarge on AWS)
# On-demand: $32.77/hr → $393/12hr = $3,000 total
# Spot: ~$10/hr (70% savings) → $120/12hr = ~$1,000 total (if no interruptions)
# With interruptions (avg 1-2 restarts): ~$1,200-1,500 total

# DeepSpeed ZeRO-3 config for 70B model across 8 GPUs
deepspeed_config = {
    "zero_optimization": {
        "stage": 3,
        "offload_optimizer": {"device": "cpu", "pin_memory": True},
        "overlap_comm": True,
        "contiguous_gradients": True,
        "reduce_scatter": True,
    },
    "bf16": {"enabled": True},
    "train_batch_size": 64,
    "train_micro_batch_size_per_gpu": 1,
    "gradient_accumulation_steps": 8,
    "gradient_clipping": 1.0,
    "steps_per_print": 10,
    # CRITICAL: Checkpoint config for spot instance resilience
    "checkpoint": {
        "tag_validation": False,
    },
}

# Training script with checkpoint resilience
from transformers import TrainingArguments

training_args = TrainingArguments(
    output_dir="s3://training-checkpoints/70b-finetune/",  # S3 for durability
    num_train_epochs=2,
    per_device_train_batch_size=1,
    gradient_accumulation_steps=8,
    
    # CHECKPOINTING (critical for spot instances)
    save_strategy="steps",
    save_steps=100,              # Save every 100 steps (~5 minutes)
    save_total_limit=3,          # Keep last 3 checkpoints
    resume_from_checkpoint=True, # Auto-resume from latest on restart
    
    # Performance
    bf16=True,
    deepspeed="ds_config.json",
    gradient_checkpointing=True,
    
    # Logging
    logging_steps=10,
    report_to="wandb",
)

# SPOT INSTANCE HANDLER (runs on the instance)
import signal
import subprocess

class SpotTerminationHandler:
    """Handle AWS spot instance termination gracefully."""
    
    def __init__(self, checkpoint_dir: str):
        self.checkpoint_dir = checkpoint_dir
        # AWS gives 2-minute warning via instance metadata
        signal.signal(signal.SIGTERM, self.handle_termination)
    
    def handle_termination(self, signum, frame):
        """Save checkpoint immediately on termination signal."""
        print("SPOT TERMINATION WARNING - saving checkpoint...")
        # Training frameworks handle SIGTERM by saving current state
        # The next instance startup will resume_from_checkpoint=True
    
    def monitor_spot_status(self):
        """Poll AWS metadata endpoint for termination notice."""
        import requests
        while True:
            try:
                resp = requests.get(
                    "http://169.254.169.254/latest/meta-data/spot/termination-time",
                    timeout=1
                )
                if resp.status_code == 200:
                    print(f"Termination scheduled: {resp.text}")
                    self.handle_termination(None, None)
                    break
            except:
                pass
            time.sleep(5)

# LAUNCH SCRIPT (auto-restart on termination)
# Uses AWS Batch or SageMaker with spot instance support
# On interruption: new instance starts, loads latest checkpoint from S3, continues
```

**Key Points:**
- Checkpoint every 100 steps to S3 (~5 min intervals) — max data loss: 5 minutes of training
- AWS spot gives 2-minute termination warning — enough to flush current checkpoint
- Expected 1-2 interruptions in 12 hours → adds ~30 min total restart overhead
- Net savings: $3,000 → ~$1,300 (57% reduction) with ~13 hours total time
- DeepSpeed ZeRO-3 shards the 70B model across 8 GPUs (each holds 1/8)
- S3-backed checkpoints survive instance termination (local disk doesn't)
- Alternative: use SageMaker managed spot training (handles restart logic automatically)

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Cost Optimization — $50K to $15K/Month

**Scenario:** Your company runs 5 fine-tuned models serving 10M total queries/month via OpenAI API. Monthly cost: $50K. The CFO demands 70% reduction. Design an optimization strategy.

<details>
<summary>💡 Hint</summary>
Levers: consolidate models, self-host on GPUs, implement caching, route simple queries to cheaper models, reduce token usage through shorter prompts.
</details>

<details>
<summary>✅ Solution</summary>

```python
# CURRENT STATE:
# 5 fine-tuned models × 2M queries each = 10M queries/month
# Average: 300 input tokens + 100 output tokens per query
# Cost: Fine-tuned gpt-4o-mini: $0.30/1M input + $1.20/1M output
# Monthly: 10M × (300 × $0.0000003 + 100 × $0.0000012) = $2,100 (hmm, that's not $50K)
# Ah — they must be using fine-tuned gpt-4o:
# Fine-tuned gpt-4o: $5/1M input + $15/1M output
# Monthly: 10M × (300 × $0.000005 + 100 × $0.000015) = $15,000 + $15,000 = $30K
# Plus training reruns, testing, overheads → ~$50K

OPTIMIZATION_PLAN = {
    "step_1_model_consolidation": {
        "action": "Merge 5 models into 2 (general + specialized)",
        "savings": "$5K/mo (fewer training runs, simpler infra)",
    },
    "step_2_switch_to_mini": {
        "action": "Route 70% of queries to fine-tuned gpt-4o-mini (most don't need 4o)",
        "savings": "$20K/mo (7M queries × mini pricing instead of 4o)",
    },
    "step_3_response_caching": {
        "action": "Cache identical/similar queries (expect 25% hit rate)",
        "savings": "$5K/mo (2.5M fewer API calls)",
    },
    "step_4_prompt_shortening": {
        "action": "Fine-tuned models don't need few-shot examples → shorter prompts",
        "savings": "$3K/mo (reduce avg input from 300 to 150 tokens)",
    },
    "step_5_self_host_high_volume": {
        "action": "Self-host the highest-volume model on 2× A100 ($3K/mo infra)",
        "savings": "$7K/mo (eliminate API cost for 4M queries)",
    },
    "total_monthly_after": "$10K (80% reduction)",
}

# Implementation priority:
# Week 1: Step 2 (biggest impact, lowest effort — just change model name)
# Week 2: Step 3 (Redis cache, moderate effort)
# Week 3: Step 4 (prompt optimization, needs testing)
# Week 4-6: Step 1 + 5 (consolidation and self-hosting, highest effort)
```

**Key Points:**
- Biggest lever: switch from gpt-4o to gpt-4o-mini for most queries (90% cheaper)
- Model consolidation reduces maintenance overhead and training costs
- Caching: high hit rate for repetitive workloads (customer support, classification)
- Self-hosting: wins at 4M+ queries/month per model (break-even point)
- Validate quality at each step: never trade unacceptable quality for cost savings
- Phased rollout: quick wins first (model swap), infrastructure changes later

</details>

</article>

---

## ⚡ Quick-fire Q&A

**Q: When should you fine-tune an LLM vs. relying solely on RAG?**
A: Use RAG when the knowledge base changes frequently or is too large to bake into weights. Use fine-tuning when you need the model to learn a specific style, format, or reasoning pattern that can't be achieved through prompting alone. They are complementary—fine-tuning teaches "how to respond," RAG provides "what to respond with."

**Q: What is LoRA and why is it popular for fine-tuning LLMs?**
A: LoRA (Low-Rank Adaptation) injects trainable low-rank matrices into frozen model weights, drastically reducing the number of trainable parameters. This lowers GPU memory requirements and training time while achieving performance close to full fine-tuning, making it practical for teams without massive compute budgets.

**Q: What data do you need for instruction fine-tuning?**
A: Instruction fine-tuning requires prompt-completion pairs where the prompt is a user instruction and the completion is the desired model response. For RAG-specific fine-tuning, you'd include context passages in the prompt and train the model to generate answers grounded in that context.

**Q: What is catastrophic forgetting and how do you mitigate it?**
A: Catastrophic forgetting occurs when fine-tuning on new data causes the model to lose capabilities learned during pretraining. Mitigations include using LoRA (which leaves base weights frozen), mixing in general-purpose examples with domain-specific ones, and evaluating on a broad benchmark after fine-tuning.

**Q: What is RLHF and how does it differ from supervised fine-tuning?**
A: RLHF (Reinforcement Learning from Human Feedback) trains a reward model on human preference rankings, then uses RL (PPO) to optimize the LLM to maximize that reward. Supervised fine-tuning directly trains on labeled examples. RLHF is better at shaping open-ended behavior like helpfulness or safety but is more complex to implement.

**Q: How do you evaluate a fine-tuned model for a RAG use case?**
A: Evaluate on a held-out set using RAG-specific metrics: faithfulness (does it stay grounded?), answer relevancy, and task-specific accuracy (e.g., exact match for extractive QA). Also run regression tests against the base model on general benchmarks to detect capability degradation.

**Q: What is QLoRA and how does it differ from LoRA?**
A: QLoRA combines LoRA with 4-bit quantization of the base model, further reducing GPU memory. The base model is quantized to 4-bit (NF4 format) and kept frozen, while LoRA adapters are trained in higher precision. This enables fine-tuning large models (e.g., 70B) on consumer-grade GPUs.

**Q: What are the risks of fine-tuning on proprietary or sensitive data?**
A: Fine-tuned models can memorize and reproduce training data, creating privacy and compliance risks if that data contains PII or trade secrets. Mitigations include differential privacy training, data deduplication and scrubbing, and careful access controls on the fine-tuned model itself.

---

## 💼 Interview Tips

- Always frame fine-tuning as one tool in a broader toolkit—leading with "should we fine-tune or use better prompting/RAG?" shows mature judgment over jumping straight to training.
- Be specific about parameter-efficient methods (LoRA, QLoRA) and why they matter in practice—cost and infrastructure constraints are real, and interviewers at data-intensive companies will probe your awareness of them.
- Senior interviewers want to hear about your evaluation strategy before and after fine-tuning, not just training details. Showing you measure capability regressions demonstrates production readiness.
- Avoid conflating instruction fine-tuning with RLHF—know when each is appropriate and what human annotation requirements differ between them.
- Bring up data quality as the most critical factor. A small, high-quality dataset almost always beats a large noisy one for fine-tuning—this insight signals experience over theory.
