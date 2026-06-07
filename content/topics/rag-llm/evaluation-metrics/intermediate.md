---
title: "Evaluation Metrics - Intermediate"
topic: rag-llm
subtopic: evaluation-metrics
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [rag, llm, evaluation, ragas, deepeval, llm-as-judge, a-b-testing]
---

# RAG Evaluation Metrics — Intermediate

## RAGAS Framework

RAGAS (Retrieval Augmented Generation Assessment) is the standard framework for RAG evaluation. It measures four key dimensions without requiring ground truth answers for every question.

```python
from ragas import evaluate
from ragas.metrics import (
    faithfulness, answer_relevancy, 
    context_precision, context_recall
)
from datasets import Dataset

# Prepare evaluation data
eval_data = {
    "question": [
        "What is the default shuffle partition count in Spark?",
        "How does Kafka handle message ordering?",
    ],
    "answer": [
        "The default value of spark.sql.shuffle.partitions is 200.",
        "Kafka guarantees ordering within a partition using offset-based sequencing.",
    ],
    "contexts": [
        ["Spark's spark.sql.shuffle.partitions defaults to 200. This controls parallelism after shuffle operations."],
        ["Kafka maintains message order within each partition. Messages are appended with sequential offsets."],
    ],
    "ground_truth": [
        "200",
        "Kafka guarantees message ordering within a single partition, not across partitions.",
    ]
}

dataset = Dataset.from_dict(eval_data)

# Run RAGAS evaluation
results = evaluate(
    dataset,
    metrics=[faithfulness, answer_relevancy, context_precision, context_recall],
)

print(results)
# {"faithfulness": 0.92, "answer_relevancy": 0.88, "context_precision": 0.85, "context_recall": 0.78}
```

### RAGAS Metrics Explained

| Metric | What It Measures | Requires Ground Truth? |
|--------|-----------------|----------------------|
| Faithfulness | Can every claim in the answer be traced to the context? | No |
| Answer Relevancy | Does the answer address the question directly? | No |
| Context Precision | Are the retrieved docs relevant (in ranked order)? | Yes |
| Context Recall | Does the context contain all info needed to answer? | Yes |

---

## LLM-as-Judge Evaluation

Use a strong LLM (GPT-4o) to evaluate outputs from your RAG system:

```python
class LLMJudge:
    """Use GPT-4o as an evaluation judge for RAG outputs."""
    
    def __init__(self):
        self.client = OpenAI()
    
    def evaluate_answer(self, question: str, answer: str, context: list[str], ground_truth: str = None) -> dict:
        """Score a RAG response across multiple dimensions."""
        
        context_str = "\n".join(context[:3])
        
        judge_prompt = f"""Evaluate this RAG system's response.

Question: {question}
Context provided to the system: {context_str}
System's answer: {answer}
{"Expected answer: " + ground_truth if ground_truth else ""}

Score each dimension from 1-5:
1. Correctness: Is the answer factually correct?
2. Faithfulness: Does the answer ONLY use information from the context (no hallucination)?
3. Relevance: Does the answer directly address the question?
4. Completeness: Does the answer cover all aspects of the question?
5. Clarity: Is the answer well-written and easy to understand?

For each score, provide a brief justification.

Respond as JSON: {{
  "correctness": {{"score": 1-5, "reason": "..."}},
  "faithfulness": {{"score": 1-5, "reason": "..."}},
  "relevance": {{"score": 1-5, "reason": "..."}},
  "completeness": {{"score": 1-5, "reason": "..."}},
  "clarity": {{"score": 1-5, "reason": "..."}}
}}"""
        
        response = self.client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": judge_prompt}],
            response_format={"type": "json_object"},
            temperature=0,
        )
        
        return json.loads(response.choices[0].message.content)
    
    def batch_evaluate(self, test_cases: list[dict]) -> dict:
        """Evaluate a batch of test cases and compute aggregate scores."""
        all_scores = []
        for case in test_cases:
            scores = self.evaluate_answer(
                question=case["question"],
                answer=case["answer"],
                context=case["context"],
                ground_truth=case.get("ground_truth"),
            )
            all_scores.append(scores)
        
        # Aggregate
        dimensions = ["correctness", "faithfulness", "relevance", "completeness", "clarity"]
        averages = {
            dim: np.mean([s[dim]["score"] for s in all_scores])
            for dim in dimensions
        }
        
        return {"per_case": all_scores, "averages": averages, "total_cases": len(test_cases)}
```

### Reducing Judge Bias

```python
# Problem: LLM judges tend to be overly generous (positivity bias)
# Solutions:

# 1. Calibration examples (anchor the scoring)
CALIBRATION = """Before scoring, review these calibrated examples:
Score 1 (terrible): "I don't know" when answer exists in context
Score 3 (mediocre): Partially correct but missing key details
Score 5 (excellent): Complete, accurate, well-cited answer"""

# 2. Pairwise comparison instead of absolute scoring
# "Which answer is better: A or B?" is more reliable than "Score this 1-5"

# 3. Multiple judge runs + majority vote
def robust_evaluate(question, answer, context, n_runs=3):
    scores = [judge.evaluate_answer(question, answer, context) for _ in range(n_runs)]
    # Take median score across runs
    return {dim: np.median([s[dim]["score"] for s in scores]) for dim in dimensions}
```

---

## Building an Evaluation Pipeline

Run evaluations automatically on schedule:

```python
class RAGEvaluationPipeline:
    """Nightly evaluation pipeline for RAG quality monitoring."""
    
    def __init__(self, rag_system, eval_dataset: list[dict], judge: LLMJudge):
        self.rag = rag_system
        self.eval_dataset = eval_dataset
        self.judge = judge
    
    def run_nightly_eval(self) -> dict:
        """Run full evaluation suite and store results."""
        results = []
        
        for case in self.eval_dataset:
            # Run RAG system
            rag_output = self.rag.answer(case["question"])
            
            # Evaluate with LLM judge
            evaluation = self.judge.evaluate_answer(
                question=case["question"],
                answer=rag_output["answer"],
                context=rag_output["retrieved_contexts"],
                ground_truth=case.get("ground_truth"),
            )
            
            results.append({
                "question": case["question"],
                "scores": evaluation,
                "retrieval_score": rag_output.get("top_similarity_score"),
            })
        
        # Compute summary metrics
        summary = self.compute_summary(results)
        
        # Check for regression
        previous = self.load_previous_results()
        if previous:
            regression = self.detect_regression(summary, previous)
            if regression:
                self.send_alert(f"RAG quality regression detected: {regression}")
        
        # Store results
        self.store_results(summary, results)
        return summary
    
    def detect_regression(self, current: dict, previous: dict) -> str:
        """Alert if any metric drops more than 5%."""
        regressions = []
        for metric in ["faithfulness", "relevance", "correctness"]:
            if current[metric] < previous[metric] - 0.05:
                regressions.append(f"{metric}: {previous[metric]:.2f} → {current[metric]:.2f}")
        return "; ".join(regressions) if regressions else None
```

---

## A/B Testing RAG Systems

```python
class RAGABTest:
    """Compare two RAG configurations with statistical rigor."""
    
    def __init__(self, system_a, system_b, eval_set: list[dict]):
        self.system_a = system_a  # Control
        self.system_b = system_b  # Treatment
        self.eval_set = eval_set
    
    def run_comparison(self) -> dict:
        """Run both systems on same queries, compare with paired test."""
        scores_a, scores_b = [], []
        
        for case in self.eval_set:
            # Run both systems on same question
            answer_a = self.system_a.answer(case["question"])
            answer_b = self.system_b.answer(case["question"])
            
            # Judge both
            score_a = self.judge.evaluate_answer(case["question"], answer_a, case.get("ground_truth"))
            score_b = self.judge.evaluate_answer(case["question"], answer_b, case.get("ground_truth"))
            
            scores_a.append(score_a["correctness"]["score"])
            scores_b.append(score_b["correctness"]["score"])
        
        # Paired t-test (same questions, different systems)
        from scipy.stats import ttest_rel
        t_stat, p_value = ttest_rel(scores_a, scores_b)
        
        return {
            "system_a_mean": np.mean(scores_a),
            "system_b_mean": np.mean(scores_b),
            "improvement": np.mean(scores_b) - np.mean(scores_a),
            "p_value": p_value,
            "significant": p_value < 0.05,
            "n_samples": len(self.eval_set),
            "recommendation": "deploy_b" if (np.mean(scores_b) > np.mean(scores_a) and p_value < 0.05) else "keep_a"
        }
```

---

## Synthetic Evaluation Set Generation

```python
def generate_eval_set_from_corpus(documents: list[dict], n_questions: int = 200) -> list[dict]:
    """Generate evaluation questions from your document corpus."""
    eval_set = []
    
    # Sample documents evenly
    sample = random.sample(documents, min(n_questions // 3, len(documents)))
    
    for doc in sample:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": f"""Based on this document, generate 3 question-answer pairs.

Requirements:
1. Questions should be natural (how a real user would ask)
2. Answers must be directly stated in or derivable from the document
3. Include one factual question, one "how to" question, and one conceptual question
4. Answers should be concise (1-3 sentences)

Document:
{doc['text'][:3000]}

Return as JSON array: [{{"question": "...", "answer": "...", "difficulty": "easy|medium|hard"}}]"""}],
            response_format={"type": "json_object"},
            temperature=0.3,
        )
        
        pairs = json.loads(response.choices[0].message.content)
        for pair in pairs.get("questions", pairs.get("pairs", [])):
            pair["source_doc_id"] = doc["id"]
            eval_set.append(pair)
    
    return eval_set[:n_questions]
```

---

## Interview Tips

> **Tip 1:** "How do you evaluate without ground truth?" — Use RAGAS faithfulness and answer relevancy metrics — they don't need ground truth, only the question, context, and answer. Faithfulness checks if the answer is supported by context. Answer relevancy checks if it addresses the question. Both use LLM-as-judge under the hood.

> **Tip 2:** "LLM-as-judge reliability?" — GPT-4o agrees with human evaluators ~85% of the time (comparable to inter-annotator agreement). Reduce bias with: calibration examples, pairwise comparisons instead of absolute scores, and multiple evaluation runs with majority voting. Always validate your judge against a small set of human-labeled examples.

> **Tip 3:** "How often should you evaluate?" — Nightly automated eval against a fixed 200-question test set (catches regressions). On every change (chunking, prompt, model): run full eval before merging. Weekly: sample production queries for manual review. Monthly: refresh eval dataset with new question types.
