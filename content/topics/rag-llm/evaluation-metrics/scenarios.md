---
title: "Evaluation Metrics - Scenario Questions"
topic: rag-llm
subtopic: evaluation-metrics
content_type: scenario_question
tags: [rag, llm, evaluation, interview, scenarios]
---

# Scenario Questions — Evaluation Metrics

<article data-difficulty="junior">

## 🟢 Junior: Computing Recall@K

**Scenario:** Your RAG system retrieves 5 documents for the query "What is data skew in Spark?". The relevant documents in your corpus are doc_A, doc_B, and doc_C. Your system retrieved: [doc_A, doc_X, doc_B, doc_Y, doc_Z]. Calculate Precision@5, Recall@5, and Hit Rate@5.

<details>
<summary>💡 Hint</summary>
Precision = relevant retrieved / total retrieved. Recall = relevant retrieved / total relevant that exist. Hit Rate = 1 if any relevant found, else 0.
</details>

<details>
<summary>✅ Solution</summary>

```python
retrieved = ["doc_A", "doc_X", "doc_B", "doc_Y", "doc_Z"]  # Top 5
relevant = {"doc_A", "doc_B", "doc_C"}  # Ground truth

# Precision@5: of the 5 retrieved, how many are relevant?
relevant_retrieved = set(retrieved) & relevant  # {"doc_A", "doc_B"}
precision_5 = len(relevant_retrieved) / 5  # 2/5 = 0.40

# Recall@5: of all 3 relevant docs, how many did we find?
recall_5 = len(relevant_retrieved) / len(relevant)  # 2/3 = 0.67

# Hit Rate@5: did we find at least one relevant doc?
hit_rate_5 = 1.0 if relevant_retrieved else 0.0  # 1.0 (yes, found doc_A and doc_B)

# MRR (Mean Reciprocal Rank): position of first relevant doc
first_relevant_position = next(i for i, d in enumerate(retrieved, 1) if d in relevant)
mrr = 1.0 / first_relevant_position  # 1/1 = 1.0 (doc_A is at position 1)

print(f"Precision@5: {precision_5:.2f}")  # 0.40
print(f"Recall@5: {recall_5:.2f}")        # 0.67
print(f"Hit Rate@5: {hit_rate_5:.2f}")    # 1.00
print(f"MRR: {mrr:.2f}")                  # 1.00
```

**Key Points:**
- Precision@5 = 0.40 means 60% of retrieved docs are noise (irrelevant)
- Recall@5 = 0.67 means we missed one relevant doc (doc_C not retrieved)
- Hit Rate = 1.0 — we found at least one relevant doc (sufficient for many use cases)
- MRR = 1.0 — the first result is relevant (ideal ranking)
- For RAG: Recall matters most (did we find the info?) whereas Precision matters for context quality (less noise for the LLM)

</details>

</article>

<article data-difficulty="junior">

## 🟢 Junior: Choosing the Right Metric

**Scenario:** Your team debates which metric to optimize. The ML engineer wants NDCG, the PM wants "answer accuracy," and the designer wants "user satisfaction." Which metrics should you track and why?

<details>
<summary>💡 Hint</summary>
Different stakeholders care about different aspects. You need metrics at each level: retrieval quality, generation quality, and end-user experience.
</details>

<details>
<summary>✅ Solution</summary>

```python
# Layered metrics approach — track all, optimize the one that's weakest

METRICS_FRAMEWORK = {
    "retrieval_layer": {
        "who_cares": "ML Engineer",
        "metrics": ["recall@5", "MRR", "NDCG"],
        "why": "Measures if the right documents are found",
        "when_to_fix": "If recall < 80% — fix embeddings, chunking, or hybrid search",
    },
    "generation_layer": {
        "who_cares": "ML Engineer + PM",
        "metrics": ["faithfulness", "correctness", "format_compliance"],
        "why": "Measures if the LLM produces accurate, grounded answers",
        "when_to_fix": "If faithfulness < 85% — fix prompt, reduce temperature",
    },
    "user_layer": {
        "who_cares": "PM + Designer",
        "metrics": ["user_satisfaction (thumbs_up_rate)", "task_completion", "time_to_answer"],
        "why": "Measures if users actually find the system helpful",
        "when_to_fix": "If satisfaction < 70% — fix UX, answer clarity, or underlying quality",
    },
}

# Priority order for optimization:
# 1. Retrieval recall (foundation — if you don't find the right docs, nothing else matters)
# 2. Faithfulness (trust — users must be able to trust the answers)
# 3. User satisfaction (outcome — the ultimate measure of success)
```

**Key Points:**
- No single metric captures everything — you need a stack
- Retrieval metrics (NDCG, recall) are the foundation: fix these first
- Generation metrics (faithfulness) build trust: essential for production
- User metrics (satisfaction) are the ultimate goal but lag behind improvements
- Start optimizing from bottom (retrieval) up — each layer builds on the one below
- Report different metrics to different stakeholders (engineers vs PMs)

</details>

</article>

<article data-difficulty="junior">

## 🟢 Junior: Creating a Test Set

**Scenario:** You just built a RAG system over 5,000 internal documents. You have no evaluation dataset. Create a minimal test set to measure quality before launching to users.

<details>
<summary>💡 Hint</summary>
You need (question, expected_answer) pairs. Sources: write some manually, generate some with GPT-4, and collect some from real user questions (beta testers).
</details>

<details>
<summary>✅ Solution</summary>

```python
# Three sources for building a test set quickly:

# SOURCE 1: Manual creation (highest quality, 30 minutes for 20 questions)
manual_test_set = [
    {"question": "What is the default shuffle partition count?", "answer": "200", "difficulty": "easy"},
    {"question": "How do you handle data skew in Spark joins?", "answer": "Salting, broadcast join, or AQE skew handling", "difficulty": "medium"},
    # Write 20 questions covering your key topics
]

# SOURCE 2: Synthetic generation from docs (fast, good coverage)
def generate_test_questions(documents: list[dict], n: int = 80) -> list[dict]:
    test_set = []
    for doc in random.sample(documents, min(n // 3, len(documents))):
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": f"""Generate 3 question-answer pairs from this document.
Questions should be what a real engineer would ask. Answers must be in the document.

Document: {doc['text'][:2000]}

Return JSON: [{{"question": "...", "answer": "..."}}]"""}],
            response_format={"type": "json_object"},
            temperature=0.3,
        )
        pairs = json.loads(response.choices[0].message.content)
        for p in pairs.get("pairs", pairs.get("questions", [])):
            p["source_doc"] = doc["id"]
            test_set.append(p)
    return test_set[:n]

# SOURCE 3: Real user questions from beta (most realistic)
# Have 5-10 colleagues use the system for a day, collect their questions
# Review and add correct answers manually

# COMBINE: 20 manual + 80 synthetic + 50 real = 150 test questions
# This is enough for a meaningful baseline measurement
```

**Key Points:**
- Minimum viable test set: 50 questions (but 150-200 is much better)
- Manual questions: hardest to create but highest quality and most realistic
- Synthetic questions: fast to generate, good coverage, but may miss real user patterns
- Real user questions: most realistic but need manual answer annotation
- Store test set in version control — it's as important as your code
- NEVER use test set examples in your RAG knowledge base (data leakage)

</details>

</article>

<article data-difficulty="junior">

## 🟢 Junior: Interpreting Evaluation Scores

**Scenario:** Your evaluation results: faithfulness=0.72, answer_relevancy=0.85, context_precision=0.60, context_recall=0.55. What's the diagnosis and what should you fix first?

<details>
<summary>💡 Hint</summary>
Low context_precision and context_recall point to retrieval problems. Faithfulness at 0.72 means some hallucination. Fix retrieval first since it's the foundation.
</details>

<details>
<summary>✅ Solution</summary>

```python
scores = {
    "faithfulness": 0.72,        # 72% of claims are grounded in context
    "answer_relevancy": 0.85,    # 85% of answers address the question
    "context_precision": 0.60,   # Only 60% of retrieved docs are relevant
    "context_recall": 0.55,      # Only 55% of needed info is retrieved
}

# DIAGNOSIS:
# 1. Context Recall 0.55 (CRITICAL): Missing 45% of relevant information
#    → The right documents are NOT being retrieved
#    → Root cause: poor embeddings, wrong chunk size, or missing docs
#
# 2. Context Precision 0.60 (BAD): 40% of retrieved context is noise
#    → Irrelevant docs are diluting the context
#    → Root cause: similarity threshold too low, need re-ranking
#
# 3. Faithfulness 0.72 (CONCERNING): 28% of claims are hallucinated
#    → LLM is making up information not in context
#    → Partly caused by noisy context (precision issue)
#    → Also fix: stricter prompt, lower temperature
#
# 4. Answer Relevancy 0.85 (OK): Answers mostly address the question

# FIX PRIORITY (bottom-up):
priority = [
    "1. Fix retrieval recall: try hybrid search, better embeddings, smaller chunks",
    "2. Fix retrieval precision: add re-ranking, increase similarity threshold",
    "3. Fix faithfulness: stricter prompt (ONLY use context), temperature=0",
    "4. Answer relevancy is OK — will improve naturally as other metrics improve",
]

# Expected improvement path:
# After fixing retrieval: context_recall 0.55→0.80, context_precision 0.60→0.80
# After fixing generation: faithfulness 0.72→0.90
# Net: overall answer quality improves from ~65% to ~85%
```

**Key Points:**
- Always fix retrieval first — everything downstream depends on it
- Low recall = missing relevant docs (system doesn't find the answer)
- Low precision = too much noise (irrelevant docs confuse the LLM)
- Low faithfulness = hallucination (LLM makes up facts not in context)
- Fixes compound: better retrieval → less noise → less hallucination
- Target scores: recall >0.80, precision >0.75, faithfulness >0.85

</details>

</article>

<article data-difficulty="junior">

## 🟢 Junior: Precision vs Recall Trade-off

**Scenario:** You can tune your retrieval to get high precision (only return very relevant docs, top_k=3, high threshold) or high recall (return many docs including less relevant ones, top_k=10, low threshold). Which should you optimize for in a RAG system?

<details>
<summary>💡 Hint</summary>
In RAG, recall is generally more important than precision because: if the right document isn't retrieved, the LLM can't answer correctly. Noise (low precision) can be handled by the LLM.
</details>

<details>
<summary>✅ Solution</summary>

```python
# Scenario A: High precision, low recall (top_k=3, threshold=0.8)
# Retrieved: [highly_relevant_doc_1, relevant_doc_2, relevant_doc_3]
# Problem: If the answer is in doc_4 (score 0.75), we miss it entirely!
# LLM output: "I don't have information about this" → user frustrated

# Scenario B: High recall, lower precision (top_k=10, threshold=0.4)
# Retrieved: [relevant_1, relevant_2, irrelevant_3, relevant_4, noise_5, ...]
# The answer IS in the retrieved set — LLM can find and use it
# LLM is good at ignoring irrelevant context if the right info is present
# LLM output: "Based on the documentation, the answer is..." → user happy

# RECOMMENDATION: Optimize for recall, then clean up precision with re-ranking

def balanced_retrieval(query: str, top_k: int = 10, min_score: float = 0.4):
    """Retrieve many candidates (high recall), then re-rank (improve precision)."""
    
    # Step 1: High recall retrieval
    candidates = vector_db.search(embed(query), top_k=top_k)
    candidates = [c for c in candidates if c.score >= min_score]
    
    # Step 2: Re-rank for precision (put best docs first)
    reranked = cross_encoder_rerank(query, candidates, top_k=5)
    
    # Result: top-5 after re-ranking has BOTH high recall AND high precision
    return reranked

# Summary:
# - Recall = "did we find the answer?" (CRITICAL for RAG)
# - Precision = "is the context clean?" (NICE TO HAVE, LLM handles noise)
# - Best approach: over-retrieve (recall), then re-rank (precision)
```

**Key Points:**
- In RAG: missing the right document = can't answer (fatal failure)
- Including some noise = LLM usually handles it fine (minor issue)
- Retrieval recall is the single most important retrieval metric for RAG
- The re-ranking stage exists specifically to boost precision without sacrificing recall
- Trade-off: more context = more tokens = higher cost. Balance with budget constraints.
- Exception: if context window is very limited (small model), precision matters more

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: RAGAS Implementation

**Scenario:** Set up an automated RAGAS evaluation pipeline that runs nightly and alerts if any metric drops below threshold. Your test set has 200 questions with ground truth answers.

<details>
<summary>💡 Hint</summary>
Use the ragas library with your test set. Store results daily. Compare to rolling baseline. Alert via Slack/PagerDuty if metrics regress.
</details>

<details>
<summary>✅ Solution</summary>

```python
from ragas import evaluate
from ragas.metrics import faithfulness, answer_relevancy, context_precision, context_recall
from datasets import Dataset
import json
from datetime import datetime

class NightlyRAGASEval:
    """Automated nightly RAGAS evaluation with regression detection."""
    
    def __init__(self, rag_system, test_set_path: str, thresholds: dict):
        self.rag = rag_system
        self.test_set = json.load(open(test_set_path))
        self.thresholds = thresholds  # {"faithfulness": 0.80, "answer_relevancy": 0.75, ...}
    
    def run(self) -> dict:
        """Run RAGAS eval and check thresholds."""
        
        # Step 1: Generate answers for all test questions
        questions, answers, contexts, ground_truths = [], [], [], []
        
        for case in self.test_set:
            result = self.rag.answer(case["question"])
            questions.append(case["question"])
            answers.append(result["answer"])
            contexts.append(result["contexts"])
            ground_truths.append(case["ground_truth"])
        
        # Step 2: Run RAGAS evaluation
        dataset = Dataset.from_dict({
            "question": questions,
            "answer": answers,
            "contexts": contexts,
            "ground_truth": ground_truths,
        })
        
        results = evaluate(
            dataset,
            metrics=[faithfulness, answer_relevancy, context_precision, context_recall],
        )
        
        # Step 3: Check thresholds
        violations = []
        for metric_name, score in results.items():
            if metric_name in self.thresholds and score < self.thresholds[metric_name]:
                violations.append(f"{metric_name}: {score:.3f} (threshold: {self.thresholds[metric_name]})")
        
        # Step 4: Alert if violations
        if violations:
            self.send_alert(violations, results)
        
        # Step 5: Store results for trend tracking
        self.store_results(results)
        
        return {"scores": dict(results), "violations": violations, "status": "PASS" if not violations else "FAIL"}
    
    def send_alert(self, violations: list[str], scores: dict):
        message = f"""RAG Quality Alert ({datetime.now().strftime('%Y-%m-%d')})
Threshold violations:
{chr(10).join(f'  - {v}' for v in violations)}

Full scores: {json.dumps(dict(scores), indent=2)}

Action required: check recent changes to embeddings, chunking, or prompts."""
        
        slack_webhook(self.alert_channel, message)

# Configuration
eval_pipeline = NightlyRAGASEval(
    rag_system=production_rag,
    test_set_path="eval/golden_test_set.json",
    thresholds={
        "faithfulness": 0.80,
        "answer_relevancy": 0.75,
        "context_precision": 0.70,
        "context_recall": 0.70,
    }
)

# Run via Airflow DAG at 2 AM nightly
# eval_pipeline.run()
```

**Key Points:**
- RAGAS evaluates without custom training (uses LLM-as-judge internally)
- Thresholds should be set based on your baseline (measure once, set threshold 5% below)
- Ground truth is required for context_precision and context_recall
- Cost: ~$5-10 per nightly run (200 questions × LLM judge calls)
- Store historical results: enables trend analysis and regression detection
- Alert severity: >5% drop = critical (page on-call), 2-5% = warning (Slack)

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: LLM-as-Judge Setup

**Scenario:** RAGAS is too slow and expensive for your use case (5000 daily test queries). Design a custom LLM-as-judge that's cheaper and faster while still reliable.

<details>
<summary>💡 Hint</summary>
Use GPT-4o-mini instead of GPT-4o (10x cheaper), batch multiple evaluations per call, cache repeated judgments, and use simpler scoring (binary yes/no instead of 1-5 scale).
</details>

<details>
<summary>✅ Solution</summary>

```python
class FastLLMJudge:
    """Cost-optimized LLM judge for high-volume evaluation."""
    
    def __init__(self, batch_size: int = 5):
        self.client = OpenAI()
        self.batch_size = batch_size
        self.cache = {}
    
    def batch_evaluate(self, cases: list[dict]) -> list[dict]:
        """Evaluate multiple cases in a single LLM call (cheaper)."""
        
        # Batch cases into groups
        results = []
        for i in range(0, len(cases), self.batch_size):
            batch = cases[i:i + self.batch_size]
            batch_results = self._evaluate_batch(batch)
            results.extend(batch_results)
        
        return results
    
    def _evaluate_batch(self, batch: list[dict]) -> list[dict]:
        """Evaluate up to 5 cases in one LLM call."""
        
        evaluations_prompt = "Evaluate each Q&A pair. For each, score as: CORRECT, PARTIAL, or INCORRECT.\n\n"
        
        for i, case in enumerate(batch, 1):
            evaluations_prompt += f"""Case {i}:
Question: {case['question']}
Expected: {case['ground_truth']}
Generated: {case['answer'][:200]}
---
"""
        
        evaluations_prompt += "\nFor each case, respond with: Case N: SCORE (one word reason)\n"
        
        response = self.client.chat.completions.create(
            model="gpt-4o-mini",  # 10x cheaper than gpt-4o
            messages=[{"role": "user", "content": evaluations_prompt}],
            temperature=0,
            max_tokens=200,
        )
        
        # Parse results
        text = response.choices[0].message.content
        scores = []
        for i in range(len(batch)):
            if f"Case {i+1}: CORRECT" in text:
                scores.append({"score": 1.0, "label": "correct"})
            elif f"Case {i+1}: PARTIAL" in text:
                scores.append({"score": 0.5, "label": "partial"})
            else:
                scores.append({"score": 0.0, "label": "incorrect"})
        
        return scores

# Cost comparison:
# RAGAS (gpt-4o, individual calls): 5000 queries × ~$0.02 = $100/day
# Fast judge (gpt-4o-mini, batched): 5000/5 = 1000 calls × ~$0.001 = $1/day
# Savings: 99%! Quality: ~90% agreement with RAGAS (good enough for daily monitoring)
```

**Key Points:**
- Batch 5 evaluations per LLM call: 5x fewer API calls
- Use GPT-4o-mini: 10x cheaper, still 85-90% agreement with GPT-4o judgments
- Simple scoring (CORRECT/PARTIAL/INCORRECT) is more reliable than 1-5 scale
- Cache: if same question+answer pair seen before, skip re-evaluation
- Use detailed (RAGAS) evaluation weekly, fast judge daily
- Validate fast judge against RAGAS periodically (ensure correlation holds)

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: A/B Testing Design

**Scenario:** You want to compare two RAG configurations: (A) current production with chunk_size=500 and (B) new config with semantic chunking + re-ranking. Design an A/B test that proves B is better with statistical confidence.

<details>
<summary>💡 Hint</summary>
Use your eval set to get paired comparisons (same questions, both systems). Paired t-test for statistical significance. Need at least 100+ paired samples.
</details>

<details>
<summary>✅ Solution</summary>

```python
from scipy import stats
import numpy as np

class PairedRAGABTest:
    """Paired comparison: same questions, both systems, statistical test."""
    
    def run(self, system_a, system_b, test_set: list[dict]) -> dict:
        scores_a, scores_b = [], []
        
        for case in test_set:
            # Run both systems on same question
            answer_a = system_a.answer(case["question"])
            answer_b = system_b.answer(case["question"])
            
            # Score both with same judge
            score_a = self.judge.evaluate(case["question"], answer_a, case["ground_truth"])
            score_b = self.judge.evaluate(case["question"], answer_b, case["ground_truth"])
            
            scores_a.append(score_a)
            scores_b.append(score_b)
        
        # Paired t-test (accounts for question difficulty variation)
        t_stat, p_value = stats.ttest_rel(scores_b, scores_a)  # B - A
        
        # Effect size (Cohen's d for paired samples)
        differences = np.array(scores_b) - np.array(scores_a)
        cohens_d = differences.mean() / differences.std()
        
        # Win/loss/tie analysis
        wins_b = sum(1 for a, b in zip(scores_a, scores_b) if b > a)
        wins_a = sum(1 for a, b in zip(scores_a, scores_b) if a > b)
        ties = len(scores_a) - wins_b - wins_a
        
        return {
            "system_a_mean": np.mean(scores_a),
            "system_b_mean": np.mean(scores_b),
            "improvement": np.mean(scores_b) - np.mean(scores_a),
            "p_value": p_value,
            "significant": p_value < 0.05,
            "cohens_d": cohens_d,  # >0.2 small, >0.5 medium, >0.8 large effect
            "wins": {"B_wins": wins_b, "A_wins": wins_a, "ties": ties},
            "n_samples": len(test_set),
            "power_sufficient": len(test_set) >= 100,  # Need 100+ for reliable test
            "recommendation": "DEPLOY B" if (p_value < 0.05 and np.mean(scores_b) > np.mean(scores_a)) else "KEEP A",
        }

# Run test
test = PairedRAGABTest()
result = test.run(system_a=production_rag, system_b=new_rag, test_set=golden_200)
print(f"B improves by {result['improvement']:.3f} (p={result['p_value']:.4f})")
# "B improves by 0.082 (p=0.003)" → statistically significant, deploy B!
```

**Key Points:**
- PAIRED test (same questions) removes question-difficulty confound
- Need 100+ test questions for statistical power (200 recommended)
- p < 0.05 = statistically significant (unlikely due to chance)
- Cohen's d shows practical significance: >0.2 = meaningful improvement
- Win/loss analysis gives intuitive understanding (B wins on 60% of questions)
- If p > 0.05: systems are equivalent — keep A (avoid unnecessary change risk)

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Evaluation Pipeline

**Scenario:** Your team makes changes to the RAG system weekly. Sometimes changes help, sometimes they regress quality (and you don't notice for days). Build an automated evaluation gate in your CI/CD pipeline.

<details>
<summary>💡 Hint</summary>
Run evaluation as part of the PR/merge process. If quality drops below baseline, block the deployment. Store baseline from the last successful deploy.
</details>

<details>
<summary>✅ Solution</summary>

```python
# ci_eval.py — runs in CI/CD pipeline on every PR that touches RAG code
import sys
import json

class CIEvalGate:
    """Block deploys that degrade RAG quality."""
    
    def __init__(self, baseline_path: str = "eval/baseline.json"):
        self.baseline = json.load(open(baseline_path))
        self.test_set = json.load(open("eval/golden_test_set.json"))
        self.max_regression = 0.03  # Allow max 3% regression
    
    def run_gate(self) -> bool:
        """Returns True if quality is acceptable, False to block deploy."""
        
        # Run evaluation on current code
        current_scores = self.evaluate_current()
        
        # Compare to baseline
        regressions = []
        for metric, score in current_scores.items():
            baseline_score = self.baseline.get(metric, 0)
            if score < baseline_score - self.max_regression:
                regressions.append({
                    "metric": metric,
                    "baseline": baseline_score,
                    "current": score,
                    "drop": baseline_score - score,
                })
        
        if regressions:
            print("EVAL GATE: FAILED")
            for r in regressions:
                print(f"  {r['metric']}: {r['baseline']:.3f} → {r['current']:.3f} (dropped {r['drop']:.3f})")
            print("\nBlock deploy. Fix quality before merging.")
            return False
        else:
            print("EVAL GATE: PASSED")
            print(f"  Scores: {json.dumps(current_scores, indent=2)}")
            
            # Update baseline if scores improved
            if all(current_scores[m] >= self.baseline.get(m, 0) for m in current_scores):
                self.update_baseline(current_scores)
                print("  Baseline updated (scores improved)")
            
            return True
    
    def evaluate_current(self) -> dict:
        """Run RAG system on test set, return scores."""
        # ... (standard evaluation logic)
        pass
    
    def update_baseline(self, new_scores: dict):
        with open("eval/baseline.json", "w") as f:
            json.dump(new_scores, f, indent=2)

# Usage in CI (GitHub Actions, etc.):
if __name__ == "__main__":
    gate = CIEvalGate()
    passed = gate.run_gate()
    sys.exit(0 if passed else 1)

# .github/workflows/rag-eval.yml:
# on: pull_request
# jobs:
#   eval:
#     runs-on: ubuntu-latest
#     steps:
#       - run: python ci_eval.py  # Fails the PR if quality drops
```

**Key Points:**
- Runs on every PR that touches RAG-related code (chunking, prompts, retrieval)
- Blocks merge if ANY metric drops >3% below baseline
- Auto-updates baseline when scores improve (ratchet effect — quality only goes up)
- Uses a small test set (50-100 questions) for fast CI runs (~2-5 minutes)
- Full nightly eval (200 questions) catches slower regressions
- Teams that gate on eval see 3-5x fewer production quality incidents

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Continuous Evaluation Platform

**Scenario:** Design a comprehensive evaluation platform that: (1) runs nightly evals on a fixed test set, (2) samples and evaluates 1% of production traffic, (3) detects regression automatically, (4) integrates user feedback, and (5) drives optimization decisions with a dashboard.

<details>
<summary>💡 Hint</summary>
This is a full system design. Key components: eval scheduler (Airflow), production sampler (1% traffic), LLM judge service, metrics store (TimescaleDB/Prometheus), regression detector, alert system, and Grafana dashboard.
</details>

<details>
<summary>✅ Solution</summary>

```python
# ARCHITECTURE:
# Airflow DAG (nightly) → runs golden test set → stores scores in TimescaleDB
# Production sampler (continuous) → scores 1% of live traffic → Prometheus metrics
# Regression detector (hourly) → compares rolling avg to baseline → alerts
# Dashboard (Grafana) → visualizes all metrics + trends
# Feedback loop: user corrections → training data pipeline

class EvaluationPlatform:
    """Production-grade RAG evaluation platform."""
    
    def __init__(self):
        self.nightly_evaluator = NightlyEvaluator(test_set_size=200)
        self.production_sampler = ProductionSampler(sample_rate=0.01)
        self.regression_detector = RegressionDetector(sensitivity=0.05)
        self.metrics_store = TimescaleDB("metrics")
        self.dashboard = GrafanaDashboard()
    
    # Component 1: Nightly offline eval (fixed test set)
    def nightly_eval(self):
        """Airflow DAG: 2 AM daily."""
        scores = self.nightly_evaluator.run(self.rag_system)
        self.metrics_store.store("nightly_eval", scores)
        self.regression_detector.check(scores)
    
    # Component 2: Production sampling (live traffic)
    def on_production_request(self, query: str, answer: str, contexts: list[str]):
        """Called for every RAG response, samples 1%."""
        if random.random() < 0.01:
            # Async: don't block the response
            background_eval.delay(query, answer, contexts)
    
    # Component 3: Regression detection
    def hourly_regression_check(self):
        """Compare last hour's scores to 7-day rolling baseline."""
        recent = self.metrics_store.query(last_hours=1)
        baseline = self.metrics_store.query(last_days=7)
        
        for metric in ["faithfulness", "relevance", "retrieval_score"]:
            recent_avg = np.mean([r[metric] for r in recent])
            baseline_avg = np.mean([r[metric] for r in baseline])
            
            if recent_avg < baseline_avg - 0.05:
                self.alert(f"REGRESSION: {metric} dropped from {baseline_avg:.2f} to {recent_avg:.2f}")
    
    # Component 4: User feedback integration
    def process_feedback(self, query_id: str, feedback: str, correction: str = None):
        """Integrate user feedback into evaluation metrics."""
        self.metrics_store.store("user_feedback", {
            "query_id": query_id,
            "type": feedback,
            "correction": correction,
            "timestamp": datetime.now(),
        })
        
        # Corrections become training data
        if correction:
            self.training_data_pipeline.add(query_id, correction)
    
    # Component 5: Dashboard metrics
    def get_dashboard_data(self) -> dict:
        return {
            "quality_7d_trend": self.metrics_store.query_trend("correctness", days=7),
            "user_satisfaction": self.metrics_store.query_trend("thumbs_up_rate", days=7),
            "retrieval_score_distribution": self.metrics_store.query_histogram("retrieval_score"),
            "hallucination_rate": self.metrics_store.query_latest("hallucination_rate"),
            "no_answer_rate": self.metrics_store.query_latest("no_answer_rate"),
            "latency_p99": self.metrics_store.query_latest("latency_p99"),
        }

# COST:
# Nightly eval (200 questions): ~$5/day (LLM judge calls)
# Production sampling (1% of 10K daily): 100 evals × $0.01 = $1/day
# Infrastructure (TimescaleDB, Grafana): ~$100/month
# Total: ~$280/month for comprehensive quality monitoring
```

**Key Points:**
- Nightly fixed test set: consistent, catches code-related regressions
- Production sampling: catches data-related issues, query distribution shifts
- Regression detection: automated alerting prevents silent quality degradation
- User feedback: ground truth from real users, drives training improvements
- Dashboard: gives entire team visibility into RAG health
- Cost: ~$280/month is trivial compared to the value of catching quality issues early
- ROI: prevents incidents that would cost engineering days to debug

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Multi-Dimensional Optimization

**Scenario:** You need to improve RAG quality from 78% to 90%+ accuracy while keeping p99 latency under 2 seconds and cost under $0.02/query. These constraints conflict: better quality usually means more compute. Design the optimization approach.

<details>
<summary>💡 Hint</summary>
Multi-objective optimization: find the Pareto frontier of quality vs latency vs cost. Key: not all quality improvements add latency — some are free (better chunking), some add latency (re-ranking), some are expensive (stronger model).
</details>

<details>
<summary>✅ Solution</summary>

```python
class MultiObjectiveOptimizer:
    """Find optimal RAG config balancing quality, latency, and cost."""
    
    def __init__(self, eval_set: list[dict], constraints: dict):
        self.eval_set = eval_set
        self.constraints = constraints  # {"max_latency_p99_ms": 2000, "max_cost_per_query": 0.02}
    
    def evaluate_config(self, config: dict) -> dict:
        """Run evaluation with a specific RAG configuration."""
        rag = build_rag_system(config)
        
        scores, latencies, costs = [], [], []
        for case in self.eval_set:
            start = time.time()
            result = rag.answer(case["question"])
            latency = (time.time() - start) * 1000
            
            score = self.judge.evaluate(case["question"], result["answer"], case["ground_truth"])
            cost = result.get("total_cost", 0)
            
            scores.append(score)
            latencies.append(latency)
            costs.append(cost)
        
        return {
            "config": config,
            "quality": np.mean(scores),
            "latency_p99": np.percentile(latencies, 99),
            "avg_cost": np.mean(costs),
            "feasible": (
                np.percentile(latencies, 99) <= self.constraints["max_latency_p99_ms"]
                and np.mean(costs) <= self.constraints["max_cost_per_query"]
            ),
        }
    
    def search_configs(self) -> list[dict]:
        """Evaluate a grid of configurations."""
        configs = [
            # Free improvements (no extra cost/latency)
            {"chunk_size": 400, "model": "gpt-4o-mini", "rerank": False, "top_k": 5},
            {"chunk_size": 500, "model": "gpt-4o-mini", "rerank": False, "top_k": 5},
            # Add re-ranking (small latency cost, significant quality gain)
            {"chunk_size": 400, "model": "gpt-4o-mini", "rerank": True, "top_k": 5},
            # Stronger model (higher cost)
            {"chunk_size": 400, "model": "gpt-4o", "rerank": True, "top_k": 5},
            # Hybrid search (small latency cost)
            {"chunk_size": 400, "model": "gpt-4o-mini", "rerank": True, "hybrid": True, "top_k": 5},
        ]
        
        results = [self.evaluate_config(c) for c in configs]
        
        # Filter to feasible configs only
        feasible = [r for r in results if r["feasible"]]
        
        # Sort by quality (among feasible)
        feasible.sort(key=lambda r: r["quality"], reverse=True)
        
        return feasible

# Typical optimization path:
# Baseline: quality=0.78, latency=800ms, cost=$0.003
# + Better chunking: quality=0.82, latency=800ms, cost=$0.003 (FREE improvement)
# + Re-ranking: quality=0.87, latency=950ms, cost=$0.004 (small latency cost)
# + Hybrid search: quality=0.90, latency=1000ms, cost=$0.004 (hit target!)
# + Stronger model: quality=0.93, latency=1500ms, cost=$0.015 (over budget — skip)

# Winner: chunking=400 + re-ranking + hybrid, gpt-4o-mini
# quality=0.90, latency_p99=1200ms, cost=$0.004 — meets ALL constraints!
```

**Key Points:**
- Not all improvements cost latency/money — always try free wins first (chunking, prompts)
- Re-ranking adds 50-100ms but gives 5-8% quality gain (best ROI intervention)
- Stronger models (4o vs mini) give 3-5% but at 10x cost — usually not worth it
- Hybrid search adds 10-20ms for 3-5% quality (nearly free)
- Evaluate the FULL trade-off space before deciding — some configs dominate others
- "Feasible" = meets ALL hard constraints; among feasible, pick highest quality

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Regression Detection System

**Scenario:** Last month, a chunking change caused a 12% quality drop that wasn't detected for 5 days (only noticed when users complained). Design an automated regression detection system that catches quality drops within hours, not days.

<details>
<summary>💡 Hint</summary>
Statistical process control: track quality metrics as a time series, detect when they shift outside normal variance. Use CUSUM or exponential weighted moving average for sensitive change detection.
</details>

<details>
<summary>✅ Solution</summary>

```python
import numpy as np
from collections import deque

class QualityRegressionDetector:
    """Detect RAG quality shifts within 1-2 hours using statistical process control."""
    
    def __init__(self, warmup_samples: int = 500, sensitivity: float = 2.0):
        self.warmup_samples = warmup_samples
        self.sensitivity = sensitivity
        self.baseline_mean = None
        self.baseline_std = None
        self.cusum_pos = 0.0
        self.cusum_neg = 0.0
        self.samples = deque(maxlen=10000)
        self.alert_cooldown = 0
    
    def observe(self, quality_score: float) -> dict:
        """Called for each evaluated query. Returns alert if regression detected."""
        self.samples.append(quality_score)
        
        # Warmup phase: collect baseline statistics
        if len(self.samples) < self.warmup_samples:
            return {"status": "warmup", "samples": len(self.samples)}
        
        # Compute baseline (rolling window)
        if self.baseline_mean is None:
            self.baseline_mean = np.mean(list(self.samples)[:self.warmup_samples])
            self.baseline_std = np.std(list(self.samples)[:self.warmup_samples])
        
        # CUSUM (Cumulative Sum) change detection
        deviation = self.baseline_mean - quality_score
        slack = self.sensitivity * self.baseline_std
        
        self.cusum_pos = max(0, self.cusum_pos + deviation - slack)
        self.cusum_neg = max(0, self.cusum_neg - deviation - slack)
        
        threshold = 5 * self.baseline_std  # Alert threshold
        
        regression_detected = self.cusum_pos > threshold
        improvement_detected = self.cusum_neg > threshold
        
        if regression_detected and self.alert_cooldown <= 0:
            self.alert_cooldown = 100  # Don't alert again for 100 samples
            recent_mean = np.mean(list(self.samples)[-50:])
            return {
                "status": "REGRESSION",
                "baseline_mean": self.baseline_mean,
                "recent_mean": recent_mean,
                "drop_pct": (self.baseline_mean - recent_mean) / self.baseline_mean * 100,
                "cusum_value": self.cusum_pos,
                "message": f"Quality dropped {(self.baseline_mean - recent_mean)/self.baseline_mean*100:.1f}% below baseline",
            }
        
        self.alert_cooldown = max(0, self.alert_cooldown - 1)
        return {"status": "normal", "cusum_pos": self.cusum_pos, "cusum_neg": self.cusum_neg}
    
    def reset_baseline(self):
        """Called after a legitimate change (e.g., intentional model update)."""
        recent = list(self.samples)[-200:]
        self.baseline_mean = np.mean(recent)
        self.baseline_std = np.std(recent)
        self.cusum_pos = 0.0
        self.cusum_neg = 0.0

# Integration:
detector = QualityRegressionDetector(warmup_samples=500, sensitivity=1.5)

# Called for every production-sampled evaluation:
def on_eval_result(quality_score: float):
    result = detector.observe(quality_score)
    if result["status"] == "REGRESSION":
        send_pagerduty_alert(
            f"RAG quality regression: {result['message']}\n"
            f"Baseline: {result['baseline_mean']:.3f}, Current: {result['recent_mean']:.3f}"
        )

# Detection speed:
# With 1% sampling rate and 10K queries/day = 100 evals/day
# CUSUM detects 10% drop within ~50 samples = ~12 hours
# For faster detection: increase sample rate to 5% → detect within 3 hours
```

**Key Points:**
- CUSUM detects small, sustained shifts that simple threshold alerting misses
- Sensitivity parameter trades detection speed vs false alarm rate
- 1% sampling + CUSUM: detects 10% drop in 12 hours (vs 5 days with complaints)
- 5% sampling: detects same drop in 3 hours (more expensive but faster)
- Cooldown prevents alert storms during known issues
- Reset baseline after intentional changes (model updates, re-chunking)
- Store CUSUM values: helps post-mortem analysis of when degradation started

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Eval-Driven Architecture Decisions

**Scenario:** Your RAG system scores 82% accuracy. The team proposes three improvements: (A) switch from fixed to semantic chunking, (B) add cross-encoder re-ranking, (C) upgrade from GPT-4o-mini to GPT-4o. Each has different cost/complexity. Use evaluation to decide which to implement.

<details>
<summary>💡 Hint</summary>
Implement each change in isolation, evaluate on the same test set, compare improvements. The best ROI = biggest quality gain per unit of added cost/complexity.
</details>

<details>
<summary>✅ Solution</summary>

```python
class EvalDrivenDecision:
    """Use evaluation data to prioritize engineering work."""
    
    def compare_interventions(self, test_set: list[dict]) -> dict:
        """Evaluate each proposed change in isolation."""
        
        # Baseline
        baseline = self.evaluate(current_system, test_set)
        
        # Option A: Semantic chunking (requires re-indexing, 1 week effort)
        system_a = build_system(chunking="semantic")
        result_a = self.evaluate(system_a, test_set)
        
        # Option B: Add re-ranking (simple addition, 2 day effort)
        system_b = build_system(reranking=True)
        result_b = self.evaluate(system_b, test_set)
        
        # Option C: Upgrade to GPT-4o (config change, 1 hour, but 10x cost)
        system_c = build_system(model="gpt-4o")
        result_c = self.evaluate(system_c, test_set)
        
        comparison = {
            "baseline": {"quality": baseline, "cost": "$0.003/query", "effort": "0"},
            "A_semantic_chunking": {
                "quality": result_a,
                "improvement": result_a - baseline,
                "cost": "$0.003/query (same)",
                "effort": "1 week",
                "roi": (result_a - baseline) / 5,  # improvement per day of work
            },
            "B_reranking": {
                "quality": result_b,
                "improvement": result_b - baseline,
                "cost": "$0.004/query (+$0.001)",
                "effort": "2 days",
                "roi": (result_b - baseline) / 2,
            },
            "C_gpt4o": {
                "quality": result_c,
                "improvement": result_c - baseline,
                "cost": "$0.015/query (5x increase!)",
                "effort": "1 hour",
                "roi": (result_c - baseline) / 0.1,  # High ROI in effort, but expensive
            },
        }
        
        # Decision matrix
        # A: +4% quality, $0 extra cost, 5 day effort → good long-term
        # B: +6% quality, $0.001 extra, 2 day effort → BEST ROI
        # C: +3% quality, $0.012 extra, 0.1 day effort → too expensive per query
        
        return {
            "recommendation": "Implement B (re-ranking) first — highest quality gain per effort.",
            "second_priority": "Then A (semantic chunking) for additional free improvement.",
            "skip": "C (GPT-4o) — marginal quality gain doesn't justify 5x cost increase.",
            "details": comparison,
        }

# The eval data PROVES which intervention is best — no guessing, no opinions.
# Without eval: "let's just use GPT-4o" (expensive, small gain)
# With eval: "re-ranking gives 6% gain for $0.001 extra" (much better ROI)
```

**Key Points:**
- Evaluate each change in ISOLATION to measure its individual impact
- ROI = quality improvement / (effort + ongoing cost) — pick the highest ROI first
- Some changes are free (better chunking) — always try these first
- Expensive changes (model upgrades) often give surprisingly small marginal gains
- The eval data removes opinion from architecture debates — "the numbers say..."
- Stack interventions: after implementing the winner, re-evaluate to see if the next one still helps (improvements may not be additive)

</details>

</article>
