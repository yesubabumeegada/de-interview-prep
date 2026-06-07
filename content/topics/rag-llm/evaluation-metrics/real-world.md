---
title: "Evaluation Metrics - Real-World Production Examples"
topic: rag-llm
subtopic: evaluation-metrics
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [rag, llm, evaluation, production, monitoring, dashboard, case-study]
---

# RAG Evaluation Metrics — Real-World Production Examples

## Pattern 1: Nightly Evaluation Pipeline

```python
from datetime import datetime
import json

class NightlyEvalPipeline:
    """Automated nightly evaluation with regression alerts."""
    
    def __init__(self, rag_service, judge, test_set_path: str, results_db):
        self.rag = rag_service
        self.judge = judge
        self.test_set = self.load_test_set(test_set_path)
        self.db = results_db
    
    def run(self) -> dict:
        """Execute full nightly evaluation. Called by Airflow at 2 AM."""
        run_id = f"eval_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        results = []
        
        for case in self.test_set:
            # Run RAG pipeline
            rag_response = self.rag.answer(case["question"])
            
            # Score with LLM judge
            scores = self.judge.evaluate_answer(
                question=case["question"],
                answer=rag_response["answer"],
                context=rag_response["contexts"],
                ground_truth=case.get("expected_answer"),
            )
            
            results.append({
                "question": case["question"],
                "answer": rag_response["answer"],
                "scores": scores,
                "retrieval_top_score": rag_response.get("top_score", 0),
                "latency_ms": rag_response.get("latency_ms", 0),
            })
        
        # Compute aggregates
        summary = {
            "run_id": run_id,
            "timestamp": datetime.now().isoformat(),
            "total_questions": len(results),
            "avg_correctness": np.mean([r["scores"]["correctness"]["score"] for r in results]),
            "avg_faithfulness": np.mean([r["scores"]["faithfulness"]["score"] for r in results]),
            "avg_relevance": np.mean([r["scores"]["relevance"]["score"] for r in results]),
            "avg_retrieval_score": np.mean([r["retrieval_top_score"] for r in results]),
            "avg_latency_ms": np.mean([r["latency_ms"] for r in results]),
            "failure_rate": sum(1 for r in results if r["scores"]["correctness"]["score"] <= 2) / len(results),
        }
        
        # Store results
        self.db.store_eval_run(summary, results)
        
        # Check for regression
        previous = self.db.get_previous_run()
        if previous and summary["avg_correctness"] < previous["avg_correctness"] - 0.05:
            self.send_regression_alert(summary, previous)
        
        return summary
    
    def send_regression_alert(self, current: dict, previous: dict):
        """Alert team about quality regression."""
        message = f"""RAG Quality Regression Detected!
Correctness: {previous['avg_correctness']:.2f} → {current['avg_correctness']:.2f} ({(current['avg_correctness']-previous['avg_correctness'])*100:+.1f}%)
Faithfulness: {previous['avg_faithfulness']:.2f} → {current['avg_faithfulness']:.2f}
Retrieval: {previous['avg_retrieval_score']:.2f} → {current['avg_retrieval_score']:.2f}

Action: Check recent changes to embeddings, chunking, or prompts."""
        
        self.alerts.send_slack("#ml-ops", message)
```

---

## Pattern 2: Production Quality Dashboard Metrics

```python
from prometheus_client import Gauge, Histogram, Counter

# Prometheus metrics for Grafana dashboard
RAG_QUALITY_SCORE = Gauge("rag_quality_score", "Average quality score (1-5)", ["dimension"])
RAG_FAILURE_RATE = Gauge("rag_failure_rate", "Percentage of poor-quality responses")
RAG_RETRIEVAL_SCORE = Histogram("rag_retrieval_top_score", "Top-1 similarity score", buckets=[0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9])
USER_SATISFACTION = Gauge("rag_user_satisfaction_rate", "Thumbs-up rate over last hour")
NO_ANSWER_RATE = Gauge("rag_no_answer_rate", "Rate of I dont know responses")

class DashboardMetrics:
    """Emit real-time metrics for monitoring dashboard."""
    
    def record_response(self, query: str, answer: str, top_score: float, user_feedback: str = None):
        """Called for every production RAG response."""
        RAG_RETRIEVAL_SCORE.observe(top_score)
        
        # Track "I don't know" rate
        if "don't have information" in answer.lower() or "cannot answer" in answer.lower():
            self.no_answer_count += 1
        
        # Track user feedback
        if user_feedback == "positive":
            self.positive_feedback += 1
        elif user_feedback == "negative":
            self.negative_feedback += 1
    
    def update_gauges(self):
        """Called every 5 minutes to update dashboard gauges."""
        total = self.positive_feedback + self.negative_feedback
        if total > 0:
            USER_SATISFACTION.set(self.positive_feedback / total)
        
        total_responses = self.response_count
        if total_responses > 0:
            NO_ANSWER_RATE.set(self.no_answer_count / total_responses)

# Grafana dashboard panels:
# 1. Quality score trend (7-day rolling average)
# 2. Retrieval score distribution (histogram)
# 3. User satisfaction rate (hourly)
# 4. "I don't know" rate (should be <10%)
# 5. Latency percentiles (p50, p99)
# 6. Error rate by query category
```

---

## Pattern 3: Iterative Quality Improvement

Case study: improving RAG accuracy from 65% to 91% through systematic evaluation.

```python
# WEEK 1: Baseline measurement
# Ran 200 test queries, scored with LLM judge
# Result: 65% correctness, 58% faithfulness
# Diagnosis: retrieval_failures=35%, hallucination_rate=22%

# WEEK 2: Fix retrieval (biggest issue)
# Change: chunk_size 1000→500, added hybrid search
# Result: 74% correctness (+9%), retrieval failures down to 18%
# Diagnosis: hallucination_rate still 20% (need to fix generation)

# WEEK 3: Fix hallucination
# Change: stricter prompt, temperature 0.3→0, added "cite sources"
# Result: 82% correctness (+8%), faithfulness up to 85%
# Diagnosis: remaining failures are "no relevant docs" scenarios

# WEEK 4: Fix coverage gaps
# Change: added 500 more docs to knowledge base, improved chunking for tables
# Result: 88% correctness (+6%), no_answer_rate down from 15% to 5%

# WEEK 5: Fine-tune re-ranker
# Change: trained domain-specific cross-encoder for re-ranking
# Result: 91% correctness (+3%), retrieval precision improved significantly

improvement_log = [
    {"week": 1, "change": "Baseline", "correctness": 0.65},
    {"week": 2, "change": "Better chunking + hybrid search", "correctness": 0.74},
    {"week": 3, "change": "Stricter prompt + temp=0", "correctness": 0.82},
    {"week": 4, "change": "Expanded knowledge base", "correctness": 0.88},
    {"week": 5, "change": "Domain re-ranker", "correctness": 0.91},
]
# Each week: evaluate → diagnose → fix → re-evaluate → repeat
```

---

## Pattern 4: User Feedback Collection

```python
class FeedbackCollector:
    """Collect and analyze user feedback for quality monitoring."""
    
    def record_feedback(self, query_id: str, feedback_type: str, details: str = None):
        """Record thumbs up/down or correction."""
        self.db.insert({
            "query_id": query_id,
            "feedback_type": feedback_type,  # "positive", "negative", "correction"
            "details": details,              # User's correction text
            "timestamp": datetime.now(),
        })
    
    def analyze_feedback(self, days: int = 7) -> dict:
        """Weekly analysis of user feedback patterns."""
        feedback = self.db.query(last_n_days=days)
        
        positive = [f for f in feedback if f["feedback_type"] == "positive"]
        negative = [f for f in feedback if f["feedback_type"] == "negative"]
        corrections = [f for f in feedback if f["feedback_type"] == "correction"]
        
        # Satisfaction rate
        total_rated = len(positive) + len(negative)
        satisfaction = len(positive) / total_rated if total_rated > 0 else 0
        
        # Analyze negative feedback patterns
        negative_queries = [f["query_id"] for f in negative]
        negative_details = self.enrich_with_query_data(negative_queries)
        
        # Categorize failure types
        categories = self.categorize_failures(negative_details)
        
        return {
            "satisfaction_rate": satisfaction,
            "total_feedback": len(feedback),
            "corrections_available": len(corrections),  # These become training data!
            "top_failure_categories": categories,
            "recommendation": self.generate_recommendation(categories),
        }
    
    def corrections_to_training_data(self) -> list[dict]:
        """Convert user corrections into fine-tuning training data."""
        corrections = self.db.query(feedback_type="correction", verified=True)
        
        training_pairs = []
        for correction in corrections:
            original_query = self.get_query(correction["query_id"])
            training_pairs.append({
                "messages": [
                    {"role": "system", "content": "Answer data engineering questions accurately."},
                    {"role": "user", "content": original_query["question"]},
                    {"role": "assistant", "content": correction["details"]},  # User's correction
                ]
            })
        
        return training_pairs  # Feed to fine-tuning pipeline!
```

---

## Pattern 5: Hallucination Detection in Production

```python
class HallucinationDetector:
    """Detect hallucinated responses in real-time."""
    
    def check_response(self, question: str, answer: str, contexts: list[str]) -> dict:
        """Quick hallucination check (runs on 5% of traffic)."""
        
        # Method 1: NLI-based (fast, no LLM call)
        # Check if answer is entailed by context
        nli_score = self.nli_model.predict(
            premise=" ".join(contexts),
            hypothesis=answer
        )
        
        # Method 2: Claim decomposition (thorough, uses LLM)
        if nli_score < 0.7:  # Only if NLI is uncertain
            claims = self.extract_claims(answer)
            unsupported = []
            
            for claim in claims:
                supported = self.check_claim_support(claim, contexts)
                if not supported:
                    unsupported.append(claim)
            
            hallucination_detected = len(unsupported) > 0
        else:
            hallucination_detected = False
            unsupported = []
        
        return {
            "hallucination_detected": hallucination_detected,
            "nli_score": nli_score,
            "unsupported_claims": unsupported,
            "confidence": "high" if nli_score > 0.9 or nli_score < 0.3 else "medium",
        }
    
    def extract_claims(self, answer: str) -> list[str]:
        """Break answer into individual factual claims."""
        response = self.client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": f"List each factual claim in this text, one per line:\n{answer}"}],
            temperature=0,
        )
        return [c.strip() for c in response.choices[0].message.content.split("\n") if c.strip()]
```

---

## Interview Tips

> **Tip 1:** "How do you monitor RAG quality in production?" — Three layers: (1) Lightweight signals for every request (top retrieval score, answer length, "I don't know" rate), (2) LLM judge sampling on 1-5% of traffic (quality scoring), (3) User feedback (thumbs up/down, corrections). Dashboard with alerting on trend degradation.

> **Tip 2:** "How did you improve RAG accuracy from X to Y?" — Systematic approach: measure baseline → diagnose (retrieval failures? hallucination? coverage gaps?) → fix highest-impact issue → re-measure → repeat. Each iteration addresses one root cause. Document the improvement log for the team.

> **Tip 3:** "How do you detect hallucination in production?" — NLI model for fast screening (is answer entailed by context?), LLM-based claim verification for uncertain cases (decompose answer into claims, check each against context), and user feedback as ground truth (corrections = confirmed hallucinations). Alert if hallucination rate exceeds 5%.
