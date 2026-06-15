---
title: "LLM Guardrails - Senior Deep Dive"
topic: rag-llm
subtopic: llm-guardrails
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [rag, llm, guardrails, constitutional-ai, rlhf, production, hipaa, gdpr]
---

# LLM Guardrails — Senior Deep Dive

## Constitutional AI (CAI)

Constitutional AI (Anthropic, 2022) is a training-time technique that makes models self-critique and revise outputs against a set of principles:

1. **Generate** — model produces an initial response
2. **Critique** — model identifies violations of the constitution
3. **Revise** — model rewrites to comply
4. **RLHF** — train on revised outputs using RL with human feedback on rankings

As a DE, you can apply CAI principles at inference time (without retraining) using a critique-revise prompt pattern:

```python
from openai import OpenAI
import json

client = OpenAI()

CONSTITUTION = """
You must follow these principles:
1. ACCURACY: Never state facts you cannot verify from the provided context
2. PRIVACY: Never reveal or infer personal information about individuals
3. SAFETY: Never provide information that could cause harm
4. RELEVANCE: Stay strictly on the topic of data engineering
5. TRANSPARENCY: Acknowledge uncertainty when you don't know something
"""

def constitutional_generate(user_message: str, context: str = "") -> dict:
    """Three-step CAI pipeline: generate → critique → revise."""

    # Step 1: Initial generation
    initial = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": f"You are a data engineering assistant.\n{CONSTITUTION}"},
            {"role": "user", "content": f"Context: {context}\n\nQuestion: {user_message}"}
        ]
    )
    initial_response = initial.choices[0].message.content

    # Step 2: Self-critique
    critique = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": f"You are a strict editor enforcing these principles:\n{CONSTITUTION}"},
            {"role": "user", "content": f"""Review this response for violations:

Original question: {user_message}
Response: {initial_response}

List ALL violations as JSON: {{"violations": [{{"principle": "...", "issue": "...", "severity": "LOW|MEDIUM|HIGH"}}]}}
If no violations, return {{"violations": []}}"""}
        ],
        response_format={"type": "json_object"},
        temperature=0
    )
    violations = json.loads(critique.choices[0].message.content)

    # Step 3: Revise only if violations found
    if not violations["violations"]:
        return {"response": initial_response, "violations": [], "revised": False}

    high_violations = [v for v in violations["violations"] if v["severity"] in ("MEDIUM", "HIGH")]
    if not high_violations:
        return {"response": initial_response, "violations": violations["violations"], "revised": False}

    revision = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": CONSTITUTION},
            {"role": "user", "content": f"""Rewrite this response to fix these violations:

Violations: {json.dumps(high_violations, indent=2)}

Original response: {initial_response}

Write only the revised response, no commentary."""}
        ]
    )

    return {
        "response": revision.choices[0].message.content,
        "violations": violations["violations"],
        "revised": True,
        "original_response": initial_response
    }
```

---

## RLHF Overview for Interview

RLHF (Reinforcement Learning from Human Feedback) has three stages:

```
Stage 1: Supervised Fine-Tuning (SFT)
  - Collect high-quality (prompt, response) pairs from humans
  - Fine-tune base LLM on these examples

Stage 2: Reward Model Training
  - Collect human preference data: (prompt, response_A, response_B, winner)
  - Train a reward model: RM(prompt, response) → scalar score
  - RM learns to predict human preferences

Stage 3: RL Fine-tuning (PPO)
  - Use PPO to optimize LLM to maximize RM score
  - KL divergence penalty prevents model from diverging too far from SFT checkpoint
  - LLM learns to generate responses humans prefer
```

As a DE, you'll rarely train RLHF from scratch, but you need to understand:
- Why RLHF outputs feel "safer" than base models
- The "reward hacking" problem — model finds outputs that score high but aren't actually good
- Constitutional AI as a cheaper alternative (no human labelers needed)

---

## Production Guardrail Pipeline Architecture

```python
from dataclasses import dataclass
from enum import Enum
from typing import Callable, Optional
import time, logging

class GuardrailAction(str, Enum):
    PASS = "pass"
    BLOCK = "block"
    REDACT = "redact"
    FLAG = "flag"      # pass but log for review

@dataclass
class GuardrailResult:
    action: GuardrailAction
    original: str
    processed: str
    flags: list[str]
    latency_ms: float
    guardrail_name: str

class GuardrailPipeline:
    """
    Composable guardrail pipeline with short-circuit on BLOCK.
    Design: each guardrail is a function (text: str) -> GuardrailResult
    """

    def __init__(self, guardrails: list[tuple[str, Callable]], fail_open: bool = False):
        """
        fail_open=True: if a guardrail crashes, pass through (availability > safety)
        fail_open=False: if a guardrail crashes, block (safety > availability)
        """
        self.guardrails = guardrails
        self.fail_open = fail_open
        self.logger = logging.getLogger(__name__)

    def run(self, text: str, context: dict = None) -> tuple[str, list[GuardrailResult]]:
        """Run all guardrails. Returns (processed_text, results_list)."""
        results = []
        current_text = text

        for name, guardrail_fn in self.guardrails:
            t0 = time.time()
            try:
                result = guardrail_fn(current_text)
                result.latency_ms = (time.time() - t0) * 1000
                result.guardrail_name = name
            except Exception as e:
                self.logger.error(f"Guardrail {name} crashed: {e}")
                if self.fail_open:
                    result = GuardrailResult(
                        action=GuardrailAction.FLAG,
                        original=current_text,
                        processed=current_text,
                        flags=[f"guardrail_error:{str(e)}"],
                        latency_ms=(time.time() - t0) * 1000,
                        guardrail_name=name
                    )
                else:
                    result = GuardrailResult(
                        action=GuardrailAction.BLOCK,
                        original=current_text,
                        processed="",
                        flags=[f"guardrail_error:{str(e)}"],
                        latency_ms=(time.time() - t0) * 1000,
                        guardrail_name=name
                    )

            results.append(result)

            if result.action == GuardrailAction.BLOCK:
                return "", results  # Short-circuit on block

            current_text = result.processed  # Pass processed text to next guardrail

        return current_text, results

# Example guardrail implementations
from presidio_analyzer import AnalyzerEngine
from presidio_anonymizer import AnonymizerEngine

_analyzer = AnalyzerEngine()
_anonymizer = AnonymizerEngine()

def pii_redaction_guardrail(text: str) -> GuardrailResult:
    results = _analyzer.analyze(text=text, language="en")
    if not results:
        return GuardrailResult(GuardrailAction.PASS, text, text, [], 0, "pii")

    anonymized = _anonymizer.anonymize(text=text, analyzer_results=results)
    return GuardrailResult(
        action=GuardrailAction.REDACT,
        original=text,
        processed=anonymized.text,
        flags=[f"pii:{r.entity_type}" for r in results],
        latency_ms=0,
        guardrail_name="pii"
    )

def length_guardrail(text: str, max_chars: int = 10000) -> GuardrailResult:
    if len(text) > max_chars:
        return GuardrailResult(
            GuardrailAction.BLOCK, text, "",
            [f"input_too_long:{len(text)}>{max_chars}"], 0, "length"
        )
    return GuardrailResult(GuardrailAction.PASS, text, text, [], 0, "length")

# Compose pipeline
input_pipeline = GuardrailPipeline([
    ("length", lambda t: length_guardrail(t, max_chars=10000)),
    ("pii_redaction", pii_redaction_guardrail),
    ("injection_detection", lambda t: inject_check_guardrail(t)),
], fail_open=False)
```

---

## GDPR/HIPAA Compliance Patterns

### GDPR Data Subject Erasure (Right to be Forgotten)

If a user's data was used in an LLM prompt and logged, you must be able to delete/anonymize it:

```python
class GDPRCompliantLLMLogger:
    """Log LLM interactions with erasure support."""

    def __init__(self, db_conn):
        self.db = db_conn

    def log_interaction(self, user_id: str, session_id: str, input_hash: str, output_hash: str, metadata: dict):
        """Log hashes only — never raw content."""
        self.db.execute("""
            INSERT INTO llm_audit_log (user_id, session_id, input_hash, output_hash, metadata, created_at)
            VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """, (user_id, session_id, input_hash, output_hash, json.dumps(metadata)))

    def erase_user_data(self, user_id: str) -> int:
        """GDPR Article 17: Right to erasure."""
        # Replace user_id with anonymized token, delete hashes
        cursor = self.db.execute("""
            UPDATE llm_audit_log
            SET user_id = 'ERASED_' || user_id,
                input_hash = 'ERASED',
                output_hash = 'ERASED',
                metadata = '{"erased": true}'
            WHERE user_id = ?
        """, (user_id,))
        return cursor.rowcount

    def data_subject_access_report(self, user_id: str) -> dict:
        """GDPR Article 15: Right of access."""
        rows = self.db.execute(
            "SELECT session_id, created_at, metadata FROM llm_audit_log WHERE user_id = ?",
            (user_id,)
        ).fetchall()
        return {
            "user_id": user_id,
            "interaction_count": len(rows),
            "sessions": [{"session_id": r[0], "timestamp": r[1], "purpose": json.loads(r[2]).get("purpose")} for r in rows]
        }
```

### HIPAA PHI Handling

```python
PHI_ENTITIES = [
    "PERSON", "PHONE_NUMBER", "EMAIL_ADDRESS", "US_SSN",
    "MEDICAL_RECORD", "DATE_TIME",  # dates can identify patients
    "US_DRIVER_LICENSE", "US_PASSPORT",
    "LOCATION"  # geographic data smaller than state is PHI
]

def hipaa_safe_prompt(clinical_text: str) -> tuple[str, dict]:
    """
    De-identify clinical text before sending to external LLM.
    Returns (de-identified text, substitution map for re-identification).
    """
    results = _analyzer.analyze(text=clinical_text, language="en", entities=PHI_ENTITIES)

    # Build substitution map for potential re-identification
    substitution_map = {}
    de_identified = clinical_text

    # Replace in reverse order to preserve character positions
    for result in sorted(results, key=lambda r: r.start, reverse=True):
        original = clinical_text[result.start:result.end]
        placeholder = f"[{result.entity_type}_{len(substitution_map)}]"
        substitution_map[placeholder] = original
        de_identified = de_identified[:result.start] + placeholder + de_identified[result.end:]

    return de_identified, substitution_map

# Usage
phi_text = "Patient John Smith (DOB: 1985-03-15, MRN: 12345678) presented with chest pain."
safe_text, mapping = hipaa_safe_prompt(phi_text)
print(safe_text)
# "Patient [PERSON_0] (DOB: [DATE_TIME_1], MRN: [MEDICAL_RECORD_2]) presented with chest pain."
# Never send mapping to external LLM — store in secure local KMS
```

---

## Interview Questions — Senior Level

**Q: What's the difference between Constitutional AI and RLHF, and when would you use each?**

A: RLHF requires expensive human preference labels and retraining. Constitutional AI uses a written constitution + self-critique at inference or fine-tuning time, without human labelers. For a data engineering team, CAI-style critique-revise prompting is implementable today without ML infrastructure. RLHF is what model providers (Anthropic, OpenAI) do to train the base models you use.

**Q: How would you design a guardrail system for a healthcare LLM pipeline that must be HIPAA-compliant?**

A: Four layers: (1) De-identify all PHI before it leaves your VPC using Presidio — never send raw PHI to cloud LLMs; (2) Use a private/on-prem model if de-identification is insufficient for the use case; (3) Audit log every LLM call with input/output hashes, user ID, purpose — immutable log (CloudWatch/S3 with object lock); (4) Output validation — ensure responses don't reconstruct or leak PHI using Llama Guard or a fine-tuned classifier. BAA (Business Associate Agreement) with your LLM provider is also required.

**Q: Your guardrail is adding 800ms of latency. How do you optimize it?**

A: (1) Run guardrails in parallel where possible (PII check and injection check can run concurrently); (2) Use smaller/faster models for classification tasks (claude-haiku-3-5 instead of gpt-4o); (3) Cache guardrail results by input hash with short TTL; (4) Move simple pattern matching (regex, keyword) before LLM-based checks; (5) Consider async guardrails for non-blocking checks — pass the response, then post-hoc flag violations.
