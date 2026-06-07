---
title: "AI - Experiment Tracking"
topic: ai
subtopic: experiment-tracking
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [ai, experiment-tracking, team-scale, model-lineage, regulatory]
---

# Experiment Tracking — Real World Patterns

## Team-Scale Experiment Management

As ML teams grow, experiment management moves from individual tracking to organizational knowledge management.

### Experiment Taxonomy

```python
from dataclasses import dataclass, field
from typing import List, Optional
from enum import Enum

class ExperimentPurpose(str, Enum):
    BASELINE = "baseline"
    FEATURE_ABLATION = "feature_ablation"
    MODEL_ARCHITECTURE = "model_architecture"
    HYPERPARAMETER_SEARCH = "hyperparameter_search"
    DATA_EXPERIMENT = "data_experiment"
    BUG_INVESTIGATION = "bug_investigation"
    PRODUCTION_RETRAINING = "production_retraining"

@dataclass
class ExperimentMetadata:
    """Rich metadata for team-wide experiment discoverability."""
    
    experiment_name: str
    purpose: ExperimentPurpose
    hypothesis: str             # What are you testing?
    conclusion: Optional[str]   # What did you learn?
    
    owner: str
    team: str
    model_use_case: str         # "churn_prediction", "fraud_detection"
    
    baseline_run_id: Optional[str]  # Run ID to compare against
    parent_experiment: Optional[str]  # For experiment lineage
    
    tags: List[str] = field(default_factory=list)
    
    # Learning — most experiments don't succeed
    result: str = "in_progress"  # "success", "failure", "inconclusive"
    failure_reason: Optional[str] = None

class TeamExperimentManager:
    """
    Manages experiments across a team.
    Provides structured templates and enforces metadata standards.
    """
    
    def __init__(self, tracking_uri: str):
        import mlflow
        mlflow.set_tracking_uri(tracking_uri)
        self.client = mlflow.tracking.MlflowClient()
    
    def create_experiment(self, metadata: ExperimentMetadata) -> str:
        """Create experiment with standardized metadata."""
        import mlflow
        
        experiment_name = f"{metadata.team}/{metadata.model_use_case}/{metadata.experiment_name}"
        
        experiment_id = mlflow.create_experiment(
            name=experiment_name,
            tags={
                "purpose": metadata.purpose.value,
                "hypothesis": metadata.hypothesis,
                "owner": metadata.owner,
                "team": metadata.team,
                "use_case": metadata.model_use_case,
                "parent_experiment": metadata.parent_experiment or "",
                "tags": ",".join(metadata.tags),
            },
        )
        
        return experiment_id
    
    def conclude_experiment(
        self,
        experiment_name: str,
        result: str,
        conclusion: str,
        failure_reason: Optional[str] = None,
    ):
        """Document experiment conclusion for team learning."""
        experiment = self.client.get_experiment_by_name(experiment_name)
        
        self.client.set_experiment_tag(experiment.experiment_id, "result", result)
        self.client.set_experiment_tag(experiment.experiment_id, "conclusion", conclusion)
        
        if failure_reason:
            self.client.set_experiment_tag(experiment.experiment_id, "failure_reason", failure_reason)
        
        print(f"Experiment {experiment_name} concluded: {result}")
    
    def find_related_experiments(self, use_case: str, tags: List[str] = None) -> list:
        """Search for related past experiments to avoid duplicate work."""
        import mlflow
        experiments = self.client.search_experiments(
            filter_string=f"tags.use_case = '{use_case}'"
        )
        return experiments
```

---

## Model Lineage Tracking

Model lineage traces the full chain from raw data to production model.

```python
from dataclasses import dataclass, field
from typing import List, Optional, Dict
from datetime import datetime

@dataclass
class ModelLineage:
    """Complete lineage record for a production model."""
    
    # Identity
    model_name: str
    model_version: str
    mlflow_run_id: str
    
    # Code lineage
    git_sha: str
    git_branch: str
    pipeline_version: str
    
    # Data lineage
    training_data_sources: List[Dict]
    training_data_date_range: str
    feature_pipeline_version: str
    n_training_rows: int
    
    # Upstream models (for model-to-model dependencies)
    upstream_model_dependencies: List[str] = field(default_factory=list)
    
    # Downstream consumers
    downstream_models: List[str] = field(default_factory=list)
    api_endpoints: List[str] = field(default_factory=list)
    
    # Experiment chain
    parent_experiment_id: Optional[str] = None
    baseline_model_version: Optional[str] = None
    improvement_over_baseline: Optional[Dict[str, float]] = None
    
    # Approval
    approved_by: Optional[str] = None
    approved_at: Optional[datetime] = None
    
    def generate_lineage_report(self) -> str:
        """Generate human-readable lineage for audit."""
        return f"""
Model Lineage Report
====================
Model: {self.model_name} v{self.model_version}
MLflow Run: {self.mlflow_run_id}

Code Provenance:
  Git SHA: {self.git_sha}
  Branch: {self.git_branch}
  Pipeline: {self.pipeline_version}

Data Provenance:
  Sources: {[s['table'] for s in self.training_data_sources]}
  Date Range: {self.training_data_date_range}
  Training Rows: {self.n_training_rows:,}
  Feature Pipeline: {self.feature_pipeline_version}

Performance vs Baseline ({self.baseline_model_version}):
  {self.improvement_over_baseline}

Approved by: {self.approved_by} at {self.approved_at}
"""


def extract_lineage_from_mlflow(model_name: str, version: str) -> ModelLineage:
    """Extract lineage from MLflow metadata."""
    import mlflow
    
    client = mlflow.tracking.MlflowClient()
    mv = client.get_model_version(model_name, version)
    run = client.get_run(mv.run_id)
    
    tags = run.data.tags
    params = run.data.params
    
    return ModelLineage(
        model_name=model_name,
        model_version=version,
        mlflow_run_id=mv.run_id,
        git_sha=tags.get("git_sha", "unknown"),
        git_branch=tags.get("git_branch", "unknown"),
        pipeline_version=tags.get("pipeline_version", "unknown"),
        training_data_sources=[
            {
                "table": tags.get("source_table", "unknown"),
                "s3_path": tags.get("data_s3_path"),
            }
        ],
        training_data_date_range=tags.get("data_date_range", "unknown"),
        feature_pipeline_version=tags.get("feature_pipeline_version", "unknown"),
        n_training_rows=int(params.get("n_train", 0)),
        baseline_model_version=tags.get("baseline_version"),
        improvement_over_baseline={
            "auc_delta": float(tags.get("auc_improvement", 0)),
        },
        approved_by=mv.tags.get("approved_by"),
    )
```

---

## Regulatory Requirements for Experiment Tracking

Regulated industries (banking, healthcare, insurance) require audit trails of model development.

```python
class RegulatoryAuditLogger:
    """
    Immutable audit log for model development decisions.
    GDPR/CCPA: right to explanation requires model version at prediction time.
    Fair lending: adverse action notice requires model justification.
    """
    
    def __init__(self, mlflow_client, audit_store):
        self.client = mlflow_client
        self.audit_store = audit_store
    
    def log_model_decision(
        self,
        model_name: str,
        version: str,
        decision_type: str,  # "promote_to_staging", "reject", "promote_to_production"
        decided_by: str,
        rationale: str,
        evidence: dict,
    ):
        """Log model governance decisions with immutable audit trail."""
        
        # Store in MLflow tags (human-readable)
        self.client.set_model_version_tag(
            name=model_name,
            version=version,
            key=f"decision_{decision_type}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}",
            value=f"by={decided_by}|rationale={rationale[:200]}",
        )
        
        # Store in immutable audit store (compliance record)
        self.audit_store.write({
            "event_type": "model_governance_decision",
            "timestamp": datetime.utcnow().isoformat(),
            "model_name": model_name,
            "version": version,
            "decision_type": decision_type,
            "decided_by": decided_by,
            "rationale": rationale,
            "evidence": evidence,
            "hash": self._compute_hash({
                "model_name": model_name,
                "version": version,
                "decision_type": decision_type,
                "decided_by": decided_by,
                "timestamp": datetime.utcnow().isoformat(),
            }),
        })
    
    def log_training_data_selection(
        self,
        run_id: str,
        data_selection_rationale: str,
        excluded_data: list,
        inclusion_criteria: dict,
    ):
        """Document WHY this training data was selected."""
        self.client.set_tag(
            run_id,
            "data_selection_rationale",
            data_selection_rationale,
        )
        self.client.log_dict(
            run_id,
            {
                "excluded_data": excluded_data,
                "inclusion_criteria": inclusion_criteria,
                "rationale": data_selection_rationale,
            },
            artifact_file="data_selection.json",
        )
    
    def _compute_hash(self, record: dict) -> str:
        import json, hashlib
        return hashlib.sha256(json.dumps(record, sort_keys=True).encode()).hexdigest()
```

---

## Interview Tips

> **Tip 1:** "How do you prevent experiment duplication in large ML teams?" — "Two mechanisms: (1) Searchable experiment registry — before starting, run a search query: 'has anyone tried XGBoost with focal loss on fraud in the last 6 months?' If yes, reuse or build on those results. (2) Mandatory conclusion documentation — every experiment must be closed with a conclusion (success/failure/inconclusive) and key learnings. This creates an institutional memory that survives team turnover."

> **Tip 2:** "What is model lineage and why does it matter for compliance?" — "Model lineage traces the complete chain from raw data → features → training code → trained model → production deployment. In a regulatory audit, you need to answer: What data was this model trained on? Were any protected attributes used? Who approved this model? What was the model doing on this specific date? Without lineage, you can't answer these questions."

> **Tip 3:** "How do you handle experiment tracking in a team where some members are researchers (notebooks) and others are engineers (scripts)?" — "Adopt a dual-interface approach: researchers log from notebooks using mlflow.start_run() with flexible metadata, engineers run structured pipelines with enforced metadata schemas. Establish a minimum required tag set (git_sha, team, data_version) and validate it programmatically. Accept that research experiments will be messier than production experiments — don't over-engineer the notebook workflow."

> **Tip 4:** "How would you design experiment tracking for a company with 50+ models?" — "Three layers: (1) Experiment-level taxonomy — every experiment tagged by use_case, team, purpose, and hypothesis. (2) Model-level registry — standardized promotion workflow with quality gates and approval requirements. (3) Production lineage — every deployed model's training data and code version stored immutably for audit. Add a weekly experiment review where teams share learnings — this creates knowledge transfer across teams."
