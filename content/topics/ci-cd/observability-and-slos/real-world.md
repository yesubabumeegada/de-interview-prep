---
title: "Observability and SLOs for Data Pipelines - Real-World Patterns"
topic: ci-cd
subtopic: observability-and-slos
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [ci-cd, observability, slo, production-patterns, incident-response, data-contracts, monte-carlo, bigeye]
---

# Observability and SLOs for Data Pipelines — Real-World Patterns

## The Production Reality: What Gets Monitored vs. What Should Be

In most production data platforms, observability has two layers: **infrastructure observability** (is Airflow running? is Snowflake reachable?) and **data observability** (is the data correct, fresh, and complete?). Infrastructure observability is solved by standard DevOps tools. Data observability is the hard problem that's unique to data engineering — and where most teams have significant gaps.

**What teams typically have:**
- Airflow task failure alerts (via email or PagerDuty)
- Dashboard-level alerts when metrics look obviously wrong
- Ad-hoc SQL queries run during incidents

**What production-grade teams have:**
- Per-table freshness SLOs with burn rate alerts
- Volume anomaly detection across all critical tables
- Data lineage to trace anomalies to root causes
- Automated incident response runbooks
- Error budget dashboards visible to leadership

The maturity jump from "basic" to "production-grade" is the difference between reacting to incidents and preventing them.

## Real Pattern: The Three-Tier Alerting Hierarchy

Not every data quality issue warrants a 3am page. A tiered alerting system routes issues to the right place:

```yaml
# Tier 1: Critical (PagerDuty page — < 15 min response)
# SLA breach imminent or confirmed
criteria:
  - Any SLA-governed table freshness > threshold
  - Data volume drop > 50% from 7-day average
  - Pipeline failure rate > 30% in 1 hour

# Tier 2: High (Slack #data-alerts channel — same day response)
criteria:
  - SLO burn rate > 6x (budget depleting rapidly)
  - Data volume anomaly Z-score > 3
  - Specific business-critical model test failures
  - Pipeline running > 2x normal duration

# Tier 3: Low (JIRA ticket — next sprint)
criteria:
  - Non-critical test failures
  - Schema changes in non-SLA tables
  - Performance degradation without SLO impact
  - Data quality warnings (not yet errors)
```

```python
# alert_router.py
import boto3

def route_alert(alert: dict):
    """Route data quality alerts to the appropriate channel."""

    severity = alert['severity']
    table = alert['table']
    message = alert['message']

    if severity == 'critical':
        # PagerDuty: wakes someone up
        trigger_pagerduty_incident(
            title=f"DATA SLA BREACH: {table}",
            body=message,
            routing_key=os.environ['PAGERDUTY_DATA_KEY']
        )

    elif severity == 'high':
        # Slack: visible, urgent, but asynchronous
        post_to_slack(
            channel='#data-alerts',
            message=f":rotating_light: *DATA ALERT [{severity.upper()}]*\n{message}",
            attachments=[{
                'color': 'danger' if severity == 'critical' else 'warning',
                'fields': [
                    {'title': 'Table', 'value': table, 'short': True},
                    {'title': 'Severity', 'value': severity, 'short': True},
                ]
            }]
        )

    elif severity == 'low':
        # Create JIRA ticket for next sprint
        create_jira_ticket(
            project='DATA',
            issue_type='Bug',
            summary=f"Data quality issue: {table}",
            description=message,
            priority='Low'
        )
```

## Real Pattern: End-to-End Tracing for Debugging Latency

When the CTO asks "why did yesterday's revenue number change at 11am instead of 8am?", you need end-to-end tracing:

```python
# pipeline_tracer.py — lightweight tracing without OTel overhead
import uuid
import boto3
import json
from datetime import datetime

class PipelineTrace:
    """
    Lightweight tracing for data pipelines.
    Stores trace records in DynamoDB for querying.
    """

    def __init__(self, pipeline_id: str, run_date: str):
        self.trace_id = str(uuid.uuid4())
        self.pipeline_id = pipeline_id
        self.run_date = run_date
        self.spans = []
        self.dynamodb = boto3.resource('dynamodb')
        self.table = self.dynamodb.Table('pipeline-traces')

    def start_span(self, span_name: str, metadata: dict = None) -> str:
        span_id = str(uuid.uuid4())
        span = {
            'span_id': span_id,
            'span_name': span_name,
            'start_time': datetime.utcnow().isoformat(),
            'metadata': metadata or {},
        }
        self.spans.append(span)
        return span_id

    def end_span(self, span_id: str, status: str = 'success', metadata: dict = None):
        for span in self.spans:
            if span['span_id'] == span_id:
                span['end_time'] = datetime.utcnow().isoformat()
                span['status'] = status
                if metadata:
                    span['metadata'].update(metadata)
                span['duration_seconds'] = (
                    datetime.fromisoformat(span['end_time']) -
                    datetime.fromisoformat(span['start_time'])
                ).total_seconds()
                break

    def save(self):
        """Persist the completed trace to DynamoDB."""
        self.table.put_item(Item={
            'trace_id': self.trace_id,
            'pipeline_id': self.pipeline_id,
            'run_date': self.run_date,
            'total_duration': sum(s.get('duration_seconds', 0) for s in self.spans),
            'spans': json.dumps(self.spans),
            'created_at': datetime.utcnow().isoformat(),
        })

# Usage in pipeline
def run_orders_pipeline(date: str):
    trace = PipelineTrace('orders_pipeline', date)

    sid = trace.start_span('extract_from_api')
    df = extract_orders_api(date)
    trace.end_span(sid, metadata={'rows_extracted': len(df)})

    sid = trace.start_span('write_to_s3')
    write_to_s3(df, f"s3://raw/orders/{date}/")
    trace.end_span(sid, metadata={'bytes_written': df.memory_usage().sum()})

    sid = trace.start_span('dbt_run')
    run_dbt(['fct_orders'])
    trace.end_span(sid)

    trace.save()

# Query: "What caused yesterday's late delivery?"
# SELECT spans FROM pipeline-traces WHERE pipeline_id = 'orders_pipeline' AND run_date = '2024-01-15'
# Identify which span had the longest duration
```

## Real Pattern: SLO Review Meetings

An often-overlooked piece of production observability is the **organizational** side: who reviews SLO performance and what decisions they make:

```markdown
## Monthly Data Platform SLO Review Agenda (30 minutes)

### Attendees: Data Engineering lead, Data Analytics lead, Business stakeholders

### 1. Error Budget Status (5 min)
Present the 30-day error budget consumption for each SLO:
- Orders freshness SLO: Budget remaining: 78% ✅
- Revenue pipeline SLO: Budget remaining: 12% ⚠️ (close to breach)
- Customer data SLO: Budget remaining: 94% ✅

### 2. Incident Review (10 min)
For each SLO violation:
- What happened?
- What was the business impact?
- What was the root cause?
- What prevention is in place?

### 3. Budget Decisions (10 min)
Revenue pipeline budget at 12%:
- Option A: Freeze all revenue pipeline deployments for rest of month
- Option B: Accept the risk; continue shipping (leadership decision)
- Option C: Improve reliability now (invest in retry logic, alerting)

### 4. SLO Adjustments (5 min)
Should any SLO targets change?
- Too strict → always burning budget → frustrates team, discourages shipping
- Too loose → never burning budget → stakeholders don't trust the data quality

## The Goldilocks Rule: Your error budget should be "almost zero" at month end
If you always have 80%+ remaining, your SLO is too loose.
If you always deplete it by Week 3, your SLO is too strict or the pipeline is too unreliable.
```

## Real Incident Story: The Phantom 15% Revenue Drop

A pattern that comes up often in senior interviews — when data engineers need to reason about cascading observability failures:

```
Incident: Revenue dashboard showed 15% drop over 3 days.
Initial hypothesis: Actual revenue decline — sent to finance team.
Finance team: "This doesn't match our payment processor numbers at all."

Root cause investigation using pipeline traces:
1. Checked fct_orders volume → Normal ✅
2. Checked fct_revenue volume → 15% lower ✗
3. Checked lineage: fct_revenue ← dim_promotions ← stg_promotions
4. Checked stg_promotions → 200,000 rows (expected: 50,000) ✗ (4x volume spike!)
5. Root cause: A new promotion type was added with duplicate rows in source
   → fct_revenue JOIN expanded fan-out
   → After deduplication, revenue appeared 15% lower (correct rows, wrong denominator)

Lessons:
- Volume increase in a source table is a warning sign (our alert only checked for decreases)
- Join fan-out from unexpected source duplicates is a common silent bug
- Update volume anomaly detection to alert on BOTH decreases AND unexpected increases

Fix:
1. Add DISTINCT to stg_promotions
2. Add dbt test: dbt_utils.unique_combination_of_columns on promotion_id + date
3. Update Monte Carlo to alert on volume increases > 2x normal
```

## Key Real-World Takeaways

- **Three-tier alerting** (page/Slack/ticket) routes issues to the right place and prevents alert fatigue from over-paging
- **End-to-end tracing** stored in DynamoDB or a time-series DB answers "where was the bottleneck?" in post-incident reviews
- **SLO review meetings** are the organizational mechanism that turns error budget math into actual business decisions
- The **Goldilocks rule for SLO targets**: if budget never depletes, the SLO is too loose; if it depletes by Week 3 every month, the pipeline is too unreliable
- **Volume anomaly detection should watch for spikes as well as drops** — join fan-out from source duplicates is a common silent bug
- The most impactful observability investment is often lineage visibility: knowing which upstream sources feed which downstream tables dramatically reduces incident MTTD
