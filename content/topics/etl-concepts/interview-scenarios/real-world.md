---
title: "ETL Interview Questions: Behavioral Scenarios, Pipeline Failures, and Stakeholder Communication"
description: "How to answer behavioral ETL interview questions using the STAR method, debug production pipeline failures, and communicate with stakeholders."
content_type: study_material
topic: etl-concepts
subtopic: interview-scenarios
layer: real-world
difficulty_level: senior
tags: [behavioral-interview, STAR-method, pipeline-debugging, stakeholder-communication, incident-response, interview-prep]
---

# ETL Interview Questions: Real-World and Behavioral

## Why Behavioral Questions Matter in Data Engineering

Technical skills get you screened in. Behavioral questions determine whether you get the offer.

Senior DE interviewers want to understand:
- How you behave under pressure when production is down
- Whether you communicate proactively or reactively
- How you build systems that don't fail in the same way twice
- Whether you take ownership or look for someone else to blame

The most common behavioral questions in DE interviews all follow the same pattern: **"Tell me about a time your pipeline failed / had bad data / missed an SLA."**

---

## The STAR Method for DE Interview Answers

**STAR** = Situation, Task, Action, Result

| Component | What to Cover |
|-----------|--------------|
| **Situation** | What was the system? What was the business context? What went wrong? |
| **Task** | What was YOUR specific responsibility? |
| **Action** | What did you do, step by step? Include technical decisions and stakeholder communication. |
| **Result** | What was the outcome? Quantify impact (how much data affected, how long to recover, what you prevented). |

**Time budget:** 3-4 minutes per STAR answer. Practice this — most candidates either give too little detail (30 seconds) or ramble (8+ minutes).

---

## Sample Answer 1: "Tell Me About a Time Your Pipeline Failed"

### Situation

> "At my previous company, we ran a daily Airflow pipeline that loaded Salesforce opportunity data into our Snowflake data warehouse. The sales team used this every morning at 8 AM to review pipeline metrics in their Tableau dashboard before their weekly forecast call."

### Task

> "I was the primary owner of this pipeline. On a Monday morning, the head of Sales ops pinged our Slack channel saying the dashboard showed data from Friday, not the weekend's deals. I was responsible for diagnosing and fixing it before their 9 AM call."

### Action

> "I immediately checked Airflow and saw the Sunday run had failed at 3:17 AM. The error was: `SalesforceAuthException: API token expired`. A service account credential had expired over the weekend — this was a security rotation we weren't notified about.
>
> Here's what I did:
> 1. Messaged the sales ops lead within 5 minutes: 'I found the issue — API credential expired. I'm fixing it now and expect data to be current by 8:45 AM.'
> 2. Rotated the service account credential with IT, updated the Airflow connection, and ran a targeted backfill for Saturday and Sunday data.
> 3. Tested in a staging environment first, then triggered the backfill in production.
> 4. Monitored the backfill run, confirmed data loaded correctly, and posted a 'data is current' update at 8:42 AM.
> 5. After the incident, I set up automated alerts for credential expiration (30 days before expiry) and added the credential rotation schedule to our runbook."

### Result

> "The sales team had current data 18 minutes before their forecast call. After the incident, we had zero credential-related pipeline failures in the following 14 months. I also helped the team establish an on-call rotation for data pipelines so there was always someone responsible on weekends."

---

## Sample Answer 2: "Tell Me About a Time You Had to Communicate Bad News About Data Quality"

### Situation

> "Our e-commerce company was preparing for a board presentation on annual revenue. A senior analyst asked me to validate the revenue figures three days before the board meeting."

### Task

> "While validating, I discovered that a currency conversion pipeline had been using incorrect exchange rates for one month — specifically, it was using January rates for February transactions. This meant ~$2.3M in international orders were booked at wrong amounts. I had to tell the CFO."

### Action

> "First, I verified the issue thoroughly before escalating. I didn't want to cause panic over a data artifact. I wrote a SQL query comparing our pipeline output against the Stripe raw data for the same period, and confirmed the discrepancy.
>
> Then I requested a 15-minute call with the CFO and the head of Finance Analytics. I came prepared with:
> - A clear explanation of what happened (wrong exchange rate source used)
> - The exact scope (February transactions only, ~$2.3M in international orders)
> - The correct figures (I had already recomputed them)
> - My proposed fix and timeline (4 hours to reprocess and validate)
>
> I did NOT send a Slack message or email with half the information — I knew this needed a synchronous conversation.
>
> On the call, the CFO asked: 'Is this in any other reports?' I had already checked — it only affected the monthly revenue rollup, not the P&L statements. I was able to answer that definitively because I'd done my homework.
>
> After the fix was deployed, I sent a formal summary email with before/after numbers and root cause."

### Result

> "The board presentation used correct figures. The CFO later mentioned in a team meeting that my proactive communication — coming to them with a solution, not just a problem — was exactly how she wanted data issues handled.
>
> Post-incident, we added exchange rate source validation to our data quality suite and started validating financial figures against Stripe weekly, not just before board presentations."

---

## Debugging Methodology: A Framework for "How Would You Debug This?"

When an interviewer says "your pipeline is producing wrong numbers — walk me through how you'd debug it," they're testing your systematic thinking.

### The 4-Layer Debugging Framework

```
Layer 1: Was the pipeline triggered?
  └─ Check: Airflow/Prefect run history. Did the DAG run at the expected time?
     If NOT run: check scheduler health, dependencies, trigger conditions.
     If RAN: proceed to Layer 2.

Layer 2: Did the pipeline complete successfully?
  └─ Check: Run status (success/failure), error logs, task-level status.
     If FAILED: examine the error. What step? What error message?
     If SUCCEEDED: proceed to Layer 3.

Layer 3: Was the source data correct?
  └─ Check: Raw/staging table vs. source system directly.
     Row counts match? Key metrics match (SUM, COUNT)?
     If MISMATCH: issue is in extraction or source system.
     If MATCH: proceed to Layer 4.

Layer 4: Was the transformation correct?
  └─ Check: Each transformation step between staging and final mart.
     Find the earliest layer where numbers diverge from expectation.
     Test the specific SQL/code that transforms data at that layer.
```

### Example Walkthrough

**Symptom:** Revenue dashboard shows $12M for yesterday, but the finance team says it should be $15M.

```
Layer 1: Pipeline ran at 02:00 UTC, completed at 03:47 UTC — SUCCESS. Move on.

Layer 2: All Airflow tasks green. No errors. Move on.

Layer 3: Compare staging vs. source.
  
  -- In staging (what we extracted)
  SELECT COUNT(*), SUM(amount) FROM orders_staging WHERE event_date = '2024-01-15';
  → 45,230 rows, $12.1M
  
  -- In source system (PostgreSQL)
  SELECT COUNT(*), SUM(amount) FROM orders WHERE DATE(created_at) = '2024-01-15';
  → 51,890 rows, $15.0M
  
  MISMATCH: staging has 6,660 fewer rows. The extraction is incomplete.

Root cause investigation at Layer 3:
  - Check extract query: uses updated_at for incremental filter.
  - The watermark was set to '2024-01-15 00:00:00'.
  - Orders with created_at on 2024-01-15 but updated_at before midnight are missed.
  - 6,660 orders created and never updated — their updated_at = created_at
    from a previous day (if they were pre-populated or migrated).
  
FIX: Use MAX(created_at, updated_at) in the incremental filter, or switch
  to log-based CDC which captures inserts regardless of timestamp.
```

---

## Stakeholder Communication Templates

### Template 1: Initial Incident Notification (< 5 minutes after detection)

```
Channel: #data-incidents

⚠️ DATA INCIDENT: [Pipeline/Dashboard Name]

Status: Investigating
Impact: [e.g., Daily sales dashboard showing data from yesterday]
Affected users: [e.g., Finance team, Sales Ops]
ETA for update: 30 minutes

I'm on it. Will update at [TIME].
— [Name]
```

### Template 2: Status Update (every 30 minutes during active incident)

```
UPDATE — [Time]

Root cause identified: [1-2 sentences describing what's wrong]
Fix in progress: [What you're doing]
ETA for resolution: [Time estimate]
Risk: [Any risk to the fix itself, e.g., "backfill may take 90 min"]
```

### Template 3: Resolution Notification

```
✅ RESOLVED — [Time]

[Dashboard/Pipeline] is now showing current data as of [timestamp].

What happened: [2-3 sentences — what broke and why]
How we fixed it: [What was done]
Prevention: [What we're doing to prevent recurrence]

Full post-mortem will be shared by [date].
```

### Template 4: Post-Mortem Email (within 48 hours)

```
Subject: Post-Mortem: [Incident Name] — [Date]

SUMMARY
[One paragraph: what happened, impact, duration, resolution]

TIMELINE
[Bullet list of key events with timestamps]

ROOT CAUSE
[Technical explanation of what failed and why]

CONTRIBUTING FACTORS
• [Factor 1: e.g., No alerting on credential expiration]
• [Factor 2: e.g., Weekend coverage gap]

ACTION ITEMS
Owner | Action | Due Date
[Name] | Add credential expiration alerting | [Date]
[Name] | Add weekend on-call rotation | [Date]
[Name] | Document runbook for credential rotation | [Date]

LESSONS LEARNED
[What did this incident teach us about the system?]
```

---

## Questions to Ask the Interviewer

At the end of an ETL interview, asking good questions signals experience. Here are questions that senior engineers ask:

**About the pipeline architecture:**
- "What does the current orchestration setup look like, and what are the biggest pain points with it?"
- "How does the team handle schema evolution from source systems today?"
- "What's the worst production incident the data platform team has had in the last year, and what did you learn from it?"

**About data quality:**
- "What tools or frameworks are currently in place for data quality monitoring?"
- "How are freshness SLOs defined and who is responsible for them?"

**About team culture:**
- "How does the data engineering team coordinate with data analysts and data scientists on pipeline requirements?"
- "When a pipeline fails at 2 AM, how does the team handle on-call rotation?"

---

## Common Behavioral Interview Mistakes

| Mistake | Better Approach |
|---------|----------------|
| "We fixed the issue" (no personal ownership) | "I identified the root cause and deployed the fix" |
| Skipping the stakeholder communication step | Always mention how you communicated — this is often what the interviewer cares most about |
| Vague impact ("it affected some users") | Quantify: "3 out of 5 analyst teams couldn't use the dashboard for 4 hours" |
| No prevention/follow-up | Always describe what you did to prevent recurrence |
| Making the story too technical | Balance technical detail with business impact and communication |
