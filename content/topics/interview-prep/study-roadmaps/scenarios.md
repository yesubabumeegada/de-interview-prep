---
title: "Study Roadmaps — Readiness Scenarios"
topic: interview-prep
subtopic: study-roadmaps
content_type: scenario_question
tags: [interview-prep, roadmap, self-assessment, career]
---

# Study Roadmaps — "Are You Ready?" Checkpoints

Three self-assessment scenarios. Treat each as a closed-book exam: read the scenario, attempt the full answer out loud or on paper, *then* open the hint and solution. Your performance tells you which roadmap layer to (re)enter.

<article data-difficulty="junior">

## 🟢 Junior: The Screen-Round Simulation

**Scenario:** A recruiter schedules you for a junior DE screen in 48 hours. The format: one SQL question, one Python question, and "tell me about a data project you've worked on" — 45 minutes total. Simulate it now, closed-book: (1) Write a query returning each customer's **second** order date from `orders(order_id, customer_id, order_date)`. (2) Write a Python function that reads a CSV of transactions and returns total amount per category, skipping malformed rows without crashing. (3) Describe a project in 2 minutes including what the pipeline did and one problem you solved. Score yourself honestly.

<details>
<summary>💡 Hint</summary>

For the SQL, "second per group" should immediately trigger a window function with `ROW_NUMBER()` partitioned by customer. For the Python, the graders care more about the try/except around row parsing than about cleverness. For the project, use the shape: what it was → what you did → one obstacle → outcome. If any of the three took you more than 15 minutes or required looking something up, that area is your gap.

</details>

<details>
<summary>✅ Solution</summary>

**1. SQL — second order date per customer:**

```sql
SELECT customer_id, order_date AS second_order_date
FROM (
    SELECT customer_id,
           order_date,
           ROW_NUMBER() OVER (
               PARTITION BY customer_id
               ORDER BY order_date
           ) AS rn
    FROM orders
) t
WHERE rn = 2;
```

Talking points that earn credit: ties (use `order_id` as a tiebreaker in the `ORDER BY`), customers with only one order are correctly absent, and `ROW_NUMBER` vs `RANK` behavior under duplicate dates.

**2. Python — robust aggregation:**

```python
import csv
from collections import defaultdict

def totals_by_category(path: str) -> dict[str, float]:
    totals: dict[str, float] = defaultdict(float)
    with open(path, newline="") as f:
        for line_no, row in enumerate(csv.DictReader(f), start=2):
            try:
                totals[row["category"].strip()] += float(row["amount"])
            except (KeyError, ValueError, AttributeError):
                print(f"Skipping malformed line {line_no}")
    return dict(totals)
```

Credit comes from: `DictReader`, targeted exception types (not bare `except:`), counting/logging rejects, and mentioning you'd write rejects to a file in production.

**3. Project, 2-minute shape:** "I built a pipeline that pulled daily sales CSVs from SFTP, validated them in Python, and loaded them to Postgres for a dashboard — about 200K rows/day. My role was the whole pipeline. The problem I hit was duplicate files on re-delivery, which double-counted revenue; I fixed it by tracking processed filenames and making the load idempotent with delete-then-insert per file date. After that, reruns were safe and the dashboard numbers stopped drifting."

**Scoring:** all three smooth and inside 45 minutes → start applying; one shaky → drill that pillar for a week (see **fundamentals.md** phase order); two or more shaky → re-enter the 12-week junior roadmap at Phase 1.

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: The Slow Join Postmortem

**Scenario:** Closed-book checkpoint for the mid-level track. A nightly PySpark job joins a 1.5 TB `events` table to a 900 GB `users` table on `user_id`, then aggregates. It used to run in 50 minutes; over three months it degraded to 4+ hours, and last night it failed with executor OOM errors. Walk through — out loud, as if to an interviewer — your full diagnosis process and at least three remediation options with trade-offs. Then answer the follow-up: "How would you prevent the silent degradation from recurring?"

<details>
<summary>💡 Hint</summary>

Structure beats trivia here: start at the Spark UI (which stage, which tasks), look at task-duration skew (a few tasks taking 100× longer screams key skew — think null or default `user_id` values), then consider data growth, small files, and shuffle spill. Remediations should span more than one category: data fix, join strategy, and resource/config. The prevention question is fishing for observability — runtime trending and input-volume checks, not "add more memory."

</details>

<details>
<summary>✅ Solution</summary>

**Diagnosis narrative (what a strong answer sounds like):**

1. **Spark UI first:** find the failing stage. OOM during a shuffle-heavy stage of a large join points to skew or exploded shuffle volume.
2. **Task distribution:** if p50 task time is seconds but max is 30+ minutes, it's key skew. Confirm with:

```python
from pyspark.sql import functions as F
events.groupBy("user_id").count().orderBy(F.desc("count")).show(20)
```

Classic finding: `NULL` or a sentinel like `user_id = -1` (logged-out traffic) covering 30% of rows — and that segment *grew* over three months, explaining the slow degradation.

3. **Check growth & layout:** input volume trend, file counts/sizes (small-file explosion inflates planning and shuffle), and whether stats/partitioning changed.

**Remediations with trade-offs:**

- **Filter or separate the skewed keys:** process NULL/sentinel users in a dedicated branch (often they don't need the join at all). Cheapest fix; requires confirming business logic.
- **Salting:** add a random salt to the hot keys and replicate the matching dimension slice. Works generally; adds code complexity and doubles some data movement.
- **Enable AQE skew handling:** `spark.sql.adaptive.enabled` + `skewJoin.enabled` lets Spark split oversized partitions. Low effort; helps but may not fully solve a single monster key.
- **Broadcast — only after shrinking:** 900 GB can't be broadcast, but if the aggregate only needs 3 columns of `users`, project first; if it reduces to a few hundred MB after dedup/projection, broadcast becomes viable. Trade-off: fragile to dimension growth.
- **Not a fix:** blindly raising executor memory — treats the symptom, costs money, and the skewed task eventually outgrows it again.

**Prevention (the senior-leaning differentiator):**

- Emit job metrics per run: input rows, shuffle bytes, runtime, max/median task ratio; alert on trend breaches (e.g., runtime > 1.5× trailing-30-day median).
- Add a data quality check on key-distribution: top-key share of rows.
- Capacity review in sprint cadence rather than discovery-by-outage.

**Scoring:** if you produced a structured diagnosis, ≥3 remediations with trade-offs, and trend-based prevention — you're interview-ready on the PySpark pillar; move to system-design mocks. If you jumped straight to "increase memory," re-enter **intermediate.md** Block 1.

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: The Platform Bet

**Scenario:** Final checkpoint for the senior track. You join a company where five product teams each built their own ingestion: two Airflow installs, one cron+bash estate, one Kafka pipeline nobody fully owns, one vendor ELT contract up for renewal. Leadership asks you to "fix data engineering" and gives you two quarters and two engineers. In 45 minutes, produce: (1) how you'd assess the landscape, (2) your platform proposal with explicit trade-offs and what you'd *not* do, (3) the migration sequencing and risk plan, (4) how you'd measure success and report to leadership. Deliver it as if presenting to a director.

<details>
<summary>💡 Hint</summary>

The trap is jumping to a target architecture diagram. Senior/staff answers start with discovery (workload inventory, SLA mapping, cost baseline, interviewing the five teams) and explicitly sequence by risk-adjusted value. Strong answers also contain a "what I won't do in two quarters" section — scope refusal is a senior signal — and success metrics that leadership cares about (incidents, freshness SLAs met, cost, time-to-new-pipeline), not tool adoption percentages.

</details>

<details>
<summary>✅ Solution</summary>

**1. Assessment (first 3 weeks):**
- Inventory every pipeline: owner, SLA, consumers, volume, failure history, cost. A spreadsheet, not a tool purchase.
- Interview each team: what hurts, what they'd protect, what they'd abandon.
- Baseline metrics: incidents/month, mean freshness, total infra + vendor spend, time to stand up a new pipeline.
- Classify workloads: business-critical with SLAs / important-but-tolerant / abandonable.

**2. Proposal — one paved road, not a rewrite:**
- Converge orchestration on a single managed Airflow; standardize ELT into the warehouse with dbt for transforms; keep the Kafka pipeline *if* a real consumer needs streaming latency — otherwise schedule its sources into batch and decommission.
- Renew the vendor ELT for one year deliberately: paying a vendor is cheaper than spending the two engineers reimplementing 30 connectors. Revisit when the platform is stable.
- Explicit non-goals for the two quarters: no data mesh reorg, no multi-region, no real-time platform build, no warehouse migration. Each is a separate, later business case.
- Trade-offs stated aloud: consolidation reduces team autonomy short-term; the cron estate's owners lose familiarity; standardization costs ~6 weeks of feature pause for the two worst pipelines.

**3. Sequencing & risk:**
- Quarter 1: migrate the *second-most-critical* pipeline first (proves the road without betting the crown jewels), then the cron estate (highest incident rate per inventory). Run old and new in parallel with reconciliation checks for two cycles before cutover.
- Quarter 2: most-critical pipeline, then the orphan Kafka decision, then templates + docs so teams self-serve ("paved road" artifacts: cookiecutter repo, CI checks, on-call runbook).
- Risks: hidden consumers (mitigate with query-log analysis before each cutover), team resistance (mitigate by co-building with each team's engineer, not doing it *to* them), the two engineers becoming a bottleneck (mitigate via templates over tickets).

**4. Success metrics reported monthly to leadership:**
- Incidents/month and time-to-recover (target: −50%)
- % of SLA-bound datasets meeting freshness (target: >95%)
- Cost: infra + vendor, trending (target: flat despite growth)
- Time to stand up a new pipeline (target: 2 weeks → 2 days via templates)

**Scoring:** Senior-ready if your answer led with discovery, contained explicit non-goals, sequenced by risk, and measured outcomes leadership cares about. Staff-leaning if you also addressed how other teams self-serve after you step away. If your answer was primarily a target-state architecture diagram, revisit **senior-deep-dive.md** Blocks 1–2 and the **system-design** topic's case studies.

</details>

</article>

## Interview Tips

> **Diagnose your layer honestly.** If the junior scenario wasn't comfortably easy, mid-level prep is premature — interview loops always find the soft floor beneath the impressive ceiling.

> **Practice retrieval under clock pressure.** All three checkpoints should be attempted closed-book and timed; the gap between "I recognize this" and "I can produce this in 10 minutes" is exactly the gap interviews measure.

> **Convert every checkpoint into stories.** The slow-join diagnosis and the platform bet are also behavioral answers — rehearse them in first person past tense ("I found, I chose, it saved") and they double as project-walkthrough material.

## ⚡ Quick-fire Q&A

**Q: How many hours does the junior-to-offer journey realistically take?**
A: Roughly 150–200 focused hours (12–15 h/week × 12 weeks) plus a portfolio project — less if you come from SWE/analytics, more if SQL is new.

**Q: Should I learn AWS and Azure before my first DE job?**
A: No — one cloud done well. The second cloud is a mid-level concern learned by mapping equivalent services, not restudying from scratch.

**Q: What single topic has the highest interview ROI per hour studied?**
A: SQL window functions. They appear in nearly every DE screen at every level and decay fast without practice.

**Q: When should PySpark enter my study plan?**
A: Lightly at junior level (vocabulary), seriously at mid-level (Weeks 1–4 of the intermediate track) — most junior loops won't test it deeply, most mid-level loops will.

**Q: How do I know I'm ready for senior interviews rather than strong mid-level ones?**
A: You can present two architectures with a recommendation and a revisit-trigger, tell a quantified cost-savings story, and produce mentoring evidence with artifacts. Missing any of the three usually reads as mid-level.

**Q: Is a certification worth cram time before an interview?**
A: Almost never in the final two weeks — certifications help resume screening, not live rounds. Spend final-stretch hours on retrieval practice and stories.

**Q: What should I review the night before any DE interview?**
A: Your own resume, a one-page SQL pattern sheet, your three project numbers (volume/latency/impact), and the pipeline-design skeleton — then sleep.

**Q: I failed a loop — where do I re-enter the roadmap?**
A: At the layer that failed, not the beginning: write down every question you were asked, classify each by topic, and rebuild only those pillars before reapplying.
