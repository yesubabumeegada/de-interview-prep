---
title: "Project Walkthrough — Real-World Worked Examples"
topic: interview-prep
subtopic: project-walkthrough
content_type: study_material
difficulty_level: mid-level
layer: real-world
tags: [interview-prep, project-walkthrough, examples, career]
---

# Project Walkthrough — Real-World Worked Examples

Complete worked walkthroughs at every depth, a live whiteboard dialogue with interviewer interruptions, and the "why didn't you use X?" exchange played out both well and badly. Use these as templates: swap in your systems, keep the skeletons.

---

## Worked Example: One Project at Three Depths

**The project:** CDC replication from a transactional Postgres into Snowflake, feeding finance reporting.

### 30-second version (recruiter screen)

> "Most recently I built our change-data-capture pipeline — it streams every change from our core Postgres into Snowflake within 10 minutes, about 4M changes a day across 60 tables. It replaced a nightly full-copy job, so finance went from yesterday's data to near-real-time, and the full-copy's 3 a.m. failure pages basically disappeared."

Note the anatomy: what it is, two numbers, what it replaced, two benefits. Nothing about Debezium yet — tools wait for technical rounds.

### 3-minute version (standard rounds)

> "**Context:** finance ran on a nightly job that full-copied 60 tables from production Postgres to Snowflake. It took 5 hours, failed about weekly, and hammered the production DB enough that we could only run it at 3 a.m. — so finance always worked with day-old data.
>
> "**Scale:** ~4M row-changes a day, 60 tables, the biggest around 800M rows.
>
> "**Architecture:** Debezium reads the Postgres WAL and publishes changes to Kafka, one topic per table. A Snowflake connector lands raw change events into append-only staging tables, and dbt models merge them into current-state tables every 10 minutes, handling deletes and out-of-order events via the LSN. Airflow orchestrates the merge layer and the data quality checks.
>
> "**My role:** I designed the architecture, built the merge logic and the reconciliation framework, and led the table-by-table migration. A platform engineer ran the Kafka side; an analytics engineer helped port finance's models.
>
> "**The hard parts:** two stand out. First, initial snapshots — you can't stream changes into an empty table, and snapshotting an 800M-row table while changes keep flowing creates a consistency puzzle. We used Debezium's snapshot mode but added our own LSN-watermark reconciliation to prove no gap between snapshot end and stream start. Second, deletes: finance's history requirements meant we couldn't just apply them, so the merge layer maintains both a current-state table and a full change-history table — which later turned out to be the thing the audit team loved most.
>
> "**Impact:** data freshness went from 24 hours to under 10 minutes, the production DB load from replication dropped to near zero — WAL reading instead of table scans — and replication incidents went from about four a month to one a quarter. The history tables also got us through a finance audit with zero data-lineage findings."

### Deep-dive outline (the 20-minute version — prepare as bullets, not prose)

- Snapshot consistency mechanics: LSN watermarks, the reconciliation queries, the one table that needed three snapshot attempts
- Merge logic: `MERGE` keyed on PK + LSN ordering, late/out-of-order handling, the tombstone problem
- Schema evolution: how column adds/renames propagate; the rename that broke staging and the contract we added after
- Why Debezium over AWS DMS and over app-level dual-writes (decision ledger entries)
- Cost: connector infra ~$2K/month vs the retired job's compute, plus the 5-hour nightly window returned to other workloads
- What I'd change today: single-topic-per-table doesn't scale past ~200 tables; I'd evaluate Snowflake's native streaming ingest now

---

## Live Whiteboard Dialogue (with interruptions)

How the 3-minute version actually goes in a deep-dive round:

> **Interviewer:** "Draw it for me as you go."
>
> **You:** "Sure — left to right, sources to consumers." *[draws Postgres box]* "Production Postgres, 60 tables in scope." *[arrow]* "Debezium tails the WAL — this arrow is about 4M change events a day, peaks around 300/second during business hours…"
>
> **Interviewer (interrupting):** "Why WAL-based instead of query-based CDC — timestamps and a poller?"
>
> **You:** "Three reasons that mattered here: the source tables didn't all have reliable updated-at columns and adding them meant app changes across five services; pollers miss deletes entirely, and finance needed deletes; and polling 60 tables frequently was exactly the production load we were trying to eliminate. The trade-off we accepted was operating Kafka and Debezium — real complexity. For a smaller scope with good timestamps and no delete requirement, I'd poll without shame."
>
> **Interviewer:** "Keep going."
>
> **You:** *[continues drawing]* "Kafka, topic per table… landing into append-only staging in Snowflake — and I'll flag a decision here: we land raw change events, not merged state, which means staging is our replayable history…"
>
> **Interviewer:** "What happens when someone drops a column upstream?"
>
> **You:** "As-built? Debezium keeps publishing without it, staging absorbs it fine since we land semi-structured, but the dbt merge model referencing the column fails at the next run — loudly, which we decided is correct: a human confirms intent before reporting silently changes. After the second drill of that, I added a schema-change notification from their migration CI to our Slack, so we usually know before the failure. The honest gap: a true contract with pre-deploy checks was on the roadmap and never got prioritized before I left."

Techniques on display: narrating while drawing, annotating arrows with numbers, *flagging decisions as decisions*, answering interruptions at decision-altitude, and volunteering the honest gap before it's hunted down.

---

## "Why Didn't You Use X?" — Played Badly, Then Well

**Question:** "Why Airflow for the merge orchestration? Snowflake tasks could've done this natively."

### The bad answer

> "Uh, we already had Airflow, so… it was just easier. I guess Snowflake tasks would also work, yeah. Maybe that would've been better, honestly."

Failure modes: no decisive factor, instant capitulation, and the accidental admission that no decision was ever really made — three signals down in fifteen seconds.

### The good answer

> "We evaluated tasks, actually. Two things kept us on Airflow: the DQ checks that gate the merges call external services — our reconciliation hits the source Postgres, which native tasks couldn't reach cleanly — and our on-call story was already built around Airflow's UI and alerting, so a second orchestration plane meant a second 3 a.m. interface. Cost: we carry Airflow infra for something tasks would run nearly free. If the reconciliation pattern changed or we consolidated on-call tooling, that math flips — and for a greenfield Snowflake-only shop, I'd take tasks."

Same stack, same history — but a decisive factor, a traced requirement, an admitted cost, and a flip condition.

---

## Salvage Scripts for Common Walkthrough Emergencies

**You realize mid-drawing you've forgotten how a piece worked:**
> "I'm reconstructing this part from a couple of years back — directionally it worked like this, and I'm certain about the merge keys because I wrote those; less certain about the connector config details."

Certainty-labeling beats both bluffing and collapsing.

**Interviewer wants depth on the part a teammate built:**
> "That was our platform engineer's domain, so I'll give you the consumer's view of it — what I depended on, what its failure modes did to my layer, and where its limits shaped my design — but I won't pretend to his depth on the internals."

**You're asked for a number you never measured:**
> "I didn't measure that at the time — reasoning from what I do know, it had to be roughly N, because…"

Live estimation from known anchors is a *positive* signal; pulling suspiciously exact figures from nowhere is the negative one.

**Time is being cut short mid-walkthrough:**
> "Let me jump to the two things most worth your remaining five minutes: the snapshot-consistency problem and what I'd change today."

Owning the prioritization of your own story under time pressure is itself a senior-flavored signal.

---

## Build-Your-Own Template

Fill this in for each portfolio project; it generates all three depths:

```text
PROJECT: ____________
Status-quo pain (with number): ____________
Scale: ___ rows|GB/day, ___ tables|sources, biggest object: ___
Boxes (L→R): source → ______ → ______ → ______ → consumers
Arrow annotations (volume + frequency): ____________
My scope boundary: ____________
Decision 1 (+factor, +cost accepted, +flip condition): ____________
Decision 2: ____________
Hard problem 1 (mechanics of the fix): ____________
Hard problem 2: ____________
Impact numbers (≥2): ____________
Honest gap / what I'd change today: ____________
Likely "why not X?"s for THIS company's stack: ____________
```

The last line matters most: read the job posting's stack and prepare the X's *they* will ask about.
