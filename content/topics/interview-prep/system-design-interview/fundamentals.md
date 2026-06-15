---
title: "System Design Interview - Fundamentals"
topic: interview-prep
subtopic: system-design-interview
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [interview-prep, system-design-interview, system-design, fundamentals]
---

# System Design Interview — Fundamentals

## What Is a Data Engineering System Design Interview?

A system design interview asks you to architect a data system from scratch — live, with an interviewer watching. Unlike coding interviews, there is rarely a single correct answer. The interviewer is evaluating your ability to **think structurally**, **communicate trade-offs**, and **ask good questions**.

At the junior/mid level, interviewers care most about whether you can:
- Break a vague problem into concrete components
- Name real tools and justify your choices
- Acknowledge limitations without getting flustered

At the senior level, the bar shifts to trade-off depth, scalability reasoning, and the ability to adapt when the interviewer adds constraints mid-discussion.

---

## The Standard Framework (6 Steps)

Every data engineering system design interview can be approached with the same repeatable framework. Internalizing this sequence lets you start confidently even when you have no idea what the answer is.

### Step 1: Clarify Requirements (5 minutes)

Never start designing immediately. The problem statement is always underspecified. Ask questions to pin down scope before you draw anything.

**Key questions to ask:**
- What is the primary use case? (analytics, ML features, operational reporting, etc.)
- Who are the consumers? (data scientists, dashboards, downstream APIs)
- What is the data volume? (events per second, GB per day, row count)
- What is the acceptable latency? (real-time sub-second, near-real-time minutes, batch daily)
- What consistency guarantees are needed? (eventual consistency acceptable? exactly-once required?)
- Are there compliance or data residency requirements?
- Is this greenfield or integrating with existing systems?

Interviewers often deliberately omit scale to see if you ask. If they say "design a data lake," ask "for how much data?" before proceeding.

### Step 2: Estimate Scale (3 minutes)

Back-of-envelope math grounds your design choices. You don't need precise numbers — you need to know whether you're dealing with megabytes or petabytes, thousands or billions of rows.

**Useful estimates to keep handy:**
- 1 million events/day ≈ 12 events/second
- 1 billion events/day ≈ 12,000 events/second
- A typical event JSON payload: 500 bytes to 2 KB
- Parquet compression ratio: 5–10x over CSV
- S3 storage cost: ~$0.023 per GB/month

Walk the interviewer through your math. Say: "If we have 100 million users and each generates 10 events per day, that's 1 billion events/day. At 1 KB each, that's 1 TB/day before compression."

### Step 3: High-Level Design (10 minutes)

Draw a component diagram covering the major stages of data flow: ingestion → processing → storage → serving.

For a typical batch pipeline:
```
Source Systems → Ingestion Layer → Raw Storage → Processing → Curated Storage → Serving Layer
```

For a streaming pipeline:
```
Producers → Message Broker → Stream Processor → Sink → Query Layer
```

Name actual tools at each stage rather than abstract boxes. "Kafka for the message broker, Flink for stream processing, Delta Lake on S3 for storage, Trino for ad-hoc queries" is far better than "pub-sub → processor → storage."

### Step 4: Deep Dive (15 minutes)

The interviewer will direct you to one or two components to explore in detail. Common deep-dive areas:

- **Ingestion**: How do you handle schema evolution? What happens if the source goes down?
- **Storage**: How are you partitioning? What file format? How do you handle late-arriving data?
- **Processing**: What is your exactly-once strategy? How do you handle duplicates?
- **Serving**: How do you optimize query performance? What's your caching strategy?

Go deeper than the surface answer. If asked about partitioning, don't just say "partition by date" — explain partition pruning, cardinality considerations, and what happens to your partition scheme at 10x scale.

### Step 5: Trade-offs Discussion (5 minutes)

Explicitly name the trade-offs in your design. This is what separates mid-level from senior candidates.

- "I chose batch over streaming here because the business SLA is T+24h and batch is simpler to operate. If the requirement changed to 15-minute freshness, I'd add a streaming layer."
- "I chose Kafka over Kinesis because we need a 7-day retention window and the team has existing Kafka expertise."
- "I'm using eventual consistency here. If the business needed transactional guarantees, I'd need to reconsider this architecture."

### Step 6: Wrap-Up (2 minutes)

Summarize your design, call out the biggest risks, and mention what you'd do given more time. Ask the interviewer if there are areas they'd like to explore further.

---

## Common Question Types

| Question Type | Example |
|---|---|
| Batch pipeline | Design a daily user activity report pipeline |
| Streaming pipeline | Design a real-time fraud detection system |
| Data warehouse | Design a data warehouse for an e-commerce company |
| Feature store | Design a feature store for ML models |
| Data lake | Design a data lake ingesting 10 TB/day |

---

## How to Handle "I Don't Know"

You will hit questions where you genuinely don't know the answer. Here is how to handle it gracefully:

1. **Acknowledge it plainly**: "I'm less familiar with X, but let me reason through it."
2. **Apply first principles**: "Even if I don't know the tool, I know the problem requires low-latency reads, so I'd look for something columnar with good caching."
3. **Ask a clarifying question**: "Is there a specific technology you'd like me to focus on, or should I describe the general approach?"

Never bluff. Interviewers always know when you're bluffing, and it destroys credibility. Saying "I don't know but here's how I'd think about it" is far better than confidently describing something incorrectly.

---

## Questions to Ask the Interviewer

Good candidates ask questions throughout. Some useful ones:

- "Is there an existing tech stack I should assume, or is this greenfield?"
- "What does the team optimize for — developer velocity, cost, or reliability?"
- "Are there any regulatory requirements like GDPR or HIPAA?"
- "How important is operational simplicity vs. maximizing performance?"

These questions signal seniority. They show you understand that architecture decisions depend on context.

---

## Common Failure Modes

**Over-engineering**: Designing a Lambda architecture with exactly-once Kafka + Flink + Delta Lake for a use case that needs a weekly CSV export. Match complexity to requirements.

**Ignoring scale**: Never saying numbers. If you can't estimate whether you need one Spark executor or a hundred, your design isn't grounded.

**No trade-offs discussion**: Describing a design without ever saying "the downside of this is..." reads as shallow.

**Tool name-dropping without explanation**: Saying "I'd use Kafka" without explaining why Kafka over other options signals that you memorized buzzwords rather than understanding principles.

**Designing in silence**: System design is a conversation. If you go quiet for five minutes while drawing, you're doing it wrong. Narrate your thinking continuously.
