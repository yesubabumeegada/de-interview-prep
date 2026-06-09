---
title: "Performance Analyzer — Fundamentals"
topic: power-bi
subtopic: performance-analyzer
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [power-bi, performance-analyzer, interview, fundamentals]
---

# Performance Analyzer — Fundamentals

## What Is the Performance Analyzer?

The **Performance Analyzer** is a built-in Power BI Desktop tool that measures how long each visual takes to load. It shows the time breakdown for every visual on a report page, helping you identify which visuals are slow and why.

**Access**: View tab → **Performance Analyzer** → Start Recording

---

## Performance Analyzer Output

When you refresh a page with Performance Analyzer running, each visual shows three timing categories:

### Timing Components

| Component | Measures | Typical Cause of Slowness |
|---|---|---|
| **DAX query** | Time to run the DAX query and return results | Complex measures, large tables, slow DAX |
| **Visual display** | Time to render the visual from the results | Complex charts, many data points |
| **Other** | Network time, UI overhead | Slow network, report server latency |

```
Example output for a Bar Chart:
────────────────────────────────────────────────────
Bar Chart - Sales by Region
  DAX query         850ms  ← This is the bottleneck
  Visual display     45ms
  Other              12ms
  Total             907ms
────────────────────────────────────────────────────
```

---

## Reading Performance Analyzer Output

### What Each Section Means

**DAX query** — This is the time spent evaluating the DAX measure(s) used by the visual. A high DAX query time usually means:
- Complex or inefficient DAX measures
- Large fact tables being scanned
- Many-to-many relationships
- Missing aggregation tables (for DirectQuery)

**Visual display** — Time to render the chart/table in the browser. A high visual display time usually means:
- Too many data points (e.g., 100,000 rows in a table visual)
- Complex custom visuals
- Many visuals on the page

**Other** — Everything else: network roundtrips, page loading, report framework overhead.

---

## Starting a Performance Analysis

### Step-by-Step

1. Open Power BI Desktop
2. Navigate to the report page you want to analyze
3. Go to **View** tab → click **Performance Analyzer**
4. In the Performance Analyzer pane, click **Start Recording**
5. Click **Refresh Visuals** (clears cache and reloads all visuals)
6. Wait for all visuals to load
7. Review the timing breakdown per visual
8. Click **Stop** when done

### Copying the DAX Query

For any visual showing a DAX query time, click **Copy query** to get the exact DAX query that was sent. You can paste this into DAX Studio for deeper analysis.

---

## Import vs DirectQuery Performance

### Import Mode

In Import mode, data is loaded into VertiPaq (in-memory engine). DAX queries run against the local in-memory store.

- **Typical DAX query time**: < 1 second for well-designed models
- **Bottleneck**: DAX complexity, model size, or bad cardinality

### DirectQuery Mode

In DirectQuery mode, every visual generates a SQL query that runs against the source database in real time.

- **Typical DAX query time**: 1-10+ seconds (depends on database performance)
- **Bottleneck**: Network latency, database query performance, missing indexes on the source

```
Import Mode:
User opens report → DAX → VertiPaq (local, fast) → Results

DirectQuery Mode:
User opens report → DAX → SQL generation → Source DB → SQL results → DAX → Results
                                              ↑ Network trip here!
```

---

## Quick Wins After Seeing Performance Analyzer Results

| High Time In | Likely Cause | Quick Fix |
|---|---|---|
| DAX query (Import) | Slow measure | Open in DAX Studio; check FILTER vs column filter |
| DAX query (DirectQuery) | Source DB slow | Add index on source; check query in SSMS |
| Visual display | Too many data points | Apply top N filter; use aggregated visual |
| Other | Network latency | Check report server location; reduce visual count |

---

## Performance Analyzer + DAX Studio Workflow

1. Run Performance Analyzer → copy a slow visual's DAX query
2. Open DAX Studio → connect to the same Power BI Desktop file
3. Paste the copied query
4. Enable **Server Timings** in DAX Studio
5. Run the query
6. See the FE/SE breakdown and individual storage engine queries

```
DAX Studio Server Timings output:
Total Duration: 2,340ms
  SE Duration:     180ms   (8%)  ← Only 8% efficient SE work
  FE Duration:   2,160ms  (92%)  ← 92% slow FE work
  SE Queries:        12
  SE Cache Hits:      3
```

If FE >> SE, the measure has inefficient DAX patterns (FILTER iterators, RANKX, etc.).

---

## Common Visual Performance Issues

### Too Many Data Points

A table visual with 50,000 rows:
- DAX query: 200ms (fast)
- Visual display: 8,000ms (slow — rendering 50,000 rows)

**Fix**: Apply Top N filter or use a summary chart.

### Interaction Filters Triggering Re-Queries

Every click on one visual triggers a DAX re-query on all related visuals. With 15 visuals, one slicer click triggers 15 separate DAX queries.

**Fix**: Use **Edit Interactions** to disable unnecessary cross-filtering; use **Query Reduction** settings.

### Disconnected Slicers

Slicers that show all values regardless of other filters trigger full table scans on render.

**Fix**: Set "Single select" on slicers; use cascading parameter patterns.

---

## Query Reduction Settings

Found under **File → Options and Settings → Options → Query Reduction**:

| Setting | Effect |
|---|---|
| Reduce number of queries sent by changing defaults for cross-highlighting/filtering | Slicers show "Apply" button instead of live filtering |
| Add an Apply button to all basic slicers | User clicks Apply before refresh triggers |
| Add an Apply button to all dropdowns | Same, for dropdown slicers |

These settings reduce the number of DAX queries triggered during user interaction, significantly improving interactivity for slow reports.

---

## Summary

- Performance Analyzer shows **DAX query**, **visual display**, and **Other** times per visual
- High **DAX query** time → DAX optimization or source query tuning
- High **visual display** time → reduce data points or simplify visual
- Use **Copy query** to get the DAX for analysis in DAX Studio
- **DirectQuery** adds network round-trips; Import mode is faster
- Enable **Query Reduction** settings to reduce unnecessary re-queries
