---
title: "dbt Seeds & Snapshots - Scenarios"
topic: dbt
subtopic: seeds-and-snapshots
content_type: scenario_question
tags: [dbt, seeds, snapshots, interview, scenarios]
---

# dbt Seeds & Snapshots — Interview Scenarios

<article data-difficulty="junior">

## 🟢 Junior: Deciding Between Seed and Source

**Scenario:** Your company has two datasets they want in dbt: (1) a 200-row CSV of US state codes and region mappings maintained by the analyst team, and (2) a 50M-row customer database loaded by Fivetran every hour. Which should be a seed and which a source? Why?

<details>
<summary>💡 Hint</summary>

Seeds are for small, static reference data that fits in git and is managed by analysts. Sources are for large, frequently-updated tables loaded by external tools.

</details>

<details>
<summary>✅ Solution</summary>

**State codes → Seed:**
- 200 rows → tiny, fits in git
- Maintained by analysts → they can edit the CSV directly
- Changes rarely (maybe yearly)
- No ETL tool needed

```
seeds/state_regions.csv
```

**Customer database → Source:**
- 50M rows → way too large for a CSV in git
- Loaded by Fivetran → has its own update mechanism
- Changes hourly → seeds require manual `dbt seed` to update
- Needs freshness checks

```yaml
sources:
  - name: fivetran
    tables:
      - name: customers
        freshness:
          error_after: {count: 3, period: hour}
```

**Rule of thumb:** Seeds are for lookup tables maintained by humans. Sources are for everything loaded by pipelines.

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Understanding Snapshot Granularity Limitations

**Scenario:** Your `snap_customers` snapshot runs daily. A customer upgraded from Silver to Gold tier on Tuesday at 2pm, then downgraded back to Silver on Wednesday at 11am. It's now Thursday. The snapshot ran Tuesday night and Wednesday night. What does the snapshot table show? Is any history missing?

<details>
<summary>💡 Hint</summary>

Snapshots only capture the state at the time they run. Changes that happened and reversed within a single snapshot interval will be partially visible — but the exact timestamps won't be accurate.

</details>

<details>
<summary>✅ Solution</summary>

Snapshot runs: Tuesday night (~11pm), Wednesday night (~11pm)

| Run | State captured |
|---|---|
| Tuesday night | customer_id=X, tier=Gold (upgraded at 2pm, captured at 11pm) |
| Wednesday night | customer_id=X, tier=Silver (downgraded at 11am, captured at 11pm) |

Snapshot table:

| customer_id | tier | dbt_valid_from | dbt_valid_to |
|---|---|---|---|
| X | Silver (original) | 2023-01-01 | Tue 11pm |
| X | Gold | Tue 11pm | Wed 11pm |
| X | Silver | Wed 11pm | NULL |

**Missing history:** The Gold → Silver downgrade happened at 11am Wednesday, but the snapshot captured it at 11pm. The snapshot records show Gold was valid from Tuesday 11pm → Wednesday 11pm (24 hours), not the actual 21 hours it was truly Gold.

**Key insight:** Snapshot granularity is limited by how often you run `dbt snapshot`. For high-frequency changes, run snapshots every hour instead of daily. For extremely precise tracking, use CDC (Change Data Capture) at the source.

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Recovering a Dropped Snapshot Table

**Scenario:** A colleague accidentally ran `DROP TABLE snapshots.snap_customers` in production. The snapshot had 3 years of customer tier history. `dbt snapshot` will recreate the table but with no history — just the current state. How do you recover?

<details>
<summary>💡 Hint</summary>

Start with the warehouse's native recovery features (Time Travel on Snowflake, UNDROP) before looking at backups. The recovery path depends on what's available.

</details>

<details>
<summary>✅ Solution</summary>

**Step 1 — Check if recovery is possible:**

```sql
-- Snowflake: check Time Travel (up to 90 days)
SELECT * FROM snapshots.snap_customers
AT (TIMESTAMP => '2024-01-15 09:00:00'::TIMESTAMP);

-- If available, restore:
CREATE TABLE snapshots.snap_customers AS
SELECT * FROM snapshots.snap_customers
AT (TIMESTAMP => DATEADD('hour', -1, CURRENT_TIMESTAMP()));
```

**Step 2 — If Time Travel is available (Snowflake/Delta Lake):**
```sql
-- UNDROP (Snowflake)
UNDROP TABLE snapshots.snap_customers;
-- Done! Full history restored.
```

**Step 3 — If backup exists (S3/GCS export):**
```sql
-- Restore from nightly export
COPY INTO snapshots.snap_customers
FROM 's3://backups/snapshots/snap_customers/2024-01-14/'
FILE_FORMAT = (TYPE = PARQUET);
```

**Step 4 — If no backup:**
- Re-run `dbt snapshot` to create fresh table (current state only)
- Document the data loss incident
- Implement preventive measures:

```sql
-- Add daily export to S3 as backup
-- In post-hook or separate job:
COPY INTO 's3://backups/snapshots/snap_customers/{{ run_started_at[:10] }}/'
FROM snapshots.snap_customers
FILE_FORMAT = (TYPE = PARQUET);
```

**Prevention checklist:**
- Enable Snowflake Fail-Safe (7 days beyond Time Travel)
- Daily backup to S3 using `COPY INTO`
- Add `GRANT OWNERSHIP` restrictions so analysts can't drop production tables
- Enable Terraform/IaC for snapshot tables to detect drift

</details>

</article>

---

## Interview Tips

> **Tip 1:** "When would you use a seed vs a source?" — Seeds are for analyst-maintained lookup tables that fit in git: country codes, product categories, tax rates. Sources are for tables loaded by external tools like Fivetran. The key indicator is size and update frequency.

> **Tip 2:** "What are the limitations of dbt snapshots?" — Snapshots only capture the state at run time. Changes that happen and revert between snapshot runs are partially tracked — you'll see the change happened but the exact timestamps won't be accurate. For precision tracking, run snapshots more frequently or use CDC.

> **Tip 3:** "How would you recover from an accidentally dropped snapshot table?" — Check the warehouse's native recovery first: Snowflake UNDROP or Time Travel, Delta Lake time travel. If that's not available, restore from your most recent backup. Going forward: enable Time Travel retention, add daily S3 exports, and restrict DROP privileges on production snapshot tables.
