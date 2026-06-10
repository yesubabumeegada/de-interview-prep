---
title: "Text Processing - Scenario Questions"
topic: bash-scripting
subtopic: text-processing
content_type: scenario_question
tags: [bash, text-processing, interview, scenarios]
---

# Scenario Questions — Bash Text Processing

<article data-difficulty="junior">

## 🟢 Junior: Extract and Count from CSV

**Scenario:** Given `orders.csv` with columns: order_id, customer_id, amount, date, region — find the top 5 regions by order count using only bash commands.

<details>
<summary>💡 Hint</summary>
Extract the region column (cut/awk), skip header (tail -n +2), count each value (sort | uniq -c), sort descending, take top 5.
</details>

<details>
<summary>✅ Solution</summary>

```bash
# Extract region (column 5), skip header, count, sort, top 5:
tail -n +2 orders.csv | cut -d',' -f5 | sort | uniq -c | sort -rn | head -5

# Output:
#   15234 US
#   10567 EU
#    5890 APAC
#    3201 LATAM
#    1456 AFRICA

# Alternative with awk (handles more complex cases):
awk -F',' 'NR > 1 { count[$5]++ } END { for(r in count) print count[r], r }' orders.csv | sort -rn | head -5
```

**Key Points:**
- `tail -n +2`: skip header (line 1)
- `cut -d',' -f5`: extract column 5 (comma-delimited)
- `sort | uniq -c`: count occurrences of each unique value
- `sort -rn | head -5`: top 5 by count (descending numeric)
- This processes multi-GB files with constant memory (streaming!)
- Total: one line, instant answer, no Python/SQL needed

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: JSON API Processing

**Scenario:** An API returns JSON: `{"orders": [{"id": 1, "amount": 99.50, "status": "complete"}, ...]}`. Extract all order IDs and amounts where status is "complete", output as CSV.

<details>
<summary>💡 Hint</summary>
Use jq: select by status, extract fields, format as CSV with @csv.
</details>

<details>
<summary>✅ Solution</summary>

```bash
# Using jq:
cat response.json | jq -r '.orders[] | select(.status == "complete") | [.id, .amount] | @csv'

# Output:
# 1,99.50
# 3,150.00
# 5,75.25

# With header:
(echo "order_id,amount"; cat response.json | jq -r '.orders[] | select(.status == "complete") | [.id, .amount] | @csv') > complete_orders.csv

# From a live API:
curl -s "https://api.example.com/orders" | \
    jq -r '.orders[] | select(.status == "complete") | [.id, .amount] | @csv' > output.csv

# Count by status (quick aggregate):
cat response.json | jq -r '.orders[].status' | sort | uniq -c | sort -rn
#   45 complete
#   12 pending
#    3 failed
```

**Key Points:**
- `jq -r`: raw output (no JSON quotes)
- `.orders[]`: iterate over array
- `select(.status == "complete")`: filter
- `[.id, .amount] | @csv`: format as CSV
- Combine with `curl` for direct API → CSV pipeline
- jq is essential for any DE working with JSON APIs

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Log-Based SLA Monitoring

**Scenario:** Pipeline logs have format: `2024-03-15T10:30:45 [INFO] pipeline=daily_orders step=complete duration_ms=45000`. Write a bash script that: (1) finds all pipeline completions today, (2) checks if any took longer than SLA (30 minutes = 1800000ms), (3) alerts on SLA breaches.

<details>
<summary>💡 Hint</summary>
grep for "step=complete" + today's date, extract duration_ms with grep -oP, compare against threshold, alert if exceeded.
</details>

<details>
<summary>✅ Solution</summary>

```bash
#!/bin/bash
LOG="/var/log/pipelines/etl.log"
SLA_MS=1800000  # 30 minutes in milliseconds
TODAY=$(date +%Y-%m-%d)
SLACK_WEBHOOK="${SLACK_WEBHOOK_URL}"

echo "=== SLA Check: $TODAY ==="

# Find completions today, extract pipeline name and duration
breaches=0
grep "$TODAY" "$LOG" | grep "step=complete" | while IFS= read -r line; do
    pipeline=$(echo "$line" | grep -oP 'pipeline=\K\S+')
    duration=$(echo "$line" | grep -oP 'duration_ms=\K[0-9]+')
    timestamp=$(echo "$line" | grep -oP '^\S+')
    
    duration_min=$((duration / 60000))
    
    if [ "$duration" -gt "$SLA_MS" ]; then
        echo "❌ SLA BREACH: $pipeline took ${duration_min}min (limit: 30min) at $timestamp"
        breaches=$((breaches + 1))
        
        # Alert
        if [ -n "$SLACK_WEBHOOK" ]; then
            curl -s -X POST "$SLACK_WEBHOOK" \
                -d "{\"text\":\"🚨 SLA Breach: $pipeline took ${duration_min}min (limit: 30min)\"}" > /dev/null
        fi
    else
        echo "✓ $pipeline: ${duration_min}min (within SLA)"
    fi
done

# Summary
total=$(grep "$TODAY" "$LOG" | grep -c "step=complete" || echo 0)
echo ""
echo "Total completions: $total, SLA breaches: $breaches"
[ "$breaches" -gt 0 ] && exit 1 || exit 0
```

**Key Points:**
- `grep -oP 'pattern=\K\S+'`: extract value after key= (Perl regex, \K resets match start)
- Compare numeric duration against threshold (bash arithmetic)
- Alert via curl to Slack webhook (instant notification)
- Exit code: 0 (all good) or 1 (breaches) — integrates with monitoring systems
- Production pattern: run this every 15 minutes via cron to catch SLA issues early
- Can be extended: track trends (log to file), historical analysis, multiple SLA tiers

</details>

</article>

---

## ⚡ Quick-fire Q&A

**Q: What is the difference between `grep`, `sed`, and `awk`?**
A: `grep` filters lines by pattern. `sed` is a stream editor for substitutions and deletions on lines. `awk` is a full text-processing language that splits lines into fields and supports arithmetic, conditionals, and aggregation. Use `grep` to find, `sed` to transform, and `awk` to compute.

**Q: How do you extract a specific column from a CSV file using bash tools?**
A: Use `awk -F',' '{print $3}' file.csv` to print the third column. For CSV with quoted fields, use `cut -d',' -f3` for simple cases, or a proper CSV-aware tool like `csvkit` (`csvcut -c 3`) for robustness.

**Q: How do you count the occurrences of each unique value in a column?**
A: Pipeline: `awk -F',' '{print $2}' file.csv | sort | uniq -c | sort -rn`. `sort` groups identical values, `uniq -c` counts them, and the final `sort -rn` ranks by frequency descending.

**Q: How do you perform an in-place substitution with `sed`?**
A: Use the `-i` flag: `sed -i 's/old/new/g' file.txt`. On macOS, BSD `sed` requires an explicit backup extension: `sed -i '' 's/old/new/g' file.txt`. Always back up files before in-place edits in production.

**Q: How do you join two files on a common key in bash?**
A: Use `join` after sorting both files on the key field: `join -t',' -1 1 -2 1 <(sort -t',' -k1 file1.csv) <(sort -t',' -k1 file2.csv)`. For complex joins, `awk` with an associative array is more flexible.

**Q: How do you remove duplicate lines from a file while preserving order?**
A: Use `awk '!seen[$0]++'` — it prints a line only the first time it is encountered, preserving order without sorting. `sort -u` removes duplicates but sorts the output, which changes order.

**Q: How do you handle files with Windows line endings (CRLF) in bash?**
A: Use `dos2unix file.txt` to convert in place, or `sed 's/\r//' file.txt` to strip carriage returns. Always check for CRLF when processing files received from Windows systems, as `\r` causes subtle bugs in field parsing and comparisons.

**Q: How do you extract lines between two patterns using `sed` or `awk`?**
A: With `sed`: `sed -n '/START/,/END/p' file.txt`. With `awk`: `awk '/START/{p=1} p; /END/{p=0}' file.txt`. The `awk` version gives finer control over whether to include the delimiter lines.

---

## 💼 Interview Tips

- Show that you know when to reach for `awk` versus Python — for quick field extraction and counting, `awk` in a pipeline beats spinning up a Python process; for complex logic or large structured data, Python or Spark is more appropriate.
- Mention `csvkit` when discussing CSV processing; it handles quoted fields, headers, and encodings correctly, whereas naive `cut`/`awk` breaks on real-world CSV data.
- Demonstrate awareness of encoding issues (UTF-8, CRLF) — they are a constant pain in data pipelines and mentioning them proactively signals real-world experience.
- For senior DE roles, connect text processing to log analysis and data quality checks: describe using `grep`/`awk` pipelines in on-call scenarios to diagnose pipeline failures quickly.
- Senior interviewers appreciate `awk` one-liners for aggregation — being able to produce a frequency table or sum a column without writing a Python script is a valued practical skill.
- Avoid writing overly clever one-liners during interviews; prioritize readability and be ready to explain each stage of a pipeline step by step.
