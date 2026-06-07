---
title: "Text Processing - Real-World Production Examples"
topic: bash-scripting
subtopic: text-processing
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [bash, text-processing, production, log-analysis, data-profiling]
---

# Bash Text Processing — Real-World Production Examples

## Pattern 1: Quick Data Profiling Script

```bash
#!/bin/bash
# Profile any CSV file — instant insights without Python/Spark

profile_csv() {
    local file="$1"
    echo "═══════════════════════════════════════════"
    echo "Profile: $(basename $file)"
    echo "═══════════════════════════════════════════"
    echo "Size: $(ls -lh "$file" | awk '{print $5}')"
    echo "Rows: $(tail -n +2 "$file" | wc -l) (excl. header)"
    echo "Columns: $(head -1 "$file" | awk -F',' '{print NF}')"
    echo ""
    echo "Header:"
    head -1 "$file" | tr ',' '\n' | nl
    echo ""
    echo "Sample (first 3 rows):"
    head -4 "$file" | column -t -s','
    echo ""
    echo "Column Stats:"
    # For each column: null count, unique count, sample values
    local cols=$(head -1 "$file" | awk -F',' '{print NF}')
    for i in $(seq 1 $cols); do
        local col_name=$(head -1 "$file" | cut -d',' -f$i)
        local nulls=$(tail -n +2 "$file" | cut -d',' -f$i | grep -cE '^$|^NULL$|^null$' || echo 0)
        local uniq=$(tail -n +2 "$file" | cut -d',' -f$i | sort -u | wc -l)
        local sample=$(tail -n +2 "$file" | cut -d',' -f$i | head -3 | tr '\n' ', ')
        printf "  %-20s nulls: %-5s unique: %-8s sample: %s\n" "$col_name" "$nulls" "$uniq" "$sample"
    done
}

profile_csv "$1"
```

---

## Pattern 2: Pipeline Error Analyzer

```bash
#!/bin/bash
# Analyze pipeline logs: find failures, group by type, suggest fixes

LOG_DIR="/var/log/pipelines"
REPORT="/tmp/error_report_$(date +%Y%m%d).txt"

{
echo "╔══════════════════════════════════════════════╗"
echo "║  Pipeline Error Report: $(date +%Y-%m-%d)   ║"
echo "╚══════════════════════════════════════════════╝"
echo ""

echo "## Error Summary (last 24 hours)"
grep -h "ERROR" "$LOG_DIR"/*.log | grep "$(date -d 'yesterday' +%Y-%m-%d)\|$(date +%Y-%m-%d)" | \
    grep -oP '(?<=msg=")[^"]+|(?<=error=)\S+' | sort | uniq -c | sort -rn | head -10
echo ""

echo "## Errors by Pipeline"
for log in "$LOG_DIR"/*.log; do
    pipeline=$(basename "$log" .log)
    count=$(grep -c "ERROR" "$log" 2>/dev/null || echo 0)
    [ "$count" -gt 0 ] && printf "  %-30s %d errors\n" "$pipeline" "$count"
done | sort -k2 -rn
echo ""

echo "## Error Timeline (errors per hour)"
grep -h "ERROR" "$LOG_DIR"/*.log | \
    grep -oP '\d{4}-\d{2}-\d{2}T\d{2}' | sort | uniq -c | \
    awk '{printf "  %s:00  %s errors %s\n", $2, $1, ($1>10?"⚠️":"")}'
echo ""

echo "## Most Recent Errors (last 5)"
grep -h "ERROR" "$LOG_DIR"/*.log | sort -t'=' -k1 | tail -5

} > "$REPORT"

cat "$REPORT"
```

---

## Pattern 3: ETL Data Comparison (Source vs Target)

```bash
#!/bin/bash
# Compare row counts between source files and loaded target (validation)

echo "=== Source vs Target Reconciliation ==="
echo ""
printf "%-30s %10s %10s %10s %s\n" "Table" "Source" "Target" "Diff" "Status"
printf "%-30s %10s %10s %10s %s\n" "-----" "------" "------" "----" "------"

for source_file in /data/landing/today/*.csv; do
    table=$(basename "$source_file" .csv)
    
    # Source count (file rows minus header)
    source_count=$(($(wc -l < "$source_file") - 1))
    
    # Target count (from database)
    target_count=$(psql -t -c "SELECT COUNT(*) FROM silver.$table WHERE load_date = CURRENT_DATE" 2>/dev/null | tr -d ' ')
    target_count=${target_count:-0}
    
    # Compare
    diff=$((target_count - source_count))
    if [ "$diff" -eq 0 ]; then
        status="✓"
    elif [ "$diff" -gt 0 ]; then
        status="⚠️ +$diff (duplicates?)"
    else
        status="❌ $diff (missing rows!)"
    fi
    
    printf "%-30s %10d %10d %10d %s\n" "$table" "$source_count" "$target_count" "$diff" "$status"
done
```

---

## Pattern 4: JSON API Response Processor

```bash
#!/bin/bash
# Fetch paginated API data, transform to CSV, validate, and load

API_URL="https://api.company.com/v2/orders"
OUTPUT="/data/landing/api_orders_$(date +%Y%m%d).csv"
TOKEN="${API_TOKEN}"

# Header
echo "order_id,customer_id,amount,status,created_at" > "$OUTPUT"

page=1
total_rows=0

while true; do
    # Fetch page
    response=$(curl -s -H "Authorization: Bearer $TOKEN" "${API_URL}?page=$page&per_page=1000")
    
    # Check for errors
    error=$(echo "$response" | jq -r '.error // empty')
    if [ -n "$error" ]; then
        echo "API Error on page $page: $error" >&2
        break
    fi
    
    # Extract records and convert to CSV
    rows=$(echo "$response" | jq -r '.data[] | [.id, .customer_id, .amount, .status, .created_at] | @csv')
    
    if [ -z "$rows" ]; then
        echo "No more data (page $page empty)"
        break
    fi
    
    echo "$rows" >> "$OUTPUT"
    count=$(echo "$rows" | wc -l)
    total_rows=$((total_rows + count))
    echo "Page $page: $count rows (total: $total_rows)"
    
    page=$((page + 1))
    sleep 0.5  # Rate limiting
done

echo "Complete: $total_rows rows written to $OUTPUT"

# Validate
if [ $total_rows -gt 0 ]; then
    echo "File size: $(ls -lh "$OUTPUT" | awk '{print $5}')"
    echo "Sample: $(head -3 "$OUTPUT")"
fi
```

---

## Pattern 5: Schema Change Detector

```bash
#!/bin/bash
# Detect schema changes in incoming files (alert if columns change)

SCHEMA_DIR="/data/schemas"
LANDING="/data/landing"

for file in "$LANDING"/*.csv; do
    [ -f "$file" ] || continue
    table=$(basename "$file" .csv | sed 's/_[0-9]*$//')  # Remove date suffix
    
    # Get current header
    current_header=$(head -1 "$file")
    expected_file="$SCHEMA_DIR/${table}.schema"
    
    if [ ! -f "$expected_file" ]; then
        # First time seeing this table — save schema
        echo "$current_header" > "$expected_file"
        echo "NEW SCHEMA: $table (saved)"
    else
        expected_header=$(cat "$expected_file")
        if [ "$current_header" != "$expected_header" ]; then
            # Schema changed!
            echo "⚠️ SCHEMA CHANGE DETECTED: $table"
            echo "  Expected: $expected_header"
            echo "  Got:      $current_header"
            
            # Find differences
            diff <(echo "$expected_header" | tr ',' '\n') <(echo "$current_header" | tr ',' '\n')
            
            # Alert
            curl -X POST "$SLACK_WEBHOOK" -d "{\"text\":\"Schema change in $table!\"}"
        fi
    fi
done
```

---

## Interview Tips

> **Tip 1:** "How do you do source-target reconciliation in bash?" — Count source rows (`wc -l - 1` for CSV), query target count from DB (`psql -t -c "SELECT COUNT(*)"`), compare. Flag: zero diff = good, positive diff = duplicates, negative diff = missing data. Automate: run after each load, alert on mismatches.

> **Tip 2:** "How do you handle paginated API data in bash?" — While-loop: fetch page → check for empty/error → extract with jq → append to CSV → increment page. Add: rate limiting (sleep), error handling (retry on timeout), progress logging (rows per page). Use for: bootstrapping data extraction before building proper Airflow/Python ETL.

> **Tip 3:** "How do you detect schema drift in incoming files?" — Save expected header (first seen) to a schema file. On each new file: compare header with saved schema. If different: alert team (new/removed/renamed columns). This catches upstream changes before they break your pipelines.
