---
title: "Text Processing - Intermediate"
topic: bash-scripting
subtopic: text-processing
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [bash, text-processing, awk, jq, json, csv, regex]
---

# Bash Text Processing — Intermediate

## Advanced awk Patterns

### Multi-Line Processing and Aggregation

```bash
# Group-by aggregation (like SQL GROUP BY):
# "Sum of amount by region" from a CSV
awk -F',' 'NR > 1 {
    region[$5] += $3
    count[$5]++
}
END {
    for (r in region) {
        printf "%-15s Revenue: %10.2f  Orders: %d  Avg: %.2f\n", r, region[r], count[r], region[r]/count[r]
    }
}' orders.csv

# Output:
# US              Revenue:  1250000.00  Orders: 15000  Avg: 83.33
# EU              Revenue:   890000.00  Orders: 10500  Avg: 84.76
# APAC            Revenue:   450000.00  Orders: 5200   Avg: 86.54

# Data validation: find rows with wrong column count
awk -F',' 'NF != 8 { print NR": "$0 }' data.csv
# Prints: line_number: full_line (for rows not having exactly 8 columns)

# Pivot: transform rows to columns
# Input: date,metric,value → Output: date,metric1_value,metric2_value
awk -F',' 'NR>1 { data[$1][$2] = $3 }
END {
    for (date in data) {
        printf "%s,%s,%s\n", date, data[date]["revenue"], data[date]["orders"]
    }
}' metrics.csv
```

---

## JSON Processing with jq

```bash
# jq: the "awk for JSON" — essential for API responses and JSON data files

# Basic extraction:
echo '{"name":"John","age":30}' | jq '.name'          # "John"
echo '{"name":"John","age":30}' | jq -r '.name'       # John (raw, no quotes)

# Nested objects:
cat response.json | jq '.data.orders[0].amount'        # First order's amount

# Array processing:
cat orders.json | jq '.orders[] | .amount'             # All amounts
cat orders.json | jq '.orders | length'                # Count of orders
cat orders.json | jq '[.orders[].amount] | add'        # Sum of all amounts

# Filter:
cat orders.json | jq '.orders[] | select(.amount > 100)'  # Orders > $100
cat orders.json | jq '.orders[] | select(.region == "US")' # US orders only

# Transform (reshape JSON):
cat orders.json | jq '.orders[] | {id: .order_id, total: .amount, date: .order_date}'
# Outputs: {"id": 1001, "total": 99.50, "date": "2024-03-15"}

# JSON to CSV:
cat orders.json | jq -r '.orders[] | [.order_id, .amount, .date] | @csv'
# Outputs: 1001,99.50,"2024-03-15"

# API response processing:
curl -s "https://api.example.com/orders" | jq -r '
    .data[] | [.id, .customer_name, .total, .status] | @csv
' > orders_export.csv
```

---

## Regular Expressions (Advanced Patterns)

```bash
# Extract structured data from unstructured text:

# Extract email addresses:
grep -oP '[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}' contacts.txt

# Extract dates (YYYY-MM-DD):
grep -oP '\d{4}-\d{2}-\d{2}' log.txt

# Extract key=value pairs from log lines:
echo "ts=2024-03-15 level=ERROR msg=connection_timeout host=db-prod-01" | \
    grep -oP '(?<=host=)\S+'
# Output: db-prod-01

# Validate email format:
if echo "$email" | grep -qP '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'; then
    echo "Valid email"
fi

# Extract and reformat dates:
echo "March 15, 2024" | sed -E 's/(\w+) ([0-9]+), ([0-9]+)/\3-\1-\2/'
# → 2024-March-15 (would need further mapping for month numbers)

# Parse Apache log format:
awk '{print $1, $4, $7, $9}' access.log
# Output: IP [timestamp] /path status_code
```

---

## CSV Manipulation

```bash
# csvkit: proper CSV handling (respects quoting, escaping)
# pip install csvkit

csvcut -c 1,3,5 data.csv              # Extract columns by number
csvcut -c name,amount data.csv         # Extract columns by name
csvgrep -c status -m "active" data.csv # Filter by column value
csvstat data.csv                       # Statistics per column
csvsort -c amount -r data.csv          # Sort by column (reverse)
csvjoin -c customer_id orders.csv customers.csv  # JOIN two CSVs!

# Without csvkit (pure bash — handle simple cases):
# Remove a column (column 4):
cut -d',' --complement -f4 data.csv

# Reorder columns (3,1,5,2,4):
awk -F',' -v OFS=',' '{print $3,$1,$5,$2,$4}' data.csv

# Add row numbers:
awk -F',' -v OFS=',' '{print NR","$0}' data.csv

# Concatenate CSVs (keep header from first file only):
head -1 file1.csv > combined.csv
for f in file*.csv; do tail -n +2 "$f" >> combined.csv; done
```

---

## Log Parsing for Data Engineers

```bash
# Parse structured log lines (key=value format):
# Input: ts=2024-03-15T10:30:00 level=ERROR pipeline=daily_orders msg="OOM in executor 3" duration=45.2

# Extract all errors with their pipeline and duration:
grep "level=ERROR" pipeline.log | \
    awk '{
        for (i=1; i<=NF; i++) {
            split($i, kv, "=")
            if (kv[1] == "pipeline") pipeline = kv[2]
            if (kv[1] == "duration") duration = kv[2]
            if (kv[1] == "msg") msg = substr($0, index($0,"msg=")+4)
        }
        printf "%s | %ss | %s\n", pipeline, duration, msg
    }'

# Count errors per hour:
grep "ERROR" pipeline.log | \
    grep -oP '\d{4}-\d{2}-\d{2}T\d{2}' | \
    sort | uniq -c | sort -rn
# Output: count YYYY-MM-DDThh (errors per hour)

# Find slowest operations:
grep "duration=" pipeline.log | \
    grep -oP 'duration=\K[0-9.]+' | \
    sort -rn | head -10
# Top 10 longest durations
```

---

## Text Transformation Pipelines

```bash
# Complete transformation: raw API response → clean CSV for loading

# Step 1: Fetch data
curl -s "https://api.example.com/orders?date=2024-03-15" | \

# Step 2: Extract relevant fields (jq)
jq -r '.data[] | [.id, .customer.name, .amount, .status, .created_at] | @csv' | \

# Step 3: Clean (remove quotes around numbers, fix nulls)
sed 's/"null"//g; s/""//g' | \

# Step 4: Filter (only completed orders)
awk -F',' '$4 ~ /completed/' | \

# Step 5: Sort by amount (descending)
sort -t',' -k3 -rn | \

# Step 6: Add header and save
(echo "order_id,customer_name,amount,status,created_at"; cat) > clean_orders.csv

echo "Rows: $(wc -l < clean_orders.csv)"
```

---

## Interview Tips

> **Tip 1:** "How do you process JSON in bash?" — Use `jq`: extract fields (`.data.field`), filter arrays (`select(.amount > 100)`), transform to CSV (`@csv`). For API responses: `curl | jq` pipeline. jq is as important as grep/awk for modern DE work (everything is JSON now).

> **Tip 2:** "How do you aggregate data in bash (like GROUP BY)?" — `awk` with associative arrays: `{region[$5] += $3} END {for (r in region) print r, region[r]}`. Alternative: `sort | uniq -c` for counting. These are quick analysis tools — for production aggregations, use SQL/Spark (but bash is perfect for ad-hoc exploration).

> **Tip 3:** "How do you handle CSV quoting in bash?" — For simple CSVs (no quoted commas): `cut -d','` and `awk -F','` work fine. For complex CSVs (quoted fields with commas inside): use `csvkit` (Python-based, proper CSV parsing) or `python -c "import csv; ..."`. Never trust bare `cut` for production CSV processing with quoted fields.
