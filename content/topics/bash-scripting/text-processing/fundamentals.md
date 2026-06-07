---
title: "Text Processing - Fundamentals"
topic: bash-scripting
subtopic: text-processing
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [bash, text-processing, grep, awk, sed, data-engineering]
---

# Bash Text Processing — Fundamentals

## Why Text Processing Matters for DE

Data engineers work with: CSV/JSON files, log files, config files, API responses, and command output. Bash text processing tools (`grep`, `awk`, `sed`, `sort`, `uniq`) let you quickly inspect, transform, and validate data without writing Python scripts.

```bash
# Common DE text processing tasks:
# - Count rows in a CSV: wc -l data.csv
# - Find errors in logs: grep "ERROR" pipeline.log
# - Extract specific columns: awk -F',' '{print $1,$3}' data.csv
# - Replace values: sed 's/NULL//g' data.csv
# - Get unique values: cut -d',' -f5 data.csv | sort -u
# - Quick data profiling: sort | uniq -c | sort -rn
```

---

## grep (Search Text)

```bash
# Basic search:
grep "ERROR" /var/log/pipeline.log              # Lines containing "ERROR"
grep -i "error" log.txt                          # Case-insensitive
grep -c "ERROR" log.txt                          # COUNT of matching lines
grep -n "ERROR" log.txt                          # Show line numbers
grep -v "DEBUG" log.txt                          # EXCLUDE lines with "DEBUG"

# Regex patterns:
grep -E "ERROR|FATAL" log.txt                    # Multiple patterns (OR)
grep -E "order_id=[0-9]+" log.txt                # Regex: order_id=<digits>
grep -P '\d{4}-\d{2}-\d{2}' log.txt             # Perl regex: date pattern

# Search in multiple files:
grep -r "connection_string" /etc/config/         # Recursive search
grep -l "ERROR" *.log                            # List FILES with matches (not lines)

# DE-specific:
grep -c "^" data.csv                             # Row count (same as wc -l)
grep ",," data.csv | wc -l                       # Count rows with empty fields
grep -v "^$" data.csv                            # Remove blank lines
```

---

## awk (Column Processing)

```bash
# awk processes text COLUMN BY COLUMN (perfect for CSV/TSV!)

# Basic column extraction:
awk -F',' '{print $1, $3}' data.csv              # Print columns 1 and 3 (comma-delimited)
awk -F'\t' '{print $2}' data.tsv                 # Tab-delimited

# Filtering rows:
awk -F',' '$3 > 100' data.csv                    # Rows where column 3 > 100
awk -F',' '$5 == "US"' data.csv                  # Rows where column 5 is "US"
awk -F',' 'NR > 1 && $3 > 100' data.csv         # Skip header (NR > 1), filter

# Aggregation:
awk -F',' 'NR > 1 { sum += $3; count++ } END { print "Avg:", sum/count }' data.csv
# Calculate average of column 3!

# Column count (schema check):
awk -F',' '{print NF}' data.csv | sort -u        # Unique column counts
# If output is not a single number → inconsistent schema!

# Add/modify columns:
awk -F',' -v OFS=',' '{ $6 = $3 * 1.1; print }' data.csv  # Add 10% markup column

# Print specific rows:
awk 'NR == 1 || NR == 100' data.csv              # Header + row 100
awk 'NR >= 10 && NR <= 20' data.csv              # Rows 10-20
```

---

## sed (Stream Editor)

```bash
# sed transforms text LINE BY LINE (find/replace, delete, insert)

# Find and replace:
sed 's/NULL//g' data.csv                         # Replace all "NULL" with empty string
sed 's/,$//' data.csv                            # Remove trailing comma
sed 's/  */ /g' data.txt                         # Collapse multiple spaces to one

# Delete lines:
sed '1d' data.csv                                # Delete first line (header)
sed '/^$/d' data.csv                             # Delete blank lines
sed '/^#/d' config.txt                           # Delete comment lines

# Insert/Append:
sed '1i\order_id,amount,date' data.csv           # Insert header at line 1
sed '$a\TOTAL,999,2024-03-15' data.csv           # Append line at end

# In-place editing:
sed -i 's/old_host/new_host/g' config.yaml       # Edit file directly (careful!)
sed -i.bak 's/old/new/g' config.yaml             # Edit with backup (.bak created)

# Multiple operations:
sed -e 's/NULL//g' -e 's/  / /g' -e '1d' data.csv  # Chain multiple edits
```

---

## sort and uniq (Sorting and Dedup)

```bash
# Sort:
sort data.csv                                    # Alphabetical sort
sort -n data.csv                                 # Numeric sort
sort -t',' -k3 -n data.csv                       # Sort by column 3 (numeric, comma-delimited)
sort -t',' -k3 -rn data.csv                      # Reverse numeric sort (descending)
sort -u data.csv                                 # Sort + remove duplicates

# Unique values:
sort data.csv | uniq                             # Deduplicate (requires sorted input!)
sort data.csv | uniq -c                          # Count occurrences
sort data.csv | uniq -c | sort -rn               # Top values (most common first)
sort data.csv | uniq -d                          # Show ONLY duplicates

# DE patterns:
# "What are the top 10 most common regions?"
cut -d',' -f5 orders.csv | sort | uniq -c | sort -rn | head -10

# "Are there duplicate order_ids?"
cut -d',' -f1 orders.csv | sort | uniq -d | head
# If output → duplicates exist!

# "How many unique customers?"
cut -d',' -f2 orders.csv | tail -n +2 | sort -u | wc -l
```

---

## Combining Tools (Pipes)

The real power is COMBINING tools with pipes (`|`):

```bash
# Quick data profile of a CSV:
echo "=== File: orders.csv ==="
echo "Rows: $(wc -l < orders.csv)"
echo "Columns: $(head -1 orders.csv | awk -F',' '{print NF}')"
echo "Header: $(head -1 orders.csv)"
echo ""
echo "=== Top 5 Regions ==="
tail -n +2 orders.csv | cut -d',' -f5 | sort | uniq -c | sort -rn | head -5
echo ""
echo "=== Amount Stats ==="
tail -n +2 orders.csv | awk -F',' '{sum+=$3; if($3>max)max=$3; count++} END {print "Count:", count, "Sum:", sum, "Max:", max, "Avg:", sum/count}'

# Find the most recent ERROR in logs with context:
grep -B2 -A2 "ERROR" pipeline.log | tail -20
# -B2: 2 lines before, -A2: 2 lines after (context!)

# Extract dates from filenames and count files per date:
ls /data/landing/ | grep -oP '\d{4}-\d{2}-\d{2}' | sort | uniq -c
```

---

## Interview Tips

> **Tip 1:** "How do you quickly check a CSV file's structure?" — `wc -l` for row count, `head -1 | awk -F',' '{print NF}'` for column count, `head -5` to see sample data, `awk -F',' '{print NF}' | sort -u` to verify consistent column count across all rows.

> **Tip 2:** "grep vs awk vs sed — when to use which?" — grep: SEARCH/FILTER lines by pattern. awk: PROCESS columns (extract, compute, aggregate). sed: TRANSFORM lines (find/replace, delete/insert). They compose via pipes: grep filters → awk extracts columns → sed cleans values.

> **Tip 3:** "How do you find duplicates in a large file?" — `cut -d',' -f1 file.csv | sort | uniq -d` (extracts key column, sorts, shows only duplicated values). For count of duplicates: `sort | uniq -c | awk '$1 > 1'`. Scales to any file size (streaming sort, constant memory per unique value).
