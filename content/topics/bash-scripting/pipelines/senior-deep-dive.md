---
title: "Bash Pipelines - Senior Deep Dive"
topic: bash-scripting
subtopic: pipelines
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [bash, pipelines, production, streaming, architecture, performance]
---

# Bash Pipelines — Senior-Level Deep Dive

## Streaming ETL Architecture

```bash
#!/bin/bash
# Production streaming ETL: S3 → transform → validate → S3
# Processes 50 GB with zero local disk usage (pure streaming!)
set -euo pipefail

SOURCE="s3://data-lake/landing/$(date +%Y/%m/%d)/orders.csv.gz"
TARGET="s3://data-lake/processed/$(date +%Y/%m/%d)/orders_clean.csv.gz"
REJECT="s3://data-lake/rejected/$(date +%Y/%m/%d)/orders_rejected.csv.gz"

# Stream: download → decompress → validate → split (good/bad) → compress → upload
# All in ONE pipeline — zero temp files!

aws s3 cp "$SOURCE" - | \
    gunzip | \
    awk -F',' -v OFS=',' '
        NR == 1 { print; next }                    # Pass header through
        NF == 8 && $3+0 > 0 && $4 ~ /^[0-9]{4}-/ { print }  # Valid rows
    ' | \
    gzip | \
    aws s3 cp - "$TARGET"

echo "Streaming ETL complete: $SOURCE → $TARGET"
# 50 GB processed with <20 MB RAM, zero disk, ~15 minutes on a t3.micro!
```

---

## Concurrent Pipeline Architecture

```bash
#!/bin/bash
# Process multiple streams simultaneously with coordination

# Named pipes for complex topologies:
mkfifo /tmp/pipe_valid /tmp/pipe_invalid

# PRODUCER: read source, split into valid + invalid streams
split_stream() {
    awk -F',' '
        NR == 1 { print > "/tmp/pipe_valid"; print > "/tmp/pipe_invalid"; next }
        $3+0 > 0 { print > "/tmp/pipe_valid" }
        $3+0 <= 0 { print > "/tmp/pipe_invalid" }
    ' < /data/input.csv
}

# CONSUMERS: each processes its stream independently
process_valid() { sort -t',' -k1 < /tmp/pipe_valid | gzip > /data/output/valid.csv.gz; }
process_invalid() { cat /tmp/pipe_invalid | gzip > /data/output/rejected.csv.gz; }

# Run all in parallel:
split_stream &
process_valid &
process_invalid &
wait

rm -f /tmp/pipe_valid /tmp/pipe_invalid
echo "Split complete: valid + rejected files created"
```

---

## Pipeline Monitoring and Metrics

```bash
#!/bin/bash
# Instrumented pipeline: measures throughput at each stage

pipeline_with_metrics() {
    local input="$1"
    local output="$2"
    local metrics_file="/tmp/pipeline_metrics_$$.json"
    
    local start=$(date +%s%N)
    
    # Count input rows
    local input_rows=$(wc -l < "$input")
    
    # Pipeline with row counters at each stage:
    local after_filter after_dedup
    
    tail -n +2 "$input" | \
        tee >(wc -l > /tmp/stage1_$$) | \
        awk -F',' '$3 > 0' | \
        tee >(wc -l > /tmp/stage2_$$) | \
        sort -t',' -k1 -u | \
        tee >(wc -l > /tmp/stage3_$$) | \
        (echo "$(head -1 "$input")"; cat) > "$output"
    
    local end=$(date +%s%N)
    local duration_ms=$(( (end - start) / 1000000 ))
    
    after_filter=$(cat /tmp/stage2_$$)
    after_dedup=$(cat /tmp/stage3_$$)
    
    # Emit metrics:
    cat > "$metrics_file" << EOF
{
    "input_rows": $((input_rows - 1)),
    "after_filter": $after_filter,
    "after_dedup": $after_dedup,
    "output_rows": $(( $(wc -l < "$output") - 1 )),
    "duration_ms": $duration_ms,
    "rows_per_sec": $(( (input_rows - 1) * 1000 / (duration_ms + 1) )),
    "filter_drop_pct": $(echo "scale=1; (1 - $after_filter / ($input_rows - 1)) * 100" | bc),
    "dedup_drop_pct": $(echo "scale=1; (1 - $after_dedup / $after_filter) * 100" | bc)
}
EOF
    
    cat "$metrics_file"
    rm -f /tmp/stage*_$$ "$metrics_file"
}

pipeline_with_metrics "/data/input.csv" "/data/output.csv"
# Output: {"input_rows":1000000,"after_filter":920000,"after_dedup":850000,...}
```

---

## Advanced: Parallel Pipeline with Fan-Out/Fan-In

```bash
#!/bin/bash
# Fan-out: split data by region, process each in parallel, fan-in (combine)

INPUT="/data/orders.csv"
OUTPUT="/data/output/orders_enriched.csv"
TEMP="/tmp/pipeline_$$"
REGIONS=(US EU APAC LATAM)

mkdir -p "$TEMP"

# STEP 1: Fan-out (split by region into separate files — parallel)
echo "Splitting by region..."
awk -F',' -v dir="$TEMP" 'NR==1{hdr=$0;next} {print >> dir"/"$5".csv"}' "$INPUT"

# STEP 2: Process each region in parallel
echo "Processing regions in parallel..."
for region in "${REGIONS[@]}"; do
    if [ -f "$TEMP/$region.csv" ]; then
        (
            echo "$hdr"  # Add header
            python /opt/etl/enrich_region.py --region="$region" < "$TEMP/$region.csv"
        ) > "$TEMP/${region}_enriched.csv" &
    fi
done
wait
echo "All regions processed."

# STEP 3: Fan-in (combine all regional outputs)
echo "Combining results..."
head -1 "$TEMP/US_enriched.csv" > "$OUTPUT"  # Header from first file
for region in "${REGIONS[@]}"; do
    [ -f "$TEMP/${region}_enriched.csv" ] && tail -n +2 "$TEMP/${region}_enriched.csv" >> "$OUTPUT"
done

echo "Output: $(wc -l < "$OUTPUT") rows → $OUTPUT"
rm -rf "$TEMP"
```

---

## Interview Tips

> **Tip 1:** "Design a streaming ETL that processes 50 GB with zero disk space" — `aws s3 cp source - | gunzip | awk 'transform' | gzip | aws s3 cp - target`. All streaming: download directly to pipe → decompress in-stream → transform in-stream → compress in-stream → upload from pipe. Zero disk. Constant memory (~20 MB). Works on the smallest EC2 instance!

> **Tip 2:** "Fan-out/fan-in in bash pipelines?" — Split: awk routes rows to different files by key. Process: each subset processed in parallel (background &, then wait). Combine: cat all outputs together (head from first for header, tail +2 from rest for data). Pattern: partition → parallel process → merge. Scales linearly with CPU cores.

> **Tip 3:** "How do you monitor pipeline throughput?" — Insert `tee >(wc -l > /tmp/stage_N)` between stages to count rows without interfering with data flow. Calculate: rows_per_second = total_rows / duration. Track drop rates per stage (filter_drop_pct, dedup_drop_pct). Emit as JSON metrics for dashboards.
