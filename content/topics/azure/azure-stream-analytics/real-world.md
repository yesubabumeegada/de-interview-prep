---
title: "Azure Stream Analytics — Real World"
topic: azure
subtopic: azure-stream-analytics
content_type: study_material
difficulty_level: mid-level
layer: real-world
tags: [azure, stream-analytics, iot, production, fraud, monitoring]
---

# Azure Stream Analytics — Real World

## Pattern 1: IoT Fleet Monitoring

```sql
-- Fleet of 10,000 delivery trucks, each sending GPS + telemetry every 30 sec
-- Goal: alert on speeding, low fuel, maintenance triggers

-- Input: IoT Hub (truck_telemetry)
-- {
--   "device_id": "truck_0042",
--   "speed_kmh": 112,
--   "fuel_level_pct": 8,
--   "engine_temp_c": 105,
--   "latitude": 40.7128,
--   "longitude": -74.0060,
--   "odometer_km": 145823,
--   "event_time": "2024-01-15T14:23:45Z"
-- }

-- Output 1: Speeding alerts (Event Hubs → driver app notification)
SELECT
    device_id,
    speed_kmh,
    latitude,
    longitude,
    event_time,
    'SPEEDING' AS alert_type
INTO speed_alerts_eventhub
FROM truck_telemetry TIMESTAMP BY event_time
WHERE speed_kmh > 100;

-- Output 2: Low fuel alerts (within 100km of refueling) → SQL DB for dispatch
SELECT
    device_id,
    fuel_level_pct,
    latitude,
    longitude,
    System.Timestamp() AS detected_at,
    'LOW_FUEL' AS alert_type
INTO low_fuel_sql
FROM truck_telemetry TIMESTAMP BY event_time
WHERE fuel_level_pct < 10;

-- Output 3: 1-minute fleet summary → Power BI real-time dashboard
SELECT
    System.Timestamp() AS window_end,
    COUNT(*)           AS active_trucks,
    AVG(speed_kmh)     AS avg_speed,
    MIN(fuel_level_pct) AS min_fuel,
    COUNT(CASE WHEN speed_kmh > 100 THEN 1 END) AS speeding_trucks
INTO powerbi_dashboard
FROM truck_telemetry TIMESTAMP BY event_time
GROUP BY TumblingWindow(minute, 1);

-- Output 4: All telemetry → ADLS (for historical analysis)
SELECT *
INTO adls_raw_telemetry
FROM truck_telemetry TIMESTAMP BY event_time;
-- Path pattern: telemetry/{date}/{time}/trucks.parquet
-- File size: 10,000 trucks × 30-sec interval × 1KB/event × 60 events/min = ~600MB/min
```

---

## Pattern 2: Real-Time Financial Fraud Detection

```sql
-- Credit card transaction fraud detection
-- Rules: (1) 3+ transactions in 1 min, (2) 2+ countries in 5 min, (3) amount spike

-- Step 1: High frequency detection (tumbling window 1 min)
WITH HighFrequency AS (
    SELECT
        card_id,
        COUNT(*)         AS txn_count,
        SUM(amount)      AS total_amount,
        System.Timestamp() AS window_end,
        TumblingWindow(minute, 1) AS window_info
    FROM transactions TIMESTAMP BY event_time
    GROUP BY
        card_id,
        TumblingWindow(minute, 1)
    HAVING COUNT(*) >= 3 OR SUM(amount) >= 5000
),

-- Step 2: Multi-country detection (hopping window 5 min, hop 1 min)
MultiCountry AS (
    SELECT
        card_id,
        COUNT(DISTINCT country) AS country_count,
        System.Timestamp()      AS window_end
    FROM transactions TIMESTAMP BY event_time
    GROUP BY
        card_id,
        HoppingWindow(minute, 5, 1)
    HAVING COUNT(DISTINCT country) >= 2
)

-- Output fraud alerts
SELECT
    h.card_id,
    h.txn_count,
    h.total_amount,
    m.country_count,
    h.window_end,
    CASE
        WHEN m.country_count >= 2 THEN 'MULTI_COUNTRY'
        WHEN h.txn_count >= 5 THEN 'HIGH_FREQUENCY'
        ELSE 'HIGH_AMOUNT'
    END AS fraud_type
INTO fraud_alerts
FROM HighFrequency h
LEFT JOIN MultiCountry m ON h.card_id = m.card_id
    AND DATEDIFF(minute, m.window_end, h.window_end) BETWEEN -1 AND 1
WHERE h.txn_count >= 3 OR h.total_amount >= 5000 OR m.country_count >= 2;
```

---

## Pattern 3: Monitoring and Alerting for ASA Itself

```python
# Monitor ASA job health via Azure Monitor + Logic Apps

import subprocess

def check_asa_job_health(resource_group: str, job_name: str):
    """Check ASA job metrics for operational health."""
    
    # Check job state
    result = subprocess.run([
        "az", "stream-analytics", "job", "show",
        "--resource-group", resource_group,
        "--name", job_name,
        "--query", "{state:properties.jobState, lastOutputTime:properties.lastOutputEventTime}"
    ], capture_output=True, text=True)
    
    import json
    status = json.loads(result.stdout)
    print(f"Job state: {status['state']}")
    print(f"Last output: {status.get('lastOutputTime', 'Never')}")
    
    # Key health metrics to alert on:
    metrics_to_monitor = {
        "InputEventBytes":           "Data volume (MB/sec)",
        "OutputEventBytes":          "Output volume",
        "ResourceUtilization":       "SU% utilization — alert if > 80%",
        "WatermarkDelaySeconds":     "How far behind real-time — alert if > 60sec",
        "DroppedOrAdjustedEvents":   "Late events dropped — alert if > 0",
        "ConversionErrors":          "Deserialization failures",
        "RuntimeErrors":             "Query execution errors"
    }
    
    # Azure Monitor alert example:
    # Alert: ResourceUtilization > 80% for 5 minutes
    # Action: send Teams notification, optionally auto-scale SUs via Logic App

# Azure Monitor alert JSON (ARM):
alert_rule = {
    "type": "Microsoft.Insights/metricAlerts",
    "properties": {
        "criteria": {
            "metricName": "ResourceUtilization",
            "threshold": 80,
            "timeAggregation": "Average",
            "operator": "GreaterThan"
        },
        "evaluationFrequency": "PT5M",
        "windowSize": "PT5M",
        "actions": [{"actionGroupId": "/subscriptions/.../actionGroups/StreamingAlerts"}]
    }
}
```

---

## Interview Tips

> **Tip 1:** "An ASA job is showing high SU% utilization (>90%). What do you do?" — First, check if it's sustained or a burst: look at the 24-hour utilization graph. If sustained: (a) increase SUs (portal → Scale, or via API — no restart needed), (b) optimize the query — add PARTITION BY to enable parallel execution, (c) simplify complex joins (stream-to-stream joins are expensive). If it's a burst: configure auto-scaling via Azure Automation or Logic App that monitors the metric and scales SUs. Always keep 20% headroom (target < 80%) to handle traffic spikes without dropping events.

> **Tip 2:** "How do you test ASA queries before deploying to production?" — Three approaches: (1) VS Code ASA extension: run query locally with sample JSON files, see output in VS Code (free, fast, no Azure needed). (2) ASA portal query testing: paste sample events directly in the Azure portal query editor, see output in seconds. (3) End-to-end test: deploy a test job against a dev Event Hubs namespace with recorded production events (use Event Hubs capture to save events as Avro, replay in test). For CI/CD: use the `sa.exe` CLI tool in Azure Pipelines to run local unit tests with input/expected-output JSON files.

> **Tip 3:** "What happens when ASA fails and how does it recover?" — ASA checkpoints job state (window accumulators, consumer offsets) to Azure Storage every few seconds. On failure (node crash, Azure platform issue): the service automatically restarts the job on a healthy node and recovers state from the last checkpoint. The job re-reads Event Hubs from the saved checkpoint offset, so no events are permanently lost (Event Hubs retains data for up to 7 days). The tradeoff: events received after the last checkpoint and before the failure may be reprocessed (at-least-once delivery). Recovery time: typically 2-5 minutes.
