---
title: "Oozie - Intermediate"
topic: hadoop
subtopic: oozie
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [hadoop, oozie, coordinator, sla, fork, join, el-functions]
---

# Oozie — Intermediate

## Coordinator Jobs

Coordinator jobs trigger workflows on a schedule (time-based) or when data becomes available (data-based). They are the Oozie equivalent of cron.

### Time-Based Coordinator

```xml
<!-- coordinator.xml -->
<coordinator-app name="daily-etl-coordinator"
                 frequency="${coord:days(1)}"
                 start="2024-01-01T02:00Z"
                 end="2025-12-31T02:00Z"
                 timezone="UTC"
                 xmlns="uri:oozie:coordinator:0.4">
  <controls>
    <concurrency>1</concurrency>
    <execution>FIFO</execution>
    <throttle>5</throttle>
    <timeout>120</timeout>
  </controls>

  <action>
    <workflow>
      <app-path>${nameNode}/user/etl/workflows/daily-etl</app-path>
      <configuration>
        <property>
          <name>date</name>
          <value>${coord:formatTime(coord:nominalTime(), 'yyyy-MM-dd')}</value>
        </property>
        <property>
          <name>year</name>
          <value>${coord:formatTime(coord:nominalTime(), 'yyyy')}</value>
        </property>
      </configuration>
    </workflow>
  </action>
</coordinator-app>
```

### Data-Triggered Coordinator

```xml
<!-- Trigger workflow when upstream data is available -->
<coordinator-app name="data-triggered-etl"
                 frequency="${coord:hours(1)}"
                 start="2024-01-01T00:00Z"
                 end="2025-12-31T00:00Z"
                 timezone="UTC"
                 xmlns="uri:oozie:coordinator:0.4">

  <datasets>
    <dataset name="upstream-orders"
             frequency="${coord:hours(1)}"
             initial-instance="2024-01-01T00:00Z"
             timezone="UTC">
      <uri-template>
        hdfs://namenode:8020/data/raw/orders/${YEAR}/${MONTH}/${DAY}/${HOUR}
      </uri-template>
      <done-flag>_SUCCESS</done-flag>
    </dataset>
  </datasets>

  <input-events>
    <data-in name="orders-input" dataset="upstream-orders">
      <instance>${coord:current(0)}</instance>
    </data-in>
  </input-events>

  <output-events>
    <data-out name="refined-output" dataset="refined-orders">
      <instance>${coord:current(0)}</instance>
    </data-out>
  </output-events>

  <action>
    <workflow>
      <app-path>${nameNode}/user/etl/workflows/hourly-etl</app-path>
      <configuration>
        <property>
          <name>inputPath</name>
          <value>${coord:dataIn('orders-input')}</value>
        </property>
      </configuration>
    </workflow>
  </action>
</coordinator-app>
```

## EL Functions (Expression Language)

Oozie's EL functions are used in XML config to compute dynamic values:

| Function | Returns | Example |
|----------|---------|---------|
| `coord:nominalTime()` | Coordinator scheduled time | `2024-01-15T02:00Z` |
| `coord:actualTime()` | When the action actually ran | `2024-01-15T02:03Z` |
| `coord:formatTime(time, pattern)` | Formatted time string | `2024-01-15` |
| `coord:days(n)` | Frequency in days | `86400` (seconds) |
| `coord:hours(n)` | Frequency in hours | `3600` (seconds) |
| `coord:current(offset)` | Dataset instance at offset | Current hour data |
| `coord:dataIn(name)` | Input dataset path | `/data/raw/orders/2024/01/15` |
| `wf:id()` | Current workflow job ID | `0000001-...-W` |
| `wf:name()` | Workflow name | `daily-etl-workflow` |
| `wf:actionData(action)` | Shell action captured output | `{"count": "1000"}` |
| `wf:lastErrorNode()` | Name of failed action | `transform-data` |
| `wf:errorMessage(node)` | Error message of failed action | `OOM in reducer` |

```xml
<!-- EL functions in practice -->
<property>
  <name>processingDate</name>
  <!-- Yesterday's date in yyyy-MM-dd format -->
  <value>${coord:formatTime(coord:dateOffset(coord:nominalTime(), -1, 'DAY'), 'yyyy-MM-dd')}</value>
</property>

<property>
  <name>inputPath</name>
  <value>/data/raw/orders/${coord:formatTime(coord:nominalTime(), 'yyyy/MM/dd')}</value>
</property>
```

## Sub-Workflows (Workflow Reusability)

Sub-workflows allow modular workflow design — reuse a common workflow as a step in a parent workflow:

```xml
<!-- parent_workflow.xml -->
<action name="run-validation">
  <sub-workflow>
    <app-path>${nameNode}/user/etl/workflows/validation-workflow</app-path>
    <propagate-configuration/>
    <configuration>
      <property>
        <name>table_name</name>
        <value>orders</value>
      </property>
    </configuration>
  </sub-workflow>
  <ok to="load-to-hive"/>
  <error to="fail"/>
</action>
```

## Fork and Join for Parallelism

Use `<fork>` and `<join>` to run multiple actions in parallel:

```xml
<workflow-app name="parallel-imports" xmlns="uri:oozie:workflow:0.5">
  <start to="fork-imports"/>

  <!-- Fork: launch 3 actions in parallel -->
  <fork name="fork-imports">
    <path start="import-customers"/>
    <path start="import-products"/>
    <path start="import-orders"/>
  </fork>

  <action name="import-customers">
    <shell xmlns="uri:oozie:shell-action:0.3">
      <exec>sqoop_import.sh</exec>
      <argument>CUSTOMERS</argument>
    </shell>
    <ok to="join-imports"/>
    <error to="fail"/>
  </action>

  <action name="import-products">
    <shell xmlns="uri:oozie:shell-action:0.3">
      <exec>sqoop_import.sh</exec>
      <argument>PRODUCTS</argument>
    </shell>
    <ok to="join-imports"/>
    <error to="fail"/>
  </action>

  <action name="import-orders">
    <shell xmlns="uri:oozie:shell-action:0.3">
      <exec>sqoop_import.sh</exec>
      <argument>ORDERS</argument>
    </shell>
    <ok to="join-imports"/>
    <error to="fail"/>
  </action>

  <!-- Join: wait for all parallel paths -->
  <join name="join-imports" to="aggregate-data"/>

  <action name="aggregate-data">
    <hive xmlns="uri:oozie:hive-action:0.6">
      <script>aggregate.hql</script>
    </hive>
    <ok to="end"/>
    <error to="fail"/>
  </action>

  <kill name="fail">
    <message>Job failed: ${wf:errorMessage(wf:lastErrorNode())}</message>
  </kill>
  <end name="end"/>
</workflow-app>
```

## Fork-Join Diagram

```
graph TD
    A["Start"] --> B["fork-imports"]
    B --> C["import-customers"]
    B --> D["import-products"]
    B --> E["import-orders"]
    C -->|"ok"| F["join-imports"]
    D -->|"ok"| F
    E -->|"ok"| F
    F --> G["aggregate-data<br>Hive action"]
    G -->|"ok"| H["End"]
    C -->|"error"| I["Kill: fail"]
    D -->|"error"| I
    E -->|"error"| I
```

## SLA Monitoring

Oozie SLA alerts notify when jobs start late, run too long, or miss their completion deadline:

```xml
<coordinator-app name="daily-etl-with-sla" ...>
  ...
  <action>
    <workflow>
      <app-path>${nameNode}/user/etl/workflows/daily-etl</app-path>
    </workflow>

    <!-- SLA: must start within 30 min, complete within 3 hours -->
    <sla:info xmlns:sla="uri:oozie:sla:0.2">
      <sla:nominal-time>${coord:nominalTime()}</sla:nominal-time>
      <sla:should-start>${30 * MINUTES}</sla:should-start>
      <sla:should-end>${3 * HOURS}</sla:should-end>
      <sla:max-duration>${3 * HOURS}</sla:max-duration>
      <sla:alert-events>START_MISS,END_MISS,DURATION_MISS</sla:alert-events>
      <sla:alert-contact>de-oncall@company.com</sla:alert-contact>
    </sla:info>
  </action>
</coordinator-app>
```

```bash
# Query SLA status via CLI
oozie sla -oozie http://oozie-host:11000/oozie \
          -filter "appName=daily-etl-coordinator;eventStatus=END_MISS" \
          -len 10
```

## Coordinator Execution Policies

| Policy | Behavior |
|--------|----------|
| `FIFO` | Run late instances in order (default) |
| `LIFO` | Run most recent first (skip old ones) |
| `LAST_ONLY` | Only run the latest missed instance |
| `NONE` | Don't run missed instances |

```xml
<controls>
  <concurrency>2</concurrency>       <!-- Max 2 instances running simultaneously -->
  <execution>LAST_ONLY</execution>   <!-- If behind, only catch up the latest -->
  <throttle>5</throttle>             <!-- Max 5 waiting in queue -->
  <timeout>60</timeout>              <!-- Minutes to wait for input data -->
</controls>
```

## Oozie CLI — Common Commands

```bash
# Run a coordinator
oozie job -oozie http://oozie-host:11000/oozie \
          -config coord_job.properties \
          -run

# List coordinator actions (runs)
oozie job -oozie http://oozie-host:11000/oozie \
          -info 0000001-240115120000000-oozie-oozi-C \
          -len 20 -offset 1

# Rerun a coordinator action (e.g., action 5)
oozie job -oozie http://oozie-host:11000/oozie \
          -rerun 0000001-240115120000000-oozie-oozi-C \
          -action 5 \
          -config coord_job.properties \
          -refresh

# Change coordinator end time
oozie job -oozie http://oozie-host:11000/oozie \
          -change 0000001-240115120000000-oozie-oozi-C \
          -value endtime=2025-12-31T00:00Z

# Suspend and resume
oozie job -oozie http://oozie-host:11000/oozie -suspend JOB_ID
oozie job -oozie http://oozie-host:11000/oozie -resume JOB_ID
```

## Interview Tips

> **Tip 1:** The `<done-flag>` in dataset definitions is critical. Oozie waits for this file (e.g., `_SUCCESS`) to exist before triggering the coordinator action. If the upstream job doesn't write `_SUCCESS`, the coordinator waits indefinitely until the timeout.

> **Tip 2:** `coord:current(0)` refers to the current nominal time instance of the dataset. `coord:current(-1)` refers to the previous instance (e.g., yesterday's data if frequency is daily). This is how you express "wait for yesterday's data" in Oozie.

> **Tip 3:** Fork-Join must be perfectly balanced — every path in a `<fork>` must eventually reach the same `<join>`. If any path goes to a different node, Oozie throws a validation error. This is different from Airflow where branching is more flexible.

> **Tip 4:** SLA monitoring requires the Oozie SLA service to be enabled (`oozie.service.EventHandlerService.event.listeners` in oozie-site.xml). Many clusters don't enable it by default — always check if asked to set up SLA alerting.

> **Tip 5:** For large coordinator intervals (e.g., monthly), use `${coord:months(1)}` not a manual calculation. Oozie's EL functions handle DST and variable month lengths correctly.
