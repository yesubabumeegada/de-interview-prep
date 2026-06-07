---
title: "Oozie - Scenario Questions"
topic: hadoop
subtopic: oozie
content_type: scenario_question
tags: [hadoop, oozie, scenarios, interview, workflow, coordinator, bundle]
---

# Scenario Questions — Oozie

<article data-difficulty="junior">

## 🟢 Junior: Write a Simple Hive Workflow

**Scenario:** You are asked to create an Oozie workflow that runs a Hive script `transform.hql` daily. The script accepts a `date` parameter and writes to `/data/refined/orders/dt=${date}`. If it fails, the workflow should send an email to `de-team@company.com`.

<details><summary>💡 Hint</summary>

Think about:
- What files are needed? (`workflow.xml`, `job.properties`, where the script lives on HDFS)
- How do you pass the `date` parameter to the Hive script?
- What are `<ok>` and `<error>` transitions in a workflow?

</details>

<details><summary>✅ Solution</summary>

**HDFS structure:**
```
/user/etl/workflows/daily-orders/
├── workflow.xml
├── hql/
│   └── transform.hql
```

**workflow.xml:**
```xml
<workflow-app name="daily-orders-transform" xmlns="uri:oozie:workflow:0.5">
  <start to="transform-hive"/>

  <action name="transform-hive">
    <hive xmlns="uri:oozie:hive-action:0.6">
      <resource-manager>${resourceManager}</resource-manager>
      <name-node>${nameNode}</name-node>
      <jdbc-url>jdbc:hive2://hiveserver2:10000</jdbc-url>
      <script>hql/transform.hql</script>
      <param>date=${date}</param>
      <param>output=/data/refined/orders/dt=${date}</param>
    </hive>
    <ok to="end"/>
    <error to="notify-failure"/>
  </action>

  <action name="notify-failure">
    <email xmlns="uri:oozie:email-action:0.2">
      <to>de-team@company.com</to>
      <subject>Oozie FAILED: daily-orders-transform ${date}</subject>
      <body>
        Job ${wf:id()} failed at ${wf:lastErrorNode()}.
        Error: ${wf:errorMessage(wf:lastErrorNode())}
      </body>
    </email>
    <ok to="fail"/>
    <error to="fail"/>
  </action>

  <kill name="fail">
    <message>Transform failed: ${wf:errorMessage(wf:lastErrorNode())}</message>
  </kill>
  <end name="end"/>
</workflow-app>
```

**job.properties:**
```properties
nameNode=hdfs://namenode:8020
resourceManager=resourcemanager:8032
oozie.wf.application.path=${nameNode}/user/etl/workflows/daily-orders
date=2024-01-15
oozie.use.system.libpath=true
```

**transform.hql:**
```sql
INSERT OVERWRITE TABLE refined.orders
PARTITION (dt='${date}')
SELECT
  order_id,
  customer_id,
  amount,
  status,
  created_at
FROM raw.orders
WHERE dt = '${date}' AND amount > 0;
```

**Submit the workflow:**
```bash
# Upload to HDFS
hdfs dfs -put -f workflow.xml /user/etl/workflows/daily-orders/
hdfs dfs -put -f hql/ /user/etl/workflows/daily-orders/

# Run
oozie job -oozie http://oozie-host:11000/oozie \
          -config job.properties \
          -run
```

</details>
</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Coordinator Job for Daily Partitioned Hive Load with Upstream Data Dependency

**Scenario:** You have a daily Hive ETL that must not run until upstream raw data is available (signaled by a `_SUCCESS` file in `/data/landing/orders/${YEAR}/${MONTH}/${DAY}/`). Design a coordinator job that:
1. Runs daily at 2 AM UTC
2. Waits for the upstream `_SUCCESS` file (with up to 3 hours timeout)
3. Triggers the Hive transform workflow
4. Passes the correct date parameters

<details><summary>💡 Hint</summary>

Use `<input-events>` with a `<dataset>` that has a `<done-flag>` pointing to `_SUCCESS`. The `timeout` in `<controls>` sets how long to wait. Use `coord:formatTime(coord:nominalTime(), ...)` to get the date string.

</details>

<details><summary>✅ Solution</summary>

**coordinator.xml:**
```xml
<coordinator-app name="orders-daily-etl"
                 frequency="${coord:days(1)}"
                 start="2024-01-01T02:00Z"
                 end="2025-12-31T02:00Z"
                 timezone="UTC"
                 xmlns="uri:oozie:coordinator:0.4">

  <controls>
    <timeout>180</timeout>       <!-- Wait max 3 hours for upstream data -->
    <concurrency>1</concurrency>
    <execution>FIFO</execution>
    <throttle>3</throttle>
  </controls>

  <datasets>
    <!-- Upstream landing zone dataset -->
    <dataset name="raw-orders-landing"
             frequency="${coord:days(1)}"
             initial-instance="2024-01-01T00:00Z"
             timezone="UTC">
      <uri-template>
        hdfs://namenode:8020/data/landing/orders/${YEAR}/${MONTH}/${DAY}
      </uri-template>
      <done-flag>_SUCCESS</done-flag>
    </dataset>

    <!-- Output dataset (for downstream coordinators to depend on) -->
    <dataset name="refined-orders-output"
             frequency="${coord:days(1)}"
             initial-instance="2024-01-01T00:00Z"
             timezone="UTC">
      <uri-template>
        hdfs://namenode:8020/data/refined/orders/${YEAR}/${MONTH}/${DAY}
      </uri-template>
      <done-flag>_SUCCESS</done-flag>
    </dataset>
  </datasets>

  <input-events>
    <data-in name="upstream-orders" dataset="raw-orders-landing">
      <!-- Current day's data -->
      <instance>${coord:current(0)}</instance>
    </data-in>
  </input-events>

  <output-events>
    <data-out name="refined-orders" dataset="refined-orders-output">
      <instance>${coord:current(0)}</instance>
    </data-out>
  </output-events>

  <action>
    <workflow>
      <app-path>${nameNode}/user/etl/workflows/daily-orders</app-path>
      <configuration>
        <property>
          <name>date</name>
          <value>${coord:formatTime(coord:nominalTime(), 'yyyy-MM-dd')}</value>
        </property>
        <property>
          <name>year</name>
          <value>${coord:formatTime(coord:nominalTime(), 'yyyy')}</value>
        </property>
        <property>
          <name>month</name>
          <value>${coord:formatTime(coord:nominalTime(), 'MM')}</value>
        </property>
        <property>
          <name>day</name>
          <value>${coord:formatTime(coord:nominalTime(), 'dd')}</value>
        </property>
        <property>
          <name>inputPath</name>
          <value>${coord:dataIn('upstream-orders')}</value>
        </property>
        <property>
          <name>outputPath</name>
          <value>${coord:dataOut('refined-orders')}</value>
        </property>
      </configuration>
    </workflow>
  </action>
</coordinator-app>
```

**coord_job.properties:**
```properties
nameNode=hdfs://namenode:8020
resourceManager=resourcemanager:8032
oozie.coord.application.path=${nameNode}/user/etl/workflows/daily-orders/coordinator.xml
oozie.use.system.libpath=true
```

**Submit and verify:**
```bash
oozie job -oozie http://oozie-host:11000/oozie \
          -config coord_job.properties \
          -run

# Check coordinator actions
oozie job -oozie http://oozie-host:11000/oozie \
          -info COORD_JOB_ID -len 7
```

**Key points:**
- The coordinator waits at `WAITING` status until `_SUCCESS` appears
- After 180 minutes without `_SUCCESS`, the action becomes `TIMEDOUT`
- `coord:dataIn` and `coord:dataOut` resolve to actual HDFS paths for the workflow

</details>
</article>

<article data-difficulty="senior">

## 🔴 Senior: Design a Complex Oozie Bundle for Multi-Stage DWH Refresh with SLA Monitoring

**Scenario:** You are the lead data engineer at a financial services company. You need to design a complete Oozie bundle that orchestrates a multi-stage data warehouse refresh:
- **Stage 1**: Ingest from 3 source systems (Oracle, MySQL, Salesforce API) in parallel
- **Stage 2**: Dimension table refresh (SCD Type 2 for customers/products)
- **Stage 3**: Fact table load (dependent on Stage 2 completion)
- **Stage 4**: Aggregation layer rebuild
- SLA requirement: entire pipeline must complete by 6 AM UTC (starts at 1 AM)

Design the bundle structure, coordinator dependencies, SLA configuration, and alerting strategy.

<details><summary>💡 Hint</summary>

Model Stage dependencies using coordinator `<input-events>` with `<done-flag>`. Each stage's coordinator writes a `_SUCCESS` file that the next stage watches. The bundle groups all coordinators. SLA is defined in `<sla:info>` within each coordinator action.

</details>

<details><summary>✅ Solution</summary>

**Architecture:**

```
graph TD
    A["Bundle: dwh-nightly-refresh"] --> B["Coord: ingest-oracle<br>1 AM"]
    A --> C["Coord: ingest-mysql<br>1 AM"]
    A --> D["Coord: ingest-salesforce<br>1 AM"]
    B -->|"_SUCCESS"| E["Coord: dim-refresh<br>waits for B+C+D"]
    C -->|"_SUCCESS"| E
    D -->|"_SUCCESS"| E
    E -->|"_SUCCESS"| F["Coord: fact-load<br>waits for E"]
    F -->|"_SUCCESS"| G["Coord: aggregation<br>waits for F"]
    G -->|"done by 6 AM"| H["SLA Alert if missed"]
```

**bundle.xml:**
```xml
<bundle-app name="dwh-nightly-refresh" xmlns="uri:oozie:bundle:0.2">
  <controls>
    <kick-off-time>2024-01-01T01:00Z</kick-off-time>
  </controls>

  <coordinator name="ingest-oracle">
    <app-path>${nameNode}/user/etl/coordinators/ingest-oracle</app-path>
    <configuration>
      <property><name>env</name><value>prod</value></property>
    </configuration>
  </coordinator>

  <coordinator name="ingest-mysql">
    <app-path>${nameNode}/user/etl/coordinators/ingest-mysql</app-path>
    <configuration>
      <property><name>env</name><value>prod</value></property>
    </configuration>
  </coordinator>

  <coordinator name="ingest-salesforce">
    <app-path>${nameNode}/user/etl/coordinators/ingest-salesforce</app-path>
    <configuration>
      <property><name>env</name><value>prod</value></property>
    </configuration>
  </coordinator>

  <coordinator name="dim-refresh">
    <app-path>${nameNode}/user/etl/coordinators/dim-refresh</app-path>
  </coordinator>

  <coordinator name="fact-load">
    <app-path>${nameNode}/user/etl/coordinators/fact-load</app-path>
  </coordinator>

  <coordinator name="aggregation">
    <app-path>${nameNode}/user/etl/coordinators/aggregation</app-path>
  </coordinator>
</bundle-app>
```

**dim-refresh coordinator (waits for all 3 ingests):**
```xml
<coordinator-app name="dim-refresh-coordinator"
                 frequency="${coord:days(1)}"
                 start="2024-01-01T01:00Z"
                 end="2025-12-31T01:00Z"
                 timezone="UTC"
                 xmlns="uri:oozie:coordinator:0.4">
  <controls>
    <timeout>180</timeout>
    <concurrency>1</concurrency>
  </controls>

  <datasets>
    <dataset name="oracle-ingest-done"
             frequency="${coord:days(1)}"
             initial-instance="2024-01-01T00:00Z"
             timezone="UTC">
      <uri-template>hdfs://namenode/data/ingest/oracle/${YEAR}/${MONTH}/${DAY}</uri-template>
      <done-flag>_SUCCESS</done-flag>
    </dataset>
    <dataset name="mysql-ingest-done" ...>
      <uri-template>hdfs://namenode/data/ingest/mysql/${YEAR}/${MONTH}/${DAY}</uri-template>
      <done-flag>_SUCCESS</done-flag>
    </dataset>
    <dataset name="salesforce-ingest-done" ...>
      <uri-template>hdfs://namenode/data/ingest/salesforce/${YEAR}/${MONTH}/${DAY}</uri-template>
      <done-flag>_SUCCESS</done-flag>
    </dataset>
  </datasets>

  <input-events>
    <!-- Wait for ALL three ingest coordinators to complete -->
    <data-in name="oracle-done" dataset="oracle-ingest-done">
      <instance>${coord:current(0)}</instance>
    </data-in>
    <data-in name="mysql-done" dataset="mysql-ingest-done">
      <instance>${coord:current(0)}</instance>
    </data-in>
    <data-in name="salesforce-done" dataset="salesforce-ingest-done">
      <instance>${coord:current(0)}</instance>
    </data-in>
  </input-events>

  <action>
    <workflow>
      <app-path>${nameNode}/user/etl/workflows/dim-refresh</app-path>
      <configuration>
        <property>
          <name>date</name>
          <value>${coord:formatTime(coord:nominalTime(), 'yyyy-MM-dd')}</value>
        </property>
      </configuration>
    </workflow>

    <!-- SLA: dim refresh must complete by 4 AM (180 min after 1 AM start) -->
    <sla:info xmlns:sla="uri:oozie:sla:0.2">
      <sla:nominal-time>${coord:nominalTime()}</sla:nominal-time>
      <sla:should-start>${30 * MINUTES}</sla:should-start>
      <sla:should-end>${180 * MINUTES}</sla:should-end>
      <sla:max-duration>${180 * MINUTES}</sla:max-duration>
      <sla:alert-events>START_MISS,END_MISS,DURATION_MISS</sla:alert-events>
      <sla:alert-contact>dwh-oncall@company.com</sla:alert-contact>
    </sla:info>
  </action>
</coordinator-app>
```

**Aggregation coordinator with critical SLA:**
```xml
<!-- Must finish by 6 AM = 300 minutes after 1 AM start -->
<sla:info xmlns:sla="uri:oozie:sla:0.2">
  <sla:nominal-time>${coord:nominalTime()}</sla:nominal-time>
  <sla:should-end>${300 * MINUTES}</sla:should-end>
  <sla:alert-events>END_MISS</sla:alert-events>
  <sla:alert-contact>dwh-oncall@company.com,pagerduty-endpoint@company.com</sla:alert-contact>
</sla:info>
```

**Alerting strategy:**
```
Level 1 (Warning): Email to DE team at END_MISS on dim-refresh (4 AM target)
Level 2 (Critical): PagerDuty via shell action if aggregation not done by 5:30 AM
Level 3 (Escalation): Automatic bundle suspension + manager notification at 6 AM miss

Monitoring metrics to track:
- Coordinator WAITING time (upstream latency)
- Workflow execution duration per stage
- SLA miss rate per week
- Rerun frequency per coordinator
```

**Operational runbook:**
```bash
# Morning check: verify all coordinators succeeded
oozie jobs -oozie http://oozie-host:11000/oozie \
           -jobtype coordinator \
           -filter "status=FAILED;startCreatedTime=2024-01-15T00:00Z" \
           -len 20

# Rerun a specific coordinator action if failed
oozie job -oozie http://oozie-host:11000/oozie \
          -rerun COORD_JOB_ID \
          -action 1 \
          -config coord_job.properties \
          -refresh

# Check SLA violations
oozie sla -oozie http://oozie-host:11000/oozie \
          -filter "appName=dwh-nightly-refresh;eventStatus=END_MISS" \
          -len 7
```

</details>
</article>
