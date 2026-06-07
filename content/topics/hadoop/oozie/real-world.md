---
title: "Oozie - Real World"
topic: hadoop
subtopic: oozie
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [hadoop, oozie, etl, coordinator, alerting, pagerduty, hbase]
---

# Oozie — Real-World Patterns

## Full ETL Pipeline with Oozie Coordinator

A complete production ETL: Sqoop import → Hive transformation → data quality check → publish.

```
graph TD
    A["Coordinator<br>daily @ 2AM UTC"] -->|"triggers"| B["Workflow: daily-etl"]
    B --> C["Action: sqoop-import<br>Shell action"]
    C -->|"ok"| D["Action: hive-transform<br>Hive action"]
    D -->|"ok"| E["Action: data-quality<br>Shell/Python action"]
    E -->|"ok"| F["Action: publish-partition<br>Hive MSCK action"]
    F -->|"ok"| G["Action: notify-success<br>Email action"]
    C -->|"error"| H["Action: notify-failure<br>Email action"]
    D -->|"error"| H
    E -->|"error"| H
    F -->|"error"| H
    H --> I["Kill: fail"]
    G --> J["End"]
```

### workflow.xml
```xml
<workflow-app name="daily-orders-etl" xmlns="uri:oozie:workflow:0.5">
  <start to="sqoop-import"/>

  <action name="sqoop-import">
    <shell xmlns="uri:oozie:shell-action:0.3">
      <resource-manager>${resourceManager}</resource-manager>
      <name-node>${nameNode}</name-node>
      <exec>sqoop_import.sh</exec>
      <argument>${date}</argument>
      <argument>${nameNode}/user/etl/oracle.pass</argument>
      <file>scripts/sqoop_import.sh#sqoop_import.sh</file>
      <capture-output/>
    </shell>
    <ok to="hive-transform"/>
    <error to="notify-failure"/>
  </action>

  <action name="hive-transform">
    <hive xmlns="uri:oozie:hive-action:0.6">
      <resource-manager>${resourceManager}</resource-manager>
      <name-node>${nameNode}</name-node>
      <jdbc-url>jdbc:hive2://hiveserver2:10000</jdbc-url>
      <script>hql/transform_orders.hql</script>
      <param>date=${date}</param>
      <param>input_path=${nameNode}/data/raw/orders/dt=${date}</param>
      <param>output_path=${nameNode}/data/refined/orders/dt=${date}</param>
    </hive>
    <ok to="data-quality"/>
    <error to="notify-failure"/>
  </action>

  <action name="data-quality">
    <shell xmlns="uri:oozie:shell-action:0.3">
      <exec>python3</exec>
      <argument>scripts/data_quality.py</argument>
      <argument>--date</argument>
      <argument>${date}</argument>
      <argument>--table</argument>
      <argument>refined.orders</argument>
      <file>scripts/data_quality.py#data_quality.py</file>
      <capture-output/>
    </shell>
    <ok to="add-hive-partition"/>
    <error to="notify-failure"/>
  </action>

  <action name="add-hive-partition">
    <hive xmlns="uri:oozie:hive-action:0.6">
      <script>hql/add_partition.hql</script>
      <param>date=${date}</param>
    </hive>
    <ok to="notify-success"/>
    <error to="notify-failure"/>
  </action>

  <action name="notify-success">
    <email xmlns="uri:oozie:email-action:0.2">
      <to>de-team@company.com</to>
      <subject>Daily Orders ETL Completed: ${date}</subject>
      <body>
        Workflow ${wf:id()} completed successfully.
        Date: ${date}
        Records imported: ${wf:actionData('sqoop-import')['row_count']}
      </body>
    </email>
    <ok to="end"/>
    <error to="end"/>
  </action>

  <action name="notify-failure">
    <email xmlns="uri:oozie:email-action:0.2">
      <to>de-oncall@company.com</to>
      <subject>ALERT: Daily Orders ETL Failed: ${date}</subject>
      <body>
        Workflow ${wf:id()} FAILED at action: ${wf:lastErrorNode()}
        Error: ${wf:errorMessage(wf:lastErrorNode())}
        Date: ${date}
      </body>
    </email>
    <ok to="fail"/>
    <error to="fail"/>
  </action>

  <kill name="fail">
    <message>ETL Failed: ${wf:errorMessage(wf:lastErrorNode())}</message>
  </kill>
  <end name="end"/>
</workflow-app>
```

## Handling Late Data with Coordinator Timeouts

```xml
<!-- coordinator.xml with late data handling -->
<coordinator-app name="orders-with-late-data"
                 frequency="${coord:hours(1)}"
                 start="2024-01-01T00:00Z"
                 end="2025-12-31T00:00Z"
                 timezone="UTC"
                 xmlns="uri:oozie:coordinator:0.4">
  <controls>
    <timeout>240</timeout>       <!-- Wait up to 4 hours for late data -->
    <concurrency>3</concurrency>
    <execution>FIFO</execution>
  </controls>

  <datasets>
    <dataset name="raw-orders"
             frequency="${coord:hours(1)}"
             initial-instance="2024-01-01T00:00Z"
             timezone="UTC">
      <uri-template>
        hdfs://namenode:8020/data/landing/orders/${YEAR}/${MONTH}/${DAY}/${HOUR}
      </uri-template>
      <done-flag>_SUCCESS</done-flag>
    </dataset>
  </datasets>

  <input-events>
    <data-in name="hourly-orders" dataset="raw-orders">
      <!-- Accept data up to 2 hours late -->
      <start-instance>${coord:current(-2)}</start-instance>
      <end-instance>${coord:current(0)}</end-instance>
    </data-in>
  </input-events>

  <action>
    <workflow>
      <app-path>${nameNode}/user/etl/workflows/orders-etl</app-path>
      <configuration>
        <property>
          <name>inputPath</name>
          <value>${coord:dataIn('hourly-orders')}</value>
        </property>
      </configuration>
    </workflow>
  </action>
</coordinator-app>
```

## Multi-Environment Deployment (Dev/Prod)

```bash
#!/bin/bash
# deploy_oozie_workflow.sh
ENV=$1  # dev or prod

if [ "$ENV" == "prod" ]; then
  OOZIE_URL="http://oozie-prod:11000/oozie"
  HDFS_BASE="/user/etl-prod"
  DB_CONNECT="jdbc:oracle:thin:@//prod-oracle:1521/ORCL"
  NUM_MAPPERS=16
else
  OOZIE_URL="http://oozie-dev:11000/oozie"
  HDFS_BASE="/user/etl-dev"
  DB_CONNECT="jdbc:oracle:thin:@//dev-oracle:1521/DEVDB"
  NUM_MAPPERS=2
fi

# Upload workflow to HDFS
hdfs dfs -put -f workflow.xml ${HDFS_BASE}/workflows/daily-etl/workflow.xml
hdfs dfs -put -f coordinator.xml ${HDFS_BASE}/workflows/daily-etl/coordinator.xml
hdfs dfs -put -f hql/ ${HDFS_BASE}/workflows/daily-etl/
hdfs dfs -put -f scripts/ ${HDFS_BASE}/workflows/daily-etl/

# Generate environment-specific job.properties
cat > job_${ENV}.properties << EOF
nameNode=hdfs://namenode:8020
resourceManager=resourcemanager:8032
oozie.use.system.libpath=true
oozie.coord.application.path=${HDFS_BASE}/workflows/daily-etl/coordinator.xml
db.connect=${DB_CONNECT}
num.mappers=${NUM_MAPPERS}
env=${ENV}
oozie.libpath=${HDFS_BASE}/lib
EOF

# Submit
oozie job -oozie ${OOZIE_URL} \
          -config job_${ENV}.properties \
          -run

echo "Deployed to ${ENV}: ${OOZIE_URL}"
```

## HBase Compaction Scheduling with Oozie

```xml
<!-- coordinator.xml for weekly HBase compaction -->
<coordinator-app name="hbase-compaction-weekly"
                 frequency="${coord:days(7)}"
                 start="2024-01-07T03:00Z"
                 end="2025-12-31T03:00Z"
                 timezone="UTC"
                 xmlns="uri:oozie:coordinator:0.4">
  <action>
    <workflow>
      <app-path>${nameNode}/user/etl/workflows/hbase-compaction</app-path>
    </workflow>
  </action>
</coordinator-app>
```

```xml
<!-- hbase compaction workflow.xml -->
<workflow-app name="hbase-compaction-workflow" xmlns="uri:oozie:workflow:0.5">
  <start to="compact-orders-table"/>

  <action name="compact-orders-table">
    <shell xmlns="uri:oozie:shell-action:0.3">
      <exec>hbase</exec>
      <argument>shell</argument>
      <argument>scripts/compact_orders.hbase</argument>
    </shell>
    <ok to="compact-customers-table"/>
    <error to="fail"/>
  </action>

  <action name="compact-customers-table">
    <shell xmlns="uri:oozie:shell-action:0.3">
      <exec>hbase</exec>
      <argument>shell</argument>
      <argument>scripts/compact_customers.hbase</argument>
    </shell>
    <ok to="end"/>
    <error to="fail"/>
  </action>

  <kill name="fail">
    <message>HBase compaction failed: ${wf:errorMessage(wf:lastErrorNode())}</message>
  </kill>
  <end name="end"/>
</workflow-app>
```

```bash
# compact_orders.hbase script
major_compact 'orders'
major_compact 'orders_history'
```

## Integrating with PagerDuty via Shell Action

```xml
<!-- Add PagerDuty alerting alongside email -->
<action name="pagerduty-alert">
  <shell xmlns="uri:oozie:shell-action:0.3">
    <exec>python3</exec>
    <argument>scripts/pagerduty_alert.py</argument>
    <argument>--workflow</argument>
    <argument>${wf:name()}</argument>
    <argument>--failed-action</argument>
    <argument>${wf:lastErrorNode()}</argument>
    <argument>--error</argument>
    <argument>${wf:errorMessage(wf:lastErrorNode())}</argument>
    <file>scripts/pagerduty_alert.py#pagerduty_alert.py</file>
    <env-var>PD_API_KEY=your-pagerduty-key</env-var>
  </shell>
  <ok to="fail"/>
  <error to="fail"/>
</action>
```

```python
# pagerduty_alert.py
import requests
import argparse
import os

def trigger_pagerduty_incident(workflow, failed_action, error_msg):
    api_key = os.environ['PD_API_KEY']
    payload = {
        "routing_key": api_key,
        "event_action": "trigger",
        "payload": {
            "summary": f"Oozie Workflow Failed: {workflow}",
            "severity": "critical",
            "source": "oozie",
            "custom_details": {
                "workflow": workflow,
                "failed_action": failed_action,
                "error": error_msg
            }
        }
    }
    r = requests.post("https://events.pagerduty.com/v2/enqueue", json=payload)
    print(f"PagerDuty response: {r.status_code}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--workflow")
    parser.add_argument("--failed-action")
    parser.add_argument("--error")
    args = parser.parse_args()
    trigger_pagerduty_incident(args.workflow, args.failed_action, args.error)
```

## Interview Tips

> **Tip 1:** Multi-environment Oozie deployment is best done with a parameterized `job.properties` file and a deployment script that substitutes environment-specific values. Avoid hardcoding connection strings in `workflow.xml` — use `${nameNode}` and `${db.connect}` style parameters.

> **Tip 2:** The `timeout` in `<controls>` is in minutes and refers to how long the coordinator waits for input data before marking the action as `TIMEDOUT`. This is crucial for SLA enforcement when upstream pipelines are delayed.

> **Tip 3:** HBase compaction via Oozie is a simple shell action pattern. The key is scheduling it during off-peak hours (3 AM) and not overlapping with peak read traffic. Major compaction is expensive and should be weekly, not daily.

> **Tip 4:** For PagerDuty integration, the recommended pattern is a Python Shell action that calls the Events API v2. This is more reliable than email-only alerting for P1/P2 incidents that require on-call response.

> **Tip 5:** In production, always set `oozie.use.system.libpath=true` and upload your JARs to `oozie.libpath`. Without this, Oozie actions may fail to find Hive or HBase libraries that aren't on the default classpath.
