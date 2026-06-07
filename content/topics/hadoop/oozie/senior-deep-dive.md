---
title: "Oozie - Senior Deep Dive"
topic: hadoop
subtopic: oozie
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [hadoop, oozie, bundle, ha, airflow, comparison, custom-actions]
---

# Oozie — Senior Deep Dive

## Bundle Jobs

Bundle jobs group multiple coordinators into a single logical unit, enabling coordinated management of entire data pipelines.

```xml
<!-- bundle.xml -->
<bundle-app name="daily-dwh-refresh" xmlns="uri:oozie:bundle:0.2">
  <controls>
    <kick-off-time>2024-01-01T02:00Z</kick-off-time>
  </controls>

  <coordinator name="ingest-coordinator">
    <app-path>${nameNode}/user/etl/coordinators/ingest</app-path>
    <configuration>
      <property>
        <name>env</name>
        <value>prod</value>
      </property>
    </configuration>
  </coordinator>

  <coordinator name="transform-coordinator">
    <app-path>${nameNode}/user/etl/coordinators/transform</app-path>
    <configuration>
      <property>
        <name>env</name>
        <value>prod</value>
      </property>
    </configuration>
  </coordinator>

  <coordinator name="aggregate-coordinator">
    <app-path>${nameNode}/user/etl/coordinators/aggregate</app-path>
    <configuration>
      <property>
        <name>env</name>
        <value>prod</value>
      </property>
    </configuration>
  </coordinator>
</bundle-app>
```

```bash
# Submit bundle
oozie job -oozie http://oozie-host:11000/oozie \
          -config bundle_job.properties \
          -run

# Suspend entire bundle (pauses all coordinators)
oozie job -oozie http://oozie-host:11000/oozie -suspend BUNDLE_JOB_ID

# Rerun a coordinator within a bundle
oozie job -oozie http://oozie-host:11000/oozie \
          -coord COORDINATOR_ID \
          -rerun -action 5 \
          -config job.properties
```

## Custom Oozie Actions

Custom actions extend Oozie to support non-standard executors:

```java
// Custom Action Executor
public class CustomApiActionExecutor extends ActionExecutor {

    @Override
    public void initActionType() {
        super.initActionType();
        registerError("API_ERROR", ActionExecutorException.ErrorType.TRANSIENT, "API.T");
    }

    @Override
    public void start(Context context, WorkflowAction action) throws ActionExecutorException {
        try {
            Element actionXml = XmlUtils.parseXml(action.getConf());
            String endpoint = actionXml.getChildTextTrim("endpoint", actionXml.getNamespace());
            String payload = actionXml.getChildTextTrim("payload", actionXml.getNamespace());

            // Call external API
            HttpClient client = HttpClientBuilder.create().build();
            HttpPost post = new HttpPost(endpoint);
            post.setEntity(new StringEntity(payload));
            HttpResponse response = client.execute(post);

            if (response.getStatusLine().getStatusCode() == 200) {
                context.setExecutionData("OK", null);
            } else {
                throw new ActionExecutorException(
                    ActionExecutorException.ErrorType.FAILED, "API_ERROR",
                    "API call failed: " + response.getStatusLine()
                );
            }
        } catch (Exception e) {
            throw convertException(e);
        }
    }

    @Override
    public boolean isCompleted(String externalStatus) {
        return externalStatus.equals("OK") || externalStatus.equals("FAILED");
    }
}
```

```xml
<!-- Register custom action in oozie-site.xml -->
<property>
  <name>oozie.service.ActionService.executor.ext.classes</name>
  <value>com.company.CustomApiActionExecutor</value>
</property>

<!-- Use in workflow.xml -->
<action name="call-external-api">
  <custom-api xmlns="uri:oozie:custom-api-action:0.1">
    <endpoint>https://api.company.com/trigger</endpoint>
    <payload>{"date": "${date}", "pipeline": "daily-etl"}</payload>
  </custom-api>
  <ok to="end"/>
  <error to="fail"/>
</action>
```

## Oozie HA Setup

Oozie HA uses ZooKeeper for leader election and a shared database for state:

```
graph LR
    A["Oozie Server 1<br>(Active)"] -->|"writes state"| C[("Shared DB<br>MySQL/PostgreSQL")]
    B["Oozie Server 2<br>(Standby)"] -->|"reads state"| C
    A -->|"leader election"| D["ZooKeeper<br>Ensemble"]
    B -->|"leader election"| D
    E["HAProxy<br>Load Balancer"] -->|"routes to active"| A
    E -->|"failover"| B
```

```xml
<!-- oozie-site.xml for HA -->
<property>
  <name>oozie.services.ext</name>
  <value>
    org.apache.oozie.service.ZKLocksService,
    org.apache.oozie.service.ZKXLogStreamingService,
    org.apache.oozie.service.ZKJobsConcurrencyService,
    org.apache.oozie.service.ZKUUIDService
  </value>
</property>
<property>
  <name>oozie.zookeeper.connection.string</name>
  <value>zk1:2181,zk2:2181,zk3:2181</value>
</property>
<property>
  <name>oozie.zookeeper.namespace</name>
  <value>oozie</value>
</property>
```

## Action Retry Strategies

```xml
<!-- Configure retries and retry interval per action -->
<action name="flaky-api-call" retry-max="3" retry-interval="10">
  <shell xmlns="uri:oozie:shell-action:0.3">
    <exec>call_api.sh</exec>
  </shell>
  <ok to="end"/>
  <error to="fail"/>
</action>
```

```xml
<!-- Global retry in oozie-site.xml -->
<property>
  <name>oozie.action.max.output.data</name>
  <value>2048</value>
</property>
<property>
  <name>oozie.action.retries.max</name>
  <value>3</value>
</property>
```

## Workflow Patterns: Decision Nodes

Decision nodes implement conditional branching (if/else):

```xml
<!-- workflow.xml with decision node -->
<decision name="check-data-volume">
  <switch>
    <case to="full-refresh">
      ${wf:actionData('get-row-count')['count'] gt 1000000}
    </case>
    <case to="incremental-load">
      ${wf:actionData('get-row-count')['count'] gt 0}
    </case>
    <default to="no-data-alert"/>
  </switch>
</decision>

<action name="full-refresh">
  <hive xmlns="uri:oozie:hive-action:0.6">
    <script>full_refresh.hql</script>
  </hive>
  <ok to="end"/>
  <error to="fail"/>
</action>

<action name="incremental-load">
  <hive xmlns="uri:oozie:hive-action:0.6">
    <script>incremental_load.hql</script>
  </hive>
  <ok to="end"/>
  <error to="fail"/>
</action>
```

## Oozie vs Airflow: Detailed Comparison

| Dimension | Oozie | Airflow |
|-----------|-------|---------|
| **DAG definition** | XML | Python code |
| **Scheduling** | Cron-like, data-triggered | Cron-like, custom sensors |
| **Cluster dependency** | Runs inside Hadoop cluster | External service |
| **Language** | XML (verbose) | Python (flexible) |
| **Custom operators** | Java required | Python |
| **UI** | Basic web console | Rich UI with task history |
| **Monitoring** | SLA emails | Grafana/Prometheus integration |
| **Community** | Declining (Hadoop-centric) | Growing (industry standard) |
| **Cloud support** | Minimal | Native (MWAA, Cloud Composer) |
| **Dynamic DAGs** | Difficult | Native support |
| **Testing** | Hard (no local mode) | Easy (unit tests) |
| **Backfill** | Manual coordinator rerun | `airflow dags backfill` command |

### Migration Path: Oozie → Airflow

```
Step 1: Inventory
  - List all Oozie workflows and coordinators
  - Identify dependencies and data triggers

Step 2: Convert
  - workflow.xml actions → Airflow operators
  - coordinator.xml schedule → DAG schedule_interval
  - coordinator data-in → FileSensor / ExternalTaskSensor
  - fork/join → parallel task groups

Step 3: Map Oozie concepts to Airflow:
  Oozie fork/join       → Airflow task groups
  Oozie coordinator     → Airflow DAG with schedule
  Oozie bundle          → Airflow DAG with dependencies
  Oozie decision node   → Airflow BranchPythonOperator
  Oozie sub-workflow    → Airflow SubDAG or TaskGroup
  coord:dataIn          → FileSensor / ExternalTaskSensor
  wf:actionData         → XCom

Step 4: Validate
  - Run both Oozie and Airflow in parallel for 2 weeks
  - Compare output datasets
  - Migrate cron schedules
```

```python
# Airflow equivalent of an Oozie coordinator
from airflow import DAG
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.sensors.filesystem import FileSensor
from datetime import datetime

dag = DAG(
    'daily-etl',
    schedule_interval='0 2 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=True
)

# coord:dataIn equivalent
wait_for_upstream = FileSensor(
    task_id='wait_for_upstream_data',
    filepath='/data/raw/orders/{{ ds }}/_SUCCESS',
    dag=dag
)

transform = HiveOperator(
    task_id='transform_data',
    hql='transform.hql',
    hive_cli_conn_id='hive_default',
    dag=dag
)

wait_for_upstream >> transform
```

## Workflow Loops via Decision Nodes

Oozie doesn't have native loops, but you can simulate them with decision nodes and workflow parameters:

```xml
<!-- Retry loop pattern (max 3 attempts) -->
<action name="increment-attempt">
  <shell xmlns="uri:oozie:shell-action:0.3">
    <exec>increment.sh</exec>
    <capture-output/>
  </shell>
  <ok to="check-attempts"/>
  <error to="fail"/>
</action>

<decision name="check-attempts">
  <switch>
    <case to="do-work">
      ${wf:actionData('increment-attempt')['attempt'] lt 3}
    </case>
    <default to="fail"/>
  </switch>
</decision>
```

## Interview Tips

> **Tip 1:** Bundle jobs are the answer to "how do you manage pipeline dependencies in Oozie?" — bundles group coordinators, giving a single point to suspend, resume, or rerun the entire pipeline. Know how this compares to Airflow DAG dependencies.

> **Tip 2:** Oozie HA is often asked in senior interviews. The key is that Oozie uses ZooKeeper for leader election and a shared RDBMS (MySQL/PostgreSQL) for state. Active and standby share the database, and ZooKeeper coordinates which is active.

> **Tip 3:** When comparing Oozie to Airflow, don't just say "Airflow is better." The nuanced answer: Oozie excels at data-triggered scheduling with `<done-flag>` and integrates natively with HDFS/YARN. Airflow wins for developer productivity, Python operators, and cloud-native deployments.

> **Tip 4:** Decision nodes in Oozie are the equivalent of Airflow's `BranchPythonOperator`. A common use case: check if the landing zone has data before running the pipeline, and route to a no-op/alert path if empty.

> **Tip 5:** The migration conversation usually includes: "What's the biggest risk?" Answer: data trigger equivalents. Oozie's `<done-flag>` is simple and reliable; Airflow's `FileSensor` has polling overhead and can miss files if the check interval is too long. Plan your sensors carefully.
