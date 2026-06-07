---
title: "Oozie - Fundamentals"
topic: hadoop
subtopic: oozie
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [hadoop, oozie, workflow, scheduler, mapreduce, hive]
---

# Oozie — Fundamentals

## What is Oozie?

Apache Oozie is a workflow scheduler system for Hadoop jobs. It coordinates sequences of MapReduce, Hive, Pig, Spark, Shell, and other jobs into multi-step workflows that run on a schedule or based on data availability.

**Why Oozie?**
- Hadoop jobs rarely run in isolation — you need sequencing, error handling, and scheduling
- Oozie runs inside the Hadoop cluster — no separate server to manage
- It integrates with YARN for resource tracking
- It provides a web UI, REST API, and CLI

## Job Types

| Job Type | Purpose | Key Use Case |
|----------|---------|--------------|
| **Workflow** | Directed Acyclic Graph (DAG) of actions | Run a multi-step ETL pipeline once |
| **Coordinator** | Schedule workflows on time or data triggers | Run workflow daily at 2 AM |
| **Bundle** | Group multiple coordinators | Manage a full data warehouse refresh |

## Workflow Job

A workflow defines a DAG of actions. Each action is a step (MapReduce, Hive, Shell, etc.) with `<ok>` and `<error>` transitions.

```xml
<!-- workflow.xml -->
<workflow-app name="daily-etl-workflow" xmlns="uri:oozie:workflow:0.5">
  <start to="import-raw-data"/>

  <action name="import-raw-data">
    <shell xmlns="uri:oozie:shell-action:0.3">
      <exec>sqoop_import.sh</exec>
      <argument>${date}</argument>
    </shell>
    <ok to="transform-data"/>
    <error to="send-failure-email"/>
  </action>

  <action name="transform-data">
    <hive xmlns="uri:oozie:hive-action:0.6">
      <jdbc-url>jdbc:hive2://hiveserver2:10000/raw</jdbc-url>
      <script>transform.hql</script>
      <param>date=${date}</param>
    </hive>
    <ok to="end"/>
    <error to="send-failure-email"/>
  </action>

  <action name="send-failure-email">
    <email xmlns="uri:oozie:email-action:0.2">
      <to>de-team@company.com</to>
      <subject>Oozie Workflow Failed: ${wf:name()}</subject>
      <body>Workflow ${wf:id()} failed at action ${wf:lastErrorNode()}</body>
    </email>
    <ok to="fail"/>
    <error to="fail"/>
  </action>

  <kill name="fail">
    <message>Workflow failed: ${wf:errorMessage(wf:lastErrorNode())}</message>
  </kill>

  <end name="end"/>
</workflow-app>
```

## job.properties File

The properties file sets the configuration for a workflow run:

```properties
# job.properties
nameNode=hdfs://namenode:8020
jobTracker=resourcemanager:8032
oozie.use.system.libpath=true
oozie.wf.application.path=${nameNode}/user/etl/workflows/daily-etl

# Custom parameters
date=2024-01-15
input_dir=/data/raw/orders
output_dir=/data/refined/orders

# Oozie libs
oozie.libpath=${nameNode}/user/oozie/share/lib
```

## Action Types

### MapReduce Action
```xml
<action name="wordcount">
  <map-reduce>
    <resource-manager>${resourceManager}</resource-manager>
    <name-node>${nameNode}</name-node>
    <configuration>
      <property>
        <name>mapred.mapper.class</name>
        <value>org.example.WordCountMapper</value>
      </property>
      <property>
        <name>mapred.reducer.class</name>
        <value>org.example.WordCountReducer</value>
      </property>
    </configuration>
  </map-reduce>
  <ok to="end"/>
  <error to="fail"/>
</action>
```

### Hive Action
```xml
<action name="hive-transform">
  <hive xmlns="uri:oozie:hive-action:0.6">
    <jdbc-url>jdbc:hive2://hiveserver2:10000</jdbc-url>
    <script>hive/transform.hql</script>
    <param>INPUT=/data/raw/orders</param>
    <param>OUTPUT=/data/refined/orders</param>
  </hive>
  <ok to="end"/>
  <error to="fail"/>
</action>
```

### Spark Action
```xml
<action name="spark-aggregation">
  <spark xmlns="uri:oozie:spark-action:0.2">
    <resource-manager>${resourceManager}</resource-manager>
    <name-node>${nameNode}</name-node>
    <master>yarn</master>
    <mode>cluster</mode>
    <name>order-aggregation</name>
    <class>com.company.OrderAggregation</class>
    <jar>lib/etl-jobs.jar</jar>
    <spark-opts>--executor-memory 4G --num-executors 10</spark-opts>
    <arg>${date}</arg>
  </spark>
  <ok to="end"/>
  <error to="fail"/>
</action>
```

### Shell Action
```xml
<action name="run-python">
  <shell xmlns="uri:oozie:shell-action:0.3">
    <resource-manager>${resourceManager}</resource-manager>
    <name-node>${nameNode}</name-node>
    <exec>python3</exec>
    <argument>validate.py</argument>
    <argument>${date}</argument>
    <file>validate.py#validate.py</file>
    <env-var>PYTHONPATH=/opt/etl/lib</env-var>
    <capture-output/>
  </shell>
  <ok to="end"/>
  <error to="fail"/>
</action>
```

## Workflow Diagram

```
graph TD
    A["Start"] --> B["import-raw-data<br>Shell/Sqoop action"]
    B -->|"ok"| C["transform-data<br>Hive action"]
    B -->|"error"| E["send-failure-email"]
    C -->|"ok"| D["End"]
    C -->|"error"| E
    E -->|"ok/error"| F["Kill: fail"]
```

## Oozie Web Console

Access via browser at `http://oozie-host:11000/oozie`

- View running/completed workflow jobs
- Rerun failed actions from specific nodes
- Monitor coordinator job runs
- Download logs for debugging

## Oozie CLI Basics

```bash
# Submit and run a workflow
oozie job -oozie http://oozie-host:11000/oozie \
          -config job.properties \
          -run

# Check job status
oozie job -oozie http://oozie-host:11000/oozie \
          -info 0000001-240115120000000-oozie-oozi-W

# List running jobs
oozie jobs -oozie http://oozie-host:11000/oozie \
           -jobtype workflow \
           -status RUNNING

# Rerun a failed workflow from a specific action
oozie job -oozie http://oozie-host:11000/oozie \
          -rerun 0000001-240115120000000-oozie-oozi-W \
          -config job.properties \
          -rerun-fail-nodes true

# Kill a running job
oozie job -oozie http://oozie-host:11000/oozie \
          -kill 0000001-240115120000000-oozie-oozi-W
```

## HDFS Directory Structure for Oozie

```
/user/etl/workflows/daily-etl/
├── workflow.xml           # Workflow definition
├── coordinator.xml        # Coordinator definition (optional)
├── job.properties         # Job configuration
├── hive/
│   └── transform.hql      # Hive script
├── scripts/
│   └── sqoop_import.sh    # Shell scripts
└── lib/
    └── etl-jobs.jar       # Custom JARs
```

## Interview Tips

> **Tip 1:** Oozie workflow is a DAG (Directed Acyclic Graph), not a sequential script. Every action must have both `<ok>` and `<error>` transitions — forgetting `<error>` is a common mistake that causes jobs to hang.

> **Tip 2:** The `<start>` node names the first action, `<end>` marks success, and `<kill>` marks failure. There can only be one `<start>` and one `<end>`, but multiple `<kill>` nodes are allowed.

> **Tip 3:** Oozie uses HDFS to store workflow definitions — `oozie.wf.application.path` must point to an HDFS directory containing `workflow.xml`. Local filesystem paths will not work.

> **Tip 4:** For debugging failures, check two log sources: the Oozie web console for action logs, and YARN for the underlying MapReduce/Hive/Spark job logs. Oozie logs tell you what failed; YARN logs tell you why.

> **Tip 5:** The `<capture-output/>` element in Shell actions lets you pass output back to the workflow via `wf:actionData()`. This is useful for dynamic values like row counts or validation results that downstream actions need.
