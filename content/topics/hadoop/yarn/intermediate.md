---
title: "YARN - Intermediate"
topic: hadoop
subtopic: yarn
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [hadoop, yarn, capacity-scheduler, fair-scheduler, preemption, containers]
---

# YARN Intermediate Concepts

## Capacity Scheduler Deep Dive

### Queue Hierarchy
```xml
<!-- Multi-level queue hierarchy -->
<property>
  <name>yarn.scheduler.capacity.root.queues</name>
  <value>production,analytics,default</value>
</property>

<!-- Production sub-queues -->
<property>
  <name>yarn.scheduler.capacity.root.production.queues</name>
  <value>critical,batch</value>
</property>
<property>
  <name>yarn.scheduler.capacity.root.production.critical.capacity</name>
  <value>70</value>  <!-- 70% of production queue -->
</property>
<property>
  <name>yarn.scheduler.capacity.root.production.batch.capacity</name>
  <value>30</value>
</property>
```

### User and Queue ACLs
```xml
<!-- Who can submit to production queue -->
<property>
  <name>yarn.scheduler.capacity.root.production.acl_submit_applications</name>
  <value>alice,bob prod-team</value>  <!-- users alice,bob and group prod-team -->
</property>

<!-- Who can administer (kill jobs, change queue state) -->
<property>
  <name>yarn.scheduler.capacity.root.production.acl_administer_queue</name>
  <value>admin yarn-admins</value>
</property>

<!-- Per-user resource limits within a queue -->
<property>
  <name>yarn.scheduler.capacity.root.production.user-limit-factor</name>
  <value>2</value>  <!-- Any user can use up to 2x their fair share -->
</property>
<property>
  <name>yarn.scheduler.capacity.root.production.maximum-am-resource-percent</name>
  <value>0.1</value>  <!-- Max 10% of queue for ApplicationMasters -->
</property>
```

### Queue State Management
```bash
# Stop a queue (drain running apps, reject new)
yarn queue -status root.development
yarn scheduler -format-conf  # Print current config

# Change queue state at runtime (without restart)
# Edit capacity-scheduler.xml then:
yarn rmadmin -refreshQueues

# Move application to different queue (if enabled)
yarn application -movetoqueue application_12345_0001 \
  -queue root.production.batch
```

## Resource Preemption

Preemption allows higher-priority queues to reclaim resources from lower-priority ones:

```xml
<!-- yarn-site.xml -->
<property>
  <name>yarn.resourcemanager.scheduler.monitor.enable</name>
  <value>true</value>
</property>
<property>
  <name>yarn.resourcemanager.scheduler.monitor.policies</name>
  <value>org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.ProportionalCapacityPreemptionPolicy</value>
</property>

<!-- capacity-scheduler.xml -->
<property>
  <name>yarn.scheduler.capacity.preemption.enabled</name>
  <value>true</value>
</property>
<property>
  <name>yarn.scheduler.capacity.preemption.natural_termination_factor</name>
  <value>0.2</value>  <!-- Preempt 20% of over-capacity resources per cycle -->
</property>
<property>
  <name>yarn.scheduler.capacity.preemption.max_wait_before_kill</name>
  <value>15000</value>  <!-- Give container 15s to finish before killing -->
</property>
```

**How preemption works:**
1. Queue A uses more than its guaranteed capacity
2. Queue B submits jobs but lacks resources
3. ProportionalCapacityPreemptionPolicy identifies containers to preempt from Queue A
4. Containers are sent a "preempt" signal (application can checkpoint)
5. After `max_wait_before_kill` ms, container is killed
6. Resources reassigned to Queue B

## Node Labels

Node labels allow reserving specific nodes for specific workloads:

```bash
# Label nodes (e.g., GPU nodes, high-memory nodes)
yarn rmadmin -addToClusterNodeLabels "gpu,high-memory"

# Assign labels to nodes
yarn rmadmin -replaceLabelsOnNode "node1:8041,node2:8041=gpu"
yarn rmadmin -replaceLabelsOnNode "node3:8041,node4:8041=high-memory"

# Verify
yarn node -list --all | grep label
```

```xml
<!-- Queue that runs on GPU nodes only -->
<property>
  <name>yarn.scheduler.capacity.root.ml-training.accessible-node-labels</name>
  <value>gpu</value>
</property>
<property>
  <name>yarn.scheduler.capacity.root.ml-training.default-node-label-expression</name>
  <value>gpu</value>
</property>
```

```bash
# Submit ML job to GPU nodes
spark-submit \
  --master yarn \
  --queue root.ml-training \
  --conf spark.yarn.am.nodeLabelExpression=gpu \
  --conf spark.executor.nodeLabelExpression=gpu \
  train_model.py
```

## Container Resource Isolation

### Memory Isolation with cgroups
```xml
<!-- yarn-site.xml: Enable Linux Container Executor (required for cgroups) -->
<property>
  <name>yarn.nodemanager.container-executor.class</name>
  <value>org.apache.hadoop.yarn.server.nodemanager.LinuxContainerExecutor</value>
</property>
<property>
  <name>yarn.nodemanager.linux-container-executor.cgroups.hierarchy</name>
  <value>/hadoop-yarn</value>
</property>
<property>
  <name>yarn.nodemanager.linux-container-executor.cgroups.mount</name>
  <value>true</value>
</property>
```

### Strict vs. Lenient Memory
```xml
<!-- Strict: kill container immediately if it exceeds memory limit -->
<property>
  <name>yarn.nodemanager.pmem-check-enabled</name>
  <value>true</value>
</property>
<property>
  <name>yarn.nodemanager.vmem-check-enabled</name>
  <value>true</value>
</property>
<!-- Virtual memory ratio (JVM may use more virtual than physical) -->
<property>
  <name>yarn.nodemanager.vmem-pmem-ratio</name>
  <value>2.1</value>  <!-- virtual memory limit = 2.1 × physical memory -->
</property>
```

## YARN Timeline Server

Stores history of completed applications for later analysis:

```xml
<!-- yarn-site.xml -->
<property>
  <name>yarn.timeline-service.enabled</name>
  <value>true</value>
</property>
<property>
  <name>yarn.resourcemanager.system-metrics-publisher.enabled</name>
  <value>true</value>
</property>
<property>
  <name>yarn.timeline-service.hostname</name>
  <value>timeline-server.example.com</value>
</property>
```

```bash
# Access Timeline Server API
curl http://timeline-server:8188/ws/v1/timeline/YARN_APPLICATION_ATTEMPT

# Get application history
curl http://timeline-server:8188/ws/v1/timeline/MAPREDUCE_JOB/\
  job_1234_0001

# Application Timeline UI
# http://timeline-server:8188/applicationhistory
```

## Log Aggregation

YARN aggregates container logs to HDFS after application completion:

```xml
<!-- yarn-site.xml -->
<property>
  <name>yarn.log-aggregation-enable</name>
  <value>true</value>
</property>
<property>
  <name>yarn.nodemanager.remote-app-log-dir</name>
  <value>/yarn/logs</value>
</property>
<property>
  <name>yarn.log-aggregation.retain-seconds</name>
  <value>604800</value>  <!-- Keep logs for 7 days -->
</property>
```

```bash
# View aggregated logs for completed application
yarn logs -applicationId application_12345_0001

# View specific container logs
yarn logs -applicationId application_12345_0001 \
  -containerId container_12345_0001_01_000001 \
  -nodeAddress node1.example.com:8041

# Download all logs
yarn logs -applicationId application_12345_0001 \
  -out /local/debug/app_12345_logs/
```

## Capacity Scheduler vs Fair Scheduler

| Feature | Capacity Scheduler | Fair Scheduler |
|---------|-------------------|----------------|
| Default | Yes (Hadoop) | No |
| Queue allocation | Percentage capacity | Weighted shares |
| Preemption | Yes (configurable) | Yes (configurable) |
| Short job behavior | Can wait behind long jobs | Gets resources quickly |
| Configuration | XML-based | XML-based |
| Multiple AM types | Supported | Supported |
| Best for | Enterprise multi-tenant | Mixed interactive/batch |
| SLA guarantees | Strong (min capacity) | Eventual fairness |

## YARN RM High Availability

```xml
<!-- yarn-site.xml -->
<property>
  <name>yarn.resourcemanager.ha.enabled</name>
  <value>true</value>
</property>
<property>
  <name>yarn.resourcemanager.cluster-id</name>
  <value>yarn-cluster</value>
</property>
<property>
  <name>yarn.resourcemanager.ha.rm-ids</name>
  <value>rm1,rm2</value>
</property>
<property>
  <name>yarn.resourcemanager.hostname.rm1</name>
  <value>rm-host-1</value>
</property>
<property>
  <name>yarn.resourcemanager.hostname.rm2</name>
  <value>rm-host-2</value>
</property>
<property>
  <name>yarn.resourcemanager.zk-address</name>
  <value>zk1:2181,zk2:2181,zk3:2181</value>
</property>
<!-- Store RM state in ZooKeeper for failover -->
<property>
  <name>yarn.resourcemanager.store.class</name>
  <value>org.apache.hadoop.yarn.server.resourcemanager.recovery.ZKRMStateStore</value>
</property>
```

## Interview Tips

> **Tip 1:** Preemption is a critical topic for multi-tenant clusters. Know both sides: it enables SLA guarantees for high-priority queues, but can disrupt long-running batch jobs in lower-priority queues. Discuss the `max_wait_before_kill` grace period and how applications can handle preemption signals.

> **Tip 2:** Node labels are a powerful feature often overlooked by candidates. Use case: heterogeneous clusters where GPU nodes should only run ML training, high-memory nodes for Spark jobs with large shuffles, and SSD nodes for latency-sensitive workloads.

> **Tip 3:** When asked about YARN RM HA, explain the ZooKeeper role: RM state (running applications, queue state) is persisted to ZK so the Standby RM can take over without losing in-flight applications. This is different from HDFS HA (which uses JournalNodes for edit logs).

> **Tip 4:** The `maximum-am-resource-percent` setting is often overlooked. If too many applications submit at once, their AMs can consume all queue resources, leaving nothing for actual tasks. Default is 10% — important for clusters with many small jobs.

> **Tip 5:** For container OOM kills, the culprit is often the JVM heap plus off-heap memory (DirectByteBuffer, Netty buffers) exceeding the container limit. The fix: ensure `-Xmx` is 75-80% of container memory, and test with actual data to catch off-heap growth.
