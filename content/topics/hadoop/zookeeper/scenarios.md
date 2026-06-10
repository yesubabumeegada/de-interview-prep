---
title: "ZooKeeper - Scenario Questions"
topic: hadoop
subtopic: zookeeper
content_type: scenario_question
tags: [hadoop, zookeeper, scenarios, interview, leader-election, distributed-lock, service-discovery]
---

# Scenario Questions — ZooKeeper

<article data-difficulty="junior">

## 🟢 Junior: Explain Leader Election with ZooKeeper ZNodes

**Scenario:** You are asked in an interview to explain how ZooKeeper enables leader election. Describe the process using ZNodes, including what happens when the leader fails. Illustrate with a concrete example of 3 candidate nodes.

<details><summary>💡 Hint</summary>

Think about: What type of ZNode is used (ephemeral, persistent, sequential)? Why ephemeral? What do the non-leaders do while waiting? How is the new leader elected when the current leader crashes?

</details>

<details><summary>✅ Solution</summary>

**Simple Leader Election (ephemeral ZNode):**

```
Step 1: All 3 nodes start up and try to become leader

Node A (starts first):
  zk.create("/election/leader", "nodeA:8080".encode(), EPHEMERAL)
  → SUCCESS → Node A is LEADER

Node B (starts 0.1s later):
  zk.create("/election/leader", "nodeB:8080".encode(), EPHEMERAL)
  → FAILURE (NodeExistsException)
  → Node B watches /election/leader (getData with watch=True)
  → Node B is FOLLOWER

Node C (starts 0.1s later):
  zk.create("/election/leader", "nodeC:8080".encode(), EPHEMERAL)
  → FAILURE (NodeExistsException)
  → Node C watches /election/leader (getData with watch=True)
  → Node C is FOLLOWER

Step 2: Leader failure scenario (Node A crashes)

t=0:   Node A JVM crashes (network cut, OOM, etc.)
t=30s: ZooKeeper session timeout for Node A expires
t=30s: ZK deletes /election/leader (it was ephemeral)
t=30s: Node B and Node C both receive NodeDeleted event (they were watching)
t=30s: Both Node B and C race to create /election/leader:
  - Node B: zk.create("/election/leader", "nodeB:8080", EPHEMERAL) → SUCCESS
  - Node C: zk.create("/election/leader", "nodeC:8080", EPHEMERAL) → FAILURE
t=30s: Node B is the new LEADER
t=30s: Node C re-registers as follower, watching /election/leader again
```

**Python implementation:**
```python
from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError
import socket, time

class LeaderElector:
    def __init__(self, zk_hosts):
        self.zk = KazooClient(hosts=zk_hosts)
        self.zk.start()
        self.is_leader = False
        self.host = socket.gethostname()

    def try_become_leader(self):
        try:
            self.zk.create(
                "/election/leader",
                self.host.encode(),
                ephemeral=True,
                makepath=True
            )
            self.is_leader = True
            print(f"{self.host}: I am now the LEADER")
            self.on_become_leader()

        except NodeExistsError:
            self.is_leader = False
            print(f"{self.host}: I am a FOLLOWER")
            # Watch the leader ZNode
            @self.zk.DataWatch("/election/leader")
            def watch_leader(data, stat, event):
                if event and event.type == "DELETED":
                    print(f"{self.host}: Leader died! Attempting election...")
                    self.try_become_leader()

    def on_become_leader(self):
        # Start doing leader-specific work
        print(f"{self.host}: Starting leader tasks...")
        # E.g., distribute work, manage state, etc.

# Usage
elector = LeaderElector("zk1:2181,zk2:2181,zk3:2181")
elector.try_become_leader()

# Keep running
while True:
    if elector.is_leader:
        print("Doing leader work...")
    time.sleep(5)
```

**Key ZNode choice — why ephemeral?**
```
Ephemeral: auto-deleted when ZK session expires (client disconnects/crashes)
→ Failure detection is automatic, no separate heartbeat mechanism needed

Persistent: survives disconnections
→ Would require manual cleanup if a leader crashes
→ Could leave a "ghost" leader preventing new election

Conclusion: Ephemeral ZNode = automatic failure detection via ZK session
```

</details>
</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Design Distributed Lock with Proper Timeout and Cleanup

**Scenario:** You're building a data pipeline where only one worker should process a given partition at a time. Design a distributed lock using ZooKeeper that:
1. Prevents concurrent processing of the same partition
2. Automatically releases the lock if the worker crashes
3. Has a maximum lock hold time (timeout) to prevent indefinite locking
4. Properly handles ZooKeeper session expiration
5. Uses the sequential ephemeral pattern (not the simple ephemeral) to avoid the herd effect

<details><summary>✅ Solution</summary>

**Distributed Lock with Sequential Ephemeral ZNodes:**

```python
import time
import threading
from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError, SessionExpiredError

class DistributedLock:
    """
    Thread-safe distributed lock using ZooKeeper sequential ephemeral ZNodes.
    Avoids herd effect by watching only the predecessor node.
    """
    
    def __init__(self, zk_hosts, lock_path, max_hold_seconds=300):
        self.zk = KazooClient(hosts=zk_hosts)
        self.zk.start()
        self.lock_path = lock_path
        self.max_hold_seconds = max_hold_seconds
        self.my_node = None
        self.lock_timer = None
        
        # Ensure lock path exists
        self.zk.ensure_path(lock_path)

    def acquire(self, timeout_seconds=60):
        """
        Acquire the lock within timeout_seconds.
        Returns True if acquired, False if timed out.
        """
        deadline = time.time() + timeout_seconds
        
        # Step 1: Create sequential ephemeral ZNode
        self.my_node = self.zk.create(
            f"{self.lock_path}/lock-",
            ephemeral=True,
            sequence=True
        )
        my_seq = int(self.my_node.split("-")[-1])
        print(f"Created lock candidate: {self.my_node}")
        
        while True:
            if time.time() > deadline:
                # Timed out waiting for lock
                self.zk.delete(self.my_node)
                self.my_node = None
                return False
            
            # Step 2: Check if I'm the lock holder (smallest sequence)
            children = sorted(self.zk.get_children(self.lock_path))
            candidates = sorted([c for c in children if c.startswith("lock-")])
            my_index = candidates.index(f"lock-{my_seq:010d}")
            
            if my_index == 0:
                # I have the smallest sequence number — I hold the lock!
                print(f"Lock ACQUIRED: {self.my_node}")
                # Start a timer to auto-release after max_hold_seconds
                self._start_hold_timer()
                return True
            
            # Step 3: Watch only the predecessor (not all nodes — no herd effect)
            predecessor = candidates[my_index - 1]
            predecessor_path = f"{self.lock_path}/{predecessor}"
            
            event = threading.Event()
            
            @self.zk.DataWatch(predecessor_path)
            def watch_predecessor(data, stat, event_obj):
                if event_obj and event_obj.type in ("DELETED", "NONE"):
                    event.set()
                    return False  # Stop watching
            
            # Wait until predecessor is deleted or timeout
            remaining = deadline - time.time()
            event.wait(timeout=remaining)
            # Loop back to check if we're now the smallest

    def release(self):
        """Release the lock."""
        if self.lock_timer:
            self.lock_timer.cancel()
        if self.my_node:
            try:
                self.zk.delete(self.my_node)
                print(f"Lock RELEASED: {self.my_node}")
            except NoNodeError:
                print("Lock ZNode already gone (session expired?)")
            finally:
                self.my_node = None

    def _start_hold_timer(self):
        """Auto-release lock after max_hold_seconds."""
        def auto_release():
            print(f"WARNING: Lock held too long, auto-releasing!")
            self.release()
        
        self.lock_timer = threading.Timer(self.max_hold_seconds, auto_release)
        self.lock_timer.daemon = True
        self.lock_timer.start()

    def __enter__(self):
        if not self.acquire():
            raise TimeoutError("Could not acquire distributed lock")
        return self

    def __exit__(self, *args):
        self.release()


# Usage example in a data pipeline
def process_partition(date_str, partition_id):
    zk = KazooClient(hosts="zk1:2181,zk2:2181,zk3:2181")
    zk.start()
    
    lock = DistributedLock(
        zk,
        f"/locks/partitions/{date_str}/{partition_id}",
        max_hold_seconds=600  # 10 min max
    )
    
    with lock:
        print(f"Processing partition {partition_id} for {date_str}")
        # Only ONE worker processes this partition at a time
        run_etl_job(date_str, partition_id)
        print("Done!")
```

**Handling ZooKeeper Session Expiration:**
```python
def on_session_expired():
    """Called when ZK session expires (e.g., prolonged GC pause)."""
    print("ZK SESSION EXPIRED — lock is automatically released!")
    # Ephemeral ZNodes are auto-deleted on session expiry
    # Safe: ZK guarantees we no longer hold the lock
    # Required action: create new ZK connection and re-register
    reconnect_and_retry()

# Register session expiry handler
@zk.add_listener
def zk_state_change(state):
    if state == KazooState.LOST:
        on_session_expired()
    elif state == KazooState.SUSPENDED:
        print("ZK connection suspended (may recover)")
    elif state == KazooState.CONNECTED:
        print("ZK connected/reconnected")
```

</details>
</article>

<article data-difficulty="senior">

## 🔴 Senior: Design High-Availability Service Discovery Using ZooKeeper for a Microservices Platform

**Scenario:** You're architecting a microservices platform with 50 services, each running 3-10 instances. Services need to discover each other dynamically (instances can start/stop at any time). Requirements:
- Sub-100ms discovery latency
- Automatic de-registration on instance crash
- Support health checks (unhealthy instances should not be discoverable)
- Handle 10,000 instances total across all services
- Handle 1,000 discovery requests/second
- Zero-downtime rolling deployments

Design the complete service discovery system using ZooKeeper.

<details><summary>✅ Solution</summary>

**ZooKeeper ZNode Structure:**

```
/services/
├── user-service/
│   ├── instances/
│   │   ├── us-east-1a-pod-001    (ephemeral: data = host:port:version:weight)
│   │   ├── us-east-1a-pod-002    (ephemeral)
│   │   └── us-east-1b-pod-003    (ephemeral)
│   └── config/
│       └── lb-strategy           (persistent: data = "round-robin")
├── order-service/
│   └── instances/
│       ├── us-east-1a-pod-010    (ephemeral)
│       └── us-east-1b-pod-011    (ephemeral)
└── product-service/
    └── instances/
```

**Service Registration (startup):**
```python
import json, socket, os
from kazoo.client import KazooClient

class ServiceRegistry:
    def __init__(self, zk_hosts, service_name, port, version):
        self.zk = KazooClient(hosts=zk_hosts)
        self.zk.start()
        self.service_name = service_name
        self.instance_id = f"{os.environ.get('POD_NAME', socket.gethostname())}"
        self.zk_path = f"/services/{service_name}/instances/{self.instance_id}"
        self.port = port
        self.version = version

    def register(self):
        instance_data = json.dumps({
            "host": socket.gethostname(),
            "port": self.port,
            "version": self.version,
            "weight": 100,
            "zone": os.environ.get("AVAILABILITY_ZONE", "default"),
            "registered_at": int(time.time())
        }).encode()

        self.zk.create(
            self.zk_path,
            instance_data,
            ephemeral=True,
            makepath=True
        )
        print(f"Registered: {self.zk_path}")

        # Re-register on reconnect (session expiry)
        @self.zk.add_listener
        def state_listener(state):
            if state == KazooState.CONNECTED:
                self.register()

    def deregister(self):
        try:
            self.zk.delete(self.zk_path)
        except Exception:
            pass  # Already removed (crash recovery)
```

**Client-Side Service Discovery with Local Cache:**
```python
class ServiceDiscoveryClient:
    """
    Caches service instance list locally.
    Updates cache via ZK watches (push-based, not polling).
    """
    
    def __init__(self, zk_hosts):
        self.zk = KazooClient(hosts=zk_hosts)
        self.zk.start()
        self._cache = {}  # service_name -> list of instance info
        self._rr_index = {}  # round-robin cursor per service

    def get_instances(self, service_name):
        """Returns cached list of healthy instances."""
        if service_name not in self._cache:
            self._load_and_watch(service_name)
        return self._cache.get(service_name, [])

    def get_instance(self, service_name, strategy="round-robin"):
        """Get a single instance for a request."""
        instances = self.get_instances(service_name)
        if not instances:
            raise ValueError(f"No instances for {service_name}")
        
        if strategy == "round-robin":
            idx = self._rr_index.get(service_name, 0)
            instance = instances[idx % len(instances)]
            self._rr_index[service_name] = idx + 1
            return instance
        elif strategy == "random":
            import random
            return random.choice(instances)

    def _load_and_watch(self, service_name):
        path = f"/services/{service_name}/instances"
        self.zk.ensure_path(path)
        
        @self.zk.ChildrenWatch(path)
        def watch_instances(children):
            # Called immediately + whenever children change
            instances = []
            for child in children:
                try:
                    data, _ = self.zk.get(f"{path}/{child}")
                    instance = json.loads(data)
                    instances.append(instance)
                except Exception:
                    pass  # Instance gone during read
            self._cache[service_name] = instances
            print(f"Updated {service_name}: {len(instances)} instances")
```

**Health Check Integration:**
```python
class HealthAwareRegistry(ServiceRegistry):
    def __init__(self, *args, health_check_fn, **kwargs):
        super().__init__(*args, **kwargs)
        self.health_check_fn = health_check_fn
        self.is_healthy = True

    def start_health_reporter(self):
        """Update ZNode data with health status every 10s."""
        def report_health():
            while True:
                try:
                    healthy = self.health_check_fn()
                    if healthy != self.is_healthy:
                        self.is_healthy = healthy
                        # Update ZNode data with health status
                        self._update_instance_data({"healthy": healthy})
                except Exception as e:
                    print(f"Health check error: {e}")
                time.sleep(10)
        
        thread = threading.Thread(target=report_health, daemon=True)
        thread.start()

    def _update_instance_data(self, extra_fields):
        current = json.loads(self.zk.get(self.zk_path)[0])
        current.update(extra_fields)
        self.zk.set(self.zk_path, json.dumps(current).encode())
```

**Zero-Downtime Rolling Deployment:**
```
Deployment sequence for zero downtime:
1. Start new instance v2 → registers in ZK → traffic starts flowing
2. Health check passes → v2 fully serving
3. Drain old instance v1 (set weight=0 in ZK, wait for in-flight requests)
4. Stop v1 → ephemeral ZNode auto-deleted
5. v1 removed from client cache via watch notification
6. Repeat for each instance

Client sees: smooth transition, no 503 errors
```

**Capacity analysis:**
```
10,000 instances × 300 bytes/ZNode = 3 MB total ZK memory
1,000 discovery requests/sec → served from local cache (no ZK read!)
ZK watches: 50 services × N clients per service → optimize with client-side caching
ZK write operations: instance start/stop events only (~10/minute in steady state)

Verdict: ZooKeeper easily handles this at 10K instances with client-side caching.
Bottleneck would be 100K+ instances — at that scale, use etcd or Consul.
```

</details>
</article>

---

## ⚡ Quick-fire Q&A

**Q: What is Apache ZooKeeper and what is it used for?**
A: ZooKeeper is a distributed coordination service that provides reliable primitives like distributed locks, leader election, configuration management, and service discovery for distributed systems. Hadoop uses it for HDFS NameNode HA failover, HBase region assignment, and Kafka broker coordination.

**Q: How does ZooKeeper achieve consistency?**
A: ZooKeeper uses the ZAB (ZooKeeper Atomic Broadcast) protocol, which is similar to Paxos. All write requests go to the elected leader, which broadcasts them to a quorum of followers before acknowledging the client. This ensures all nodes see writes in the same order.

**Q: What is a ZNode?**
A: A ZNode is a node in ZooKeeper's hierarchical namespace (similar to a filesystem tree). ZNodes store small amounts of data (up to 1MB) and can be persistent (survive restarts) or ephemeral (automatically deleted when the creating client session ends).

**Q: How does ZooKeeper enable leader election?**
A: Competing processes each create an ephemeral sequential ZNode under a common path. The process with the lowest sequence number becomes the leader. Others set watches on the ZNode immediately before them, so only the next-in-line is notified when the current leader's session expires.

**Q: What is a ZooKeeper watch?**
A: A watch is a one-time notification mechanism. A client can set a watch on a ZNode and be notified (once) when the ZNode's data changes, is deleted, or new children are created. Watches enable event-driven coordination without polling.

**Q: How does ZooKeeper handle failures and ensure availability?**
A: ZooKeeper requires a quorum (majority) of nodes to be available. With 2N+1 nodes, it can tolerate N failures. Data is persisted to a transaction log and snapshots on disk for recovery after restarts.

**Q: What are the limitations of ZooKeeper?**
A: ZooKeeper is designed for small coordination data — not large file storage. It requires an odd number of nodes (3, 5) for quorum. Write throughput is limited by the leader, and all reads and writes go through the ZooKeeper ensemble, making it a potential bottleneck in high-frequency use cases.

**Q: What replaced ZooKeeper in Kafka and how does it affect data engineers?**
A: Apache Kafka replaced ZooKeeper with KRaft (Kafka Raft metadata mode) in Kafka 3.x, eliminating the ZooKeeper dependency. For data engineers, this simplifies Kafka cluster operations (no separate ZooKeeper ensemble to manage) and improves scalability of Kafka metadata.

---

## 💼 Interview Tips

- Know ZooKeeper's role in each Hadoop component specifically — HDFS HA failover, HBase region assignment, Kafka broker registration — showing context-aware knowledge rather than generic descriptions.
- Explain the ephemeral ZNode pattern for leader election and distributed locking — it's the most common ZooKeeper interview question and demonstrates understanding of the core coordination primitives.
- Be clear about ZooKeeper's data size limitation (1MB per ZNode) — it's a common misconception that ZooKeeper can store arbitrary data; it's designed only for small coordination state.
- Discuss quorum requirements (2N+1 for N fault tolerance) — interviewers use this to test whether you understand ZooKeeper's availability model and how to size ensembles.
- Mention KRaft as Kafka's ZooKeeper replacement for modern clusters — showing awareness of the ecosystem evolution signals you keep up with current developments.
- For senior roles, discuss ZooKeeper operational concerns: session timeouts affecting connected clients (HBase region servers), watch thundering herd on large clusters, and the operational burden of running a separate ensemble alongside your main cluster.
