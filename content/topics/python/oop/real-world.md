---
title: "Python OOP - Real-World Production Examples"
topic: python
subtopic: oop
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [python, oop, production, plugin-system, pipeline-builder, configuration, event-driven]
---

# Python OOP — Real-World Production Examples

## Pattern 1: Plugin System for Data Connectors

A production plugin system that auto-discovers and registers connectors:

```python
"""
Plugin-based connector system.
Add new data sources by creating a class — no framework changes needed.
"""
from abc import ABC, abstractmethod
from typing import Iterator, Dict, Any, Optional
import importlib
import pkgutil
import logging

logger = logging.getLogger(__name__)

class ConnectorRegistry:
    """Central registry for all data connectors."""
    _connectors: Dict[str, type] = {}
    
    @classmethod
    def register(cls, name: str):
        """Decorator to register a connector class."""
        def decorator(connector_cls):
            cls._connectors[name] = connector_cls
            logger.info(f"Registered connector: {name}")
            return connector_cls
        return decorator
    
    @classmethod
    def create(cls, name: str, **kwargs) -> "BaseConnector":
        if name not in cls._connectors:
            raise ValueError(
                f"Unknown connector '{name}'. "
                f"Available: {list(cls._connectors.keys())}"
            )
        return cls._connectors[name](**kwargs)
    
    @classmethod
    def discover_plugins(cls, package_name: str):
        """Auto-discover connector plugins from a package."""
        package = importlib.import_module(package_name)
        for _, module_name, _ in pkgutil.iter_modules(package.__path__):
            importlib.import_module(f"{package_name}.{module_name}")

class BaseConnector(ABC):
    """Base interface for all data connectors."""
    
    @abstractmethod
    def connect(self) -> None: ...
    
    @abstractmethod
    def extract(self, query: str) -> Iterator[Dict[str, Any]]: ...
    
    @abstractmethod
    def close(self) -> None: ...
    
    def __enter__(self):
        self.connect()
        return self
    
    def __exit__(self, *exc):
        self.close()

@ConnectorRegistry.register("postgres")
class PostgresConnector(BaseConnector):
    def __init__(self, host: str, port: int, database: str, **kwargs):
        self.conn_string = f"postgresql://{host}:{port}/{database}"
        self._conn = None
    
    def connect(self):
        import psycopg2
        self._conn = psycopg2.connect(self.conn_string)
    
    def extract(self, query: str) -> Iterator[Dict]:
        with self._conn.cursor() as cur:
            cur.execute(query)
            columns = [d[0] for d in cur.description]
            for row in cur:
                yield dict(zip(columns, row))
    
    def close(self):
        if self._conn:
            self._conn.close()

@ConnectorRegistry.register("s3_parquet")
class S3ParquetConnector(BaseConnector):
    def __init__(self, bucket: str, prefix: str, **kwargs):
        self.bucket = bucket
        self.prefix = prefix
        self._client = None
    
    def connect(self):
        import boto3
        self._client = boto3.client("s3")
    
    def extract(self, query: str) -> Iterator[Dict]:
        import pyarrow.parquet as pq
        # query is used as a filter expression
        dataset = pq.ParquetDataset(f"s3://{self.bucket}/{self.prefix}")
        for batch in dataset.to_batches():
            for record in batch.to_pylist():
                yield record
    
    def close(self):
        pass

# Usage from config file
config = {
    "source": {"type": "postgres", "host": "db.prod", "port": 5432, "database": "app"},
    "destination": {"type": "s3_parquet", "bucket": "lake", "prefix": "raw/users/"}
}

with ConnectorRegistry.create(**config["source"]) as source:
    records = source.extract("SELECT * FROM users WHERE active = true")
```

---

## Pattern 2: Pipeline Builder (Method Chaining)

Fluent interface for constructing complex data pipelines:

```python
"""
Pipeline builder with method chaining.
Enables declarative pipeline construction with validation.
"""
from typing import Callable, List, Dict, Any, Optional
from dataclasses import dataclass, field

@dataclass
class PipelineStep:
    name: str
    func: Callable
    config: Dict[str, Any] = field(default_factory=dict)
    depends_on: List[str] = field(default_factory=list)

class PipelineBuilder:
    """
    Fluent builder for data pipelines.
    
    Analogy: Like building with LEGO — snap pieces together in any order,
    but validate the final structure before running.
    """
    
    def __init__(self, name: str):
        self._name = name
        self._steps: List[PipelineStep] = []
        self._config: Dict[str, Any] = {}
        self._error_handler: Optional[Callable] = None
        self._validators: List[Callable] = []
    
    def extract_from(self, source_type: str, **kwargs) -> "PipelineBuilder":
        """Add an extraction step."""
        self._steps.append(PipelineStep(
            name=f"extract_{source_type}",
            func=lambda cfg: ConnectorRegistry.create(source_type, **kwargs).extract(cfg.get("query", "")),
            config=kwargs
        ))
        return self
    
    def transform(self, name: str, func: Callable) -> "PipelineBuilder":
        """Add a transformation step."""
        self._steps.append(PipelineStep(
            name=f"transform_{name}",
            func=func,
            depends_on=[self._steps[-1].name] if self._steps else []
        ))
        return self
    
    def validate(self, validator: Callable) -> "PipelineBuilder":
        """Add a data quality validation."""
        self._validators.append(validator)
        return self
    
    def load_to(self, target_type: str, **kwargs) -> "PipelineBuilder":
        """Add a load step."""
        self._steps.append(PipelineStep(
            name=f"load_{target_type}",
            func=lambda data: load_data(target_type, data, **kwargs),
            config=kwargs,
            depends_on=[self._steps[-1].name] if self._steps else []
        ))
        return self
    
    def on_error(self, handler: Callable) -> "PipelineBuilder":
        """Set error handling strategy."""
        self._error_handler = handler
        return self
    
    def with_config(self, **kwargs) -> "PipelineBuilder":
        """Add runtime configuration."""
        self._config.update(kwargs)
        return self
    
    def build(self) -> "Pipeline":
        """Validate and build the final pipeline."""
        if not self._steps:
            raise ValueError("Pipeline must have at least one step")
        
        # Validate DAG structure
        self._validate_dependencies()
        
        return Pipeline(
            name=self._name,
            steps=self._steps,
            config=self._config,
            error_handler=self._error_handler,
            validators=self._validators
        )
    
    def _validate_dependencies(self):
        step_names = {s.name for s in self._steps}
        for step in self._steps:
            for dep in step.depends_on:
                if dep not in step_names:
                    raise ValueError(f"Step '{step.name}' depends on unknown step '{dep}'")

# Usage — declarative pipeline construction
pipeline = (
    PipelineBuilder("daily_user_sync")
    .extract_from("postgres", host="db.prod", port=5432, database="app")
    .transform("deduplicate", deduplicate_by_key("user_id"))
    .transform("normalize", normalize_emails)
    .validate(lambda df: assert_no_nulls(df, ["user_id", "email"]))
    .load_to("s3_parquet", bucket="lake", prefix="curated/users/")
    .on_error(notify_slack_channel)
    .with_config(batch_size=50000, parallelism=4)
    .build()
)

pipeline.run()
```

---

## Pattern 3: Configuration Hierarchy

Layered configuration with environment overrides:

```python
"""
Hierarchical configuration system for data pipelines.
Layers: defaults < config file < environment variables < runtime overrides
"""
from dataclasses import dataclass, field, fields
from typing import Any, Optional, Dict
import os
import json

class ConfigLayer:
    """Single layer in the config hierarchy."""
    
    def __init__(self, name: str, values: Dict[str, Any]):
        self.name = name
        self._values = values
    
    def get(self, key: str, default=None):
        return self._values.get(key, default)
    
    def has(self, key: str) -> bool:
        return key in self._values

class HierarchicalConfig:
    """
    Configuration with layered overrides.
    Later layers override earlier layers.
    
    Analogy: Like CSS specificity — inline styles override class styles,
    which override element styles. Each layer can override the previous.
    """
    
    def __init__(self):
        self._layers: list[ConfigLayer] = []
    
    def add_layer(self, name: str, values: Dict[str, Any]) -> "HierarchicalConfig":
        self._layers.append(ConfigLayer(name, values))
        return self
    
    def get(self, key: str, default=None) -> Any:
        """Get value from highest-priority layer that has it."""
        for layer in reversed(self._layers):
            if layer.has(key):
                return layer.get(key)
        return default
    
    def get_all(self) -> Dict[str, Any]:
        """Merge all layers into a flat dict."""
        merged = {}
        for layer in self._layers:
            merged.update(layer._values)
        return merged
    
    def explain(self, key: str) -> str:
        """Show which layer provides a value (for debugging)."""
        for layer in reversed(self._layers):
            if layer.has(key):
                return f"{key}={layer.get(key)!r} (from {layer.name})"
        return f"{key} not found in any layer"

    @classmethod
    def from_environment(cls, prefix: str = "PIPELINE_") -> "HierarchicalConfig":
        """Build config from standard sources."""
        config = cls()
        
        # Layer 1: Defaults
        config.add_layer("defaults", {
            "batch_size": 10000,
            "max_retries": 3,
            "log_level": "INFO",
            "parallelism": 4,
        })
        
        # Layer 2: Config file
        config_file = os.environ.get(f"{prefix}CONFIG_FILE", "pipeline_config.json")
        if os.path.exists(config_file):
            with open(config_file) as f:
                config.add_layer("config_file", json.load(f))
        
        # Layer 3: Environment variables
        env_values = {
            k[len(prefix):].lower(): v
            for k, v in os.environ.items()
            if k.startswith(prefix)
        }
        config.add_layer("environment", env_values)
        
        return config

# Usage
config = HierarchicalConfig.from_environment()
config.add_layer("runtime", {"batch_size": 50000})  # Override for this run

batch_size = config.get("batch_size")  # 50000 (from runtime layer)
print(config.explain("batch_size"))    # batch_size=50000 (from runtime)
```

---

## Pattern 4: Event-Driven ETL Framework

An OOP framework for building event-driven data pipelines:

```python
"""
Event-driven ETL framework using Observer + Command patterns.
Pipeline steps emit events that trigger downstream actions.
"""
from abc import ABC, abstractmethod
from typing import Callable, Dict, List, Any
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import logging

logger = logging.getLogger(__name__)

class EventType(Enum):
    STEP_STARTED = "step_started"
    STEP_COMPLETED = "step_completed"
    STEP_FAILED = "step_failed"
    DATA_VALIDATED = "data_validated"
    DATA_QUALITY_ALERT = "data_quality_alert"
    PIPELINE_COMPLETED = "pipeline_completed"

@dataclass
class PipelineEvent:
    event_type: EventType
    source: str
    timestamp: datetime = field(default_factory=datetime.utcnow)
    payload: Dict[str, Any] = field(default_factory=dict)

class EventBus:
    """Central event dispatcher."""
    
    def __init__(self):
        self._handlers: Dict[EventType, List[Callable]] = {}
    
    def on(self, event_type: EventType, handler: Callable):
        self._handlers.setdefault(event_type, []).append(handler)
        return self
    
    def emit(self, event: PipelineEvent):
        for handler in self._handlers.get(event.event_type, []):
            try:
                handler(event)
            except Exception as e:
                logger.error(f"Event handler failed: {e}")

class ETLStep(ABC):
    """Base class for pipeline steps."""
    
    def __init__(self, name: str, event_bus: EventBus):
        self.name = name
        self._bus = event_bus
    
    @abstractmethod
    def execute(self, data: Any) -> Any:
        ...
    
    def run(self, data: Any) -> Any:
        self._bus.emit(PipelineEvent(EventType.STEP_STARTED, self.name))
        try:
            result = self.execute(data)
            self._bus.emit(PipelineEvent(
                EventType.STEP_COMPLETED, self.name,
                payload={"record_count": len(result) if hasattr(result, '__len__') else 0}
            ))
            return result
        except Exception as e:
            self._bus.emit(PipelineEvent(
                EventType.STEP_FAILED, self.name,
                payload={"error": str(e)}
            ))
            raise

class ExtractStep(ETLStep):
    def __init__(self, name, event_bus, connector):
        super().__init__(name, event_bus)
        self.connector = connector
    
    def execute(self, data):
        with self.connector as conn:
            return list(conn.extract(data))

class TransformStep(ETLStep):
    def __init__(self, name, event_bus, transform_func):
        super().__init__(name, event_bus)
        self.transform_func = transform_func
    
    def execute(self, data):
        return self.transform_func(data)

class ValidateStep(ETLStep):
    def __init__(self, name, event_bus, rules):
        super().__init__(name, event_bus)
        self.rules = rules
    
    def execute(self, data):
        failures = []
        for rule in self.rules:
            if not rule.check(data):
                failures.append(rule.name)
        
        if failures:
            self._bus.emit(PipelineEvent(
                EventType.DATA_QUALITY_ALERT, self.name,
                payload={"failed_rules": failures}
            ))
        return data

# Wire it all together
bus = EventBus()
bus.on(EventType.STEP_FAILED, lambda e: send_alert(e.payload["error"]))
bus.on(EventType.DATA_QUALITY_ALERT, lambda e: log_dq_failure(e.payload))
bus.on(EventType.PIPELINE_COMPLETED, lambda e: update_dashboard(e))

# Build pipeline from steps
steps = [
    ExtractStep("extract_users", bus, PostgresConnector(host="db.prod")),
    TransformStep("normalize", bus, normalize_user_records),
    ValidateStep("quality_check", bus, [not_null_rule, email_format_rule]),
    TransformStep("enrich", bus, add_geo_data),
]

# Execute
data = "SELECT * FROM users WHERE updated_at > '2024-01-15'"
for step in steps:
    data = step.run(data)

bus.emit(PipelineEvent(EventType.PIPELINE_COMPLETED, "user_pipeline"))
```

---

## Interview Tips

> **Tip 1:** The plugin registry pattern is the most interview-relevant OOP pattern for DE roles. It demonstrates: decorator usage, class-level state, factory pattern, and extensibility. Frame it as "adding a new data source requires ONE new class file — zero changes to existing code." This satisfies the Open/Closed Principle.

> **Tip 2:** Method chaining (builder pattern) shows you think about API design. When asked "how would you make your pipeline easy to use?", show the fluent builder interface. Explain that each method returns `self` to enable chaining, and `build()` validates the configuration before execution. This separates construction from execution.

> **Tip 3:** For the configuration hierarchy pattern, connect it to real operational pain points: "In production, I need config to come from defaults for safety, config files for standard overrides, environment variables for deployment-specific values, and runtime parameters for ad-hoc runs. A layered system with clear precedence prevents config bugs and makes debugging straightforward."
