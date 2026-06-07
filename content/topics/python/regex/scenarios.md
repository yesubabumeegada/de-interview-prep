---
title: "Python Regular Expressions - Scenario Questions"
topic: python
subtopic: regex
content_type: scenario_question
tags: [python, regex, interview, scenarios]
---

# Scenario Questions — Python Regular Expressions

<article data-difficulty="junior">

## 🟢 Junior: Extract Email Addresses from Text

**Scenario:** You're building a data pipeline that processes customer feedback forms. The feedback text contains email addresses that need to be extracted and validated. Write a function that extracts all valid email addresses from unstructured text.

**Input examples:**
- "Please contact me at alice.smith@company.com or bob+work@data.io"
- "My emails: user@domain.co.uk, test.user@sub.domain.org"
- "Not an email: @invalid, also-not@@bad.com, no-domain@"

<details>
<summary>💡 Hint</summary>

An email has a local part (letters, digits, dots, plus, hyphens), an @ symbol, and a domain part (letters, digits, hyphens, dots). Use `re.findall()` to get all matches.

</details>

<details>
<summary>✅ Solution</summary>

```python
import re

def extract_emails(text: str) -> list[str]:
    """Extract valid email addresses from unstructured text."""
    # Pattern breakdown:
    # [\w.+-]+   — local part: word chars, dots, plus, hyphens
    # @          — literal @
    # [\w-]+     — domain: word chars, hyphens
    # (?:\.[\w-]+)+ — one or more .tld parts
    email_pattern = re.compile(r'[\w.+-]+@[\w-]+(?:\.[\w-]+)+')
    
    candidates = email_pattern.findall(text)
    
    # Additional validation: filter out edge cases
    valid = []
    for email in candidates:
        # Must have at least one dot in domain
        # Local part can't start/end with dot
        local, domain = email.rsplit('@', 1)
        if (not local.startswith('.')
            and not local.endswith('.')
            and '..' not in local
            and len(domain) >= 3):
            valid.append(email.lower())
    
    return valid

# Test cases
texts = [
    "Contact alice.smith@company.com or bob+work@data.io for info",
    "Multi-domain: user@sub.domain.co.uk",
    "Invalid: @nolocal, nodomain@, double@@at.com, no.at.sign",
]

for text in texts:
    emails = extract_emails(text)
    print(f"Found: {emails}")
# Found: ['alice.smith@company.com', 'bob+work@data.io']
# Found: ['user@sub.domain.co.uk']
# Found: []
```

**Key points:**
- `[\w.+-]+` covers most valid local parts (letters, digits, dots, plus, hyphens)
- `(?:\.[\w-]+)+` requires at least one `.tld` (prevents matching `user@localhost`)
- Non-capturing group `(?:...)` avoids polluting `findall` results
- Post-validation handles edge cases regex alone can't catch

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Parse Semi-Structured Application Log Lines

**Scenario:** Your team's microservices produce logs in this format, but some fields are optional and the format varies slightly between services:

```
2024-01-15T10:30:45.123Z level=ERROR service=payment-api request_id=req_abc123 method=POST path=/api/charge status=500 duration_ms=342 error="Connection timeout to payment gateway"
```

Write a parser that:
1. Handles both quoted and unquoted values
2. Extracts all key-value pairs into a dict
3. Converts numeric values to the appropriate type
4. Works with missing fields (not all lines have all keys)

<details>
<summary>💡 Hint</summary>

The main challenge is handling quoted values (which can contain spaces) vs unquoted values. Use alternation: either match `key="quoted value"` or `key=unquoted_value`.

</details>

<details>
<summary>✅ Solution</summary>

```python
import re
from datetime import datetime
from typing import Any

class KeyValueLogParser:
    """Parse key=value log lines with mixed quoting styles."""
    
    # Match: key="quoted value" OR key=unquoted_value
    KV_PATTERN = re.compile(
        r'(?P<key>\w+)=(?:"(?P<quoted>[^"]*?)"|(?P<unquoted>\S+))'
    )
    
    # ISO timestamp at the start of the line
    TIMESTAMP_PATTERN = re.compile(
        r'^(?P<timestamp>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[\d.]*Z?)'
    )
    
    def parse_line(self, line: str) -> dict[str, Any]:
        """Parse a single log line into a structured dict."""
        result: dict[str, Any] = {}
        
        # Extract leading timestamp (not key=value format)
        ts_match = self.TIMESTAMP_PATTERN.match(line)
        if ts_match:
            result['timestamp'] = self._parse_timestamp(ts_match.group('timestamp'))
        
        # Extract all key=value pairs
        for match in self.KV_PATTERN.finditer(line):
            key = match.group('key')
            # Use quoted value if present, otherwise unquoted
            value = match.group('quoted') or match.group('unquoted')
            result[key] = self._coerce_type(value)
        
        return result
    
    def _coerce_type(self, value: str) -> Any:
        """Auto-detect and convert numeric types."""
        # Integer
        if re.fullmatch(r'-?\d+', value):
            return int(value)
        # Float
        if re.fullmatch(r'-?\d+\.\d+', value):
            return float(value)
        # Boolean
        if value.lower() in ('true', 'false'):
            return value.lower() == 'true'
        return value
    
    def _parse_timestamp(self, ts: str) -> datetime:
        formats = [
            "%Y-%m-%dT%H:%M:%S.%fZ",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%dT%H:%M:%S.%f",
        ]
        for fmt in formats:
            try:
                return datetime.strptime(ts, fmt)
            except ValueError:
                continue
        return ts  # Return raw string if unparseable
    
    def parse_file(self, filepath: str) -> list[dict[str, Any]]:
        """Parse entire log file with error tracking."""
        records = []
        errors = 0
        
        with open(filepath) as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    record = self.parse_line(line)
                    record['_line_number'] = line_num
                    records.append(record)
                except Exception as e:
                    errors += 1
                    if errors <= 10:
                        print(f"Parse error line {line_num}: {e}")
        
        print(f"Parsed {len(records)} lines, {errors} errors")
        return records

# Usage
parser = KeyValueLogParser()

log_line = '2024-01-15T10:30:45.123Z level=ERROR service=payment-api request_id=req_abc123 method=POST path=/api/charge status=500 duration_ms=342 error="Connection timeout to payment gateway"'

result = parser.parse_line(log_line)
print(result)
# {
#   'timestamp': datetime(2024, 1, 15, 10, 30, 45, 123000),
#   'level': 'ERROR',
#   'service': 'payment-api',
#   'request_id': 'req_abc123',
#   'method': 'POST',
#   'path': '/api/charge',
#   'status': 500,           # auto-converted to int
#   'duration_ms': 342,      # auto-converted to int
#   'error': 'Connection timeout to payment gateway'
# }
```

**Design decisions:**
- Alternation pattern handles both quoted and unquoted values
- Type coercion via `fullmatch` avoids partial matches (e.g., "abc123" stays string)
- `finditer` instead of `findall` gives access to named groups
- Error tracking prevents a few bad lines from killing the entire parse

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Build a Configurable Field Extraction Engine

**Scenario:** Your data platform receives logs from 50+ microservices, each with a different format. Your team spends hours writing custom parsers. Design a configurable extraction engine where:
1. Non-engineers can define extraction rules via YAML config
2. Named groups map to output columns
3. Rules can chain (one rule's output feeds another)
4. Performance is sufficient for 100K lines/second
5. It reports extraction coverage metrics

<details>
<summary>💡 Hint</summary>

Design a pipeline: raw text → primary pattern match → secondary enrichment patterns → type coercion → output record. Use compiled patterns, prioritized matching, and a rule chain mechanism.

</details>

<details>
<summary>✅ Solution</summary>

```python
import re
import time
from dataclasses import dataclass, field
from typing import Any, Callable
import yaml

@dataclass
class ExtractionRule:
    name: str
    pattern: str
    priority: int = 0  # Higher = tried first
    source_field: str = "_raw"  # Which field to apply pattern to
    transforms: dict[str, str] = field(default_factory=dict)  # field → transform
    
@dataclass
class ExtractionConfig:
    service: str
    rules: list[ExtractionRule]
    fallback_rule: ExtractionRule | None = None

class FieldExtractionEngine:
    """High-performance configurable regex extraction."""
    
    TRANSFORMS: dict[str, Callable] = {
        "int": lambda v: int(v) if v else None,
        "float": lambda v: float(v) if v else None,
        "lower": lambda v: v.lower() if v else None,
        "upper": lambda v: v.upper() if v else None,
        "strip": lambda v: v.strip() if v else None,
        "bool": lambda v: v.lower() in ('true', '1', 'yes') if v else None,
    }
    
    def __init__(self):
        self._configs: dict[str, ExtractionConfig] = {}
        self._compiled: dict[str, list[tuple[int, re.Pattern, ExtractionRule]]] = {}
        self._metrics: dict[str, dict[str, int]] = {}
    
    def load_config(self, yaml_path: str) -> None:
        """Load extraction rules from YAML config."""
        with open(yaml_path) as f:
            raw = yaml.safe_load(f)
        
        for service_config in raw.get("services", []):
            service = service_config["service"]
            rules = [
                ExtractionRule(**rule) for rule in service_config["rules"]
            ]
            config = ExtractionConfig(service=service, rules=rules)
            self.register_config(config)
    
    def register_config(self, config: ExtractionConfig) -> None:
        """Register and compile extraction rules for a service."""
        self._configs[config.service] = config
        compiled_rules = []
        for rule in config.rules:
            pattern = re.compile(rule.pattern)
            compiled_rules.append((rule.priority, pattern, rule))
        # Sort by priority (highest first)
        compiled_rules.sort(key=lambda x: -x[0])
        self._compiled[config.service] = compiled_rules
        self._metrics[config.service] = {"total": 0, "matched": 0, "unmatched": 0}
    
    def extract(self, service: str, text: str) -> dict[str, Any]:
        """Extract fields from text using service-specific rules."""
        self._metrics[service]["total"] += 1
        result: dict[str, Any] = {"_raw": text}
        matched_any = False
        
        compiled_rules = self._compiled.get(service, [])
        for priority, pattern, rule in compiled_rules:
            source_text = result.get(rule.source_field, text)
            if not isinstance(source_text, str):
                continue
            
            match = pattern.search(source_text)
            if match:
                matched_any = True
                groups = match.groupdict()
                
                for field_name, value in groups.items():
                    if value is not None:
                        transform_name = rule.transforms.get(field_name)
                        if transform_name and transform_name in self.TRANSFORMS:
                            value = self.TRANSFORMS[transform_name](value)
                        result[field_name] = value
        
        if matched_any:
            self._metrics[service]["matched"] += 1
        else:
            self._metrics[service]["unmatched"] += 1
        
        # Remove internal field
        result.pop("_raw", None)
        return result if matched_any else {"_raw": text, "_unmatched": True}
    
    def extract_batch(self, service: str, lines: list[str]) -> list[dict]:
        """Batch extraction with performance tracking."""
        start = time.perf_counter()
        results = [self.extract(service, line) for line in lines]
        duration = time.perf_counter() - start
        
        rate = len(lines) / duration if duration > 0 else 0
        print(f"[{service}] Processed {len(lines)} lines in {duration:.2f}s "
              f"({rate:.0f} lines/sec)")
        return results
    
    def get_coverage_report(self) -> dict[str, dict]:
        """Report extraction success rate per service."""
        report = {}
        for service, metrics in self._metrics.items():
            total = metrics["total"]
            if total == 0:
                continue
            report[service] = {
                "total_lines": total,
                "matched": metrics["matched"],
                "unmatched": metrics["unmatched"],
                "coverage_pct": round(metrics["matched"] / total * 100, 1),
            }
        return report

# YAML config example (what non-engineers would write):
CONFIG_YAML = """
services:
  - service: payment-api
    rules:
      - name: primary_fields
        priority: 10
        pattern: '(?P<timestamp>\\d{4}-\\d{2}-\\d{2}T[\\d:.]+Z)\\s+level=(?P<level>\\w+)\\s+service=(?P<service>[\\w-]+)'
        transforms:
          level: upper
      - name: request_details
        priority: 5
        pattern: 'method=(?P<method>\\w+)\\s+path=(?P<path>\\S+)\\s+status=(?P<status>\\d+)\\s+duration_ms=(?P<duration_ms>\\d+)'
        transforms:
          status: int
          duration_ms: int
      - name: error_message
        priority: 1
        pattern: 'error="(?P<error>[^"]*)"'
"""

# Usage
engine = FieldExtractionEngine()
# In production, load from file: engine.load_config("extraction_rules.yaml")

# Programmatic config for demo
from io import StringIO
config_data = yaml.safe_load(CONFIG_YAML)
for svc in config_data["services"]:
    rules = [ExtractionRule(**r) for r in svc["rules"]]
    engine.register_config(ExtractionConfig(service=svc["service"], rules=rules))

# Extract
lines = [
    '2024-01-15T10:30:45.123Z level=ERROR service=payment-api method=POST path=/api/charge status=500 duration_ms=342 error="Connection timeout"',
    '2024-01-15T10:30:46.000Z level=INFO service=payment-api method=GET path=/api/status status=200 duration_ms=12',
]

results = engine.extract_batch("payment-api", lines)
coverage = engine.get_coverage_report()
print(f"Coverage: {coverage['payment-api']['coverage_pct']}%")
```

**Architecture highlights:**
- Rules are prioritized — most specific patterns tried first
- Rule chaining via `source_field` — one rule's output feeds another
- Compiled patterns for performance (100K+ lines/second achievable)
- Coverage metrics identify services needing new rules
- YAML config enables non-engineers to define extraction logic

</details>

</article>
