---
title: "Python Regular Expressions - Real-World Production Examples"
topic: python
subtopic: regex
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [python, regex, production, log-parsing, data-cleaning]
---

# Python Regular Expressions — Real-World Production Examples

## Pattern 1: Log File Parser

A production-grade log parser that extracts timestamp, level, service, and message from multiple log formats.

```python
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Iterator

@dataclass
class LogEntry:
    timestamp: datetime
    level: str
    service: str
    message: str
    raw: str

class MultiFormatLogParser:
    """Parse logs from multiple services with different formats."""
    
    FORMATS = {
        # Format: "2024-01-15T10:30:45.123Z ERROR [auth-service] Login failed"
        "iso_bracket": re.compile(
            r'(?P<ts>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[\d.]*Z?)\s+'
            r'(?P<level>DEBUG|INFO|WARN(?:ING)?|ERROR|CRITICAL|FATAL)\s+'
            r'\[(?P<service>[^\]]+)\]\s+'
            r'(?P<message>.+)'
        ),
        # Format: "Jan 15 10:30:45 auth-service ERROR: Login failed"
        "syslog": re.compile(
            r'(?P<ts>\w{3}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2})\s+'
            r'(?P<service>[\w-]+)\s+'
            r'(?P<level>\w+):\s+'
            r'(?P<message>.+)'
        ),
        # Format: "[ERROR] 2024-01-15 10:30:45 auth-service - Login failed"
        "bracketed_level": re.compile(
            r'\[(?P<level>\w+)\]\s+'
            r'(?P<ts>\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})\s+'
            r'(?P<service>[\w-]+)\s+-\s+'
            r'(?P<message>.+)'
        ),
    }
    
    TS_FORMATS = [
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d %H:%M:%S",
        "%b %d %H:%M:%S",
    ]
    
    def parse_line(self, line: str) -> LogEntry | None:
        for format_name, pattern in self.FORMATS.items():
            match = pattern.match(line.strip())
            if match:
                data = match.groupdict()
                return LogEntry(
                    timestamp=self._parse_timestamp(data["ts"]),
                    level=data["level"].upper(),
                    service=data["service"],
                    message=data["message"],
                    raw=line.strip()
                )
        return None
    
    def parse_file(self, filepath: str) -> Iterator[LogEntry]:
        unparsed_count = 0
        with open(filepath) as f:
            for line in f:
                entry = self.parse_line(line)
                if entry:
                    yield entry
                else:
                    unparsed_count += 1
        if unparsed_count > 0:
            print(f"Warning: {unparsed_count} lines could not be parsed")
    
    def _parse_timestamp(self, ts_str: str) -> datetime:
        for fmt in self.TS_FORMATS:
            try:
                return datetime.strptime(ts_str, fmt)
            except ValueError:
                continue
        return datetime.min  # Fallback for unparseable timestamps

# Usage
parser = MultiFormatLogParser()
errors = [
    entry for entry in parser.parse_file("application.log")
    if entry.level in ("ERROR", "CRITICAL", "FATAL")
]
```

---

## Pattern 2: Data Cleaning (Standardize Phone Numbers and Addresses)

```python
import re

class PhoneNormalizer:
    """Normalize phone numbers from various formats to E.164."""
    
    # Match common US phone formats
    PHONE_PATTERNS = re.compile(r'''
        (?:(?:\+?1)[-.\s]?)?      # Optional country code (+1, 1)
        (?:\(?(\d{3})\)?[-.\s]?)  # Area code (with or without parens)
        (\d{3})[-.\s]?            # Exchange
        (\d{4})                   # Number
    ''', re.VERBOSE)
    
    def normalize(self, raw: str) -> str | None:
        """Convert any phone format to +1XXXXXXXXXX."""
        # Strip non-phone characters
        cleaned = re.sub(r'[^\d+()\s.-]', '', raw.strip())
        match = self.PHONE_PATTERNS.search(cleaned)
        if match:
            area, exchange, number = match.groups()
            return f"+1{area}{exchange}{number}"
        return None

class AddressCleaner:
    """Standardize US address components."""
    
    DIRECTION_MAP = {
        r'\bN\.?\b': 'N', r'\bNorth\b': 'N',
        r'\bS\.?\b': 'S', r'\bSouth\b': 'S',
        r'\bE\.?\b': 'E', r'\bEast\b': 'E',
        r'\bW\.?\b': 'W', r'\bWest\b': 'W',
    }
    
    SUFFIX_MAP = {
        r'\bSt\.?\b': 'St', r'\bStreet\b': 'St',
        r'\bAve\.?\b': 'Ave', r'\bAvenue\b': 'Ave',
        r'\bBlvd\.?\b': 'Blvd', r'\bBoulevard\b': 'Blvd',
        r'\bDr\.?\b': 'Dr', r'\bDrive\b': 'Dr',
        r'\bLn\.?\b': 'Ln', r'\bLane\b': 'Ln',
        r'\bRd\.?\b': 'Rd', r'\bRoad\b': 'Rd',
    }
    
    APT_PATTERN = re.compile(
        r'\b(?:apt|apartment|suite|ste|unit|#)\s*\.?\s*(\w+)',
        re.IGNORECASE
    )
    
    def standardize(self, address: str) -> str:
        result = address.strip()
        
        # Normalize directions
        for pattern, replacement in self.DIRECTION_MAP.items():
            result = re.sub(pattern, replacement, result, flags=re.IGNORECASE)
        
        # Normalize suffixes
        for pattern, replacement in self.SUFFIX_MAP.items():
            result = re.sub(pattern, replacement, result, flags=re.IGNORECASE)
        
        # Normalize apartment/unit notation
        apt_match = self.APT_PATTERN.search(result)
        if apt_match:
            result = self.APT_PATTERN.sub(f'Unit {apt_match.group(1)}', result)
        
        # Collapse multiple spaces
        result = re.sub(r'\s+', ' ', result)
        return result

# Usage
phone_norm = PhoneNormalizer()
samples = ["(555) 123-4567", "555.123.4567", "+1-555-123-4567", "5551234567"]
for s in samples:
    print(f"{s:20} → {phone_norm.normalize(s)}")
# All normalize to: +15551234567

addr_cleaner = AddressCleaner()
print(addr_cleaner.standardize("123 North Main Street Apt. 4B"))
# "123 N Main St Unit 4B"
```

---

## Pattern 3: PySpark regexp_extract for Column Extraction

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    regexp_extract, regexp_replace, col, when, trim
)

spark = SparkSession.builder.appName("regex_extraction").getOrCreate()

# Raw data: user-agent strings from web logs
df = spark.createDataFrame([
    ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0",),
    ("Mozilla/5.0 (iPhone; CPU iPhone OS 17_0) Safari/605.1",),
    ("Python-urllib/3.11",),
    ("Apache-HttpClient/4.5.13 (Java/11.0.12)",),
], ["user_agent"])

# Extract OS, browser, version using regexp_extract
parsed = df.select(
    col("user_agent"),
    regexp_extract("user_agent", r'\((.*?)\)', 1).alias("platform_info"),
    regexp_extract("user_agent", r'(Chrome|Safari|Firefox|Python-urllib|Apache-HttpClient)[/\s]*([\d.]+)', 1).alias("client"),
    regexp_extract("user_agent", r'(Chrome|Safari|Firefox|Python-urllib|Apache-HttpClient)[/\s]*([\d.]+)', 2).alias("version"),
)

# Classify traffic type
classified = parsed.withColumn(
    "traffic_type",
    when(col("client").rlike("Python|Apache|HttpClient"), "bot")
    .otherwise("human")
)

# Extract and clean S3 paths from a config column
config_df = spark.createDataFrame([
    ("input_path=s3://bucket/raw/dt=2024-01-15/ output=s3://bucket/curated/",),
], ["config"])

paths = config_df.select(
    regexp_extract("config", r'input_path=(s3://[^\s]+)', 1).alias("input_path"),
    regexp_extract("config", r'output=(s3://[^\s]+)', 1).alias("output_path"),
)

# Bulk data masking with regexp_replace
masked = df.withColumn(
    "masked_ua",
    regexp_replace("user_agent", r'\d+\.\d+\.\d+\.\d+', '[VERSION]')
)
```

---

## Pattern 4: Configurable Regex-Based Data Quality Validator

```python
import re
from dataclasses import dataclass
from typing import Any

@dataclass
class ValidationRule:
    field: str
    pattern: str
    description: str
    severity: str = "error"  # "error" or "warning"
    allow_null: bool = False

class RegexValidator:
    """Configurable regex-based validation for data quality."""
    
    def __init__(self, rules: list[ValidationRule]):
        self.rules = rules
        self._compiled = {
            rule.field: re.compile(rule.pattern) for rule in rules
        }
    
    def validate_record(self, record: dict[str, Any]) -> list[dict]:
        """Validate a single record, return list of violations."""
        violations = []
        
        for rule in self.rules:
            value = record.get(rule.field)
            
            if value is None or value == "":
                if not rule.allow_null:
                    violations.append({
                        "field": rule.field,
                        "value": value,
                        "rule": rule.description,
                        "severity": rule.severity,
                        "issue": "null_or_empty"
                    })
                continue
            
            compiled = self._compiled[rule.field]
            if not compiled.fullmatch(str(value)):
                violations.append({
                    "field": rule.field,
                    "value": str(value)[:50],
                    "rule": rule.description,
                    "severity": rule.severity,
                    "issue": "pattern_mismatch"
                })
        
        return violations
    
    def validate_batch(self, records: list[dict]) -> dict:
        """Validate a batch and return summary statistics."""
        all_violations = []
        error_count = 0
        warning_count = 0
        
        for i, record in enumerate(records):
            violations = self.validate_record(record)
            for v in violations:
                v["record_index"] = i
                if v["severity"] == "error":
                    error_count += 1
                else:
                    warning_count += 1
            all_violations.extend(violations)
        
        return {
            "total_records": len(records),
            "valid_records": len(records) - len(set(
                v["record_index"] for v in all_violations if v["severity"] == "error"
            )),
            "error_count": error_count,
            "warning_count": warning_count,
            "violations": all_violations[:100],  # Limit for reporting
        }

# Configuration — can be loaded from YAML/JSON
rules = [
    ValidationRule("email", r'[\w.+-]+@[\w-]+\.[\w.-]+', "Valid email format"),
    ValidationRule("phone", r'\+1\d{10}', "E.164 US phone", allow_null=True),
    ValidationRule("date", r'\d{4}-(?:0[1-9]|1[0-2])-(?:0[1-9]|[12]\d|3[01])', "ISO date"),
    ValidationRule("amount", r'\d+\.?\d{0,2}', "Numeric, max 2 decimal places"),
    ValidationRule("country_code", r'[A-Z]{2}', "ISO 3166-1 alpha-2", severity="warning"),
]

validator = RegexValidator(rules)
result = validator.validate_batch(records)
print(f"Valid: {result['valid_records']}/{result['total_records']}")
print(f"Errors: {result['error_count']}, Warnings: {result['warning_count']}")
```

---

## Production Tips

| Concern | Solution |
|---------|----------|
| Performance | Compile patterns once, reuse. Use specific char classes over `.` |
| Maintainability | Named groups, VERBOSE flag, pattern config files |
| Robustness | Handle unmatched lines gracefully. Log parse failures |
| Testing | Test patterns against edge cases: empty strings, unicode, very long lines |
| Monitoring | Track parse success rate. Alert if failure rate exceeds threshold |

---

## Interview Tips

> **Tip 1:** "How would you parse logs from multiple services with different formats?" — "Build a multi-format parser with a list of compiled regex patterns. Try each pattern in order (most common first for performance). Use named groups so the output is the same regardless of which format matched. Track which patterns fail frequently and monitor parse success rate as a data quality metric."

> **Tip 2:** "How do you use regex for data quality validation?" — "Define validation rules as regex patterns per field (configurable via YAML). Run fullmatch() against each field value. Separate errors from warnings. Return structured violation reports with field, value, and which rule failed. This pattern works well for format validation (emails, phones, dates) but not for business logic or cross-field rules."

> **Tip 3:** "How does regex work in PySpark vs Python re?" — "PySpark's `regexp_extract` and `regexp_replace` execute on the JVM using Java regex. The syntax is mostly identical but there are edge cases: Java requires double-escaping in some contexts, and doesn't support Python-specific syntax like `(?P<name>...)`. Always test patterns on a sample DataFrame. For complex extraction, consider a UDF with Python re, but be aware of the serialization overhead."
