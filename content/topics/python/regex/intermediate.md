---
title: "Python Regular Expressions - Intermediate"
topic: python
subtopic: regex
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [python, regex, groups, lookahead, substitution]
---

# Python Regular Expressions — Intermediate

## Capturing Groups

Parentheses `()` create groups that capture matched text for extraction.

```python
import re

# Basic groups — access with .group(n)
log_line = "2024-01-15 10:30:45 ERROR [auth-service] Login failed for user_42"
pattern = r'(\d{4}-\d{2}-\d{2}) (\d{2}:\d{2}:\d{2}) (\w+) \[([^\]]+)\] (.+)'
match = re.search(pattern, log_line)

if match:
    date = match.group(1)      # "2024-01-15"
    time = match.group(2)      # "10:30:45"
    level = match.group(3)     # "ERROR"
    service = match.group(4)   # "auth-service"
    message = match.group(5)   # "Login failed for user_42"

# findall with groups returns tuples
logs = """2024-01-15 INFO Started
2024-01-15 ERROR Failed
2024-01-16 WARNING Slow"""

results = re.findall(r'(\d{4}-\d{2}-\d{2}) (\w+) (.+)', logs)
# [('2024-01-15', 'INFO', 'Started'), ('2024-01-15', 'ERROR', 'Failed'), ...]
```

---

## Named Groups

Named groups improve readability and make code self-documenting.

```python
import re

# Named groups: (?P<name>pattern)
log_pattern = re.compile(
    r'(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s+'
    r'(?P<level>\w+)\s+'
    r'\[(?P<service>[^\]]+)\]\s+'
    r'(?P<message>.+)'
)

log_line = "2024-01-15 10:30:45 ERROR [payment-svc] Transaction timeout"
match = log_pattern.search(log_line)

if match:
    # Access by name — much clearer than .group(1), .group(2)
    print(match.group('timestamp'))  # "2024-01-15 10:30:45"
    print(match.group('level'))      # "ERROR"
    print(match.group('service'))    # "payment-svc"
    print(match.group('message'))    # "Transaction timeout"
    
    # Convert to dict in one call
    parsed = match.groupdict()
    # {'timestamp': '2024-01-15 10:30:45', 'level': 'ERROR', 
    #  'service': 'payment-svc', 'message': 'Transaction timeout'}

# Named groups in finditer (best for structured extraction)
def parse_log_file(text: str) -> list[dict]:
    pattern = re.compile(
        r'(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s+'
        r'(?P<level>\w+)\s+(?P<msg>.+)'
    )
    return [m.groupdict() for m in pattern.finditer(text)]
```

---

## Backreferences

Reference a previously captured group within the same pattern.

```python
import re

# \1 refers back to first captured group
# Find repeated words (common typo)
text = "The the data was was loaded"
repeated = re.findall(r'\b(\w+)\s+\1\b', text, re.IGNORECASE)
# ['The', 'was']

# Find matching HTML-like tags
xml = "<table>data</table> <row>value</row>"
tags = re.findall(r'<(\w+)>(.+?)</\1>', xml)
# [('table', 'data'), ('row', 'value')]

# Named backreference: (?P=name)
pattern = r'(?P<quote>["\'])(?P<value>.+?)(?P=quote)'
text = '''config = "production" and mode = 'batch' '''
matches = re.findall(pattern, text)
# [('"', 'production'), ("'", 'batch')]
```

---

## Lookahead and Lookbehind

Zero-width assertions that check context without consuming characters.

```python
import re

# Positive lookahead (?=...) — followed by
# Extract amounts that are followed by "USD"
text = "Total: 150 USD, Tax: 12 EUR, Fee: 5 USD"
usd_amounts = re.findall(r'\d+(?=\s*USD)', text)  # ['150', '5']

# Negative lookahead (?!...) — NOT followed by
# Find numbers NOT followed by a unit
text = "Values: 100px, 200, 300em, 400"
plain_numbers = re.findall(r'\b\d+\b(?!\s*(?:px|em|rem))', text)  # ['200', '400']

# Positive lookbehind (?<=...) — preceded by
# Extract values after "amount="
text = "order_id=123 amount=99.50 currency=USD"
amount = re.search(r'(?<=amount=)[\d.]+', text)
print(amount.group())  # "99.50"

# Negative lookbehind (?<!...) — NOT preceded by
# Find "test" not preceded by "unit_"
text = "unit_test, test, integration_test, test_case"
non_unit = re.findall(r'(?<!unit_)\btest\b', text)  # ['test']
```

**Common use case: Extract values from key=value pairs without the key:**
```python
config_text = "host=db.prod.internal port=5432 dbname=analytics"
port = re.search(r'(?<=port=)\d+', config_text).group()  # "5432"
```

---

## Non-Greedy (Lazy) Matching

By default, quantifiers are greedy (match as much as possible). Add `?` to make them lazy.

```python
import re

html = '<div class="container"><span>Hello</span></div>'

# GREEDY (default) — matches too much
re.findall(r'<.+>', html)
# ['<div class="container"><span>Hello</span></div>']  — one big match

# NON-GREEDY — matches as little as possible
re.findall(r'<.+?>', html)
# ['<div class="container">', '<span>', '</span>', '</div>']

# Practical example: extract quoted strings
text = 'name="Alice" age="30" city="NYC"'
# Greedy: r'"(.+)"'  → ['Alice" age="30" city="NYC']  WRONG
# Lazy:   r'"(.+?)"' → ['Alice', '30', 'NYC']         CORRECT
values = re.findall(r'"(.+?)"', text)
```

---

## re.sub with Functions

Pass a function to `re.sub` for dynamic replacements.

```python
import re

# Replace with a function — receives Match object
def mask_pii(match: re.Match) -> str:
    """Replace email username with masked version."""
    username = match.group(1)
    domain = match.group(2)
    return f"{username[0]}***@{domain}"

text = "Users: alice@company.com, bob.smith@data.io"
masked = re.sub(r'([\w.]+)@([\w.]+)', mask_pii, text)
# "Users: a***@company.com, b***@data.io"

# Dynamic replacement: increment version numbers
def increment_version(match: re.Match) -> str:
    major, minor, patch = match.groups()
    return f"v{major}.{minor}.{int(patch) + 1}"

changelog = "Released v2.3.14 and v1.8.5"
updated = re.sub(r'v(\d+)\.(\d+)\.(\d+)', increment_version, changelog)
# "Released v2.3.15 and v1.8.6"

# Template expansion
template = "Hello {name}, your order #{order_id} is ready"
data = {"name": "Alice", "order_id": "12345"}
result = re.sub(r'\{(\w+)\}', lambda m: data.get(m.group(1), m.group(0)), template)
# "Hello Alice, your order #12345 is ready"
```

---

## re.split — Split on Complex Delimiters

```python
import re

# Split on multiple delimiters
text = "field1,field2;field3|field4"
fields = re.split(r'[,;|]', text)  # ['field1', 'field2', 'field3', 'field4']

# Split preserving the delimiter (use capturing group)
log = "INFO: started | WARNING: slow | ERROR: failed"
parts = re.split(r'(\s*\|\s*)', log)
# ['INFO: started', ' | ', 'WARNING: slow', ' | ', 'ERROR: failed']

# Split CSV with quoted fields (simplified — use csv module for production)
line = 'Alice,"New York, NY",30,"Data Engineer"'
fields = re.split(r',(?=(?:[^"]*"[^"]*")*[^"]*$)', line)
# ['Alice', '"New York, NY"', '30', '"Data Engineer"']

# Limit splits
text = "key=value1=value2=value3"
parts = re.split(r'=', text, maxsplit=1)  # ['key', 'value1=value2=value3']
```

---

## finditer for Large Texts

`finditer` returns an iterator — memory-efficient for large files.

```python
import re
from typing import Iterator

def extract_errors_from_log(filepath: str) -> Iterator[dict]:
    """Parse large log file without loading all matches into memory."""
    error_pattern = re.compile(
        r'(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s+'
        r'ERROR\s+'
        r'\[(?P<service>[^\]]+)\]\s+'
        r'(?P<message>.+)'
    )
    
    with open(filepath) as f:
        for line in f:
            match = error_pattern.search(line)
            if match:
                yield match.groupdict()

# Process errors one at a time (constant memory)
for error in extract_errors_from_log("application.log"):
    print(f"{error['timestamp']} - {error['service']}: {error['message']}")
```

---

## Common DE Patterns

### Log Parsing

```python
# Apache/Nginx access log
access_pattern = re.compile(
    r'(?P<ip>\S+)\s+\S+\s+\S+\s+'
    r'\[(?P<time>[^\]]+)\]\s+'
    r'"(?P<method>\w+)\s+(?P<path>\S+)\s+\S+"\s+'
    r'(?P<status>\d+)\s+(?P<bytes>\d+)'
)

line = '192.168.1.1 - - [15/Jan/2024:10:30:45 +0000] "GET /api/users HTTP/1.1" 200 1234'
parsed = access_pattern.search(line).groupdict()
# {'ip': '192.168.1.1', 'time': '15/Jan/2024:10:30:45 +0000',
#  'method': 'GET', 'path': '/api/users', 'status': '200', 'bytes': '1234'}
```

### CSV Field Extraction with Escaped Quotes

```python
# Extract fields that may contain commas within quotes
field_pattern = re.compile(r'(?:"([^"]*(?:""[^"]*)*)"|([^,]*))')

line = 'Alice,"New York, NY","She said ""hello""",30'
fields = [g1 if g1 is not None else g2 
          for g1, g2 in field_pattern.findall(line)]
```

---

## Interview Tips

> **Tip 1:** "How would you parse a semi-structured log file?" — "Use named groups with re.compile for readability and performance. Iterate with finditer for memory efficiency. Example: `(?P<timestamp>...) (?P<level>...) (?P<message>...)`. Convert matches to dicts with `.groupdict()`. For production, add error handling for lines that don't match the expected format."

> **Tip 2:** "What's the difference between greedy and non-greedy matching?" — "Greedy (`+`, `*`) matches as much as possible. Non-greedy (`+?`, `*?`) matches as little as possible. Example: given `<b>hello</b><b>world</b>`, greedy `<.+>` matches the entire string (one match), while non-greedy `<.+?>` matches each tag separately (four matches). Default to non-greedy when extracting content between delimiters."

> **Tip 3:** "When would you use lookahead/lookbehind?" — "When you need to match based on context without including that context in the result. Example: extract numbers followed by 'USD' without including 'USD' in the match (`\d+(?=\s*USD)`). Or extract values after a key without the key (`(?<=amount=)\d+`). They're zero-width — they assert a condition without consuming characters."
