---
title: "Python Regular Expressions - Fundamentals"
topic: python
subtopic: regex
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [python, regex, re-module, pattern-matching, text-processing]
---

# Python Regular Expressions — Fundamentals

## The re Module Basics

Python's `re` module provides regex operations for pattern matching and text manipulation.

```python
import re

text = "Order #12345 placed on 2024-01-15 by user@example.com"

# re.search() — find first match anywhere in string
match = re.search(r'#(\d+)', text)
if match:
    print(match.group(0))  # '#12345' (full match)
    print(match.group(1))  # '12345' (first capture group)

# re.match() — match at START of string only
match = re.match(r'Order', text)       # Match (starts with "Order")
match = re.match(r'placed', text)      # None (not at start)

# re.findall() — find ALL non-overlapping matches
numbers = re.findall(r'\d+', text)     # ['12345', '2024', '01', '15']

# re.sub() — find and replace
cleaned = re.sub(r'\d{4}-\d{2}-\d{2}', '[DATE]', text)
# "Order #12345 placed on [DATE] by user@example.com"
```

---

## Raw Strings

Always use raw strings (`r'...'`) for regex patterns to avoid backslash confusion.

```python
# WITHOUT raw string — backslash hell
pattern = '\\d+\\.\\d+'   # Matches decimal number

# WITH raw string — clean and readable
pattern = r'\d+\.\d+'     # Same pattern, much clearer

# Why? In regular strings, \d is not a recognized escape so Python
# passes it through. But \n becomes newline, \t becomes tab, etc.
# Raw strings treat ALL backslashes literally.
```

---

## Character Classes

| Pattern | Matches | Example |
|---------|---------|---------|
| `\d` | Any digit (0-9) | `\d+` matches "123" |
| `\D` | Any non-digit | `\D+` matches "abc" |
| `\w` | Word char (a-z, A-Z, 0-9, _) | `\w+` matches "user_1" |
| `\W` | Non-word character | `\W` matches "@" |
| `\s` | Whitespace (space, tab, newline) | `\s+` matches "  " |
| `\S` | Non-whitespace | `\S+` matches "hello" |
| `.` | Any char except newline | `a.b` matches "axb" |
| `[abc]` | Any of a, b, or c | `[aeiou]` matches vowels |
| `[^abc]` | NOT a, b, or c | `[^0-9]` matches non-digits |
| `[a-z]` | Range: a through z | `[A-Za-z]` matches letters |

```python
import re

log_line = "2024-01-15 10:30:45 ERROR [main] Connection refused"

# Extract timestamp
timestamp = re.search(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', log_line)
print(timestamp.group())  # "2024-01-15 10:30:45"

# Extract log level (uppercase word)
level = re.search(r'[A-Z]{2,}', log_line)
print(level.group())  # "ERROR"
```

---

## Quantifiers

| Quantifier | Meaning | Example |
|-----------|---------|---------|
| `*` | 0 or more | `\d*` matches "" or "123" |
| `+` | 1 or more | `\d+` matches "123" (not "") |
| `?` | 0 or 1 (optional) | `colou?r` matches "color" or "colour" |
| `{n}` | Exactly n | `\d{4}` matches "2024" |
| `{n,}` | n or more | `\d{2,}` matches "12" or "123" |
| `{n,m}` | Between n and m | `\d{1,3}` matches "1" to "999" |

```python
# Phone number variants
phone_pattern = r'\d{3}[-.]?\d{3}[-.]?\d{4}'
phones = [
    "555-123-4567",   # matches
    "555.123.4567",   # matches
    "5551234567",     # matches
]
for p in phones:
    print(re.match(phone_pattern, p))  # All match
```

---

## Anchors

| Anchor | Meaning |
|--------|---------|
| `^` | Start of string (or line with MULTILINE) |
| `$` | End of string (or line with MULTILINE) |
| `\b` | Word boundary |

```python
text = "pipeline_v2 is better than pipeline_v1"

# \b word boundary — match whole words only
re.findall(r'\bpipeline_v\d\b', text)  # ['pipeline_v2', 'pipeline_v1']

# Without \b — matches partial words too
re.findall(r'pipeline_v\d', "my_pipeline_v2_final")  # ['pipeline_v2'] — partial

# ^ and $ for full string validation
def is_valid_table_name(name: str) -> bool:
    """Table name: letters, digits, underscores. Must start with letter."""
    return bool(re.match(r'^[a-zA-Z]\w*$', name))

is_valid_table_name("users_daily")    # True
is_valid_table_name("2nd_table")      # False (starts with digit)
is_valid_table_name("users daily")    # False (has space)
```

---

## Common Patterns for Data Engineering

```python
import re

# Email extraction
email_pattern = r'[\w.+-]+@[\w-]+\.[\w.-]+'
emails = re.findall(email_pattern, "Contact bob@company.com or alice+test@data.io")
# ['bob@company.com', 'alice+test@data.io']

# Date extraction (YYYY-MM-DD)
date_pattern = r'\d{4}-(?:0[1-9]|1[0-2])-(?:0[1-9]|[12]\d|3[01])'
dates = re.findall(date_pattern, "From 2024-01-15 to 2024-02-28")
# ['2024-01-15', '2024-02-28']

# S3 path parsing
s3_pattern = r's3://([^/]+)/(.+)'
match = re.match(s3_pattern, "s3://my-bucket/data/year=2024/month=01/file.parquet")
bucket = match.group(1)  # "my-bucket"
key = match.group(2)     # "data/year=2024/month=01/file.parquet"

# Extract key-value pairs from log
kv_pattern = r'(\w+)=([^\s,]+)'
log = "status=200 duration=3.2s rows=5000 source=api"
pairs = dict(re.findall(kv_pattern, log))
# {'status': '200', 'duration': '3.2s', 'rows': '5000', 'source': 'api'}

# IP address (basic)
ip_pattern = r'\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b'
ips = re.findall(ip_pattern, "Request from 192.168.1.100 to 10.0.0.1")
# ['192.168.1.100', '10.0.0.1']
```

---

## Flags

```python
import re

# IGNORECASE — case-insensitive matching
re.findall(r'error', "Error ERROR error", re.IGNORECASE)  # ['Error', 'ERROR', 'error']

# MULTILINE — ^ and $ match line boundaries (not just string boundaries)
log_text = """INFO Starting job
ERROR Connection failed
INFO Retrying
ERROR Timeout"""

errors = re.findall(r'^ERROR .+$', log_text, re.MULTILINE)
# ['ERROR Connection failed', 'ERROR Timeout']

# DOTALL — . matches newline too
multiline_json = '{"key":\n"value"}'
re.search(r'\{.+\}', multiline_json, re.DOTALL)  # Matches entire string

# VERBOSE — add comments and whitespace for readability
phone_pattern = re.compile(r'''
    (\d{3})     # area code
    [-.\s]?     # optional separator
    (\d{3})     # exchange
    [-.\s]?     # optional separator
    (\d{4})     # number
''', re.VERBOSE)
```

---

## Compiled Patterns for Performance

```python
import re

# Compile once, reuse many times — faster for repeated use
TIMESTAMP_RE = re.compile(r'(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})')
EMAIL_RE = re.compile(r'[\w.+-]+@[\w-]+\.[\w.-]+')
S3_PATH_RE = re.compile(r's3://([^/]+)/(.+)')

def parse_log_lines(lines: list[str]) -> list[dict]:
    """Using compiled patterns is ~10-30% faster in loops."""
    results = []
    for line in lines:
        ts_match = TIMESTAMP_RE.search(line)
        if ts_match:
            results.append({
                "date": ts_match.group(1),
                "time": ts_match.group(2),
                "raw": line
            })
    return results

# Compiled patterns also expose the same methods:
TIMESTAMP_RE.search(text)
TIMESTAMP_RE.findall(text)
TIMESTAMP_RE.sub(replacement, text)
```

---

## Quick Reference: re Module Functions

| Function | Returns | Use When |
|----------|---------|----------|
| `re.search(pattern, string)` | First Match or None | Finding something anywhere in text |
| `re.match(pattern, string)` | Match at start or None | Validating string format |
| `re.fullmatch(pattern, string)` | Match if entire string matches | Strict validation |
| `re.findall(pattern, string)` | List of all matches | Extracting all occurrences |
| `re.finditer(pattern, string)` | Iterator of Match objects | Large texts (memory efficient) |
| `re.sub(pattern, repl, string)` | New string with replacements | Find and replace |
| `re.split(pattern, string)` | List of split parts | Split on complex delimiters |
| `re.compile(pattern)` | Compiled pattern object | Reuse pattern many times |

---

## Interview Tips

> **Tip 1:** "When would you use regex in a data pipeline?" — "Three common cases: (1) Parsing semi-structured log files where each line has a known format but isn't delimited cleanly. (2) Data validation — checking if email, phone, or date fields match expected formats. (3) Extraction — pulling structured fields from free-text columns like addresses or error messages. For anything more complex (JSON, XML, HTML), use a proper parser."

> **Tip 2:** "What's the difference between re.match and re.search?" — "match only looks at the start of the string, search scans the entire string for the first occurrence. Use match when validating that a string begins with (or entirely matches) a pattern. Use search when finding something anywhere within a larger text."

> **Tip 3:** "Why compile regex patterns?" — "re.compile() pre-processes the pattern into an internal representation once. When you call re.search() directly, Python re-compiles the pattern every call (though there's a small cache). For patterns used in loops over thousands of records, compiling gives a 10-30% speedup and makes code more readable by naming the pattern."
