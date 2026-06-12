---
title: "Pipeline Testing - Real World"
topic: ci-cd
subtopic: pipeline-testing
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [ci-cd, testing, data-quality, real-world]
---

# Pipeline Testing — Real World

## Case Study: E-Commerce Revenue Pipeline Testing

### The Problem

A mid-size e-commerce company ran a daily revenue pipeline that calculated the previous day's revenue for finance reporting. The pipeline had been running for 2 years with no automated tests. One quarter, the finance team noticed their Q2 revenue numbers were $2.3M lower than the sales team's CRM numbers. Investigation revealed a bug introduced 6 weeks earlier — a `WHERE status = 'complete'` filter had been changed to `WHERE status = 'completed'` in a refactor, silently dropping all orders from the legacy system that used the old status string.

**6 weeks of silent data corruption. 0 automated tests. 1 character typo.**

---

### The Testing Suite They Built

**Phase 1: Retroactive unit tests (Week 1)**
```python
# Caught immediately with a simple parametrize test:
@pytest.mark.parametrize("status,should_include", [
    ("completed", True),
    ("complete", True),    # legacy status string!
    ("COMPLETED", False),  # uppercase invalid
    ("pending", False),
    ("cancelled", False),
])
def test_revenue_status_filter(status, should_include):
    df = pd.DataFrame([{"order_id": 1, "amount": 100.0, "status": status}])
    result = filter_completed_orders(df)
    if should_include:
        assert len(result) == 1
    else:
        assert len(result) == 0
```

The typo would have been caught on the PR that introduced it.

**Phase 2: Schema contract tests (Week 2)**
```python
# Validate the raw source schema never silently changes
SOURCE_SCHEMA = pa.DataFrameSchema({
    "order_id": pa.Column(int, unique=True),
    "amount": pa.Column(float),
    "status": pa.Column(str),
})

def test_source_data_matches_contract(raw_df):
    SOURCE_SCHEMA.validate(raw_df)
    # If source team changes column names → test fails immediately
```

**Phase 3: Reconciliation tests (Week 3)**
```python
# Compare pipeline output against source system count
def test_revenue_reconciles_with_source(pipeline_output, source_system):
    pipeline_total = pipeline_output["total_revenue"].sum()
    source_total = source_system.get_completed_revenue(date="2024-01-01")
    
    tolerance = 0.001  # 0.1% tolerance for rounding
    assert abs(pipeline_total - source_total) / source_total < tolerance, \
        f"Revenue mismatch: pipeline={pipeline_total}, source={source_total}"
```

---

### Results

| Metric | Before | After |
|---|---|---|
| Bugs caught before production | 0% | 94% |
| Time to detect data issue | 6 weeks | < 1 hour |
| CI pipeline duration | 0 min (no tests) | 4.5 minutes |
| Developer confidence to refactor | Low | High |
| Finance trust in data | Damaged | Restored |

---

### Lessons Learned

1. **Test for known valid values, not just type correctness.** Type checks would pass for both `"complete"` and `"completed"`. Enum/isin tests catch semantic errors.

2. **Reconciliation tests are uniquely powerful for pipelines.** Comparing output to source is a whole-system test that no unit test can replace.

3. **Add tests immediately when a bug is found.** Before fixing the bug, write a failing test for it. The fix makes the test pass. The test stays forever.

4. **Coverage > 0% was the immediate priority.** They didn't aim for 100% on day one — they prioritized the revenue calculation path that finance relied on.
