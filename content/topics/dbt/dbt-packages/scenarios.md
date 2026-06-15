---
title: "dbt Packages - Scenario Questions"
topic: dbt
subtopic: dbt-packages
content_type: scenario_question
tags: [dbt, packages, scenarios, interview]
---

# dbt Packages — Scenario Questions

<article data-difficulty="junior">

## Scenario: New Package Installation

Your team wants to start using `dbt_utils` for its `surrogate_key` macro. The package is not yet installed. Walk through the full setup process.

**What files need to change? What commands do you run?**

<details>
<summary>✅ Solution</summary>

**Step 1 — Create or update `packages.yml`** at the project root:

```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: 1.1.1
```

**Step 2 — Install the package:**

```bash
dbt deps
```

This downloads the package into `dbt_packages/dbt_utils/`.

**Step 3 — Verify `.gitignore`** contains `dbt_packages/`:

```
dbt_packages/
```

**Step 4 — Use the macro in a model:**

```sql
-- models/marts/fct_orders.sql
select
    {{ dbt_utils.generate_surrogate_key(['order_id', 'line_item_id']) }} as order_line_key,
    order_id,
    line_item_id,
    quantity,
    unit_price
from {{ ref('stg_order_items') }}
```

**Step 5 — Compile to verify:**

```bash
dbt compile --select fct_orders
```

**Key points:**
- Always pin to an exact version in production (`version: 1.1.1`, not `version: [">=1.0.0"]`)
- `dbt deps` must run before `dbt run` in any new environment (local, CI, Docker)
- The `dbt_packages/` directory should never be committed to git
- After dbt 1.7+, commit `package-lock.yml` for reproducible installs

</details>
</article>

---

<article data-difficulty="mid">

## Scenario: Dynamic Pivot with Changing Categories

You're building a `fct_sales_by_channel` model. The `channel` column in `dim_channels` has values like `web`, `mobile`, `in-store`, `partner`. Business stakeholders keep adding new channels. You want the pivot table to automatically include new columns when channels are added, without code changes.

**How do you implement this? What are the tradeoffs?**

<details>
<summary>✅ Solution</summary>

**Implementation using `dbt_utils.get_column_values`:**

```sql
-- models/marts/fct_sales_by_channel.sql
{%- set channels = dbt_utils.get_column_values(
    table=ref('dim_channels'),
    column='channel_name',
    order_by='channel_name'
) -%}

select
    sale_date,
    {% for channel in channels %}
    sum(case when s.channel_name = '{{ channel }}'
             then s.revenue else 0 end)
        as {{ channel | replace('-', '_') | replace(' ', '_') }}_revenue
    {%- if not loop.last -%},{%- endif %}
    {% endfor %}
from {{ ref('fct_sales') }} s
group by sale_date
order by sale_date
```

**Tradeoffs:**

| Aspect | `get_column_values` | Hard-coded list |
|---|---|---|
| Flexibility | Auto-updates with new channels | Requires code change |
| Compile time | Runs extra query at compile time | No extra query |
| First-run risk | Fails if `dim_channels` doesn't exist | Always works |
| Predictability | Schema changes silently | Schema is explicit |

**Mitigating the first-run problem:**

```sql
{%- set channels = dbt_utils.get_column_values(
    table=ref('dim_channels'),
    column='channel_name',
    default=['web', 'mobile', 'in-store']  -- fallback if table doesn't exist
) -%}
```

**CI/CD consideration:** Build `dim_channels` before `fct_sales_by_channel` in your DAG. Using `--defer` against a production state also resolves this.

**When to avoid this pattern:** If the number of columns changes frequently and downstream BI tools are connected, new columns will silently appear in datasets — which can break reports or confuse dashboards. Communicate schema changes with downstream consumers.

</details>
</article>

---

<article data-difficulty="senior">

## Scenario: Package Macro Conflict and Internal Package Strategy

Your organization has:
1. A project using `dbt_utils` (which defines a macro `safe_add`)
2. A new internal package your team built (`company_utils`) that also defines `safe_add` with company-specific null handling

After adding `company_utils` to `packages.yml`, dbt throws errors because two macros share the same name.

**How do you resolve this conflict? Then design a governance strategy to prevent this class of problem in the future.**

<details>
<summary>✅ Solution</summary>

**Immediate Fix — Configure Dispatch:**

```yaml
# dbt_project.yml
dispatch:
  - macro_namespace: dbt_utils
    search_order: ['company_utils', 'dbt_utils']
```

This tells dbt: when resolving macros in the `dbt_utils` namespace, check `company_utils` first. Your `safe_add` wins.

**But this only works if `company_utils` defines the macro inside the `dbt_utils` namespace dispatch pattern:**

```sql
-- In company_utils/macros/safe_add.sql
-- This overrides dbt_utils.safe_add via dispatch
{% macro safe_add(field_list) %}
    {{ return(adapter.dispatch('safe_add', 'dbt_utils')(field_list)) }}
{% endmacro %}

{% macro default__safe_add(field_list) %}
    -- Company override: treat empty string as null too
    coalesce(nullif({{ field_list | join(' + ') }}, ''), 0)
{% endmacro %}
```

**Governance Strategy to Prevent Future Conflicts:**

1. **Namespace all internal macros** — prefix with company name:
   ```
   company__safe_add()  -- not safe_add()
   company__fiscal_quarter()
   ```

2. **Macro registry in documentation** — maintain a doc (or dbt docs page) listing all custom macros with their signatures and purpose.

3. **Pre-merge CI check** — add a script that detects naming conflicts:
   ```bash
   # In CI pipeline
   python scripts/check_macro_conflicts.py \
     --project-macros macros/ \
     --package-macros dbt_packages/
   ```

4. **Package review process** — before adding any new package, an engineer reviews its macro list for collisions with existing project macros.

5. **Dispatch configuration as code** — treat `dispatch` in `dbt_project.yml` as a formal contract. Changes require PR review.

6. **Integration tests for dispatch** — write tests that explicitly call your override macros and assert on the output, so breakages surface in CI not production.

**Architectural recommendation for the long term:** If `company_utils` is large, consider splitting it into:
- `company_utils` — utility macros (safe_add, fiscal_quarter, etc.)  
- `company_tests` — generic test macros  
- `company_sources` — shared staging models per source system

Each package has a clear domain. Versioning is independent. Teams can upgrade selectively.

</details>
</article>
