---
name: review-spark-code
description: >
  Review Spark job source code (PySpark, Scala, SQL) for performance
  anti-patterns and correlate with runtime (stages, SQL plan, quantiles).
  Use when the user provides a local file path and you have app metrics.
---

# Review Spark Code

## When to Use

- The user provides the code file path (required) for the Spark job source
- You need to link runtime issues (slow stages, skew, spill, errors) to code
- Reviewing for known Spark anti-patterns
- Checking if a code change could explain a regression

## Instructions

1. **Read the code** from the path the user gave (Read tool).
2. **Map runtime to code**: Use the chosen SQL execution’s physical plan and job/stage ids to identify which transformations correspond to which stages (e.g. Exchange → shuffle stage, SortMergeJoin → join stage). Cite `<file_path>:<line>` for the relevant code.
3. **Scan for anti-patterns** and tie each to metrics when possible:

### Critical

| Pattern | Why it’s bad | Fix |
|--------|---------------|-----|
| `.collect()` on large data | Driver OOM | `.take(n)`, aggregate first, or write to storage |
| `.count()` only for logging | Full job for a number | Remove or use accumulator |
| Repeated reads of same data | Recomputes lineage | `.cache()` / `.persist()` |
| `df.repartition(1)` before write | Single task, no parallelism | `.coalesce()` or sane partition count |
| UDFs in PySpark | Serialization, no Catalyst | Prefer built-in SQL functions |
| `.toPandas()` on large data | Driver memory | `.mapInPandas()` or Spark native |
| Cross joins | Cartesian product | Add condition or filter early |
| No predicate pushdown | Read all then filter | Push filters to source |
| Schema inference on large CSV/JSON | Extra scan | Explicit schema |

### Shuffle-related

- `.groupBy().agg()` on skewed keys → skew and stragglers
- Multiple `.join()` without broadcast hint → sort-merge and shuffle
- `.distinct()` before join → extra shuffle
- `.orderBy()` before write to non-sorted sink → unnecessary global sort

4. **Correlate**: For each finding, link to a stage id, SQL execution id, or metric (e.g. “Stage 4 shuffle write 45 GB aligns with the wide join at file.py:87”).
5. **Output**: Code location (file:line), pattern, runtime correlation (metric/stage/error), suggested rewrite.

## Note

The code file path is required; the agent should not run analysis without it.
