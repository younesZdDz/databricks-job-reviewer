---
name: review-spark-code
description: >
  Review Spark job source code (PySpark, Scala, SQL) for performance
  anti-patterns, inefficient transformations, and potential causes of
  runtime issues. Use when the user provides a local file path for the
  job source code and you need to correlate code patterns with runtime behavior.
---

# Review Spark Code

## When to Use

- The user provides a local file path for the Spark job source code
- You need to correlate runtime issues with code
- Reviewing code for known Spark anti-patterns
- Investigating whether a code change caused a regression

## Instructions

1. Read the job source code from the local file path the user provides
   (use the Read tool — the file is in the workspace or on disk).
2. Scan the code for these anti-patterns:

### Critical Anti-Patterns

| Pattern | Why it's bad | Fix |
|---|---|---|
| `.collect()` on large datasets | Pulls all data to driver, causes OOM | Use `.take(n)`, aggregate first, or write to storage |
| `.count()` used only for logging | Triggers a full job just to log a number | Remove or use accumulator |
| Repeated reads of the same data | Recomputes expensive lineage | `.cache()` or `.persist()` the DataFrame |
| `df.repartition(1)` before write | Single partition = single task, no parallelism | Use `.coalesce()` or appropriate partition count |
| UDFs in PySpark | Serialization overhead, no Catalyst optimization | Rewrite with built-in Spark SQL functions |
| `.toPandas()` on large data | Collects to driver memory | Use `.mapInPandas()` or Spark native ops |
| Cross joins (explicit or implicit) | Cartesian product, O(n*m) rows | Add join condition or filter early |
| No predicate pushdown | Reading all data then filtering | Push filters to source (partition pruning) |
| Schema inference on large CSV/JSON | Extra scan pass to infer types | Provide explicit schema |

### Shuffle-Related Patterns

| Pattern | Issue |
|---|---|
| `.groupBy().agg()` on skewed keys | Skew → straggler tasks |
| Multiple `.join()` without broadcast hint | Unnecessary sort-merge joins |
| `.distinct()` before join | May add unnecessary shuffle |
| `.orderBy()` before write to non-sorted sink | Unnecessary global sort |

3. If investigating a regression, ask the user for the previous version of
   the file or a diff, then compare:
   - Changes to transformations, joins, aggregations
   - Added/removed `.cache()`, `.repartition()`, `.coalesce()`
   - New data sources or changed filter predicates
   - Spark config changes in the code

4. Correlate code findings with runtime data:
   - Map slow stages to specific transformations in the code
   - Match OOM errors to `.collect()` or `.toPandas()` calls
   - Connect shuffle explosion to join/groupBy operations

## Output Structure

For each code issue:
- **Code location**: file path, line number, function
- **Pattern**: which anti-pattern was detected
- **Runtime correlation**: which metric/stage/error this relates to
- **Suggested rewrite**: concrete code change with before/after
