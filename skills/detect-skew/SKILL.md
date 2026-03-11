---
name: detect-skew
description: >
  Detect data skew using task-level quantiles (p01/p50/p99), stage metrics,
  and executor workload. Use when a stage has long-tail tasks or one executor
  is overloaded. Prefer quantiles first, then task list and executors.
---

# Detect Skew

## When to Use

- A stage has a few tasks much slower than the rest (long tail)
- One executor has much higher shuffle read/write or duration than others
- A join or aggregation is unexpectedly slow
- Quantiles from `get_stage_task_summary` or `get_stage_attempt` show p99 >> p50

## Instructions

1. **Use quantiles first**: For the stage in question, call `get_stage_task_summary` (or `get_stage_attempt` with `withSummaries=true`) with `quantiles=0.01,0.5,0.99`. If p99 runtime or p99 shuffle read is much larger than p50, treat as skew/stragglers.
2. **Executor imbalance**: Call `get_all_executors` (or `get_executors`). Compare totalShuffleRead, totalDuration, totalGCTime across executors. Flag if max > 3x median for shuffle or tasks.
3. **Correlate with execution**: If you have a SQL execution, use the plan (join, aggregation, window) to identify the operation causing skew. If you have a job, use job detail to see which stage is the join/aggregation.
4. **Task list only if quantiles show a problem**: Use `get_stage_task_list` with `sortBy=-runtime` and small length to list the worst tasks (executorRunTime, shuffle read, GC, spill).
5. **Classify skew type**:
   - **Join skew**: one join key has disproportionate rows
   - **Aggregation skew**: few group keys have most of the data
   - **Read skew**: input partitions have very different sizes
6. **Suggest fixes** (with evidence from this app):
   - Join skew: salting skewed key, broadcast join if one side is small, AQE skew join
   - Aggregation skew: two-phase aggregation
   - Read skew: repartition input, adjust file/partition sizes
   - Check if `spark.sql.adaptive.skewJoin.enabled` is set

## Thresholds

| Signal | Threshold | Severity |
|--------|-----------|----------|
| p99 task runtime > 5x p50 | Definite skew | High |
| p99 task runtime > 3x p50 | Likely skew | Medium |
| One executor shuffle/duration > 3x median | Executor-level skew | High |
| Single partition / task input >> median | Large partition | Medium |
