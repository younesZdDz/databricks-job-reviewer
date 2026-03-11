---
name: analyze-spark-stages
description: >
  Analyze Spark stage metrics to find bottleneck stages, slow tasks, excessive
  shuffle, spill, and failures. Use after choosing the target SQL execution or
  job; rank stages by duration/shuffle/spill, then use quantiles before task list.
---

# Analyze Spark Stages

## When to Use

- You have identified the target execution (SQL or job) and need to find which stage is the bottleneck
- Investigating task failures or retries within a stage
- Checking for shuffle or spill issues in the heaviest stages

## Workflow (align with agent)

1. **Anchor first**: Work from a chosen SQL execution (`get_sql_execution`) or job (`get_job_detail`). Do not start from the full stage list without this.
2. **Pick candidate stages** from the job’s stages (or from `get_stages` after filtering by job). Rank by:
   - Duration (executor run time)
   - Shuffle read + shuffle write
   - Input bytes
   - Failed/retried attempts
   - Stage after a big Exchange, or join/aggregation stage, or slow write stage
3. **Quantiles before task list**: For each candidate stage call `get_stage_attempt` with `with_summaries=true&quantiles=0.01,0.5,0.99` or `get_stage_task_summary` with the same quantiles. Use p50/p99 to decide:
   - p99 runtime >> p50 → skew or stragglers
   - p99 shuffle read >> p50 → uneven shuffle read
   - p99 GC or peak memory >> p50 → memory pressure
   - p99 fetch wait >> p50 → shuffle/network
4. **Task list only if needed**: If quantiles indicate a problem, call `get_stage_task_list` with `sortBy=-runtime`, `length=50` to get the worst tasks and inspect executorRunTime, shuffle read, GC, spill.
5. **Cross-reference SQL plan**: If you started from SQL, use the physical plan (Exchange, SortMergeJoin, HashAggregate, BatchScan) to label which operation the bad stage corresponds to.
6. **Report**: Summary, Evidence (exact metrics and tool source), Root Cause, Suggested Fix.

## Key Metrics (from stage / task summary / task list)

| Metric | Meaning |
|--------|--------|
| executorRunTime | Total time executors spent on tasks |
| executorCpuTime | CPU time (excludes I/O wait) |
| shuffleReadBytes/Records | Data read from other executors |
| shuffleWriteBytes/Records | Data sent to other executors |
| memoryBytesSpilled | Spill from execution memory |
| diskBytesSpilled | Spill to disk |
| peakExecutionMemory | Max execution memory |
| Quantiles (p01, p50, p99) | Prefer over averages to detect skew |

## Interpretation (from Description.md)

- **High shuffle read/write** → repartitioning, wide joins/aggregations, join strategy, too many columns.
- **High shuffle fetch wait** → network-heavy shuffle, partitioning, skew, tiny partitions.
- **High spill** → working set doesn’t fit memory, partition size, skew.
- **High GC time** → memory pressure, large partitions, cache pressure, object-heavy ops.
- **High scheduler delay** → too many tiny tasks, saturation, executor churn.
- **High peak execution memory** → joins, sorts, aggregations as hotspots.
