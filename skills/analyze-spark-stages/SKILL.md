---
name: analyze-spark-stages
description: >
  Analyze Spark stage metrics to identify bottleneck stages, slow tasks,
  excessive shuffle, spill to disk, and task failures. Use when investigating
  job slowdowns or failures at the stage level.
---

# Analyze Spark Stages

## When to Use

- A job run is slower than expected
- You need to identify which stage is the bottleneck
- Investigating task failures within stages
- Looking for shuffle or spill issues

## Instructions

1. Call `get_spark_stages` with the cluster ID from the job run.
2. For each stage, compute:
   - **Task throughput**: `inputBytes / numTasks` — flag if < 1 MB (small-file problem) or > 2 GB (partition too large)
   - **Shuffle ratio**: `shuffleWriteBytes / inputBytes` — flag if > 5x (shuffle explosion)
   - **Spill indicator**: `diskBytesSpilled > 0` means partitions don't fit in memory
   - **Failure rate**: `numFailedTasks / numTasks` — any value > 0 needs investigation
   - **Compute vs I/O**: compare `executorRunTime` to `executorCpuTime` — large gap means I/O-bound
3. Rank stages by `executorRunTime` descending to find the bottleneck.
4. Cross-reference the bottleneck stage with SQL plan nodes (if SQL queries exist) to understand which logical operation it corresponds to (join, aggregation, scan, etc.).
5. Report findings using the standard format: Summary, Evidence, Root Cause, Suggested Fix.

## Key Metrics Reference

| Metric | What it means |
|---|---|
| `executorRunTime` | Total time executors spent running tasks |
| `executorCpuTime` | CPU time only (excludes I/O wait) |
| `shuffleReadBytes/Records` | Data read from other executors |
| `shuffleWriteBytes/Records` | Data sent to other executors |
| `memoryBytesSpilled` | Data spilled from execution memory |
| `diskBytesSpilled` | Data spilled all the way to disk |
| `peakExecutionMemory` | Max memory used during execution |
