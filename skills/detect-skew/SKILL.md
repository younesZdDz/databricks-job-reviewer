---
name: detect-skew
description: >
  Detect data skew in Spark jobs by analyzing task-level timing distribution,
  partition sizes, and executor workload imbalance. Use when a job has a few
  tasks running much longer than the rest, or when one executor is overloaded.
---

# Detect Skew

## When to Use

- A stage has a few tasks taking much longer than others
- One executor has significantly higher shuffle read/write than peers
- A join or aggregation is unexpectedly slow
- The job has a "long tail" where most tasks finish fast but a few straggle

## Instructions

1. Call `get_spark_stages` and look for stages where:
   - The stage has many completed tasks but is still slow
   - `shuffleReadBytes` or `shuffleWriteBytes` is very high
2. Call `get_spark_executors` and check for imbalance:
   - Compare `totalShuffleRead` across executors — flag if max > 3x median
   - Compare `totalTasks` across executors — flag if max > 2x median
   - Compare `totalDuration` across executors
3. If SQL queries exist, call `get_spark_sql_queries` and identify:
   - Join operations (most common skew source)
   - GroupBy / aggregation on low-cardinality keys
   - Window functions with uneven partitioning
4. Determine skew type:
   - **Join skew**: one join key has disproportionate rows
   - **Aggregation skew**: a few group keys contain most of the data
   - **Read skew**: input data has uneven partition sizes
5. Suggest targeted fixes:
   - For join skew: salting the skewed key, broadcast join if one side is small, or AQE skew join optimization
   - For aggregation skew: two-phase aggregation (partial + final)
   - For read skew: repartition input data, adjust file sizes
   - Always check if `spark.sql.adaptive.skewJoin.enabled` is set

## Skew Detection Thresholds

| Signal | Threshold | Severity |
|---|---|---|
| Max task time > 5x median | Definite skew | High |
| Max task time > 3x median | Likely skew | Medium |
| One executor shuffle > 3x median | Executor-level skew | High |
| One partition > 2 GB | Partition too large | Medium |
