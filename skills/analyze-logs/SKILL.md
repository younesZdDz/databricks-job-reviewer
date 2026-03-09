---
name: analyze-logs
description: >
  Analyze Databricks job logs to identify errors, warnings, OOM events,
  driver/executor failures, and stack traces. Use when a job has failed,
  thrown exceptions, or produced suspicious log output.
---

# Analyze Logs

## When to Use

- A job run has failed or been marked with errors
- You need to understand why an executor was lost
- Looking for OOM (OutOfMemory) evidence
- Investigating unexpected exceptions or warnings
- Checking for connectivity or dependency issues

## Instructions

1. Call `get_run_logs` with the run ID to fetch driver logs and error traces.
2. Call `get_run_output` for notebook output and structured error info.
3. Parse the logs for these critical patterns:

### Error Classification

Scan logs top-down and classify issues:

**Fatal / OOM**
- `java.lang.OutOfMemoryError`
- `Container killed by YARN for exceeding memory limits`
- `ExecutorLostFailure` with memory-related reasons

**Connectivity**
- `Connection refused`, `Connection reset`
- `TimeoutException` on external service calls
- JDBC/metastore connectivity errors

**Data Issues**
- `FileNotFoundException` — missing input data
- `AnalysisException` — schema mismatch or missing columns
- `SparkUpgradeException` — behavior change across Spark versions

**Resource Contention**
- `FetchFailedException` — shuffle service failure, often from executor death
- `TaskKilled` — preempted or OOM-killed
- Repeated `WARN` about full memory pools

4. For each error found:
   - Extract the full stack trace
   - Identify the originating class/method
   - Check if it's a driver-side or executor-side error
   - Correlate with stage/task failures from Spark UI data
5. If the error references user code (non-Spark-internal frames), note the exact class and line number for code review.

## Output Structure

For each log issue found:
- **Error type**: classification from above
- **Location**: driver or executor, stage ID if available
- **Stack trace**: key frames (omit Spark internals unless relevant)
- **Correlation**: link to stage metrics, executor loss events, or config issues
- **Suggested action**: concrete fix
