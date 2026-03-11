---
name: analyze-logs
description: >
  Analyze Spark/job logs for errors, OOM, executor failures, and stack traces.
  Use when the user provides log content, run output, or event log references;
  correlate with stage/executor metrics from the MCP when available.
---

# Analyze Logs

## When to Use

- User provides log content (driver logs, run output, or error paste)
- An application shows failed stages or executors in the UI/API
- Investigating OOM, executor loss, or unexpected exceptions
- Correlating stack traces with stage/task failures from `get_stage_attempt` or `get_all_executors`

## Instructions

1. **Obtain logs**: Use whatever the user provides (driver log, run output, stack trace). The MCP does not fetch run logs; analysis is on user-provided log content or on metrics (failed tasks, executor loss) from the proxy API.
2. **Parse and classify**:

### Fatal / OOM

- `java.lang.OutOfMemoryError`
- Container killed for exceeding memory limits
- `ExecutorLostFailure` with memory-related reasons

### Connectivity

- `Connection refused`, `Connection reset`
- `TimeoutException` on external calls
- JDBC/metastore errors

### Data

- `FileNotFoundException` — missing input
- `AnalysisException` — schema/missing columns
- `SparkUpgradeException` — version behavior change

### Resource / shuffle

- `FetchFailedException` — shuffle or executor death
- `TaskKilled` — preemption or OOM
- WARN about full memory pools

3. **Correlate with API data**: If you have the same app via MCP, use `get_stage_attempt` (failed tasks), `get_all_executors` (dead executors, GC), and `get_job_detail` (failed stages) to match errors to stages and executors.
4. **Code location**: If the stack trace includes user code (non-Spark frames), note file and line for the review-spark-code skill.
5. **Output per issue**: Error type, location (driver/executor, stage id if known), key stack frames, correlation to stage/executor metrics, suggested action.

## Note

Event logs can be downloaded from the Spark REST API (`/applications/[base-app-id]/logs` or `.../[attempt-id]/logs`). If the user has those or run output, use this skill on the content they provide.
