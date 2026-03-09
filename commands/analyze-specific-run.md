---
name: analyze-specific-run
description: Analyze a specific Databricks job run by run ID for detailed performance investigation.
---

# Analyze Specific Run

Perform a deep analysis of a specific Databricks job run.

## Steps

1. Ask the user for the **run ID** if not provided.
2. Ask if they want to include source code review — if yes, get the **local
   file path** to the job source code.
3. Delegate to the `/spark-job-reviewer` subagent with the run ID and
   optional file path.
4. The subagent will:
   - Fetch full run details via `get_run_details`
   - Extract the cluster ID and job ID from the run
   - Fetch stage metrics via `get_spark_stages`
   - Fetch executor metrics via `get_spark_executors`
   - Fetch SQL query plans via `get_spark_sql_queries` and `get_spark_sql_plan`
   - Fetch logs via `get_run_logs`
   - Fetch job configuration via `get_job_config`
   - Fetch cluster info via `get_cluster_info`
   - If a file path was provided, read and review the source code
   - Run all deterministic checks
   - Perform AI-driven root cause analysis
   - Produce a structured report with findings, evidence, and recommendations
5. Present the full analysis directly in the conversation.
