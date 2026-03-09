---
name: analyze-latest-run
description: Analyze the most recent run of a Databricks job for performance issues, failures, and optimization opportunities.
---

# Analyze Latest Job Run

Analyze the most recent run of a Databricks job.

## Steps

1. Ask the user for the **job ID** if not provided.
2. Ask if they want to include source code review — if yes, get the **local
   file path** to the job source code.
3. Call `list_job_runs` with the job ID and `limit=1` to get the latest run.
4. Delegate to the `/spark-job-reviewer` subagent with the run ID, job ID,
   and optional file path.
5. The subagent will:
   - Fetch run details, stage metrics, executor metrics, SQL plans, and logs
   - Fetch job configuration and cluster info
   - Run deterministic checks (skew, spill, shuffle, failures)
   - If a file path was provided, read the source code and review for anti-patterns
   - Perform AI analysis
   - Produce a structured report
6. Present the analysis report directly in the conversation.
