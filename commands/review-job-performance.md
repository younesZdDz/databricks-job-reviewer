---
name: review-job-performance
description: Comprehensive performance review of a Databricks job across recent runs — trends, recurring issues, and long-term recommendations.
---

# Review Job Performance

Perform a comprehensive performance review of a Databricks job across its
recent run history.

## Steps

1. Ask the user for the **job ID** if not provided.
2. Optionally ask for the **local file path** to the job source code.
3. Call `list_job_runs` with the job ID and `limit=20` to get recent history.
4. Call `get_job_config` to understand the job setup.
5. For the **latest run**, delegate full analysis to `/spark-job-reviewer`.
6. Across all runs, compute:
   - **Duration trend**: is the job getting slower over time?
   - **Failure rate**: how often does it fail?
   - **Duration variance**: is runtime stable or erratic?
   - **Setup time**: is cluster startup a significant overhead?
7. If the job shows a clear regression point (sudden duration increase),
   identify when it started and use `/compare-runs` logic between the
   last good run and first bad run.
8. If source code was provided, review it for anti-patterns and correlate
   with runtime observations.
9. Produce a report covering:
   - **Job health summary**: pass/fail rate, average duration, trend
   - **Recurring issues**: patterns that appear across multiple runs
   - **Configuration review**: is the cluster right-sized? Are Spark configs optimal?
   - **Code review highlights**: anti-patterns found (if source was provided)
   - **Prioritized recommendations**: ordered by expected impact
