---
name: compare-runs
description: Compare two Databricks job runs to identify regressions, improvements, and root causes of performance changes.
---

# Compare Two Runs

Compare two Databricks job runs side by side to understand what changed.

## Steps

1. Ask the user for **two run IDs**:
   - Run A: the baseline (typically a known-good run)
   - Run B: the target (typically the regression or run under investigation)
2. Optionally ask for the **local file path** to the job source code, and
   the previous version of the file if a code change is suspected.
3. Call `compare_runs` with both run IDs for a high-level diff.
4. Delegate to the `/spark-job-reviewer` subagent with both run IDs and
   optional file path(s). The subagent will:
   - Fetch full details for both runs
   - Fetch stage metrics for both runs and compare stage-by-stage
   - Fetch executor metrics for both and compare resource utilization
   - Fetch SQL plans for both and diff query strategies
   - Fetch logs for both and compare error patterns
   - Compare job configs and cluster configs for any differences
   - If source code is provided, review for anti-patterns and correlate
     with runtime differences
5. Produce a comparison report highlighting:
   - Overall duration change (absolute and percentage)
   - Stages that got slower or faster
   - New errors or warnings
   - Configuration differences
   - Code-level observations if source was provided
   - Specific recommendations to address the regression
