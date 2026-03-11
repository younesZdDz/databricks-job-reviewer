---
name: review-spark-job
description: >
  Analyze a Spark application and review the task code in one flow: runtime
  (SQL/jobs → stages → quantiles → executors) plus code review and file:line links.
---

# Review Spark Job

Run a full analysis and code review in one command: gather runtime data from the cluster, run deterministic checks, analyze bottlenecks and skew, and review the task code with findings linked to file:line.

## Steps

1. Ask the user for:
   - **cluster_id** (required): Databricks cluster ID.
   - **Code file path** (required): Local path to the task/job source (notebook, PySpark, Scala, or SQL).
   - **app_id** (optional): Spark application ID. If omitted, use the latest application (first from the cluster via `list_applications`).
2. Delegate to the **spark-job-reviewer** agent with cluster_id, code file path, and optional app_id.
3. The agent will:
   - Resolve app_id if not provided (list_applications, take first = latest).
   - Run the analysis workflow: rank SQL executions or jobs → pick heaviest stages → use quantiles (p01/p50/p99) before task list → executors.
   - Read the code file and review for anti-patterns, correlating with runtime (stages, SQL plan).
   - Link every finding to code (file:line) and produce a single report.
4. Present the combined analysis and code review report in the conversation.
