#!/usr/bin/env python3
"""
Databricks MCP Server — exposes Spark job run data, metrics,
logs, SQL plans, and configuration as MCP tools.

Requires:
  DATABRICKS_HOST   – workspace URL (e.g. https://adb-xxxx.azuredatabricks.net)
  DATABRICKS_TOKEN  – personal access token
"""

import json
import os
import sys
from typing import Any

import httpx
from mcp.server.fastmcp import FastMCP

DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN", "")

mcp = FastMCP(
    "databricks",
    instructions="Databricks Spark job analysis tools — runs, stages, executors, SQL plans, logs, and configuration.",
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _headers() -> dict[str, str]:
    return {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}


def _get(path: str, params: dict[str, Any] | None = None) -> Any:
    """GET request against the Databricks REST API."""
    url = f"{DATABRICKS_HOST}{path}"
    resp = httpx.get(url, headers=_headers(), params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def _fmt(data: Any) -> str:
    return json.dumps(data, indent=2, default=str)


# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------

@mcp.tool()
def list_job_runs(
    job_id: int | None = None,
    limit: int = 10,
    active_only: bool = False,
    completed_only: bool = False,
) -> str:
    """List recent Databricks job runs.

    Args:
        job_id: Filter by a specific job. Omit to list runs across all jobs.
        limit: Maximum number of runs to return (default 10).
        active_only: Only return currently-running runs.
        completed_only: Only return finished runs.
    """
    params: dict[str, Any] = {"limit": limit}
    if job_id is not None:
        params["job_id"] = job_id
    if active_only:
        params["active_only"] = "true"
    if completed_only:
        params["completed_only"] = "true"

    data = _get("/api/2.1/jobs/runs/list", params)
    runs = data.get("runs", [])

    summaries = []
    for r in runs:
        summaries.append({
            "run_id": r.get("run_id"),
            "job_id": r.get("job_id"),
            "run_name": r.get("run_name"),
            "state": r.get("state", {}),
            "start_time": r.get("start_time"),
            "end_time": r.get("end_time"),
            "execution_duration_ms": r.get("execution_duration"),
            "setup_duration_ms": r.get("setup_duration"),
            "cluster_spec": r.get("cluster_spec", {}).get("existing_cluster_id")
                or r.get("cluster_instance", {}).get("cluster_id"),
        })

    return _fmt({"total": len(summaries), "runs": summaries})


@mcp.tool()
def get_run_details(run_id: int) -> str:
    """Get full details of a specific job run including tasks, state, and timing.

    Args:
        run_id: The Databricks run ID.
    """
    data = _get("/api/2.1/jobs/runs/get", {"run_id": run_id})
    return _fmt(data)


@mcp.tool()
def get_run_output(run_id: int) -> str:
    """Get the output / result of a job run (notebook output, error info, logs metadata).

    Args:
        run_id: The Databricks run ID.
    """
    data = _get("/api/2.1/jobs/runs/get-output", {"run_id": run_id})
    return _fmt(data)


@mcp.tool()
def get_spark_stages(cluster_id: str, spark_context_id: str | None = None) -> str:
    """Fetch Spark stage metrics from the Spark UI REST API (via cluster proxy).

    Returns per-stage: duration, input/output bytes, shuffle read/write,
    records, task counts, failure info.

    Args:
        cluster_id: The Databricks cluster ID running the Spark app.
        spark_context_id: Optional Spark context ID if multiple contexts exist.
    """
    base = f"/api/1.2/clusters/{cluster_id}/spark"
    if spark_context_id:
        base = f"{base}/contexts/{spark_context_id}"

    apps = _get(f"{base}/api/v1/applications")
    if not apps:
        return _fmt({"error": "No Spark applications found on this cluster."})

    app_id = apps[0]["id"] if isinstance(apps, list) else apps.get("id", "unknown")
    stages = _get(f"{base}/api/v1/applications/{app_id}/stages")

    summaries = []
    for s in stages:
        summaries.append({
            "stage_id": s.get("stageId"),
            "name": s.get("name"),
            "status": s.get("status"),
            "num_tasks": s.get("numTasks"),
            "num_complete_tasks": s.get("numCompleteTasks"),
            "num_failed_tasks": s.get("numFailedTasks"),
            "executor_run_time_ms": s.get("executorRunTime"),
            "executor_cpu_time_ns": s.get("executorCpuTime"),
            "input_bytes": s.get("inputBytes"),
            "input_records": s.get("inputRecords"),
            "output_bytes": s.get("outputBytes"),
            "output_records": s.get("outputRecords"),
            "shuffle_read_bytes": s.get("shuffleReadBytes"),
            "shuffle_read_records": s.get("shuffleReadRecords"),
            "shuffle_write_bytes": s.get("shuffleWriteBytes"),
            "shuffle_write_records": s.get("shuffleWriteRecords"),
            "memory_bytes_spilled": s.get("memoryBytesSpilled"),
            "disk_bytes_spilled": s.get("diskBytesSpilled"),
            "peak_execution_memory": s.get("peakExecutionMemory"),
        })

    return _fmt({"app_id": app_id, "total_stages": len(summaries), "stages": summaries})


@mcp.tool()
def get_spark_executors(cluster_id: str, spark_context_id: str | None = None) -> str:
    """Fetch Spark executor metrics: memory, cores, active tasks, GC time, shuffle.

    Useful for detecting executor-level bottlenecks and memory pressure.

    Args:
        cluster_id: The Databricks cluster ID.
        spark_context_id: Optional Spark context ID.
    """
    base = f"/api/1.2/clusters/{cluster_id}/spark"
    if spark_context_id:
        base = f"{base}/contexts/{spark_context_id}"

    apps = _get(f"{base}/api/v1/applications")
    if not apps:
        return _fmt({"error": "No Spark applications found on this cluster."})

    app_id = apps[0]["id"] if isinstance(apps, list) else apps.get("id", "unknown")
    executors = _get(f"{base}/api/v1/applications/{app_id}/allexecutors")

    summaries = []
    for e in executors:
        summaries.append({
            "executor_id": e.get("id"),
            "host_port": e.get("hostPort"),
            "is_active": e.get("isActive"),
            "total_cores": e.get("totalCores"),
            "max_tasks": e.get("maxTasks"),
            "active_tasks": e.get("activeTasks"),
            "total_tasks": e.get("totalTasks"),
            "failed_tasks": e.get("failedTasks"),
            "total_duration_ms": e.get("totalDuration"),
            "total_gc_time_ms": e.get("totalGCTime"),
            "total_input_bytes": e.get("totalInputBytes"),
            "total_shuffle_read_bytes": e.get("totalShuffleRead"),
            "total_shuffle_write_bytes": e.get("totalShuffleWrite"),
            "max_memory_bytes": e.get("maxMemory"),
            "used_on_heap_memory": e.get("usedOnHeapStorageMemory"),
            "used_off_heap_memory": e.get("usedOffHeapStorageMemory"),
            "memory_metrics": e.get("memoryMetrics"),
            "peak_memory_metrics": e.get("peakMemoryMetrics"),
        })

    return _fmt({"app_id": app_id, "total_executors": len(summaries), "executors": summaries})


@mcp.tool()
def get_spark_sql_queries(cluster_id: str, spark_context_id: str | None = None) -> str:
    """Fetch Spark SQL query execution data including plans, durations, and metrics.

    Args:
        cluster_id: The Databricks cluster ID.
        spark_context_id: Optional Spark context ID.
    """
    base = f"/api/1.2/clusters/{cluster_id}/spark"
    if spark_context_id:
        base = f"{base}/contexts/{spark_context_id}"

    apps = _get(f"{base}/api/v1/applications")
    if not apps:
        return _fmt({"error": "No Spark applications found on this cluster."})

    app_id = apps[0]["id"] if isinstance(apps, list) else apps.get("id", "unknown")
    sql_data = _get(f"{base}/api/v1/applications/{app_id}/sql")

    summaries = []
    for q in sql_data if isinstance(sql_data, list) else []:
        summaries.append({
            "execution_id": q.get("id"),
            "status": q.get("status"),
            "description": q.get("description", "")[:200],
            "plan_description": q.get("planDescription", "")[:500],
            "duration_ms": q.get("duration"),
            "running_job_ids": q.get("runningJobIds"),
            "succeeded_job_ids": q.get("successJobIds"),
            "failed_job_ids": q.get("failedJobIds"),
        })

    return _fmt({"app_id": app_id, "total_queries": len(summaries), "queries": summaries})


@mcp.tool()
def get_spark_sql_plan(
    cluster_id: str, execution_id: int, spark_context_id: str | None = None
) -> str:
    """Fetch the physical and logical plan for a specific Spark SQL execution.

    Args:
        cluster_id: The Databricks cluster ID.
        execution_id: The SQL execution ID from get_spark_sql_queries.
        spark_context_id: Optional Spark context ID.
    """
    base = f"/api/1.2/clusters/{cluster_id}/spark"
    if spark_context_id:
        base = f"{base}/contexts/{spark_context_id}"

    apps = _get(f"{base}/api/v1/applications")
    if not apps:
        return _fmt({"error": "No Spark applications found on this cluster."})

    app_id = apps[0]["id"] if isinstance(apps, list) else apps.get("id", "unknown")
    plan = _get(f"{base}/api/v1/applications/{app_id}/sql/{execution_id}")

    return _fmt(plan)


@mcp.tool()
def get_run_logs(run_id: int) -> str:
    """Fetch driver and executor logs for a Databricks job run.

    Returns log content and metadata (log truncated if very large).

    Args:
        run_id: The Databricks run ID.
    """
    data = _get("/api/2.1/jobs/runs/get-output", {"run_id": run_id})

    logs: dict[str, Any] = {}
    if "error" in data:
        logs["error"] = data["error"]
    if "error_trace" in data:
        logs["error_trace"] = data["error_trace"][:5000]

    cluster_instance = data.get("cluster_instance", {})
    cluster_id = cluster_instance.get("cluster_id")

    if cluster_id:
        try:
            log_data = _get(
                "/api/2.0/dbfs/read",
                {"path": f"dbfs:/cluster-logs/{cluster_id}/driver/log4j-active.log", "length": 50000},
            )
            import base64
            logs["driver_log"] = base64.b64decode(log_data.get("data", "")).decode("utf-8", errors="replace")
        except Exception:
            logs["driver_log"] = "(not available via DBFS — check cluster log delivery config)"

    if "notebook_output" in data:
        logs["notebook_output"] = data["notebook_output"]

    return _fmt(logs)


@mcp.tool()
def get_job_config(job_id: int) -> str:
    """Get the full configuration of a Databricks job: cluster spec, schedule, tasks, libraries.

    Args:
        job_id: The Databricks job ID.
    """
    data = _get("/api/2.1/jobs/get", {"job_id": job_id})
    settings = data.get("settings", {})

    config = {
        "job_id": data.get("job_id"),
        "name": settings.get("name"),
        "schedule": settings.get("schedule"),
        "max_concurrent_runs": settings.get("max_concurrent_runs"),
        "timeout_seconds": settings.get("timeout_seconds"),
        "tasks": [],
    }

    for task in settings.get("tasks", []):
        task_info: dict[str, Any] = {
            "task_key": task.get("task_key"),
            "description": task.get("description"),
        }

        if "new_cluster" in task:
            nc = task["new_cluster"]
            task_info["cluster"] = {
                "type": "new_cluster",
                "spark_version": nc.get("spark_version"),
                "node_type_id": nc.get("node_type_id"),
                "num_workers": nc.get("num_workers"),
                "autoscale": nc.get("autoscale"),
                "spark_conf": nc.get("spark_conf"),
                "spark_env_vars": nc.get("spark_env_vars"),
            }
        elif "existing_cluster_id" in task:
            task_info["cluster"] = {
                "type": "existing_cluster",
                "cluster_id": task["existing_cluster_id"],
            }

        if "notebook_task" in task:
            task_info["notebook"] = task["notebook_task"]
        elif "spark_jar_task" in task:
            task_info["spark_jar"] = task["spark_jar_task"]
        elif "spark_python_task" in task:
            task_info["spark_python"] = task["spark_python_task"]
        elif "spark_submit_task" in task:
            task_info["spark_submit"] = task["spark_submit_task"]

        task_info["libraries"] = task.get("libraries", [])
        config["tasks"].append(task_info)

    return _fmt(config)


@mcp.tool()
def get_cluster_info(cluster_id: str) -> str:
    """Get cluster configuration and current state.

    Useful for understanding the compute environment: node types, memory,
    Spark config, autoscaling, and runtime version.

    Args:
        cluster_id: The Databricks cluster ID.
    """
    data = _get("/api/2.0/clusters/get", {"cluster_id": cluster_id})

    info = {
        "cluster_id": data.get("cluster_id"),
        "cluster_name": data.get("cluster_name"),
        "state": data.get("state"),
        "spark_version": data.get("spark_version"),
        "node_type_id": data.get("node_type_id"),
        "driver_node_type_id": data.get("driver_node_type_id"),
        "num_workers": data.get("num_workers"),
        "autoscale": data.get("autoscale"),
        "spark_conf": data.get("spark_conf"),
        "spark_env_vars": data.get("spark_env_vars"),
        "custom_tags": data.get("custom_tags"),
        "cluster_memory_mb": data.get("cluster_memory_mb"),
        "cluster_cores": data.get("cluster_cores"),
    }

    return _fmt(info)


@mcp.tool()
def compare_runs(run_id_a: int, run_id_b: int) -> str:
    """Compare two job runs side by side: timing, state, cluster config differences.

    Useful for investigating regressions between a known-good run and a bad run.

    Args:
        run_id_a: First run ID (typically the baseline / good run).
        run_id_b: Second run ID (typically the regression / bad run).
    """
    a = _get("/api/2.1/jobs/runs/get", {"run_id": run_id_a})
    b = _get("/api/2.1/jobs/runs/get", {"run_id": run_id_b})

    def _extract(r: dict) -> dict:
        return {
            "run_id": r.get("run_id"),
            "state": r.get("state", {}),
            "start_time": r.get("start_time"),
            "end_time": r.get("end_time"),
            "execution_duration_ms": r.get("execution_duration"),
            "setup_duration_ms": r.get("setup_duration"),
            "cluster_id": r.get("cluster_instance", {}).get("cluster_id"),
            "num_tasks": len(r.get("tasks", [])),
        }

    run_a = _extract(a)
    run_b = _extract(b)

    dur_a = run_a["execution_duration_ms"] or 0
    dur_b = run_b["execution_duration_ms"] or 0
    diff_ms = dur_b - dur_a
    diff_pct = (diff_ms / dur_a * 100) if dur_a else None

    return _fmt({
        "run_a": run_a,
        "run_b": run_b,
        "duration_diff_ms": diff_ms,
        "duration_diff_pct": round(diff_pct, 1) if diff_pct is not None else None,
        "same_cluster": run_a["cluster_id"] == run_b["cluster_id"],
    })


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    if not DATABRICKS_HOST or not DATABRICKS_TOKEN:
        print(
            "ERROR: Set DATABRICKS_HOST and DATABRICKS_TOKEN environment variables.",
            file=sys.stderr,
        )
        sys.exit(1)

    mcp.run(transport="stdio")
