#!/usr/bin/env python3
"""
Databricks MCP Server — exposes Spark application data via the driver-proxy
REST API (Spark UI /api/v1). Use for performance analysis: SQL executions,
jobs, stages, task summaries (quantiles), task lists, and executors.

Workflow: SQL executions (or jobs) → associated jobs → heaviest stages
→ task summary with quantiles → task list only if quantiles show skew.

Requires:
  DATABRICKS_HOST  – workspace URL (e.g. https://adb-xxxx.azuredatabricks.net)
  DATABRICKS_TOKEN – personal access token

User provides: cluster_id, code file path (required), and optional app_id
(default: first application).
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
    instructions=(
        "Spark application analysis via driver-proxy API. Start from SQL "
        "executions or jobs, then drill into heaviest stages, use task summary "
        "quantiles (p01/p50/p99) before task list, and cross-check executors. "
        "Tools take cluster_id and optional app_id (from /applications if omitted)."
    ),
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _headers() -> dict[str, str]:
    return {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}


def _proxy_base(cluster_id: str) -> str:
    """Base URL for Spark UI REST API via driver-proxy."""
    return f"{DATABRICKS_HOST}/driver-proxy-api/o/0/{cluster_id}/40001/api/v1"


def _get(path: str, params: dict[str, Any] | None = None) -> Any:
    """GET against Databricks workspace (e.g. /api/2.0/...)."""
    url = f"{DATABRICKS_HOST}{path}"
    resp = httpx.get(url, headers=_headers(), params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def _get_proxy(
    cluster_id: str, path: str, params: dict[str, Any] | None = None
) -> Any:
    """GET against driver-proxy Spark UI API."""
    base = _proxy_base(cluster_id)
    url = f"{base}{path}" if path.startswith("/") else f"{base}/{path}"
    resp = httpx.get(url, headers=_headers(), params=params, timeout=60)
    resp.raise_for_status()
    return resp.json()


def _ensure_app_id(cluster_id: str, app_id: str | None) -> str:
    """Resolve app_id from first application if not provided."""
    if app_id:
        return app_id
    apps = _get_proxy(cluster_id, "/applications", {"limit": 1})
    if not apps or (isinstance(apps, list) and len(apps) == 0):
        raise ValueError("No Spark applications found on this cluster.")
    first = apps[0] if isinstance(apps, list) else apps
    return first.get("id") or first.get("name") or str(first)


def _fmt(data: Any) -> str:
    return json.dumps(data, indent=2, default=str)


# ---------------------------------------------------------------------------
# Applications
# ---------------------------------------------------------------------------


@mcp.tool()
def list_applications(
    cluster_id: str,
    limit: int = 10,
    min_date: str | None = None,
    max_date: str | None = None,
    min_end_date: str | None = None,
    max_end_date: str | None = None,
    status: str | None = None,
) -> str:
    """List Spark applications (driver-proxy). Use to pick or confirm app_id.

    Args:
        cluster_id: Databricks cluster ID.
        limit: Max applications to return.
        min_date: Filter by min start date (e.g. 2024-01-01).
        max_date: Filter by max start date.
        min_end_date: Filter by min end date.
        max_end_date: Filter by max end date.
        status: Filter by status if supported.
    """
    params: dict[str, Any] = {"limit": limit}
    if min_date:
        params["minDate"] = min_date
    if max_date:
        params["maxDate"] = max_date
    if min_end_date:
        params["minEndDate"] = min_end_date
    if max_end_date:
        params["maxEndDate"] = max_end_date
    if status:
        params["status"] = status
    data = _get_proxy(cluster_id, "/applications", params)
    return _fmt(data)


@mcp.tool()
def get_application(cluster_id: str, app_id: str) -> str:
    """Get details for a given Spark application.

    Args:
        cluster_id: Databricks cluster ID.
        app_id: Spark application ID (from list_applications or user).
    """
    data = _get_proxy(cluster_id, f"/applications/{app_id}")
    return _fmt(data)


# ---------------------------------------------------------------------------
# SQL executions (preferred entry point for DataFrame/SQL workloads)
# ---------------------------------------------------------------------------


@mcp.tool()
def get_sql_executions(
    cluster_id: str,
    app_id: str | None = None,
    details: bool = False,
    plan_description: bool = False,
) -> str:
    """List SQL query executions for the application. Rank by duration to find the slowest query.

    Best entry for SQL/DataFrame workloads. Use ids with get_sql_execution.

    Args:
        cluster_id: Databricks cluster ID.
        app_id: Spark application ID. Omit to use the first application on the cluster.
        details: Include full details per execution.
        plan_description: Include plan description per execution.
    """
    aid = _ensure_app_id(cluster_id, app_id)
    params: dict[str, Any] = {"details": str(details).lower(), "planDescription": str(plan_description).lower()}
    data = _get_proxy(cluster_id, f"/applications/{aid}/sql", params)
    return _fmt(data)


@mcp.tool()
def get_sql_execution(
    cluster_id: str,
    execution_id: int,
    app_id: str | None = None,
    details: bool = True,
    plan_description: bool = True,
) -> str:
    """Details for one SQL execution: plan, metrics, associated job ids. Use after ranking by get_sql_executions.

    Look for: Exchange, SortMergeJoin, HashAggregate, BroadcastHashJoin,
    BatchScan, WholeStageCodegen; row counts, shuffle bytes, spill;
    and the list of associated job ids for this execution.

    Args:
        cluster_id: Databricks cluster ID.
        execution_id: SQL execution ID from get_sql_executions.
        app_id: Spark application ID. Omit to use the first application.
        details: Include full details.
        plan_description: Include physical plan description.
    """
    aid = _ensure_app_id(cluster_id, app_id)
    params = {"details": str(details).lower(), "planDescription": str(plan_description).lower()}
    data = _get_proxy(
        cluster_id, f"/applications/{aid}/sql/{execution_id}", params
    )
    return _fmt(data)


# ---------------------------------------------------------------------------
# Jobs (use when not SQL-driven or when you already have job id)
# ---------------------------------------------------------------------------


@mcp.tool()
def get_jobs(
    cluster_id: str,
    app_id: str | None = None,
    status: str | None = None,
) -> str:
    """List jobs for the application. status: running, succeeded, failed, unknown.

    Args:
        cluster_id: Databricks cluster ID.
        app_id: Spark application ID. Omit to use the first application.
        status: Filter by job status.
    """
    aid = _ensure_app_id(cluster_id, app_id)
    params = {} if not status else {"status": status}
    data = _get_proxy(cluster_id, f"/applications/{aid}/jobs", params)
    return _fmt(data)


@mcp.tool()
def get_job_detail(cluster_id: str, job_id: int, app_id: str | None = None) -> str:
    """Details for one Spark job: stage ids, durations, shuffle, I/O.
    Use to pick heaviest stages.

    Args:
        cluster_id: Databricks cluster ID.
        job_id: Spark job ID from get_jobs or get_sql_execution (succeededJobIds).
        app_id: Spark application ID. Omit to use the first application.
    """
    aid = _ensure_app_id(cluster_id, app_id)
    data = _get_proxy(cluster_id, f"/applications/{aid}/jobs/{job_id}")
    return _fmt(data)


# ---------------------------------------------------------------------------
# Stages (use after choosing the job; rank by duration/shuffle/spill)
# ---------------------------------------------------------------------------


@mcp.tool()
def get_stages(
    cluster_id: str,
    app_id: str | None = None,
    status: str | None = None,
    details: bool = True,
    with_summaries: bool = True,
    quantiles: str | None = None,
) -> str:
    """List stages. Prefer narrowing by job first via get_job_detail.

    status: active, complete, pending, failed, skipped. quantiles e.g. 0.01,0.5,0.99.

    Args:
        cluster_id: Databricks cluster ID.
        app_id: Spark application ID. Omit to use the first application.
        status: Filter by stage status.
        details: Include full details.
        with_summaries: Include summaries.
        quantiles: Comma-separated quantiles for task metrics.
    """
    aid = _ensure_app_id(cluster_id, app_id)
    params: dict[str, Any] = {
        "details": str(details).lower(),
        "withSummaries": str(with_summaries).lower(),
    }
    if status:
        params["status"] = status
    if quantiles:
        params["quantiles"] = quantiles
    data = _get_proxy(cluster_id, f"/applications/{aid}/stages", params)
    return _fmt(data)


@mcp.tool()
def get_stage_attempt(
    cluster_id: str,
    stage_id: int,
    stage_attempt_id: int,
    app_id: str | None = None,
    details: bool = True,
    with_summaries: bool = True,
    quantiles: str = "0.01,0.5,0.99",
) -> str:
    """Details for one stage attempt. Use quantiles to detect skew
    before opening task list. If p99 >> p50 for runtime or shuffle
    read, stage is skewed or has stragglers.

    Args:
        cluster_id: Databricks cluster ID.
        stage_id: Stage ID.
        stage_attempt_id: Stage attempt ID (usually 0).
        app_id: Spark application ID. Omit to use the first application.
        details: Include full details.
        with_summaries: Include summaries.
        quantiles: Comma-separated (e.g. 0.01,0.5,0.99).
    """
    aid = _ensure_app_id(cluster_id, app_id)
    params = {
        "details": str(details).lower(),
        "withSummaries": str(with_summaries).lower(),
        "quantiles": quantiles,
    }
    data = _get_proxy(
        cluster_id,
        f"/applications/{aid}/stages/{stage_id}/{stage_attempt_id}",
        params,
    )
    return _fmt(data)


@mcp.tool()
def get_stage_task_summary(
    cluster_id: str,
    stage_id: int,
    stage_attempt_id: int,
    app_id: str | None = None,
    quantiles: str = "0.01,0.5,0.99",
) -> str:
    """Task summary (quantiles). Use before task list to detect skew.
    p99 >> p50 for runtime or shuffle read indicates skew or stragglers.

    Args:
        cluster_id: Databricks cluster ID.
        stage_id: Stage ID.
        stage_attempt_id: Stage attempt ID.
        app_id: Spark application ID. Omit to use the first application.
        quantiles: Comma-separated quantiles.
    """
    aid = _ensure_app_id(cluster_id, app_id)
    params = {"quantiles": quantiles}
    data = _get_proxy(
        cluster_id,
        f"/applications/{aid}/stages/{stage_id}/{stage_attempt_id}/taskSummary",
        params,
    )
    return _fmt(data)


@mcp.tool()
def get_stage_task_list(
    cluster_id: str,
    stage_id: int,
    stage_attempt_id: int,
    app_id: str | None = None,
    sort_by: str = "-runtime",
    offset: int = 0,
    length: int = 50,
    status: str | None = None,
) -> str:
    """List tasks for a stage attempt. Use only after quantiles
    suggest skew/stragglers. sort_by: runtime, -runtime.
    status: running, success, killed, failed, unknown.

    Args:
        cluster_id: Databricks cluster ID.
        stage_id: Stage ID.
        stage_attempt_id: Stage attempt ID.
        app_id: Spark application ID. Omit to use the first application.
        sort_by: Sort field and direction.
        offset: Pagination offset.
        length: Page size.
        status: Filter by task status.
    """
    aid = _ensure_app_id(cluster_id, app_id)
    params: dict[str, Any] = {"sortBy": sort_by, "offset": offset, "length": length}
    if status:
        params["status"] = status
    data = _get_proxy(
        cluster_id,
        f"/applications/{aid}/stages/{stage_id}/{stage_attempt_id}/taskList",
        params,
    )
    return _fmt(data)


# ---------------------------------------------------------------------------
# Executors
# ---------------------------------------------------------------------------


@mcp.tool()
def get_executors(cluster_id: str, app_id: str | None = None) -> str:
    """List active executors. Use to check GC, shuffle, and imbalance.

    Args:
        cluster_id: Databricks cluster ID.
        app_id: Spark application ID. Omit to use the first application.
    """
    aid = _ensure_app_id(cluster_id, app_id)
    data = _get_proxy(cluster_id, f"/applications/{aid}/executors")
    return _fmt(data)


@mcp.tool()
def get_all_executors(cluster_id: str, app_id: str | None = None) -> str:
    """List all executors (active and dead). Use for full imbalance and GC analysis.

    Args:
        cluster_id: Databricks cluster ID.
        app_id: Spark application ID. Omit to use the first application.
    """
    aid = _ensure_app_id(cluster_id, app_id)
    data = _get_proxy(cluster_id, f"/applications/{aid}/allexecutors")
    return _fmt(data)


# ---------------------------------------------------------------------------
# Environment and cluster
# ---------------------------------------------------------------------------


@mcp.tool()
def get_environment(cluster_id: str, app_id: str | None = None) -> str:
    """Environment details for the application (Spark config, JVM, etc.).

    Args:
        cluster_id: Databricks cluster ID.
        app_id: Spark application ID. Omit to use the first application.
    """
    aid = _ensure_app_id(cluster_id, app_id)
    data = _get_proxy(cluster_id, f"/applications/{aid}/environment")
    return _fmt(data)


@mcp.tool()
def get_cluster_info(cluster_id: str) -> str:
    """Cluster config and state (Databricks API). Node types, memory,
    Spark version, autoscale.

    Args:
        cluster_id: Databricks cluster ID.
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


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    if not DATABRICKS_HOST or not DATABRICKS_TOKEN:
        print(
            "ERROR: Set DATABRICKS_HOST and DATABRICKS_TOKEN.",
            file=sys.stderr,
        )
        sys.exit(1)

    mcp.run(transport="stdio")
