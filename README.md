# Databricks Job Reviewer : Cursor Plugin

A Cursor plugin that analyzes Databricks / Spark jobs using Spark UI data, job logs, and local source code. It detects issues and suggests concrete improvements directly in your agent conversation.

## What it does

Given a Databricks job run, the plugin:

1. **Fetches runtime data** : stages, executors, SQL plans, logs, and cluster config via a custom MCP server
2. **Reads your job source code** : from a local file you provide (e.g. notebook or Python script)
3. **Runs deterministic checks** : skew, spill, shuffle explosion, failures, GC pressure
4. **Performs AI-driven root cause analysis** : connects metrics to code and config
5. **Produces a structured report** : findings with evidence, severity, and actionable recommendations

### Example findings

| Type | What you get |
|------|----------------|
| **Slowdown** | Root cause with evidence (e.g. “2.3x slower due to skew in Stage 12”) |
| **Data skew** | Affected stage, keys, and skew ratio; code location when source is provided |
| **Shuffle explosion** | Tied to a join or aggregation in your code |
| **Memory / spill** | Disk spill metrics and suggested executor memory or partitioning |
| **Join efficiency** | Sort-merge vs broadcast with evidence from plans |
| **Anti-patterns** | `.collect()`, UDFs, cross joins, bad repartition : with file:line when you attach source |

---

## For users : Install from the Marketplace

### 1. Install the plugin

1. In Cursor, open **Settings** → **Plugins** (or the marketplace panel).
2. Find **Databricks Job Reviewer** and install it.
3. The plugin adds rules, skills, commands, and the Databricks MCP server.

### 2. Configure Databricks access

The MCP server needs access to your workspace. Set these **environment variables** (e.g. in your shell profile or Cursor’s environment):

```bash
export DATABRICKS_HOST="https://adb-xxxxxxxxxxxx.azuredatabricks.net"
export DATABRICKS_TOKEN="dapi..."
```

- **DATABRICKS_HOST** : Your workspace URL (no trailing slash).
- **DATABRICKS_TOKEN** : A [Databricks personal access token](https://docs.databricks.com/en/dev-tools/auth/pat.html) with at least “Job” and “Cluster” read access.

If Cursor doesn’t inherit your shell env, you may need to set these in your OS user environment or in Cursor’s MCP server config (Settings → MCP → databricks → env).

### 3. Verify

- **Settings → Rules**: You should see the plugin’s rules (e.g. deterministic-first, evidence-based).
- **Settings → MCP**: The `databricks` server should be listed and show a green status.
- In **Agent chat**, type `/` : you should see: `/analyze-latest-run`, `/analyze-specific-run`, `/compare-runs`, `/review-job-performance`.

---

## Usage : Commands and examples

All usage happens in **Cursor Agent chat**. Use the slash commands below; the agent will ask for job ID or run ID and, when relevant, an optional **source file** (e.g. `@src/jobs/daily_etl.py`) for code-level analysis.

### Command reference

| Command | When to use |
|--------|----------------|
| `/analyze-latest-run` | Investigate the most recent run of a job (e.g. after a failure or slowdown). |
| `/analyze-specific-run` | Deep-dive a specific run by ID (e.g. from an alert or link). |
| `/compare-runs` | Compare two runs to find regressions (e.g. “good” vs “bad” run). |
| `/review-job-performance` | Performance review over recent run history (trends, recurring issues, config). |

---

### Example 1: Analyze the latest run (quick check after a failure)

**In Agent chat:**

```
/analyze-latest-run
```

When prompted:

- **Job ID:** `123456`
- **Source file (optional):** `@src/jobs/daily_etl.py` : include this to link findings to your code.

**Example output (shortened):**

```text
# Job Run Analysis : Run 987654

## Overview
Run 987654 of job "daily_etl" completed in 47 min (2.3x slower than the 7-day median of 20 min).
Root cause: data skew in the customer join stage and shuffle explosion from an unfiltered cross-product.

## Findings

### Finding: Data Skew in Stage 12 (SortMergeJoin)
Severity: Critical | Confidence: High

Evidence:
- Stage 12 median task time: 8s, max: 412s (51x skew ratio)
- shuffleReadBytes on executor 7: 34 GB vs median 2.1 GB
- groupBy on `customer_id` : top key has 12M rows vs median 200

Suggested fix:
  spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
  # daily_etl.py:38 : add salting before the join
  df = df.withColumn("salt", (F.rand() * 10).cast("int"))

### Finding: Disk Spill in Stage 8
Severity: High | Confidence: High

Evidence:
- diskBytesSpilled = 18.4 GB across 200 tasks
- spark.executor.memory = 4g, peak execution memory = 7.2 GB

Suggested fix: Increase executor memory to 8g (or add more partitions).

## Recommendations
1. Enable AQE skew join (Critical)
2. Increase executor memory 4g → 8g (High)
3. Add partition filter on `date` before join : daily_etl.py:25 (Medium)
```

---

### Example 2: Analyze a specific run (from an alert or link)

Use when you have an exact run ID (e.g. from the Databricks UI or an alert).

**In Agent chat:**

```
/analyze-specific-run
```

When prompted:

- **Run ID:** `987654`
- **Source file (optional):** `@notebooks/daily_etl.py`

Same style of report as Example 1, but scoped to that run.

---

### Example 3: Compare two runs (find what regressed)

Use to compare a “good” baseline run with a “bad” run.

**In Agent chat:**

```
/compare-runs
```

When prompted:

- **Baseline run (good):** `987600`
- **Target run (bad):** `987654`
- **Source file (optional):** `@src/jobs/daily_etl.py`

**Example output (shortened):**

```text
# Run Comparison : 987600 vs 987654

## Duration
- Run A (baseline): 19 min 42s
- Run B (target):   47 min 11s
- Difference:       +27 min 29s (+139%)

## What changed
- Stage 12 (SortMergeJoin): 3 min → 28 min : skew on `customer_id`
- Stage 8 (HashAggregate):  2 min → 8 min : disk spill (0 → 18.4 GB)
- Cluster config: identical
- Input volume: 120 GB → 310 GB (date range expanded in daily_etl.py:22)

## Code diff correlation
- daily_etl.py:22 : filter changed from last 7 days to last 30 days
- daily_etl.py:38 : join on `customer_id` unchanged; more data exposed skew

## Recommendations
1. Restore 7-day filter or scale partitions
2. Enable AQE skew join for customer_id
3. Increase executor memory for larger working set
```

---

### Example 4: Review overall job performance (health and trends)

Use for a periodic health check or before changing config.

**In Agent chat:**

```
/review-job-performance
```

When prompted:

- **Job ID:** `123456`

**Example output (shortened):**

```text
# Job Performance Review : "daily_etl" (Job 123456)

## Health summary
- Last 20 runs: 17 succeeded, 3 failed (85% success rate)
- Avg duration: 22 min | Min: 18 min | Max: 47 min
- Trend: duration increasing ~3 min/week
- Failure pattern: all 3 failures OOM on Stage 8

## Recurring issues
1. Executor OOM on Stage 8 in 3/20 runs : memory borderline at 4g
2. Skew on customer_id in 8/20 runs (intermittent hot keys)

## Configuration review
- spark.executor.memory = 4g : undersized for current volume
- spark.sql.adaptive.enabled = true; skewJoin not enabled
- Autoscaling: min 2, max 10 : min could be 4

## Recommendations
1. Increase spark.executor.memory to 8g
2. Enable spark.sql.adaptive.skewJoin.enabled
3. Set autoscale min workers to 4
4. Add alert when duration exceeds 30 min
```

---

## Reference

### MCP tools (Databricks)

The plugin’s MCP server exposes these tools (used by the agent under the hood):

| Tool | Description |
|------|-------------|
| `list_job_runs` | List recent job runs (optional job ID filter) |
| `get_run_details` | Full details of a run |
| `get_run_output` | Run output, errors, notebook results |
| `get_spark_stages` | Stage metrics (timing, shuffle, spill) |
| `get_spark_executors` | Executor metrics (memory, GC, tasks) |
| `get_spark_sql_queries` | SQL query execution data |
| `get_spark_sql_plan` | Physical/logical plan for a SQL execution |
| `get_run_logs` | Driver logs and error traces |
| `get_job_config` | Job config (cluster, tasks, libraries) |
| `get_cluster_info` | Cluster configuration and state |
| `compare_runs` | Side-by-side comparison of two runs |

### Rules (always-on guidance)

- **Deterministic first** : Metric-based checks run before AI analysis.
- **Evidence-based** : Every finding cites specific metrics or log lines.
- **No generic advice** : No vague suggestions without evidence.
- **Link runtime to code** : Connect stage/issues to specific code lines when source is provided.

### Skills (agent capabilities)

- **analyze-spark-stages** : Bottleneck stages, throughput, spill.
- **detect-skew** : Task/executor skew, skewed join/group keys.
- **analyze-logs** : Parse errors, classify failures (OOM, connectivity, data), stack traces.
- **review-spark-code** : Anti-patterns: collect, UDFs, cross joins, bad repartition.

---

## Plugin structure

```text
databricks-job-reviewer/
├── .cursor-plugin/
│   └── plugin.json
├── .mcp.json
├── mcp-servers/databricks/
│   ├── server.py
│   └── requirements.txt
├── agents/
├── skills/
├── rules/
├── commands/
└── README.md
```

---

## For contributors : Local development

Use this when you clone the repo to change the plugin or the MCP server.

### Prerequisites

- Python 3.11+
- [Cursor](https://cursor.com) IDE
- A Databricks workspace and [personal access token](https://docs.databricks.com/en/dev-tools/auth/pat.html)

### 1. Clone and set up

```bash
git clone https://github.com/your-org/databricks-job-reviewer.git
cd databricks-job-reviewer
python3 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r mcp-servers/databricks/requirements.txt
```

### 2. Environment variables

Create a `.env` at the project root (gitignored):

```bash
DATABRICKS_HOST=https://adb-xxxxxxxxxxxx.azuredatabricks.net
DATABRICKS_TOKEN=dapi...
```

Load in your shell when needed:

```bash
export $(grep -v '^#' .env | xargs)
```

### 3. Test the MCP server

From the project root (with env vars set):

```bash
python3 mcp-servers/databricks/server.py
```

Press `Ctrl+C` to stop. If you see `ERROR: Set DATABRICKS_HOST and DATABRICKS_TOKEN`, env vars are not loaded.

Optional : run with the MCP CLI inspector (browser UI to call tools):

```bash
mcp dev mcp-servers/databricks/server.py
```

### 4. Load the plugin in Cursor (local dev)

Cursor does **not** have an “Import plugin from folder” option. The plugin is loaded when the **workspace root** is the folder that contains `.cursor-plugin/`.

1. In Cursor: **File → Open Folder** and select the `databricks-job-reviewer` directory (the one that contains `.cursor-plugin/`). Do not open a parent or a subfolder.
2. Use **Developer: Reload Window** from the Command Palette if the plugin doesn’t appear.
3. Check **Settings → Rules** (plugin rules listed), **Settings → MCP** (`databricks` green), and in Agent chat type `/` (four commands).

The MCP server is started by Cursor using `.mcp.json`; it runs with Cursor’s `python3` and needs `DATABRICKS_HOST` and `DATABRICKS_TOKEN` in your environment (or set where Cursor reads env for MCP).

### 5. Project layout for contributors

| Directory | What to edit | Format |
|-----------|----------------|--------|
| `mcp-servers/databricks/` | MCP tools (Databricks API) | Python, `@mcp.tool()` |
| `agents/` | Subagent behavior/prompt | Markdown + YAML frontmatter |
| `skills/` | Analysis skills | `SKILL.md` in named subdirs |
| `rules/` | Always-on rules | `.mdc` + YAML frontmatter |
| `commands/` | Slash commands | `.md` + YAML frontmatter |

### 6. Adding a new MCP tool

1. Edit `mcp-servers/databricks/server.py`, add a function with `@mcp.tool()`, return a `str` (e.g. via `_fmt()` for JSON).
2. Restart the MCP server in Cursor (Settings → MCP → toggle databricks off/on).

### 7. Adding a new skill

Create `skills/<name>/SKILL.md` with YAML frontmatter (`name`, `description`) and “When to use” / “Instructions”. Reload Cursor to pick it up.

### 8. Adding a new rule

Create `rules/<name>.mdc` with frontmatter (`description`, `alwaysApply`, optional `globs`) and rule content.

### 9. Adding a new command

Create `commands/<name>.md` with frontmatter (`name`, `description`) and steps. It appears as `/<name>` in Agent chat.

---

## License

MIT
