# Databricks Job Reviewer : Cursor Plugin

A Cursor plugin that analyzes Spark applications on Databricks using the **driver-proxy Spark UI API** (`/api/v1`). It follows a **SQL → jobs → heaviest stages → quantiles → tasks/executors** workflow, detects issues with evidence, and suggests concrete improvements. You provide the task code file (required); findings are linked to file:line.

## What it does

Given **cluster_id**, **code file path** (required), and optional **app_id**, the plugin:

1. **Resolves the Spark application** : uses the first app on the cluster if app_id is omitted
2. **Starts from the right anchor** : ranks SQL executions by duration (or uses jobs directly), then inspects associated jobs and heaviest stages
3. **Uses quantiles first** : task summary with p01/p50/p99 to detect skew before opening the full task list
4. **Runs deterministic checks** : skew (p99 vs p50), spill, shuffle explosion, failures, GC pressure, executor imbalance
5. **Performs AI-driven analysis** : connects metrics to code and config (code file required)
6. **Produces a structured report** : findings with evidence, severity, and actionable recommendations

### Example findings

| Type | What you get |
|------|----------------|
| **Slowdown** | Root cause with evidence (e.g. “2.3x slower due to skew in Stage 12”) |
| **Data skew** | p99 vs p50 from task summary; stage and code location (file:line) |
| **Shuffle explosion** | Tied to a join or aggregation in your code |
| **Memory / spill** | Disk spill metrics and suggested executor memory or partitioning |
| **Join efficiency** | Sort-merge vs broadcast with evidence from SQL plan |
| **Anti-patterns** | `.collect()`, UDFs, cross joins, bad repartition : with file:line (code file required) |

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
- **DATABRICKS_TOKEN** : A [Databricks personal access token](https://docs.databricks.com/en/dev-tools/auth/pat.html). For driver-proxy and cluster info: at least **Cluster** read access.

If Cursor doesn’t inherit your shell env, set these in your OS user environment or in Cursor’s MCP server config (Settings → MCP → databricks → env).

### 3. Verify

- **Settings → Rules**: You should see the plugin’s rules (e.g. deterministic-first, evidence-based).
- **Settings → MCP**: The `databricks` server should be listed and show a green status.
- In **Agent chat**, type `/` : you should see: `/review-spark-job`.

---

## Usage : One command

All usage happens in **Cursor Agent chat**. One command does both **analysis** (runtime: SQL/jobs → stages → quantiles → executors) and **code review** (anti-patterns, file:line links).

### Command

| Command | What it does |
|--------|----------------|
| `/review-spark-job` | Analyze the Spark application and review the task code in one flow. |

**Inputs:**

- **cluster_id** (required): Databricks cluster ID.
- **Code file path** (required): Local path to the task/job source (e.g. `@src/jobs/daily_etl.py`).
- **app_id** (optional): Spark application ID. If omitted, the latest application is used (first from the cluster).

**In Agent chat:**

```
/review-spark-job
```

When prompted:

- **cluster_id:** `0123-456789-abcdef`
- **Code file path:** `@src/jobs/daily_etl.py`
- **app_id:** (optional : leave blank to use the latest app on the cluster)

The agent resolves the app (or uses your app_id), runs the full workflow (SQL/jobs → heaviest stages → quantiles → task list if needed → executors), reads your code, reviews for anti-patterns, and produces one report with findings linked to file:line.

---

## Reference

### MCP tools (driver-proxy Spark UI + cluster)

The MCP server uses the **driver-proxy** base URL:  
`https://{DATABRICKS_HOST}/driver-proxy-api/o/0/{cluster_id}/40001/api/v1`

| Tool | Description |
|------|-------------|
| `list_applications` | List Spark applications on the cluster (optional date/limit filters) |
| `get_application` | Details for one application |
| `get_sql_executions` | List SQL executions (rank by duration); best entry for SQL/DataFrame workloads |
| `get_sql_execution` | One SQL execution with plan and associated job ids |
| `get_jobs` | List jobs for the application |
| `get_job_detail` | One job: stages, durations, shuffle, I/O |
| `get_stages` | List stages (prefer narrowing by job first) |
| `get_stage_attempt` | Stage attempt with quantiles (use before task list) |
| `get_stage_task_summary` | Task summary quantiles (p01/p50/p99) to detect skew |
| `get_stage_task_list` | Task list (use only after quantiles show a problem; sortBy=-runtime) |
| `get_executors` | Active executors |
| `get_all_executors` | All executors (active + dead) for imbalance and GC |
| `get_environment` | Application environment (Spark config, JVM) |
| `get_cluster_info` | Cluster config and state (Databricks API) |

### Rules (always-on guidance)

- **Deterministic first** : Metric-based checks (including quantiles) before AI analysis.
- **Evidence-based** : Every finding cites specific metrics or log lines.
- **No generic advice** : No vague suggestions without evidence.
- **Link runtime to code** : Connect stage/issues to specific code lines when source is provided.

### Skills (agent capabilities)

- **analyze-spark-stages** : Bottleneck stages; quantiles first, then task list; shuffle/spill.
- **detect-skew** : Task/executor skew using quantiles and executor metrics.
- **analyze-logs** : Parse user-provided logs; errors, OOM, stack traces; correlate with stage/executor data.
- **review-spark-code** : Anti-patterns and correlation with runtime (stages, SQL plan).

---

## Plugin structure

```text
databricks-job-reviewer/
├── .cursor-plugin/
│   └── plugin.json
├── mcp.json
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

Note: Most tools require a valid **cluster_id** (and optional **app_id**). Use a running cluster’s ID from your workspace.

### 4. Load the plugin in Cursor (local dev)

1. In Cursor: **File → Open Folder** and select the `databricks-job-reviewer` directory (the one that contains `.cursor-plugin/`).
2. Use **Developer: Reload Window** from the Command Palette if the plugin doesn’t appear.
3. Check **Settings → Rules**, **Settings → MCP** (`databricks` green), and in Agent chat type `/` (the `/review-spark-job` command).

The MCP server is started by Cursor using `mcp.json`; it needs `DATABRICKS_HOST` and `DATABRICKS_TOKEN` in the environment (or where Cursor reads env for MCP).

### 5. Project layout for contributors

| Directory | What to edit | Format |
|-----------|----------------|--------|
| `mcp-servers/databricks/` | MCP tools (driver-proxy + cluster API) | Python, `@mcp.tool()` |
| `agents/` | Subagent behavior/prompt | Markdown + YAML frontmatter |
| `skills/` | Analysis skills | `SKILL.md` in named subdirs |
| `rules/` | Always-on rules | `.mdc` + YAML frontmatter |
| `commands/` | Slash commands | `.md` + YAML frontmatter |

### 6. Adding a new MCP tool

1. Edit `mcp-servers/databricks/server.py`, add a function with `@mcp.tool()`, return a `str` (e.g. via `_fmt()` for JSON). Use `_get_proxy(cluster_id, path, params)` for Spark UI and `_get(path, params)` for Databricks REST (e.g. cluster).
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
