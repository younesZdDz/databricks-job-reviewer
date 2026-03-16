# Databricks Job Reviewer — Cursor Plugin

A Cursor plugin that analyzes Spark applications on Databricks using the **driver-proxy Spark UI API** (`/api/v1`). It follows a **SQL → jobs → heaviest stages → quantiles → tasks/executors** workflow, detects issues with evidence, and suggests concrete improvements. You provide the task code file (required); findings are linked to file:line.

## What it does

Given **cluster_id**, **code file path** (required), and optional **app_id**, the plugin:

1. **Resolves the Spark application** — uses the first app on the cluster if app_id is omitted
2. **Starts from the right anchor** — ranks SQL executions by duration (or uses jobs directly), then inspects associated jobs and heaviest stages
3. **Uses quantiles first** — task summary with p01/p50/p99 to detect skew before opening the full task list
4. **Runs deterministic checks** — skew (p99 vs p50), spill, shuffle explosion, failures, GC pressure, executor imbalance
5. **Performs AI-driven analysis** — connects metrics to code and config (code file required)
6. **Produces a structured report** — findings with evidence, severity, and actionable recommendations

### Example findings

| Type | What you get |
|------|----------------|
| **Slowdown** | Root cause with evidence (e.g. "2.3x slower due to skew in Stage 12") |
| **Data skew** | p99 vs p50 from task summary; stage and code location (file:line) |
| **Shuffle explosion** | Tied to a join or aggregation in your code |
| **Memory / spill** | Disk spill metrics and suggested executor memory or partitioning |
| **Join efficiency** | Sort-merge vs broadcast with evidence from SQL plan |
| **Anti-patterns** | `.collect()`, UDFs, cross joins, bad repartition — with file:line (code file required) |

### Interesting use case: automated performance alerts

Set up an automation so that when a job's performance **exceeds a threshold** (e.g. duration or failure), the agent runs automatically to analyze that run and **open an issue** with the report and suggested improvements. For example, use [Cursor automations](https://cursor.com/docs/cloud-agent/automations) (or your CI/monitoring) to trigger on a Databricks job alert, then invoke the review command with the cluster and code path; the agent's output can be posted into a GitHub/GitLab issue or a Slack thread so the team gets evidence-based recommendations without manual triage.

---

## Quick start

### Prerequisites

- Python 3.11+
- [Cursor](https://cursor.com) IDE
- A Databricks workspace and [personal access token](https://docs.databricks.com/en/dev-tools/auth/pat.html)

### Step 1 — Clone and install

```bash
git clone https://github.com/your-org/databricks-job-reviewer.git
cd databricks-job-reviewer
bash scripts/install-plugin.sh
```

The script handles everything in one shot:
- Copies the plugin (commands, rules, skills, agents) to `~/.cursor/plugins/databricks-job-reviewer/`
- Copies the MCP server to the same location
- Creates a Python venv and installs dependencies there
- Prompts for your `DATABRICKS_HOST` and `DATABRICKS_TOKEN` and saves them to `~/.cursor/plugins/databricks-job-reviewer/mcp-servers/databricks/.env` (one-time, never repeated per project)
- Registers the plugin in Cursor
- Writes the MCP server entry directly into `~/.cursor/mcp.json` (Cursor's global config) — no per-project setup needed

### Step 2 — Restart Cursor and use

1. **Cmd+Shift+P** → `Developer: Reload Window` (or do a full restart — more reliable)
2. Check **Settings → MCP**: the `databricks` server should be green
3. In Agent chat:

```
/review-spark-job
```

When prompted:
- **cluster_id:** `0123-456789-abcdef` (cluster must be running)
- **Code file path:** `@src/jobs/daily_etl.py`
- **app_id:** (optional — leave blank to use the latest app on the cluster)

---

## Updating

Re-run the install script after pulling changes. Your credentials are preserved (the script never overwrites an existing `.env`).

```bash
git pull
bash scripts/install-plugin.sh
```

Then **Cmd+Shift+P** → `Developer: Reload Window`.

---

## Usage

### Command

| Command | What it does |
|--------|----------------|
| `/review-spark-job` | Analyze the Spark application and review the task code in one flow. |

**Inputs:**

- **cluster_id** (required): Databricks cluster ID (cluster must be running)
- **Code file path** (required): Local path to the task/job source (e.g. `@src/jobs/daily_etl.py`)
- **app_id** (optional): Spark application ID. If omitted, the latest application is used

The agent resolves the app, runs the full workflow (SQL/jobs → heaviest stages → quantiles → task list if needed → executors), reads your code, reviews for anti-patterns, and produces one report with findings linked to file:line.

**Note:** The plugin assumes the **cluster is already running**. It uses the driver-proxy Spark UI and does not start or manage clusters.

---

## Extending the plugin

### Project layout

| Directory | What to edit | Format |
|-----------|----------------|--------|
| `mcp-servers/databricks/` | MCP tools (driver-proxy + cluster API) | Python, `@mcp.tool()` |
| `agents/` | Subagent behavior/prompt | Markdown + YAML frontmatter |
| `skills/` | Analysis skills | `SKILL.md` in named subdirs |
| `rules/` | Always-on rules | `.mdc` + YAML frontmatter |
| `commands/` | Slash commands | `.md` + YAML frontmatter |

### Dev loop

1. Edit plugin sources in the repo
2. Run `bash scripts/install-plugin.sh`
3. **Cmd+Shift+P** → `Developer: Reload Window` (full restart is safer)
4. Test your changes in Agent chat
5. Repeat

There is no hot-reload or watch mode. You copy, restart, and test.

### Adding a new MCP tool

Edit `mcp-servers/databricks/server.py`, add a function with `@mcp.tool()`. Use `_get_proxy(cluster_id, path, params)` for Spark UI calls and `_get(path, params)` for Databricks REST (e.g. cluster info). Then restart the MCP server in Cursor (**Settings → MCP** → toggle `databricks` off/on).

### Adding a new skill

Create `skills/<name>/SKILL.md` with YAML frontmatter (`name`, `description`) and "When to use" / "Instructions" sections. Reload Cursor to pick it up.

### Adding a new rule

Create `rules/<name>.mdc` with frontmatter (`description`, `alwaysApply`, optional `globs`) and rule content.

### Adding a new command

Create `commands/<name>.md` with frontmatter (`name`, `description`) and steps. It appears as `/<name>` in Agent chat.

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

- **Deterministic first** — Metric-based checks (including quantiles) before AI analysis
- **Evidence-based** — Every finding cites specific metrics or log lines
- **No generic advice** — No vague suggestions without evidence
- **Link runtime to code** — Connect stage/issues to specific code lines when source is provided

### Skills (agent capabilities)

- **analyze-spark-stages** — Bottleneck stages; quantiles first, then task list; shuffle/spill
- **detect-skew** — Task/executor skew using quantiles and executor metrics
- **analyze-logs** — Parse user-provided logs; errors, OOM, stack traces; correlate with stage/executor data
- **review-spark-code** — Anti-patterns and correlation with runtime (stages, SQL plan)

---

## Plugin structure

```text
databricks-job-reviewer/
├── .cursor-plugin/
│   └── plugin.json
├── .env.example
├── mcp-servers/databricks/
│   ├── server.py
│   └── requirements.txt
├── agents/
├── skills/
├── rules/
├── commands/
├── scripts/
│   └── install-plugin.sh
└── README.md
```

---

## Planned improvements

- **Filter jobs by Spark job group** — When a cluster runs multiple tasks, accept an optional job group and filter to only analyze jobs that match, scoped to that task. (Spark UI `/jobs` returns `jobGroup` per job.)
- **Support stopped clusters** — Today the plugin requires a running cluster. A future improvement is to support analyzing past runs using Spark event logs or a history server.

---

## License

MIT
