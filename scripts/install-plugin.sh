#!/usr/bin/env bash
# based on: https://medium.com/@v.tajzich/how-to-write-and-test-cursor-plugins-locally-the-part-the-docs-dont-tell-you-4eee705d7f76
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PLUGIN_NAME="databricks-job-reviewer"
PLUGIN_ID="${PLUGIN_NAME}@local"
TARGET="$HOME/.cursor/plugins/$PLUGIN_NAME"
CLAUDE_PLUGINS="$HOME/.claude/plugins/installed_plugins.json"
CLAUDE_SETTINGS="$HOME/.claude/settings.json"
MCP_SERVER="$TARGET/mcp-servers/databricks/server.py"
VENV="$TARGET/.venv"
ENV_FILE="$TARGET/mcp-servers/databricks/.env"

echo "==> Installing $PLUGIN_NAME to $TARGET"

# 1. Copy plugin files (including mcp-servers/)
rm -rf "$TARGET"
mkdir -p "$TARGET" "$HOME/.claude/plugins"
for dir in .cursor-plugin commands rules skills scripts mcp-servers; do
  [[ -d "$REPO_ROOT/$dir" ]] && cp -R "$REPO_ROOT/$dir" "$TARGET/"
done

# 2. Create a venv and install Python dependencies
echo "==> Setting up Python environment"
python3 -m venv "$VENV"
"$VENV/bin/pip" install --quiet --upgrade pip
"$VENV/bin/pip" install --quiet -r "$TARGET/mcp-servers/databricks/requirements.txt"

# 3. Credentials: write .env once (never overwrite existing)
if [[ -f "$ENV_FILE" ]]; then
  echo "==> Credentials file already exists at $ENV_FILE — skipping prompt."
  echo "    Edit that file to change DATABRICKS_HOST or DATABRICKS_TOKEN."
else
  echo ""
  echo "==> Databricks credentials (press Enter to skip and set them later)"
  read -rp "    DATABRICKS_HOST (e.g. https://adb-xxxx.azuredatabricks.net): " db_host
  read -rsp "    DATABRICKS_TOKEN (dapi...): " db_token
  echo ""
  if [[ -n "$db_host" || -n "$db_token" ]]; then
    cat > "$ENV_FILE" <<EOF
DATABRICKS_HOST=${db_host}
DATABRICKS_TOKEN=${db_token}
EOF
    echo "==> Credentials saved to $ENV_FILE"
  else
    echo "==> Skipped. Add DATABRICKS_HOST and DATABRICKS_TOKEN to $ENV_FILE before using the plugin."
  fi
fi

# 4. Register in installed_plugins.json (upsert, don't clobber)
python3 - "$CLAUDE_PLUGINS" "$PLUGIN_ID" "$TARGET" <<'PY'
import json, os, sys
path, pid, ipath = sys.argv[1], sys.argv[2], sys.argv[3]
data = {}
if os.path.exists(path):
    try: data = json.load(open(path))
    except: data = {}
plugins = data.get("plugins", {})
entries = [e for e in plugins.get(pid, [])
           if not (isinstance(e, dict) and e.get("scope") == "user")]
entries.insert(0, {"scope": "user", "installPath": ipath})
plugins[pid] = entries
data["plugins"] = plugins
os.makedirs(os.path.dirname(path), exist_ok=True)
json.dump(data, open(path, "w"), indent=2)
PY

# 5. Enable in settings.json (upsert, don't clobber)
python3 - "$CLAUDE_SETTINGS" "$PLUGIN_ID" <<'PY'
import json, os, sys
path, pid = sys.argv[1], sys.argv[2]
data = {}
if os.path.exists(path):
    try: data = json.load(open(path))
    except: data = {}
data.setdefault("enabledPlugins", {})[pid] = True
os.makedirs(os.path.dirname(path), exist_ok=True)
json.dump(data, open(path, "w"), indent=2)
PY

# 6. Register the MCP server in Cursor's global ~/.cursor/mcp.json
PYTHON_BIN="$VENV/bin/python"
CURSOR_MCP="$HOME/.cursor/mcp.json"
python3 - "$CURSOR_MCP" "$PYTHON_BIN" "$MCP_SERVER" <<'PY'
import json, os, sys
path, python_bin, server_path = sys.argv[1], sys.argv[2], sys.argv[3]
data = {}
if os.path.exists(path):
    try: data = json.load(open(path))
    except: data = {}
data.setdefault("mcpServers", {})["databricks"] = {
    "command": python_bin,
    "args": [server_path]
}
os.makedirs(os.path.dirname(path), exist_ok=True)
json.dump(data, open(path, "w"), indent=2)
PY

echo ""
echo "================================================================"
echo " Done! MCP server registered in $CURSOR_MCP"
echo " Restart Cursor (Cmd+Shift+P → 'Developer: Reload Window')"
echo " then check Settings → MCP — 'databricks' should be green."
echo " Use /review-spark-job in Agent chat."
echo "================================================================"
