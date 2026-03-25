"""Quick lookup: find workspace + agent + model IDs."""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import requests
from analyzer.auth import FabricSession
from analyzer.config import load_global_config

cfg = load_global_config()
# Dummy values to satisfy FabricSession init
cfg.update(workspace_id="dummy", agent_id="dummy", semantic_model_id="dummy",
           semantic_model_name="dummy", profile_name="lookup", data_agent_url="dummy")

session = FabricSession(cfg)
API = "https://api.fabric.microsoft.com/v1"

# 1. Find workspace
search = sys.argv[1] if len(sys.argv) > 1 else "finance"
print(f"\n=== Searching workspaces for '{search}' ===")
r = requests.get(f"{API}/workspaces", headers=session.headers)
if not r.ok:
    print(f"Error listing workspaces: {r.status_code}")
    sys.exit(1)

matches = [ws for ws in r.json().get("value", [])
           if search.lower() in ws.get("displayName", "").lower()]
for ws in matches:
    print(f"  WS: {ws['displayName']}  id={ws['id']}")

if not matches:
    print("  No matching workspaces found.")
    sys.exit(1)

ws_id = matches[0]["id"]
ws_name = matches[0]["displayName"]
print(f"\n=== Items in '{ws_name}' ===")

# 2. List all items
r = requests.get(f"{API}/workspaces/{ws_id}/items", headers=session.headers)
if not r.ok:
    print(f"Error listing items: {r.status_code}")
    sys.exit(1)

items = r.json().get("value", [])
agents = [i for i in items if i.get("type") in ("AISkill", "DataAgent")]
models = [i for i in items if i.get("type") == "SemanticModel"]

print(f"\n  Agents ({len(agents)}):")
for a in agents:
    print(f"    {a['displayName']}  id={a['id']}  type={a['type']}")

print(f"\n  Semantic Models ({len(models)}):")
for m in models:
    print(f"    {m['displayName']}  id={m['id']}")

print(f"\n=== Summary ===")
print(f"  workspace_id: \"{ws_id}\"")
if agents:
    print(f"  agent_id: \"{agents[0]['id']}\"")
if models:
    # Try to find a model that matches agent name
    for m in models:
        print(f"  semantic_model_id: \"{m['id']}\"  # {m['displayName']}")
