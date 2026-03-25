import json, sys
raw = json.load(open(sys.argv[1]))
workspaces = raw.get("results", raw).get("workspaces", raw) if isinstance(raw, dict) else raw
for w in workspaces:
    name = w.get("displayName", "") if isinstance(w, dict) else ""
    ws_id = w.get("metadata", {}).get("workspaceObjectId", "?") if isinstance(w, dict) else "?"
    if "finance" in name.lower() or "cdr" in name.lower() or "demo" in name.lower():
        print(f"  {name:55s}  {ws_id}")
