"""Quick debug: test TMDL getDefinition and examine format."""
import sys, os, json, base64, requests
sys.path.insert(0, os.path.join(os.environ["TEMP"], "fabric_data_agent_client"))
from azure.identity import InteractiveBrowserCredential

TENANT_ID = "92701a21-ddea-4028-ba85-4c1f91fab881"
WORKSPACE_ID = "5fa6b81d-fabe-4363-ad3d-b09ef82d16f2"
SEMANTIC_MODEL_ID = "3d00aeaa-91b9-4567-9166-fa3fc8249e6f"
FABRIC_API = "https://api.fabric.microsoft.com/v1"

cred = InteractiveBrowserCredential(tenant_id=TENANT_ID)
token = cred.get_token("https://api.fabric.microsoft.com/.default").token

import time

# Get definition via LRO
url = f"{FABRIC_API}/workspaces/{WORKSPACE_ID}/semanticModels/{SEMANTIC_MODEL_ID}/getDefinition?format=TMDL"
r = requests.post(url, headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"})

defn = None
if r.status_code == 202:
    loc = r.headers.get("Location") or r.headers.get("Operation-Location")
    for _ in range(30):
        time.sleep(2)
        pr = requests.get(loc, headers={"Authorization": f"Bearer {token}"})
        if pr.ok:
            data = pr.json()
            if data.get("status") == "Succeeded":
                result_url = loc.rstrip("/") + "/result"
                rr = requests.get(result_url, headers={"Authorization": f"Bearer {token}"})
                if rr.ok:
                    defn = rr.json()
                break
elif r.ok:
    defn = r.json()

out = []
if defn and "definition" in defn:
    parts = defn["definition"]["parts"]
    out.append(f"Total parts: {len(parts)}")
    for p in parts:
        out.append(f"\n--- {p['path']} ---")
        payload = p.get("payload", "")
        if p.get("payloadType") == "InlineBase64" and payload:
            decoded = base64.b64decode(payload).decode("utf-8")
            # Only show first table file in detail
            if "crm_customers" in p["path"].lower() or "crm_customer" in p["path"].lower():
                out.append(decoded[:2000])
            else:
                out.append(f"({len(decoded)} chars)")
        else:
            out.append(str(payload)[:200])
else:
    out.append(f"Failed to get definition. Status: {r.status_code}")
    out.append(r.text[:500] if r.text else "No body")

with open("debug_tmdl.txt", "w", encoding="utf-8") as f:
    f.write("\n".join(out))
print("Done -> debug_tmdl.txt")
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

out = []
out.append("=== Test 1: DMV query ===")
r = requests.post(url, headers=headers, json={
    "queries": [{"query": "SELECT [ID], [Name], [Description] FROM $SYSTEM.TMSCHEMA_TABLES"}],
    "serializerSettings": {"includeNulls": True},
})
out.append(f"Status: {r.status_code}")
out.append(json.dumps(r.json(), indent=2, default=str)[:3000])

# Test 2: DAX INFO function (fallback)
out.append("\n=== Test 2: DAX INFO.TABLES() ===")
r2 = requests.post(url, headers=headers, json={
    "queries": [{"query": "EVALUATE INFO.TABLES()"}],
    "serializerSettings": {"includeNulls": True},
})
out.append(f"Status: {r2.status_code}")
data2 = r2.json()
try:
    rows = data2["results"][0]["tables"][0]["rows"]
    out.append(f"Rows: {len(rows)}")
    if rows:
        out.append(f"Keys: {list(rows[0].keys())}")
        out.append(json.dumps(rows[0], indent=2, default=str)[:500])
except Exception as e:
    out.append(f"Error parsing: {e}")
    out.append(json.dumps(data2, indent=2, default=str)[:2000])

with open("debug_output.txt", "w", encoding="utf-8") as f:
    f.write("\n".join(out))
print("Done -> debug_output.txt")
