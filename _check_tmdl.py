"""Check the TMDL content of dim_chart_of_accounts."""
import base64, json, time, requests
from azure.identity import AzureCliCredential

WORKSPACE_ID = "133c6c70-2e26-4d97-aac1-8ed423dbbf34"
MODEL_ID = "236080b8-3bea-4c14-86df-d1f9a14ac7a8"
API = "https://api.fabric.microsoft.com/v1"

cred = AzureCliCredential()
token = cred.get_token("https://api.fabric.microsoft.com/.default").token
session = requests.Session()
session.headers.update({"Authorization": f"Bearer {token}", "Content-Type": "application/json"})

resp = session.post(f"{API}/workspaces/{WORKSPACE_ID}/semanticModels/{MODEL_ID}/getDefinition", timeout=30)
print(f"Status: {resp.status_code}")

if resp.status_code == 202:
    op_id = resp.headers.get("x-ms-operation-id")
    poll_url = f"{API}/operations/{op_id}"
    for i in range(20):
        time.sleep(10)
        r = session.get(poll_url, timeout=30)
        if r.ok:
            data = r.json()
            if data.get("status") in ("Succeeded", "Completed"):
                rr = session.get(f"{API}/operations/{op_id}/result", timeout=30)
                definition = rr.json() if rr.ok else None
                break
    else:
        print("LRO timeout")
        exit(1)
elif resp.status_code == 200:
    definition = resp.json()

parts = definition["definition"]["parts"]
for p in parts:
    if "dim_chart_of_accounts" in p["path"]:
        content = base64.b64decode(p["payload"]).decode("utf-8")
        print(f"\n=== {p['path']} ===")
        for i, line in enumerate(content.split("\n")[:40], 1):
            print(f"{i:3d}| {repr(line)}")
        break
