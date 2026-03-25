"""
Step 1: Get TMDL definition and save each file locally.
Step 2: Add descriptions.
Step 3: Push back.
"""
import base64, json, time, sys, os, re, requests
from azure.identity import AzureCliCredential

WORKSPACE_ID = "133c6c70-2e26-4d97-aac1-8ed423dbbf34"
MODEL_ID = "236080b8-3bea-4c14-86df-d1f9a14ac7a8"
API = "https://api.fabric.microsoft.com/v1"
SAVE_DIR = "_tmdl_raw"

cred = AzureCliCredential()
token = cred.get_token("https://api.fabric.microsoft.com/.default").token
session = requests.Session()
session.headers.update({"Authorization": f"Bearer {token}", "Content-Type": "application/json"})


def poll_lro(resp, label=""):
    op_id = resp.headers.get("x-ms-operation-id")
    if not op_id:
        print(f"  [{label}] No operation ID", flush=True)
        return None
    poll_url = f"{API}/operations/{op_id}"
    retry_after = int(resp.headers.get("Retry-After", "10"))
    print(f"  [{label}] op={op_id} retry={retry_after}s", flush=True)

    for i in range(40):
        time.sleep(max(retry_after, 5))
        print(f"  [{label}] Poll #{i+1}...", end=" ", flush=True)
        try:
            r = session.get(poll_url, timeout=30)
        except Exception as e:
            print(f"Err: {e}", flush=True)
            continue
        if not r.ok:
            print(f"HTTP {r.status_code}", flush=True)
            continue
        data = r.json()
        status = data.get("status", "")
        print(f"{status}", flush=True)
        if status in ("Succeeded", "Completed"):
            try:
                rr = session.get(f"{API}/operations/{op_id}/result", timeout=30)
                return rr.json() if rr.ok else data
            except:
                return data
        if status in ("Failed", "Cancelled"):
            print(f"  FAILED: {json.dumps(data)[:600]}", flush=True)
            return None
    return None


# ── Step 1: Get definition ──
print("=== Step 1: Get TMDL definition ===", flush=True)
resp = session.post(f"{API}/workspaces/{WORKSPACE_ID}/semanticModels/{MODEL_ID}/getDefinition", timeout=30)
print(f"  Status: {resp.status_code}", flush=True)

if resp.status_code == 200:
    definition = resp.json()
elif resp.status_code == 202:
    definition = poll_lro(resp, "GET")
else:
    print(f"  ERROR: {resp.text[:500]}", flush=True)
    sys.exit(1)

parts = definition["definition"]["parts"]
print(f"  Got {len(parts)} parts", flush=True)

# Save locally
os.makedirs(SAVE_DIR, exist_ok=True)
for p in parts:
    path = p["path"]
    content = base64.b64decode(p["payload"])
    local_path = os.path.join(SAVE_DIR, path.replace("/", os.sep))
    os.makedirs(os.path.dirname(local_path), exist_ok=True) if os.path.dirname(local_path) else None
    with open(local_path, "wb") as f:
        f.write(content)

# Print the first table
for p in parts:
    if "dim_chart_of_accounts" in p["path"]:
        content = base64.b64decode(p["payload"]).decode("utf-8")
        print(f"\n=== {p['path']} (first 30 lines) ===", flush=True)
        for i, line in enumerate(content.split("\n")[:30], 1):
            print(f"  {i:3d}| {repr(line)}", flush=True)
        break

print(f"\nTMDL files saved to {SAVE_DIR}/", flush=True)
