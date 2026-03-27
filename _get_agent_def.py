"""Temp script: get agent definition from Fabric API using az CLI token."""
import json, base64, time, subprocess, requests

WS = "5fa6b81d-fabe-4363-ad3d-b09ef82d16f2"
AGENT_ID = "e92e5867-213a-4a7d-8fac-af1711046527"
API = "https://api.fabric.microsoft.com/v1"

# Get token via az CLI (same approach as the template)
result = subprocess.run(
    "az account get-access-token --resource https://api.fabric.microsoft.com --query accessToken -o tsv",
    capture_output=True, text=True, shell=True,
)
token = result.stdout.strip()
if not token:
    print("ERROR: Run az login first")
    exit(1)
print("Token acquired")

h = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

url = f"{API}/workspaces/{WS}/items/{AGENT_ID}/getDefinition"
r = requests.post(url, headers=h, json={}, timeout=60)
print(f"Status: {r.status_code}")

if r.status_code == 202:
    op_id = r.headers.get("x-ms-operation-id", "")
    retry = int(r.headers.get("Retry-After", "5"))
    print(f"Operation: {op_id}, retry: {retry}s")
    for i in range(20):
        time.sleep(retry)
        pr = requests.get(f"{API}/operations/{op_id}", headers=h, timeout=30)
        data = pr.json()
        status = data.get("status", "")
        print(f"Poll {i+1}: {status}")
        if status == "Succeeded":
            rr = requests.get(f"{API}/operations/{op_id}/result", headers=h, timeout=60)
            defn = rr.json()
            parts = defn.get("definition", {}).get("parts", [])
            with open("_agent_def_raw.json", "w", encoding="utf-8") as f:
                json.dump(parts, f, indent=2)
            for p in parts:
                path = p.get("path", "")
                print(f"\n=== {path} ===")
                payload = p.get("payload", "")
                if payload:
                    decoded = base64.b64decode(payload).decode("utf-8")
                    print(decoded)
            break
        if status in ("Failed", "Cancelled"):
            print(f"Failed: {data}")
            break
elif r.status_code == 200:
    defn = r.json()
    parts = defn.get("definition", {}).get("parts", [])
    with open("_agent_def_raw.json", "w", encoding="utf-8") as f:
        json.dump(parts, f, indent=2)
    for p in parts:
        path = p.get("path", "")
        print(f"\n=== {path} ===")
        payload = p.get("payload", "")
        if payload:
            decoded = base64.b64decode(payload).decode("utf-8")
            print(decoded)
else:
    print(r.text[:500])
