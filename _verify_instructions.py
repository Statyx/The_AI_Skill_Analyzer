"""Verify current agent instructions contain BPA rules."""
import json, base64, subprocess, time, requests

WS = "5fa6b81d-fabe-4363-ad3d-b09ef82d16f2"
AGENT = "e92e5867-213a-4a7d-8fac-af1711046527"
API = "https://api.fabric.microsoft.com/v1"

r = subprocess.run(
    "az account get-access-token --resource https://api.fabric.microsoft.com --query accessToken -o tsv",
    capture_output=True, text=True, shell=True,
)
token = r.stdout.strip()
h = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

r = requests.post(f"{API}/workspaces/{WS}/items/{AGENT}/getDefinition", headers=h, json={}, timeout=60)
op_id = r.headers.get("x-ms-operation-id", "")
retry = int(r.headers.get("Retry-After", "5"))
print(f"Fetching definition... (wait {retry}s)")

for i in range(20):
    time.sleep(retry)
    pr = requests.get(f"{API}/operations/{op_id}", headers=h, timeout=30)
    st = pr.json().get("status", "")
    if st == "Succeeded":
        rr = requests.get(f"{API}/operations/{op_id}/result", headers=h, timeout=60)
        parts = rr.json().get("definition", {}).get("parts", [])
        for p in parts:
            if "draft/stage_config" in p.get("path", ""):
                decoded = base64.b64decode(p["payload"]).decode()
                cfg = json.loads(decoded)
                inst = cfg.get("aiInstructions", "")
                checks = ["REMOVEFILTERS", "VAR/RETURN", "Never use ==", "DIVIDE(numerator"]
                for c in checks:
                    found = c in inst
                    print(f"  {'OK' if found else 'MISSING'}: {c}")
                if all(c in inst for c in checks):
                    print("\nSUCCESS: All 4 BPA instructions are present!")
                else:
                    print("\nWARNING: Some BPA instructions are missing")
        break
    if st in ("Failed", "Cancelled"):
        print(f"Failed: {pr.json()}")
        break
