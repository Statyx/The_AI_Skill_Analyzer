"""Debug LRO polling for agent getDefinition."""
import requests, time
from azure.identity import AzureCliCredential

cred = AzureCliCredential()
tok = cred.get_token("https://api.fabric.microsoft.com/.default").token
API = "https://api.fabric.microsoft.com/v1"
ws = "133c6c70-2e26-4d97-aac1-8ed423dbbf34"
agent = "01668d9d-0963-46cd-85ac-ee344daf714b"

session = requests.Session()
session.headers.update({"Authorization": f"Bearer {tok}", "Content-Type": "application/json"})
session.max_redirects = 0  # Prevent all redirects

r = session.post(f"{API}/workspaces/{ws}/items/{agent}/getDefinition", timeout=30, allow_redirects=False)
print(f"Status: {r.status_code}", flush=True)
op_id = r.headers.get("x-ms-operation-id")
print(f"op_id: {op_id}", flush=True)

if r.status_code == 202 and op_id:
    time.sleep(5)
    r2 = session.get(f"{API}/operations/{op_id}", timeout=30, allow_redirects=False)
    print(f"Poll: {r2.status_code} - {r2.text[:200]}", flush=True)
    
    if r2.ok and r2.json().get("status") == "Succeeded":
        print("Trying /result with 10s timeout...", flush=True)
        try:
            r3 = session.get(f"{API}/operations/{op_id}/result", timeout=(5, 10), allow_redirects=False)
            print(f"Result: {r3.status_code}", flush=True)
            if r3.ok:
                print(f"Keys: {list(r3.json().keys())}", flush=True)
            else:
                print(f"Body: {r3.text[:200]}", flush=True)
        except requests.exceptions.Timeout:
            print("TIMEOUT on /result - endpoint hangs", flush=True)
            print("Trying item-scoped endpoint instead...", flush=True)
            # Try the item-scoped result
            try:
                url = f"{API}/workspaces/{ws}/items/{agent}/getDefinition"
                print(f"Re-POSTing to: {url}", flush=True)
                r4 = session.post(url, timeout=(5, 30), allow_redirects=False)
                print(f"Re-POST status: {r4.status_code}", flush=True)
                if r4.ok:
                    print(f"Direct result keys: {list(r4.json().keys())}", flush=True)
                else:
                    print(f"Body: {r4.text[:200]}", flush=True)
            except Exception as e:
                print(f"Error: {e}", flush=True)
elif r.status_code == 200:
    print(f"Body: {r.text[:300]}", flush=True)
else:
    print(f"Error: {r.text[:300]}", flush=True)
