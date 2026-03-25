"""Fabric REST API helpers — GET, POST, LRO polling.

IMPORTANT: All requests use allow_redirects=False to prevent following
redirects to the wabi-*-redirect.analysis.windows.net domain which hangs.
"""

import time
import requests

FABRIC_API = "https://api.fabric.microsoft.com/v1"


def fabric_get(session, path):
    r = requests.get(f"{FABRIC_API}{path}", headers=session.headers,
                     timeout=30, allow_redirects=False)
    return r.json() if r.ok else {"_error": r.status_code, "_body": r.text[:500]}


def fabric_post(session, path, body=None):
    r = requests.post(f"{FABRIC_API}{path}", headers=session.headers,
                      json=body or {}, timeout=60, allow_redirects=False)
    if r.status_code == 202:
        return _poll_lro(session, r)
    if r.ok:
        return r.json()
    return {"_error": r.status_code, "_body": r.text[:500]}


def _poll_lro(session, initial_response):
    import re
    op_id = initial_response.headers.get("x-ms-operation-id")
    if not op_id:
        loc = (initial_response.headers.get("Location")
               or initial_response.headers.get("Operation-Location") or "")
        m = re.search(r"/operations/([0-9a-f-]+)", loc)
        if m:
            op_id = m.group(1)

    if not op_id:
        return {"_error": 202, "_note": "Could not extract operation ID"}

    poll_url = f"{FABRIC_API}/operations/{op_id}"
    retry_after = min(int(initial_response.headers.get("Retry-After", "5")), 5)
    print(f"  LRO polling: op={op_id}, retry={retry_after}s")
    for attempt in range(40):
        time.sleep(max(retry_after, 3))
        try:
            r = requests.get(poll_url, headers=session.headers,
                             timeout=30, allow_redirects=False)
        except Exception as e:
            print(f"  LRO poll #{attempt+1}: error {e}")
            continue
        if not r.ok:
            print(f"  LRO poll #{attempt+1}: HTTP {r.status_code}")
            continue
        data = r.json()
        status = data.get("status", "")
        print(f"  LRO poll #{attempt+1}: status={status}")
        if status in ("Succeeded", "Completed"):
            try:
                rr = requests.get(f"{FABRIC_API}/operations/{op_id}/result",
                                  headers=session.headers, timeout=30,
                                  allow_redirects=False)
                if rr.ok:
                    return rr.json()
            except Exception:
                pass
            return data
        if status in ("Failed", "Cancelled"):
            return {"_error": "lro_failed", "_data": data}
    return {"_error": "lro_timeout"}
