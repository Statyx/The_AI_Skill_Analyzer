"""Fabric REST API helpers — GET, POST, LRO polling."""

import time
import requests

FABRIC_API = "https://api.fabric.microsoft.com/v1"


def fabric_get(session, path):
    r = requests.get(f"{FABRIC_API}{path}", headers=session.headers)
    return r.json() if r.ok else {"_error": r.status_code, "_body": r.text[:500]}


def fabric_post(session, path, body=None):
    r = requests.post(f"{FABRIC_API}{path}", headers=session.headers, json=body or {})
    if r.status_code == 202:
        return _poll_lro(session, r)
    if r.ok:
        return r.json()
    return {"_error": r.status_code, "_body": r.text[:500]}


def _poll_lro(session, initial_response):
    loc = (initial_response.headers.get("Location")
           or initial_response.headers.get("Operation-Location"))
    if not loc:
        return {"_error": 202, "_note": "LRO with no Location header"}
    print(f"  LRO polling: {loc[:80]}...")
    for attempt in range(30):
        time.sleep(2)
        r = requests.get(loc, headers=session.headers)
        if not r.ok:
            print(f"  LRO poll #{attempt+1}: HTTP {r.status_code}")
            continue
        data = r.json()
        status = data.get("status", "")
        print(f"  LRO poll #{attempt+1}: status={status}")
        if status in ("Succeeded", "Completed"):
            result_url = data.get("resourceLocation")
            if result_url:
                rr = requests.get(result_url, headers=session.headers)
                return rr.json() if rr.ok else data
            result_url = loc.rstrip("/") + "/result"
            rr = requests.get(result_url, headers=session.headers)
            if rr.ok:
                return rr.json()
            return data
        if status in ("Failed", "Cancelled"):
            return {"_error": "lro_failed", "_data": data}
    return {"_error": "lro_timeout"}
