# Spec: Accelerating The AI Skill Analyzer — Single Identity

**Status**: Draft  
**Date**: 2026-04-08  
**Scope**: SDK `fabric_data_agent_client.py`, `analyzer/runner.py`, `analyzer/auth.py`  
**Constraint**: Single user identity (Azure CLI `az login`), no service principals

---

## 1. Problem Statement

Running 20 questions serially takes **~10 minutes** (~30s/question). The `--parallel` mode is broken: 404 errors, thread collisions, stale responses.

### Hard Truth

**True parallelism is impossible with a single identity.** Fabric returns the **same thread** for every `POST /threads` call with the same (agent, user) pair. Multiple workers all get the same thread_id, then stomp on each other — DELETE destroys another worker's active run → 404.

This spec focuses on **optimizing the serial path** to cut ~30-40% off wall time without requiring multiple identities.

### Current vs Target

| Metric | Current | Target |
|--------|---------|--------|
| Avg per question | ~30s | ~20s |
| 20 questions | ~600s | ~400s |
| 404 error rate | ~30% | 0% |

---

## 2. Why Parallel Fails (Single Identity)

### 2.1 One Thread Per (Agent, User) — Unfixable

```
Worker-1: POST /threads → thread_abc
Worker-2: POST /threads → thread_abc   ← SAME thread
Worker-1: DELETE thread_abc             ← kills Worker-2's context
Worker-2: POST /messages → 404         ← thread gone
```

No amount of locking or client-per-worker fixes this. The thread is a Fabric-side singleton.

### 2.2 Token Refresh Race — Fixable but Irrelevant for Serial

`FabricSession._ensure_token()` reads/writes `self._token` without a lock. Harmless in serial mode but would need `threading.Lock` if parallelism were viable.

### 2.3 404 Eventual Consistency — Fixable

After DELETE → POST (recreate), the new thread takes ~1-2s to become usable. The current `time.sleep(1)` is fragile. Needs retry-on-404 inside the SDK.

---

## 3. Acceleration Strategy

Since the bottleneck is **server-side DAX execution** (~15-25s) not client I/O, the wins come from reducing per-question overhead.

### Time Budget Per Question (Current)

```
Thread DELETE:        ~300ms
Thread CREATE:        ~200ms
Sleep (consistency):  ~1000ms
Cancel stale runs:    ~500ms  (sometimes)
POST message:         ~400ms
POST run:             ~500ms
Poll (2s × ~10):      ~20000ms  ← server processing, not reducible
GET messages:         ~300ms
GET steps:            ~300ms
─────────────────────────────
Total:                ~24s
Overhead (non-poll):  ~3.2s
```

### Reducible Overhead

| Optimization | Saves | How |
|---|---|---|
| Skip DELETE when thread is clean | ~1.3s | Reuse thread, DELETE every N |
| Adaptive polling (0.5s start) | ~3-5s | Fast answers detected sooner |
| Parallel GET messages + steps | ~0.3s | 2 concurrent requests |
| Connection pooling | ~0.5s | `requests.Session` reuse |
| Retry 404 inside SDK | N/A | Eliminates error-induced retries (saves ~30s on affected questions) |

**Combined**: ~5-7s saved per question → **~20-23s/question** → **~400-460s for 20 questions**

---

## 4. Implementation Changes

### 4.1 SDK: `requests.Session` for Connection Pooling

```python
# fabric_data_agent_client.py — __init__ or module level
class FabricDataAgentClient:
    def __init__(self, ...):
        ...
        self._http = requests.Session()
        self._http.headers.update({"Content-Type": "application/json"})
```

Replace every bare `requests.get/post/delete` in `get_raw_run_response` with `self._http.*`. TCP connections are reused across calls to the same host.

### 4.2 SDK: Adaptive Polling

Replace the fixed `time.sleep(2)` poll loop:

```python
# Current (get_raw_run_response, poll loop):
while run_status not in ("completed", "failed", "cancelled", "expired"):
    time.sleep(2)
    ...

# New:
_POLL_INTERVALS = [0.5, 0.5, 1, 1, 2, 2] + [3] * 50

poll_idx = 0
while run_status not in ("completed", "failed", "cancelled", "expired"):
    if time.time() - start_time > timeout:
        break
    time.sleep(_POLL_INTERVALS[min(poll_idx, len(_POLL_INTERVALS) - 1)])
    poll_idx += 1
    poll_r = self._http.get(...)
    ...
```

Fast answers (~10s) are detected 3-5s earlier. Long answers (~25s) have the same behavior as before.

### 4.3 SDK: Parallel Message + Steps Retrieval

After run completes, fetch both in parallel:

```python
from concurrent.futures import ThreadPoolExecutor

# After poll loop completes:
with ThreadPoolExecutor(max_workers=2) as pool:
    msgs_future = pool.submit(
        self._http.get,
        f"{base_url}/threads/{thread_id}/messages",
        headers=headers, params={**params, "limit": 10, "order": "desc"},
        timeout=30)
    steps_future = pool.submit(
        self._http.get,
        f"{base_url}/threads/{thread_id}/runs/{run_id}/steps",
        headers=headers, params={**params, "limit": 100},
        timeout=30)
    msgs_r = msgs_future.result()
    steps_r = steps_future.result()
```

### 4.4 SDK: Smart Thread Recycling

Instead of DELETE→sleep→CREATE every question, reuse the thread and only reset every N questions:

```python
# New instance variables
self._thread_id_cache = None
self._thread_use_count = 0
_THREAD_RECYCLE_EVERY = 8  # DELETE and recreate every N questions

def _get_thread(self, headers, params, force_reset=False):
    """Get a usable thread. Reuse when possible, reset every N uses."""
    base_url = self.data_agent_url.removesuffix("/")

    # Reuse cached thread if not stale
    if (self._thread_id_cache
            and not force_reset
            and self._thread_use_count < _THREAD_RECYCLE_EVERY):
        self._thread_use_count += 1
        return self._thread_id_cache

    # Get current thread
    r = self._http.post(f"{base_url}/threads", headers=headers,
                        json={}, params=params, timeout=30)
    r.raise_for_status()
    thread_id = r.json()["id"]

    # Delete to clear accumulated messages
    delete_params = {"api-version": params["api-version"]}
    try:
        self._http.delete(f"{base_url}/threads/{thread_id}",
                          headers=headers, params=delete_params, timeout=15)
    except Exception:
        pass
    time.sleep(1)

    # Recreate
    r = self._http.post(f"{base_url}/threads", headers=headers,
                        json={}, params=params, timeout=30)
    r.raise_for_status()
    self._thread_id_cache = r.json()["id"]
    self._thread_use_count = 1
    return self._thread_id_cache
```

**Why this is safe**: Message retrieval already filters by `run_id`, so accumulated messages from previous questions don't pollute results. The DELETE every 8 prevents the "50+ messages → BadRequest" limit.

### 4.5 SDK: Retry 404 Inside SDK

Wrap POST calls that hit eventual consistency issues:

```python
def _post_with_retry(self, url, headers, json_body, params, max_retries=2):
    """POST with retry on 404 (thread not yet consistent)."""
    for attempt in range(max_retries + 1):
        r = self._http.post(url, headers=headers, json=json_body,
                            params=params, timeout=30)
        if r.status_code == 404 and attempt < max_retries:
            time.sleep(1.5 * (attempt + 1))
            continue
        r.raise_for_status()
        return r
    r.raise_for_status()
```

Apply to `POST /threads/{id}/messages` and `POST /threads/{id}/runs` — the two calls that can 404 after thread recreation.

### 4.6 Runner: Remove `"404"` from Retryable Errors

With 404 handled inside the SDK (§4.5), the runner should not retry the entire question on 404:

```python
# runner.py — _is_retryable
retryable = ["429", "503", "timeout", "throttl", "temporarily unavailable",
             "connection", "reset by peer"]
# Remove "404" — now handled at SDK level
```

---

## 5. Non-Changes (Explicitly Ruled Out)

| Idea | Why Not |
|------|---------|
| `ThreadPoolExecutor` with N workers | Single thread per (agent, user). Workers collide. |
| `asyncio` / `aiohttp` | Bottleneck is server-side, not client I/O. Adds complexity for ~5% gain. |
| Multiple service principals | Not available. Single user login only. |
| Multiple Data Agent copies | Expensive, fragile, overkill for 20 questions. |

---

## 6. Implementation Order

| Step | File | Risk | Description |
|------|------|------|-------------|
| 1 | SDK | Low | Add `self._http = requests.Session()`, replace bare `requests.*` calls |
| 2 | SDK | Low | Adaptive polling intervals |
| 3 | SDK | Low | `_post_with_retry` for 404 resilience on message/run creation |
| 4 | SDK | Low | Parallel GET for messages + steps |
| 5 | SDK | Medium | Thread recycling (`_get_thread` with reuse + periodic DELETE) |
| 6 | runner.py | Low | Remove `"404"` from retryable errors |

Steps 1-4 are pure optimizations with no behavior change. Step 5 changes thread lifecycle. Step 6 is cleanup.

---

## 7. Testing Plan

| Test | Validates |
|------|-----------|
| Run 5 questions serial, 0 errors | 404 fix works |
| Run 20 questions serial, compare wall time to pre-change ~600s | Speedup measurable |
| Run 10+ questions, verify no BadRequest | Thread recycling threshold correct |
| Kill mid-run (Ctrl+C), verify partial results saved | No regression |

---

## 8. Fabric Thread Model — Empirical Facts

```
POST /threads       → always returns same thread_id for same (agent, user)
DELETE /threads/{id} → thread deleted, 200
POST /threads       → returns NEW thread_id (different GUID)

Thread reuse window: thread stays usable after run completes
Thread pollution:    ~50 messages → BadRequest
Eventual consistency: 0.5-2s after DELETE before new thread is usable

Per-call latency:
  POST /threads:     ~200ms
  DELETE /threads:    ~300ms
  POST /messages:     ~400ms
  POST /assistants:   ~300ms
  POST /runs:         ~500ms
  GET /runs/{id}:     ~200ms
  GET /messages:      ~300ms
  GET /runs/steps:    ~300ms

Server processing:   15-25s (DAX pipeline: fewshots → nl2code → execute → generate)
```

---

## 9. Future: True Parallelism (If SPs Become Available)

If service principals are ever provisioned, the path is straightforward:
- Each SP gets its own Fabric thread (different user identity)
- Distribute questions round-robin across SPs
- Each worker runs serially on its own thread
- 4 SPs → ~4x speedup → **~100s for 20 questions**

This would require a new `ParallelAgentPool` class but the serial optimizations from this spec still apply to each worker.
