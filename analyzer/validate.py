"""Validate profile configuration and Fabric connectivity."""

import requests

from .config import ROOT, load_test_cases
from .api import FABRIC_API


def validate_profile(session, cfg):
    """Run connectivity checks on the configured profile.

    Returns a list of (check_name, passed: bool, detail: str) tuples.
    """
    checks = []
    profile = cfg.get("profile_name", "default")

    # ── 1. Config completeness ──
    required = ["tenant_id", "workspace_id", "agent_id",
                "semantic_model_id", "semantic_model_name"]
    missing = [k for k in required if not cfg.get(k) or cfg.get(k) == "REPLACE_ME"]
    if missing:
        checks.append(("config", False, f"Missing or placeholder values: {', '.join(missing)}"))
    else:
        checks.append(("config", True, f"All required fields present"))

    # ── 2. Test cases ──
    try:
        test_cases = load_test_cases(cfg)
        n_graded = sum(1 for tc in test_cases if tc.get("expected") is not None)
        checks.append(("questions", True,
                        f"{len(test_cases)} questions loaded ({n_graded} with expected answers)"))
    except Exception as e:
        checks.append(("questions", False, f"Cannot load questions.yaml: {e}"))
        test_cases = []

    # ── 3. Authentication ──
    try:
        token = session.token
        checks.append(("auth", True, f"Token acquired (len={len(token)})"))
    except Exception as e:
        checks.append(("auth", False, f"Authentication failed: {e}"))
        _print_results(profile, checks)
        return checks  # No point continuing without auth

    # ── 4. Workspace access ──
    ws_id = cfg["workspace_id"]
    try:
        r = requests.get(
            f"{FABRIC_API}/workspaces/{ws_id}",
            headers=session.headers,
        )
        if r.ok:
            ws = r.json()
            checks.append(("workspace", True,
                           f"'{ws.get('displayName', ws_id)}' (capacity: {ws.get('capacityId', '?')[:8]}...)"))
        elif r.status_code == 404:
            checks.append(("workspace", False,
                           f"Workspace {ws_id} not found (404). Check workspace_id."))
        elif r.status_code == 403:
            checks.append(("workspace", False,
                           f"Access denied to workspace {ws_id} (403). Check permissions."))
        else:
            checks.append(("workspace", False, f"HTTP {r.status_code}: {r.text[:200]}"))
    except Exception as e:
        checks.append(("workspace", False, f"Connection error: {e}"))

    # ── 5. Agent (AI Skill) exists ──
    agent_id = cfg["agent_id"]
    try:
        r = requests.get(
            f"{FABRIC_API}/workspaces/{ws_id}/items/{agent_id}",
            headers=session.headers,
        )
        if r.ok:
            item = r.json()
            checks.append(("agent", True,
                           f"'{item.get('displayName', agent_id)}' (type: {item.get('type', '?')})"))
        elif r.status_code == 404:
            checks.append(("agent", False,
                           f"Agent {agent_id} not found (404). Check agent_id in profile.yaml."))
        else:
            checks.append(("agent", False, f"HTTP {r.status_code}: {r.text[:200]}"))
    except Exception as e:
        checks.append(("agent", False, f"Connection error: {e}"))

    # ── 6. Semantic model exists ──
    model_id = cfg["semantic_model_id"]
    try:
        r = requests.get(
            f"{FABRIC_API}/workspaces/{ws_id}/items/{model_id}",
            headers=session.headers,
        )
        if r.ok:
            item = r.json()
            checks.append(("model", True,
                           f"'{item.get('displayName', model_id)}' (type: {item.get('type', '?')})"))
        elif r.status_code == 404:
            checks.append(("model", False,
                           f"Model {model_id} not found (404). Check semantic_model_id."))
        else:
            checks.append(("model", False, f"HTTP {r.status_code}: {r.text[:200]}"))
    except Exception as e:
        checks.append(("model", False, f"Connection error: {e}"))

    # ── 7. Agent reachable (SDK endpoint) ──
    try:
        client = session.client
        checks.append(("sdk", True, "SDK client initialized"))
    except Exception as e:
        checks.append(("sdk", False, f"SDK client init failed: {e}"))

    _print_results(profile, checks)
    return checks


def _print_results(profile, checks):
    passed = sum(1 for _, ok, _ in checks if ok)
    total = len(checks)
    all_ok = passed == total

    W = 60
    print(f"\n{'=' * W}")
    print(f"  VALIDATE: {profile}")
    print(f"{'=' * W}")

    for name, ok, detail in checks:
        icon = "+" if ok else "X"
        print(f"  {icon} {name:12s}  {detail}")

    print(f"{'=' * W}")
    if all_ok:
        print(f"  ALL CHECKS PASSED ({passed}/{total})")
        print(f"\n  Ready to run: python -m analyzer -p {profile} run")
    else:
        failed = total - passed
        print(f"  {failed} CHECK(S) FAILED ({passed}/{total} passed)")
        print(f"\n  Fix the issues above and re-run: python -m analyzer -p {profile} validate")
    print(f"{'=' * W}\n")
