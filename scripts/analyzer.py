#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""The AI Skill Analyzer -- Fabric Data Agent Diagnostic & Grading Tool.

Runs batch questions against a Fabric Data Agent, compares answers to expected
values defined in questions.yaml, and traces the agent's internal pipeline to
identify the root cause of wrong answers.

Modes:
    snapshot  - Fetch & cache agent config + semantic model schema
    run       - Run all questions, grade answers, trace pipeline, identify root causes
    rerun     - Re-run only failed questions from a previous batch
    analyze   - Offline analysis with RCA (no Fabric connection needed)

Usage:
    python analyzer.py snapshot                      # Refresh cache
    python analyzer.py run                           # Full batch (uses cache)
    python analyzer.py run --refresh                 # Refresh cache + run
    python analyzer.py run --tag kpi                 # Run only questions tagged 'kpi'
    python analyzer.py rerun <run_id>                # Re-run failed Qs from a run
    python analyzer.py rerun <run_id> --questions 3 5  # Re-run specific Qs
    python analyzer.py analyze <run_id>              # Offline analysis with RCA
    python analyzer.py analyze --latest              # Analyze most recent run
"""

import sys
import os
import json
import time
import base64
import re
import argparse
import requests
import yaml
from pathlib import Path
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# ── Paths ─────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = ROOT / "scripts"
SNAPSHOT_DIR = ROOT / "snapshots"
CONFIG_FILE = ROOT / "config.yaml"
QUESTIONS_FILE = SCRIPTS_DIR / "questions.txt"
QUESTIONS_YAML = SCRIPTS_DIR / "questions.yaml"

# SDK bootstrap
sys.path.insert(0, os.path.join(os.environ.get("TEMP", "/tmp"), "fabric_data_agent_client"))
from fabric_data_agent_client import FabricDataAgentClient


# ══════════════════════════════════════════════════════════════
#  CONFIG
# ══════════════════════════════════════════════════════════════

def load_config():
    if not CONFIG_FILE.exists():
        print(f"ERROR: {CONFIG_FILE} not found. Copy config.yaml.example and fill in your IDs.")
        sys.exit(1)
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    # Defaults
    cfg.setdefault("stage", "sandbox")
    cfg.setdefault("snapshot_ttl_hours", 24)
    cfg.setdefault("max_workers", 4)
    cfg.setdefault("output_dir", "runs")
    cfg["data_agent_url"] = (
        f"https://api.fabric.microsoft.com/v1/workspaces/{cfg['workspace_id']}"
        f"/aiskills/{cfg['agent_id']}/aiassistant/openai"
    )
    return cfg


def load_test_cases(tag_filter=None):
    """Load questions + expected answers from questions.yaml (fallback: questions.txt).

    Returns list of dicts: [{question, expected, match_type, tolerance, tags}, ...]
    """
    cases = []
    if QUESTIONS_YAML.exists():
        with open(QUESTIONS_YAML, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
        for tc in data.get("test_cases", []):
            tc.setdefault("match_type", "contains")
            tc.setdefault("expected", None)
            tc.setdefault("tolerance", None)
            tc.setdefault("tags", [])
            cases.append(tc)
    elif QUESTIONS_FILE.exists():
        with open(QUESTIONS_FILE, "r", encoding="utf-8") as f:
            for ln in f:
                ln = ln.strip()
                if ln and not ln.startswith("#"):
                    cases.append({"question": ln, "expected": None,
                                  "match_type": "contains", "tolerance": None, "tags": []})
    else:
        cases = [{"question": "what is the churn rate", "expected": None,
                  "match_type": "contains", "tolerance": None, "tags": []}]

    if tag_filter:
        cases = [tc for tc in cases if tag_filter in tc.get("tags", [])]
    return cases


# ══════════════════════════════════════════════════════════════
#  AUTH (single browser popup, reused everywhere)
# ══════════════════════════════════════════════════════════════

class FabricSession:
    """Wraps SDK client + REST token. Created once per invocation."""

    def __init__(self, cfg):
        self.cfg = cfg
        self._client = None
        self._token = None

    @property
    def client(self):
        if self._client is None:
            self._client = FabricDataAgentClient(
                tenant_id=self.cfg["tenant_id"],
                data_agent_url=self.cfg["data_agent_url"],
            )
            self._token = self._client.token.token
        return self._client

    @property
    def token(self):
        if self._token is None:
            _ = self.client  # triggers auth
        return self._token

    @property
    def headers(self):
        return {"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"}


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
    loc = initial_response.headers.get("Location") or initial_response.headers.get("Operation-Location")
    if not loc:
        return {"_error": 202, "_note": "LRO with no Location header"}
    for _ in range(30):
        time.sleep(2)
        r = requests.get(loc, headers=session.headers)
        if not r.ok:
            continue
        data = r.json()
        status = data.get("status", "")
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


# ══════════════════════════════════════════════════════════════
#  SNAPSHOT — cache agent config + schema to disk
# ══════════════════════════════════════════════════════════════

def snapshot_path(cfg):
    return SNAPSHOT_DIR / cfg["semantic_model_name"]


def snapshot_is_fresh(cfg):
    meta_file = snapshot_path(cfg) / "snapshot_meta.json"
    if not meta_file.exists():
        return False
    ttl = cfg.get("snapshot_ttl_hours", 24)
    if ttl <= 0:
        return False
    with open(meta_file, "r") as f:
        meta = json.load(f)
    taken_at = datetime.fromisoformat(meta["taken_at"])
    return datetime.now(timezone.utc) - taken_at < timedelta(hours=ttl)


def load_snapshot(cfg):
    sp = snapshot_path(cfg)
    with open(sp / "agent_config.json", "r", encoding="utf-8") as f:
        agent_data = json.load(f)
    with open(sp / "schema.json", "r", encoding="utf-8") as f:
        schema = json.load(f)
    return agent_data, schema


def take_snapshot(session, cfg, force=False):
    """Fetch agent config + TMDL schema from Fabric and cache to disk."""
    sp = snapshot_path(cfg)
    if not force and snapshot_is_fresh(cfg):
        print(f"  Snapshot is fresh (< {cfg['snapshot_ttl_hours']}h). Use --refresh to force.")
        return load_snapshot(cfg)

    sp.mkdir(parents=True, exist_ok=True)
    ws = cfg["workspace_id"]
    agent = cfg["agent_id"]
    model = cfg["semantic_model_id"]

    # ── Agent config ──
    print("  Fetching agent config...")
    meta = fabric_get(session, f"/workspaces/{ws}/items/{agent}")
    defn = fabric_post(session, f"/workspaces/{ws}/items/{agent}/getDefinition")
    config = None
    if defn and isinstance(defn, dict) and "definition" in defn:
        for part in defn["definition"].get("parts", []):
            payload = part.get("payload", "")
            if part.get("payloadType") == "InlineBase64" and payload:
                try:
                    decoded = base64.b64decode(payload).decode("utf-8")
                    parsed = json.loads(decoded)
                    part["payload"] = parsed
                    if part.get("path", "").endswith(".json"):
                        config = parsed
                except Exception:
                    pass
    agent_data = {"meta": meta, "config": config or defn}
    with open(sp / "agent_config.json", "w", encoding="utf-8") as f:
        json.dump(agent_data, f, indent=2, default=str, ensure_ascii=False)
    print(f"  → Agent: {meta.get('displayName', '?')}")

    # ── Schema (TMDL) ──
    print("  Fetching semantic model schema (TMDL)...")
    defn = fabric_post(session, f"/workspaces/{ws}/semanticModels/{model}/getDefinition?format=TMDL")
    parts = []
    if defn and isinstance(defn, dict) and "definition" in defn:
        parts = defn["definition"].get("parts", [])
    elif defn and isinstance(defn, dict) and "_error" not in defn:
        parts = defn.get("parts", [])

    schema = _build_schema(parts, cfg) if parts else _empty_schema(cfg)
    with open(sp / "schema.json", "w", encoding="utf-8") as f:
        json.dump(schema, f, indent=2, default=str, ensure_ascii=False)

    stats = schema.get("stats", {})
    print(f"  → {stats.get('tables', 0)} tables, {stats.get('columns', 0)} columns, "
          f"{stats.get('measures', 0)} measures, {stats.get('relationships', 0)} relationships")

    # ── Meta ──
    snap_meta = {
        "taken_at": datetime.now(timezone.utc).isoformat(),
        "agent_id": agent,
        "model_id": model,
        "agent_name": meta.get("displayName", "?"),
        "model_name": cfg["semantic_model_name"],
        "stats": stats,
    }
    with open(sp / "snapshot_meta.json", "w", encoding="utf-8") as f:
        json.dump(snap_meta, f, indent=2)

    print(f"  Snapshot saved to: {sp.relative_to(ROOT)}")
    return agent_data, schema


# ══════════════════════════════════════════════════════════════
#  TMDL PARSER
# ══════════════════════════════════════════════════════════════

def _parse_tmdl_tables(parts):
    tables = {}
    relationships = []

    for part in parts:
        path = part.get("path", "")
        payload = part.get("payload", "")
        if part.get("payloadType") == "InlineBase64" and payload:
            try:
                payload = base64.b64decode(payload).decode("utf-8")
            except Exception:
                continue
        if not isinstance(payload, str):
            continue

        if path.startswith("definition/tables/") and path.endswith(".tmdl"):
            table_name = None
            table_desc = ""
            columns = []
            measures = []
            pending_desc = []

            for line in payload.split("\n"):
                stripped = line.strip()
                if stripped.startswith("///"):
                    pending_desc.append(stripped[3:].strip())
                    continue
                if stripped.startswith("table "):
                    table_name = stripped.split("table ", 1)[1].strip().strip("'")
                    table_desc = " ".join(pending_desc) if pending_desc else ""
                    pending_desc = []
                elif stripped.startswith("measure "):
                    rest = stripped.split("measure ", 1)[1].strip()
                    meas_name = rest.split(" = ")[0].strip().strip("'") if " = " in rest else rest.strip().strip("'")
                    desc = " ".join(pending_desc) if pending_desc else ""
                    pending_desc = []
                    measures.append({"name": meas_name, "description": desc, "expression": "", "format_string": ""})
                elif stripped.startswith("column "):
                    col_name = stripped.split("column ", 1)[1].strip().strip("'")
                    desc = " ".join(pending_desc) if pending_desc else ""
                    pending_desc = []
                    columns.append({"name": col_name, "description": desc, "data_type": "", "is_hidden": False})
                elif stripped.startswith("dataType:") and columns:
                    columns[-1]["data_type"] = stripped.split(":", 1)[1].strip()
                elif stripped == "isHidden" and columns:
                    columns[-1]["is_hidden"] = True
                elif stripped.startswith("formatString:") and measures:
                    measures[-1]["format_string"] = stripped.split(":", 1)[1].strip().strip('"')
                else:
                    if not stripped.startswith("lineageTag:") and not stripped.startswith("sourceLineageTag:"):
                        pending_desc = []

            if table_name:
                tables[table_name] = {"name": table_name, "description": table_desc,
                                      "columns": columns, "measures": measures}

        elif "relationships" in path and path.endswith(".tmdl"):
            for line in payload.split("\n"):
                stripped = line.strip()
                if stripped.startswith("relationship "):
                    relationships.append(stripped)

    return tables, relationships


def _build_schema(parts, cfg):
    tmap, rels = _parse_tmdl_tables(parts)
    elements = []
    for tinfo in tmap.values():
        children = []
        for col in tinfo["columns"]:
            children.append({"display_name": col["name"], "type": "semantic_model.column",
                             "description": col["description"], "data_type": col.get("data_type", ""),
                             "is_hidden": col.get("is_hidden", False)})
        for meas in tinfo["measures"]:
            children.append({"display_name": meas["name"], "type": "semantic_model.measure",
                             "description": meas["description"], "expression": meas.get("expression", ""),
                             "format_string": meas.get("format_string", "")})
        elements.append({"display_name": tinfo["name"], "type": "semantic_model.table",
                         "description": tinfo["description"], "is_selected": True, "children": children})

    n_tables = len(tmap)
    n_cols = sum(len(t["columns"]) for t in tmap.values())
    n_measures = sum(len(t["measures"]) for t in tmap.values())
    desc_t = sum(1 for t in tmap.values() if t["description"])
    desc_c = sum(1 for t in tmap.values() for c in t["columns"] if c["description"])
    desc_m = sum(1 for t in tmap.values() for m in t["measures"] if m["description"])

    return {
        "dataSourceInfo": {
            "type": "semantic_model", "semantic_model_id": cfg["semantic_model_id"],
            "semantic_model_name": cfg["semantic_model_name"],
            "semantic_model_workspace_id": cfg["workspace_id"],
        },
        "elements": elements, "relationships": rels,
        "stats": {"tables": n_tables, "columns": n_cols, "measures": n_measures,
                  "relationships": len(rels),
                  "description_coverage": {"tables": f"{desc_t}/{n_tables}",
                                           "columns": f"{desc_c}/{n_cols}",
                                           "measures": f"{desc_m}/{n_measures}"}},
    }


def _empty_schema(cfg):
    return {
        "dataSourceInfo": {
            "type": "semantic_model", "semantic_model_id": cfg["semantic_model_id"],
            "semantic_model_name": cfg["semantic_model_name"],
            "semantic_model_workspace_id": cfg["workspace_id"],
        },
        "elements": [], "relationships": [],
        "stats": {"tables": 0, "columns": 0, "measures": 0, "relationships": 0,
                  "description_coverage": {"tables": "0/0", "columns": "0/0", "measures": "0/0"}},
    }


# ══════════════════════════════════════════════════════════════
#  ANSWER COMPARISON
# ══════════════════════════════════════════════════════════════

def _extract_numbers(text):
    """Extract all numbers (int or float) from a text string."""
    return [float(x.replace(",", "")) for x in re.findall(r'-?[\d,]+\.?\d*', text)]


def _compare_answer(actual, test_case):
    """Compare the agent's answer against the expected answer.

    Returns (verdict, detail) where verdict is one of:
        pass         — answer matches expected
        fail         — answer does not match expected
        no_expected  — no expected answer provided (manual review needed)
    """
    expected = test_case.get("expected")
    if expected is None or str(expected).strip() == "":
        return "no_expected", "No expected answer provided — manual review required"

    match_type = test_case.get("match_type", "contains")
    actual_lower = actual.lower().strip()
    expected_str = str(expected).lower().strip()

    if match_type == "exact":
        if actual_lower == expected_str:
            return "pass", f"Exact match: '{expected}'"
        return "fail", f"Expected exact '{expected}', got '{actual[:120]}'"

    elif match_type == "contains":
        if expected_str in actual_lower:
            return "pass", f"Answer contains '{expected}'"
        return "fail", f"Expected answer to contain '{expected}', not found in: '{actual[:120]}'"

    elif match_type == "numeric":
        actual_nums = _extract_numbers(actual)
        expected_num = float(str(expected).replace(",", ""))
        tolerance = float(test_case.get("tolerance") or 0)

        for num in actual_nums:
            if abs(num - expected_num) <= tolerance:
                return "pass", f"Numeric match: {num} ≈ {expected_num} (±{tolerance})"
        if actual_nums:
            closest = min(actual_nums, key=lambda x: abs(x - expected_num))
            return "fail", f"Expected ~{expected_num} (±{tolerance}), closest found: {closest}"
        return "fail", f"Expected ~{expected_num}, no numbers found in answer"

    elif match_type == "regex":
        if re.search(str(expected), actual, re.IGNORECASE):
            return "pass", f"Regex match: /{expected}/"
        return "fail", f"Regex /{expected}/ not found in answer"

    elif match_type == "any_of":
        expected_list = expected if isinstance(expected, list) else [expected]
        for exp in expected_list:
            if str(exp).lower() in actual_lower:
                return "pass", f"Found '{exp}' in answer"
        return "fail", f"None of {expected_list} found in answer"

    return "no_expected", f"Unknown match_type: {match_type}"


# ══════════════════════════════════════════════════════════════
#  PIPELINE TRACER — extract what happened at each agent step
# ══════════════════════════════════════════════════════════════

# Map known tool names to pipeline stage labels
PIPELINE_STAGES = {
    "nl2sa_query":         "NL_TO_QUERY",
    "nl2sql_query":        "NL_TO_QUERY",
    "evaluate_dax":        "DAX_EXECUTION",
    "evaluate_sql":        "SQL_EXECUTION",
    "evaluate_query":      "QUERY_EXECUTION",
    "message_creation":    "ANSWER_SYNTHESIS",
}


def _trace_pipeline(run_details):
    """Extract an ordered trace of what the Data Agent did at each step.

    Returns list of step dicts with stage, tool, arguments, output, timing.
    """
    steps_data = run_details.get("run_steps", {}).get("data", [])
    trace = []

    for step in steps_data:
        tool_calls = (step.get("step_details") or {}).get("tool_calls", [])
        status = step.get("status", "unknown")
        created = step.get("created_at", 0) or 0
        completed = step.get("completed_at", 0) or 0
        duration = round(completed - created, 2) if created and completed else None

        if not tool_calls:
            trace.append({
                "stage": "ANSWER_SYNTHESIS",
                "tool": "message_creation",
                "status": status,
                "arguments": None,
                "output": None,
                "duration_s": duration,
                "error": step.get("last_error"),
            })
            continue

        for tc in tool_calls:
            fn = tc.get("function", {})
            tool_name = fn.get("name", "unknown")
            stage = PIPELINE_STAGES.get(tool_name, "TOOL_CALL")

            # Parse arguments (may contain reformulated query, DAX, etc.)
            args_raw = fn.get("arguments", "{}")
            try:
                arguments = json.loads(args_raw) if isinstance(args_raw, str) else args_raw
            except (json.JSONDecodeError, TypeError):
                arguments = {"_raw": str(args_raw)[:500]}

            # Parse output (may contain query results, errors, etc.)
            output_raw = fn.get("output", "")
            try:
                if isinstance(output_raw, str) and output_raw.strip()[:1] in ("{", "["):
                    output = json.loads(output_raw)
                else:
                    output = {"_raw": str(output_raw)[:1000] if output_raw else ""}
            except (json.JSONDecodeError, TypeError):
                output = {"_raw": str(output_raw)[:1000]}

            trace.append({
                "stage": stage,
                "tool": tool_name,
                "status": status,
                "arguments": arguments,
                "output": output,
                "duration_s": duration,
                "error": step.get("last_error"),
            })

    return trace


# ══════════════════════════════════════════════════════════════
#  ROOT CAUSE ANALYSIS
# ══════════════════════════════════════════════════════════════

# Root cause categories (ordered by severity / specificity)
RCA_CATEGORIES = {
    "AGENT_ERROR":       "Agent returned an error or non-completed status",
    "QUERY_ERROR":       "Generated query failed to execute (syntax, missing column, etc.)",
    "EMPTY_RESULT":      "Query succeeded but returned no data or empty result",
    "FILTER_CONTEXT":    "Unexpected filter applied (e.g., time intelligence auto-filter)",
    "MEASURE_SELECTION": "Wrong measure referenced in the generated query",
    "RELATIONSHIP":      "Wrong join path or missing relationship traversal",
    "REFORMULATION":     "Agent misunderstood the question — wrong entities or intent",
    "SYNTHESIS":         "Data was correct but the answer was misinterpreted or truncated",
    "UNKNOWN":           "Cannot determine root cause from available pipeline data",
}


def _identify_root_cause(test_case, result, pipeline_trace, verdict):
    """Given a failed result, analyze the pipeline trace to identify the root cause.

    Returns (category, detail) tuple.
    """
    if verdict in ("pass", "no_expected"):
        return None, None

    # Agent-level error
    if result.get("status") == "error" or result.get("error"):
        return "AGENT_ERROR", f"Agent error: {result.get('error', 'unknown')}"
    if result.get("status") not in ("completed",):
        return "AGENT_ERROR", f"Agent status: {result.get('status')}"

    # Walk pipeline trace for signals
    signals = []
    for step in pipeline_trace:
        # Failed step
        if step["status"] not in ("completed", "succeeded"):
            signals.append(("QUERY_ERROR",
                            f"Step '{step['tool']}' status='{step['status']}'",
                            step))
            if step.get("error"):
                signals.append(("QUERY_ERROR",
                                f"Error in '{step['tool']}': {step['error']}",
                                step))

        # Check output for error indicators
        output = step.get("output") or {}
        if isinstance(output, dict):
            err = output.get("error") or output.get("_error") or ""
            if err:
                signals.append(("QUERY_ERROR",
                                f"Tool '{step['tool']}' output error: {err}",
                                step))
            raw = str(output.get("_raw", ""))
            if raw and ("no data" in raw.lower() or "empty" in raw.lower()
                        or "0 rows" in raw.lower()):
                signals.append(("EMPTY_RESULT",
                                f"Tool '{step['tool']}' returned empty/no data",
                                step))

        # Analyze arguments for query issues
        args = step.get("arguments") or {}
        if isinstance(args, dict):
            query = (args.get("query", "") or args.get("dax", "")
                     or args.get("expression", "") or "")
            if isinstance(query, str) and query:
                if "__PBI_TimeIntelligenceEnabled" in query or "TREATAS" in query.upper():
                    signals.append(("FILTER_CONTEXT",
                                    "Time intelligence auto-filter detected in generated query",
                                    step))
                if "CALCULATETABLE" in query.upper() and "FILTER" in query.upper():
                    signals.append(("FILTER_CONTEXT",
                                    "Complex filter context (CALCULATETABLE + FILTER) in DAX",
                                    step))

    # No tool calls at all → reformulation failure
    tool_steps = [s for s in pipeline_trace if s["tool"] != "message_creation"]
    if not tool_steps:
        return "REFORMULATION", "No tool calls made — agent could not formulate a query from the question"

    # Return most specific signal
    if signals:
        priority = ["QUERY_ERROR", "EMPTY_RESULT", "FILTER_CONTEXT",
                     "MEASURE_SELECTION", "RELATIONSHIP", "REFORMULATION"]
        for cat in priority:
            matches = [s for s in signals if s[0] == cat]
            if matches:
                return matches[0][0], matches[0][1]

    # Answer was returned but wrong → likely synthesis or subtle query issue
    if result.get("answer"):
        return "SYNTHESIS", ("Agent returned an answer that doesn't match expected. "
                             "Inspect the generated query and result data below.")

    return "UNKNOWN", "Cannot determine root cause from available pipeline data"


def _extract_artifacts(pipeline_trace):
    """Extract key artifacts (reformulated question, DAX, result preview) from trace."""
    reformulated = None
    generated_query = None
    query_result = None
    tool_outputs = []

    for step in pipeline_trace:
        args = step.get("arguments") or {}
        output = step.get("output") or {}

        if isinstance(args, dict):
            if args.get("query") and not reformulated:
                reformulated = args["query"]
            dax = args.get("dax") or args.get("expression") or args.get("query_text") or ""
            if dax and not generated_query:
                generated_query = dax

        if step["tool"] != "message_creation" and isinstance(output, dict):
            raw = output.get("_raw", "")
            if raw and not query_result:
                query_result = str(raw)[:500]
            if output and output != {"_raw": ""}:
                tool_outputs.append({"tool": step["tool"], "output_preview": str(output)[:300]})

    return {
        "reformulated_question": reformulated,
        "generated_query": generated_query,
        "query_result_preview": query_result,
        "tool_outputs": tool_outputs,
    }


def grade_result(result, test_case):
    """Grade a single result: compare answer + trace pipeline + identify root cause.

    Returns a verdict dict to attach to the result.
    """
    # 1. Compare answer
    verdict, compare_detail = _compare_answer(result.get("answer", ""), test_case)

    # 2. Trace pipeline
    pipeline_trace = _trace_pipeline(result.get("run_details", {}))

    # 3. Root cause (only for failures)
    root_cause, rca_detail = _identify_root_cause(test_case, result, pipeline_trace, verdict)

    # 4. Extract key artifacts
    artifacts = _extract_artifacts(pipeline_trace)

    return {
        "verdict": verdict,
        "expected": test_case.get("expected"),
        "match_type": test_case.get("match_type", "contains"),
        "compare_detail": compare_detail,
        "tags": test_case.get("tags", []),
        "pipeline_trace": pipeline_trace,
        "root_cause": root_cause,
        "root_cause_detail": rca_detail,
        "artifacts": artifacts,
    }


# ══════════════════════════════════════════════════════════════
#  QUESTION RUNNER (parallel)
# ══════════════════════════════════════════════════════════════

def _run_single_question(client, question, idx, total):
    """Run one question against the agent. Thread-safe (SDK creates new thread per call)."""
    t0 = time.monotonic()
    try:
        run_details = client.get_run_details(question)
        elapsed = time.monotonic() - t0

        # Extract answer
        answer = ""
        for msg in run_details.get("messages", {}).get("data", []):
            if msg.get("role") == "assistant":
                content = msg.get("content", [])
                if content and isinstance(content[0], dict):
                    text = content[0].get("text", {})
                    answer = text.get("value", str(text)) if isinstance(text, dict) else str(text)

        # Extract tool chain
        tools = []
        for step in run_details.get("run_steps", {}).get("data", []):
            for tc in (step.get("step_details") or {}).get("tool_calls", []):
                tools.append(tc.get("function", {}).get("name", "?"))

        # Compute duration from steps
        steps = run_details.get("run_steps", {}).get("data", [])
        all_created = [s.get("created_at") or 0 for s in steps if s.get("created_at")]
        all_completed = [s.get("completed_at") or 0 for s in steps if s.get("completed_at")]
        step_duration = (max(all_completed) - min(all_created)) if all_created and all_completed else None

        status = run_details.get("run_status", "unknown")
        icon = "✅" if status == "completed" else "❌"
        print(f"  {icon} [{idx}/{total}] ({elapsed:.1f}s) {question[:60]}")

        return {
            "question": question,
            "index": idx,
            "status": status,
            "answer": answer[:300],
            "tools": tools,
            "duration_wall": round(elapsed, 2),
            "duration_steps": step_duration,
            "run_details": run_details,
            "error": None,
        }
    except Exception as e:
        elapsed = time.monotonic() - t0
        print(f"  ❌ [{idx}/{total}] ({elapsed:.1f}s) {question[:60]} → ERROR: {e}")
        return {
            "question": question,
            "index": idx,
            "status": "error",
            "answer": "",
            "tools": [],
            "duration_wall": round(elapsed, 2),
            "duration_steps": None,
            "run_details": {},
            "error": str(e),
        }


def run_questions_parallel(session, questions, cfg):
    """Run all questions in parallel using ThreadPoolExecutor."""
    max_w = min(cfg.get("max_workers", 4), len(questions))
    print(f"  Running {len(questions)} questions with {max_w} workers...\n")
    t0 = time.monotonic()

    results = [None] * len(questions)
    with ThreadPoolExecutor(max_workers=max_w) as pool:
        futures = {
            pool.submit(_run_single_question, session.client, q, i + 1, len(questions)): i
            for i, q in enumerate(questions)
        }
        for future in as_completed(futures):
            idx = futures[future]
            results[idx] = future.result()

    total_wall = time.monotonic() - t0
    print(f"\n  Total wall time: {total_wall:.1f}s (vs ~{sum(r['duration_wall'] for r in results):.1f}s serial)")
    return results, round(total_wall, 2)


def run_questions_serial(session, questions, cfg):
    """Fallback: run questions sequentially."""
    print(f"  Running {len(questions)} questions sequentially...\n")
    t0 = time.monotonic()
    results = []
    for i, q in enumerate(questions):
        r = _run_single_question(session.client, q, i + 1, len(questions))
        results.append(r)
    total_wall = time.monotonic() - t0
    print(f"\n  Total wall time: {total_wall:.1f}s")
    return results, round(total_wall, 2)


# ══════════════════════════════════════════════════════════════
#  DIAGNOSTIC BUILDER
# ══════════════════════════════════════════════════════════════

def build_diagnostic(agent_data, schema, question_result, cfg, verdict_data=None):
    steps = question_result["run_details"].get("run_steps", {}).get("data", [])
    step_times = []
    for s in steps:
        tc = (s.get("step_details") or {}).get("tool_calls", [])
        fn_name = tc[0]["function"]["name"] if tc else "message_creation"
        step_times.append({
            "tool": fn_name, "status": s.get("status"),
            "created_at": s.get("created_at"), "completed_at": s.get("completed_at"),
        })

    diag = {
        "downloaded_at": datetime.now(timezone.utc).isoformat(),
        "rolloutEnvironment": "PROD",
        "stage": cfg.get("stage", "sandbox"),
        "artifactId": cfg["agent_id"],
        "workspaceId": cfg["workspace_id"],
        "source": "analyzer.py v2 (grading + RCA)",
        "config": agent_data.get("config"),
        "datasources": {
            cfg["semantic_model_id"]: {
                "fewshots": {"fewShots": [], "parentId": cfg["semantic_model_id"], "type": "semantic_model"},
                "schema": schema,
            }
        },
        "thread": {
            "question": question_result["question"],
            "messages": question_result["run_details"].get("messages"),
            "run_status": question_result["status"],
            "run_steps": question_result["run_details"].get("run_steps"),
        },
        "timing": {
            "total_seconds": question_result["duration_steps"],
            "wall_seconds": question_result["duration_wall"],
            "steps": step_times,
        },
    }

    # Attach grading verdict + RCA if available
    if verdict_data:
        diag["grading"] = {
            "verdict": verdict_data.get("verdict"),
            "expected": verdict_data.get("expected"),
            "actual_answer": question_result.get("answer", "")[:300],
            "match_type": verdict_data.get("match_type"),
            "compare_detail": verdict_data.get("compare_detail"),
            "tags": verdict_data.get("tags", []),
            "root_cause": verdict_data.get("root_cause"),
            "root_cause_detail": verdict_data.get("root_cause_detail"),
            "artifacts": verdict_data.get("artifacts"),
            "pipeline_trace": verdict_data.get("pipeline_trace"),
        }

    return diag


# ══════════════════════════════════════════════════════════════
#  ANALYZE (offline — no Fabric connection)
# ══════════════════════════════════════════════════════════════

def analyze_run(run_dir):
    """Print rich analysis of an existing run with grading + root cause analysis."""
    summary_file = run_dir / "batch_summary.json"
    if not summary_file.exists():
        print(f"ERROR: No batch_summary.json in {run_dir}")
        return

    with open(summary_file, "r", encoding="utf-8") as f:
        summary = json.load(f)

    results = summary.get("results", [])
    total = len(results)

    # Count by verdict (new grading) — fall back to status for old runs
    graded = [r for r in results if r.get("grading")]
    n_pass = sum(1 for r in results if r.get("grading", {}).get("verdict") == "pass")
    n_fail = sum(1 for r in results if r.get("grading", {}).get("verdict") == "fail")
    n_ungraded = sum(1 for r in results if r.get("grading", {}).get("verdict") in ("no_expected", None))
    n_error = sum(1 for r in results if r.get("status") != "completed")

    # Header
    W = 72
    print(f"\n{'═' * W}")
    print(f"  ANALYSIS: {run_dir.name}")
    print(f"{'═' * W}")
    print(f"  Agent    : {summary.get('agent_id', '?')}")
    print(f"  Model    : {summary.get('model_name', summary.get('model_id', '?'))}")
    print(f"  Stage    : {summary.get('stage', '?')}")
    print(f"  Wall time: {summary.get('total_wall_seconds', '?')}s  "
          f"({summary.get('max_workers', 1)} workers)")

    stats = summary.get("schema_stats", {})
    cov = stats.get("description_coverage", {})
    print(f"  Schema   : {stats.get('tables', 0)}T / {stats.get('columns', 0)}C / "
          f"{stats.get('measures', 0)}M / {stats.get('relationships', 0)}R")
    print(f"  Desc cov : T={cov.get('tables', '?')} C={cov.get('columns', '?')} "
          f"M={cov.get('measures', '?')}")

    # Scoreboard
    print(f"\n{'─' * W}")
    print(f"  SCOREBOARD: {total} questions")
    if graded:
        print(f"    ✅ Pass: {n_pass}   ❌ Fail: {n_fail}   "
              f"⚠️  Ungraded: {n_ungraded}   💥 Error: {n_error}")
        pct = round(n_pass / max(n_pass + n_fail, 1) * 100) if (n_pass + n_fail) > 0 else 0
        print(f"    Score: {n_pass}/{n_pass + n_fail} graded = {pct}%")
    else:
        # Legacy run without grading
        passed = sum(1 for r in results if r.get("status") == "completed")
        print(f"    Completed: {passed}/{total}  (no grading data — re-run for verdicts)")

    # Per-question detail
    print(f"\n{'─' * W}")
    for r in results:
        g = r.get("grading", {})
        verdict = g.get("verdict", "?")
        idx = r.get("index", "?")
        dur = r.get("duration_wall", "?")

        # Icon by verdict
        if verdict == "pass":
            icon = "✅"
        elif verdict == "fail":
            icon = "❌"
        elif verdict == "no_expected":
            icon = "⚪"
        else:
            icon = "💥" if r.get("status") != "completed" else "⚪"

        tags_str = f"  [{', '.join(g.get('tags', []))}]" if g.get("tags") else ""
        print(f"  {icon} Q{idx} [{dur}s]{tags_str}")
        print(f"     Question : {r['question']}")
        print(f"     Tools    : {' → '.join(r.get('tools', []))}")

        ans = r.get("answer", "")[:150]
        print(f"     Answer   : {ans}")

        if g.get("expected") is not None:
            print(f"     Expected : {g['expected']} ({g.get('match_type', '?')})")
            print(f"     Verdict  : {verdict.upper()} — {g.get('compare_detail', '')}")

        if r.get("error"):
            print(f"     ERROR    : {r['error']}")

        # Root cause analysis (only for failures)
        if g.get("root_cause"):
            print(f"     ┌─ ROOT CAUSE: {g['root_cause']}")
            print(f"     │  {g.get('root_cause_detail', '')}")

            # Show artifacts that help understand the issue
            artifacts = g.get("artifacts", {})
            if artifacts.get("reformulated_question"):
                print(f"     │  Reformulated: {artifacts['reformulated_question'][:120]}")
            if artifacts.get("generated_query"):
                query_preview = artifacts["generated_query"][:200].replace("\n", " ")
                print(f"     │  Query: {query_preview}")
            if artifacts.get("query_result_preview"):
                print(f"     │  Result: {artifacts['query_result_preview'][:150]}")
            print(f"     └─")

        print()

    # Root cause summary
    rca_counts = {}
    for r in results:
        rc = r.get("grading", {}).get("root_cause")
        if rc:
            rca_counts[rc] = rca_counts.get(rc, 0) + 1

    if rca_counts:
        print(f"{'─' * W}")
        print("  ROOT CAUSE SUMMARY:")
        for cat, count in sorted(rca_counts.items(), key=lambda x: -x[1]):
            desc = RCA_CATEGORIES.get(cat, cat)
            print(f"    {count}× {cat}: {desc}")

    # Recommendations
    print(f"\n{'─' * W}")
    print("  RECOMMENDATIONS:")
    if n_fail > 0 or n_error > 0:
        fail_qs = [str(r.get("index", "?")) for r in results
                   if r.get("grading", {}).get("verdict") == "fail" or r.get("status") != "completed"]
        print(f"  → Re-run failed: python analyzer.py rerun {run_dir.name} --questions {' '.join(fail_qs)}")
    if "FILTER_CONTEXT" in rca_counts:
        print("  → Filter issues detected. Check time intelligence settings (__PBI_TimeIntelligenceEnabled)")
        print("    Consider adding REMOVEFILTERS() in measure or disabling time intelligence.")
    if "REFORMULATION" in rca_counts:
        print("  → Agent failed to understand some questions. Add verified answers or rephrase.")
    if "QUERY_ERROR" in rca_counts:
        print("  → Query errors found. Check model relationships and column visibility.")
    if "SYNTHESIS" in rca_counts:
        print("  → Answers returned but wrong. Inspect generated DAX in diagnostic JSON files.")
        print(f"    Open: {run_dir / 'diagnostics'}")
    if "EMPTY_RESULT" in rca_counts:
        print("  → Empty results. Check data freshness and filter defaults in the model.")
    if n_ungraded > 0:
        print(f"  → {n_ungraded} ungraded questions. Fill in expected answers in questions.yaml.")
    if cov.get("measures", "0/0").startswith("0/"):
        print("  → No measure descriptions. Run Prep for AI on the semantic model.")
    if cov.get("columns", "0/0").startswith("0/"):
        print("  → No column descriptions. Add descriptions via Prep for AI or MCP.")
    if n_pass == total and n_fail == 0:
        print("  → All passed! Consider adding harder questions to questions.yaml.")
    print(f"{'═' * W}")


# ══════════════════════════════════════════════════════════════
#  SAVE RUN
# ══════════════════════════════════════════════════════════════

def save_run(results, agent_data, schema, cfg, total_wall, test_cases):
    """Grade results, save per-question diagnostics + batch summary."""
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out = ROOT / cfg.get("output_dir", "runs") / ts
    diag_dir = out / "diagnostics"
    diag_dir.mkdir(parents=True, exist_ok=True)

    # Grade each result against its test case
    for i, r in enumerate(results):
        tc = test_cases[i] if i < len(test_cases) else {"expected": None, "match_type": "contains", "tags": []}
        verdict_data = grade_result(r, tc)

        # Save per-question diagnostic (with grading + RCA)
        diag = build_diagnostic(agent_data, schema, r, cfg, verdict_data=verdict_data)
        safe_q = re.sub(r"[^a-z0-9]+", "_", r["question"].lower())[:40].strip("_")
        filename = f"full_diag_{safe_q}.json"
        with open(diag_dir / filename, "w", encoding="utf-8") as f:
            json.dump(diag, f, indent=2, default=str, ensure_ascii=False)

        # Attach grading summary to result (for batch_summary.json)
        r["file"] = f"diagnostics/{filename}"
        r["grading"] = {
            "verdict": verdict_data["verdict"],
            "expected": verdict_data["expected"],
            "match_type": verdict_data["match_type"],
            "compare_detail": verdict_data["compare_detail"],
            "tags": verdict_data["tags"],
            "root_cause": verdict_data["root_cause"],
            "root_cause_detail": verdict_data["root_cause_detail"],
            "artifacts": verdict_data["artifacts"],
        }
        # Don't persist raw run_details in summary (it's in the diag file)
        r.pop("run_details", None)

    # Compute grading stats
    n_pass = sum(1 for r in results if r.get("grading", {}).get("verdict") == "pass")
    n_fail = sum(1 for r in results if r.get("grading", {}).get("verdict") == "fail")
    n_ungraded = sum(1 for r in results if r.get("grading", {}).get("verdict") in ("no_expected", None))

    # Root cause distribution
    rca_dist = {}
    for r in results:
        rc = r.get("grading", {}).get("root_cause")
        if rc:
            rca_dist[rc] = rca_dist.get(rc, 0) + 1

    # Save batch summary
    summary = {
        "timestamp": ts,
        "agent_id": cfg["agent_id"],
        "model_id": cfg["semantic_model_id"],
        "model_name": cfg.get("semantic_model_name", "?"),
        "stage": cfg.get("stage", "sandbox"),
        "schema_stats": schema.get("stats"),
        "total_wall_seconds": total_wall,
        "max_workers": cfg.get("max_workers", 1),
        "total_questions": len(results),
        "passed": sum(1 for r in results if r.get("status") == "completed"),
        "failed": sum(1 for r in results if r.get("status") != "completed"),
        "grading": {
            "pass": n_pass,
            "fail": n_fail,
            "ungraded": n_ungraded,
            "score_pct": round(n_pass / max(n_pass + n_fail, 1) * 100) if (n_pass + n_fail) > 0 else None,
            "root_cause_distribution": rca_dist,
        },
        "results": results,
    }
    with open(out / "batch_summary.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, default=str, ensure_ascii=False)

    # Save test cases snapshot for reproducibility
    with open(out / "test_cases.yaml", "w", encoding="utf-8") as f:
        yaml.dump({"test_cases": test_cases}, f, default_flow_style=False, allow_unicode=True)

    return ts, out


# ══════════════════════════════════════════════════════════════
#  CLI
# ══════════════════════════════════════════════════════════════

def cmd_snapshot(args, cfg):
    print("\n[SNAPSHOT] Fetching agent config + schema from Fabric...\n")
    session = FabricSession(cfg)
    take_snapshot(session, cfg, force=True)
    print("\nDone.")


def cmd_run(args, cfg):
    tag_filter = getattr(args, "tag", None)
    test_cases = load_test_cases(tag_filter=tag_filter)
    questions = [tc["question"] for tc in test_cases]
    n_graded = sum(1 for tc in test_cases if tc.get("expected") is not None)

    W = 72
    print(f"\n{'═' * W}")
    print("  THE AI SKILL ANALYZER — BATCH RUN + GRADING")
    print(f"{'═' * W}")
    print(f"  Agent    : {cfg['agent_id']}")
    print(f"  Model    : {cfg['semantic_model_name']}")
    print(f"  Questions: {len(questions)}  ({n_graded} with expected answers)")
    print(f"  Workers  : {cfg.get('max_workers', 4)}")
    print(f"  Stage    : {cfg.get('stage', 'sandbox')}")
    if tag_filter:
        print(f"  Tag      : {tag_filter}")
    print(f"{'═' * W}")

    session = FabricSession(cfg)

    # Snapshot (cached or refresh)
    refresh = getattr(args, "refresh", False)
    if refresh or not snapshot_is_fresh(cfg):
        print("\n[1/3] Taking snapshot...")
        agent_data, schema = take_snapshot(session, cfg, force=refresh)
    else:
        print(f"\n[1/3] Using cached snapshot (< {cfg['snapshot_ttl_hours']}h old)")
        agent_data, schema = load_snapshot(cfg)

    # Run
    print(f"\n[2/4] Running questions...")
    if cfg.get("max_workers", 4) > 1:
        results, total_wall = run_questions_parallel(session, questions, cfg)
    else:
        results, total_wall = run_questions_serial(session, questions, cfg)

    # Grade + Save
    print(f"\n[3/4] Grading answers + root cause analysis...")
    ts, out = save_run(results, agent_data, schema, cfg, total_wall, test_cases)

    # Summary
    print(f"\n[4/4] Results\n")
    n_pass = sum(1 for r in results if r.get("grading", {}).get("verdict") == "pass")
    n_fail = sum(1 for r in results if r.get("grading", {}).get("verdict") == "fail")
    n_ungraded = sum(1 for r in results if r.get("grading", {}).get("verdict") in ("no_expected", None))

    print(f"{'═' * W}")
    print(f"  Run ID : {ts}")
    print(f"  Output : {out.relative_to(ROOT)}")
    if (n_pass + n_fail) > 0:
        pct = round(n_pass / (n_pass + n_fail) * 100)
        print(f"  Score  : {n_pass}/{n_pass + n_fail} = {pct}%")
    print(f"  ✅ Pass: {n_pass}  ❌ Fail: {n_fail}  ⚪ Ungraded: {n_ungraded}  ⏱ {total_wall}s")
    print(f"{'═' * W}")

    for r in results:
        g = r.get("grading", {})
        verdict = g.get("verdict", "?")
        if verdict == "pass":
            icon = "✅"
        elif verdict == "fail":
            icon = "❌"
        else:
            icon = "⚪"
        detail = ""
        if verdict == "fail" and g.get("root_cause"):
            detail = f" [{g['root_cause']}]"
        print(f"  {icon} Q{r['index']} [{r['duration_wall']}s] {r['question']}{detail}")

    if n_fail > 0:
        fail_idx = [str(r["index"]) for r in results if r.get("grading", {}).get("verdict") == "fail"]
        print(f"\n  Re-run failed: python analyzer.py rerun {ts} --questions {' '.join(fail_idx)}")

    print(f"\n  Full analysis: python analyzer.py analyze {ts}")
    print(f"  Detail files:  {out.relative_to(ROOT)}/diagnostics/")


def cmd_rerun(args, cfg):
    run_id = args.run_id
    runs_dir = ROOT / cfg.get("output_dir", "runs")

    # Find the run
    if run_id == "--latest":
        candidates = sorted(runs_dir.iterdir()) if runs_dir.exists() else []
        if not candidates:
            print("ERROR: No runs found.")
            return
        run_dir = candidates[-1]
    else:
        run_dir = runs_dir / run_id
    if not run_dir.exists():
        print(f"ERROR: Run {run_dir} not found.")
        return

    summary_file = run_dir / "batch_summary.json"
    with open(summary_file, "r", encoding="utf-8") as f:
        prev_summary = json.load(f)

    prev_results = prev_summary.get("results", [])

    # Determine which questions to re-run
    specific_qs = getattr(args, "questions", None)
    if specific_qs:
        indices = set(int(q) for q in specific_qs)
        to_rerun = [r for r in prev_results if r.get("index") in indices]
    else:
        # Default: re-run failed verdicts + agent errors
        to_rerun = [r for r in prev_results
                    if r.get("grading", {}).get("verdict") == "fail"
                    or r.get("status") != "completed"]

    if not to_rerun:
        print("Nothing to re-run — all questions passed!")
        return

    questions = [r["question"] for r in to_rerun]

    # Build test cases from current questions.yaml for grading
    all_test_cases = load_test_cases()
    tc_map = {tc["question"]: tc for tc in all_test_cases}
    rerun_test_cases = [tc_map.get(q, {"question": q, "expected": None,
                        "match_type": "contains", "tolerance": None, "tags": []})
                        for q in questions]

    print(f"\n[RERUN] Re-running {len(questions)} question(s) from run {run_dir.name}...\n")

    session = FabricSession(cfg)

    # Use cached snapshot
    if snapshot_is_fresh(cfg):
        agent_data, schema = load_snapshot(cfg)
    else:
        print("  Snapshot expired — refreshing...")
        agent_data, schema = take_snapshot(session, cfg, force=True)

    # Run
    if cfg.get("max_workers", 4) > 1 and len(questions) > 1:
        results, total_wall = run_questions_parallel(session, questions, cfg)
    else:
        results, total_wall = run_questions_serial(session, questions, cfg)

    # Grade + Save
    ts, out = save_run(results, agent_data, schema, cfg, total_wall, rerun_test_cases)

    n_pass = sum(1 for r in results if r.get("grading", {}).get("verdict") == "pass")
    n_fail = sum(1 for r in results if r.get("grading", {}).get("verdict") == "fail")
    print(f"\n  RERUN: ✅ {n_pass} pass  ❌ {n_fail} fail  |  {total_wall}s  |  Run: {ts}")
    print(f"  Output: {out.relative_to(ROOT)}")
    print(f"  Full analysis: python analyzer.py analyze {ts}")


def cmd_analyze(args, cfg):
    runs_dir = ROOT / cfg.get("output_dir", "runs")
    if getattr(args, "latest", False):
        candidates = sorted(runs_dir.iterdir()) if runs_dir.exists() else []
        if not candidates:
            print("ERROR: No runs found.")
            return
        run_dir = candidates[-1]
    else:
        run_dir = runs_dir / args.run_id
    if not run_dir.exists():
        print(f"ERROR: Run {run_dir} not found.")
        return
    analyze_run(run_dir)


def main():
    parser = argparse.ArgumentParser(
        prog="analyzer",
        description="The AI Skill Analyzer - Fabric Data Agent Diagnostic & Grading Tool",
    )
    sub = parser.add_subparsers(dest="command")

    # snapshot
    sub.add_parser("snapshot", help="Fetch & cache agent config + schema")

    # run
    run_p = sub.add_parser("run", help="Run all questions, grade answers, trace pipeline")
    run_p.add_argument("--refresh", action="store_true", help="Force refresh snapshot before run")
    run_p.add_argument("--serial", action="store_true", help="Run questions sequentially")
    run_p.add_argument("--tag", type=str, help="Run only questions with this tag")

    # rerun
    rerun_p = sub.add_parser("rerun", help="Re-run failed/specific questions from a previous run")
    rerun_p.add_argument("run_id", help="Run ID (timestamp folder) or --latest")
    rerun_p.add_argument("--questions", nargs="+", help="Specific question indices to re-run")

    # analyze
    analyze_p = sub.add_parser("analyze", help="Analyze an existing run with RCA (offline)")
    analyze_p.add_argument("run_id", nargs="?", help="Run ID (timestamp folder)")
    analyze_p.add_argument("--latest", action="store_true", help="Analyze most recent run")

    args = parser.parse_args()
    cfg = load_config()

    if getattr(args, "serial", False):
        cfg["max_workers"] = 1

    if args.command == "snapshot":
        cmd_snapshot(args, cfg)
    elif args.command == "run":
        cmd_run(args, cfg)
    elif args.command == "rerun":
        cmd_rerun(args, cfg)
    elif args.command == "analyze":
        cmd_analyze(args, cfg)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
