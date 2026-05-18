"""Diagnose — parse and analyze Fabric Data Agent diagnostic JSON files.

Datasource-agnostic: supports Semantic Model (DAX), Lakehouse (SQL/Spark),
Warehouse (T-SQL), KQL Database (Kusto), Mirrored Database.

Usage:
    python -m analyzer diagnose path/to/diagnostic.json
    python -m analyzer diagnose path/to/folder/
    python -m analyzer diagnose path/to/file.json --format json
    python -m analyzer diagnose-diff before.json after.json
"""

import hashlib
import json
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path


# ── Tool name → human-readable stage ─────────────────────────
# Covers all datasource pipelines (semantic_model, kusto, lakehouse, warehouse).

STAGE_MAP = {
    # Generic database pipeline (semantic_model used this historically)
    "analyze.database.fewshots.loading":  "Fewshot Loading",
    "analyze.database.fewshots.matching": "Fewshot Matching",
    "analyze.database.nl2code":           "NL → Query Generation",
    "analyze.database.execute":           "Query Execution",
    # Per-datasource traces (newer schemas)
    "trace.analyze_semantic_model":       "Query Execution (DAX trace)",
    "trace.analyze_kusto_database":       "Query Execution (KQL trace)",
    "trace.analyze_lakehouse":            "Query Execution (Lakehouse trace)",
    "trace.analyze_warehouse":            "Query Execution (Warehouse trace)",
    # Per-datasource explicit pipelines
    "analyze.kusto_database.fewshots.loading":  "Fewshot Loading",
    "analyze.kusto_database.fewshots.matching": "Fewshot Matching",
    "analyze.kusto_database.nl2code":           "NL → KQL Generation",
    "analyze.kusto_database.execute":           "KQL Execution",
    "analyze.lakehouse.nl2code":                "NL → SQL Generation",
    "analyze.lakehouse.execute":                "SQL Execution",
    "analyze.warehouse.nl2code":                "NL → T-SQL Generation",
    "analyze.warehouse.execute":                "T-SQL Execution",
    "analyze.semantic_model.nl2code":           "NL → DAX Generation",
    "analyze.semantic_model.execute":           "DAX Execution",
    # Orchestration
    "generate.filename":                  "Output Naming",
    "message_creation":                   "Answer Synthesis",
    # Legacy / alternate
    "nl2sa_query":    "NL → Query Generation",
    "nl2sql_query":   "NL → SQL Generation",
    "evaluate_dax":   "DAX Execution",
    "evaluate_sql":   "SQL Execution",
    "evaluate_query": "Query Execution",
}

# ── Datasource type detection (handles snake_case + PascalCase) ─

DATASOURCE_TYPES = {
    "semantic_model":   "Semantic Model (DAX)",
    "semanticmodel":    "Semantic Model (DAX)",
    "SemanticModel":    "Semantic Model (DAX)",
    "kusto":            "KQL Database (Kusto)",
    "kql_database":     "KQL Database (Kusto)",
    "kqldatabase":      "KQL Database (Kusto)",
    "KQLDatabase":      "KQL Database (Kusto)",
    "lakehouse":        "Lakehouse (SQL/Spark)",
    "Lakehouse":        "Lakehouse (SQL/Spark)",
    "warehouse":        "Warehouse (T-SQL)",
    "Warehouse":        "Warehouse (T-SQL)",
    "mirrored_database": "Mirrored Database",
    "MirroredDatabase": "Mirrored Database",
}

# Approx OpenAI/Azure OpenAI pricing for gpt-4o-class models (USD per 1k tokens).
_PRICE_PER_1K = {"prompt": 0.0025, "completion": 0.01}


# ══════════════════════════════════════════════════════════════
#  HELPERS
# ══════════════════════════════════════════════════════════════

def _label_datasource(t):
    if not t:
        return None
    return DATASOURCE_TYPES.get(t, DATASOURCE_TYPES.get(str(t).lower(), str(t)))


def _normalize_ts(ts):
    """Normalize timestamps to epoch seconds. Handles s, ms, us, ISO strings."""
    if ts is None:
        return None
    if isinstance(ts, str):
        try:
            s = ts.replace("Z", "+00:00")
            return datetime.fromisoformat(s).timestamp()
        except (ValueError, TypeError):
            return None
    try:
        v = float(ts)
    except (ValueError, TypeError):
        return None
    if v > 1e14:
        return v / 1_000_000.0
    if v > 1e11:
        return v / 1_000.0
    return v


def _parse_timestamp(ts):
    n = _normalize_ts(ts)
    if n is None:
        return None
    try:
        return datetime.fromtimestamp(n, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    except (ValueError, OSError):
        return str(ts)


def _safe_json_parse(s):
    if not s or not isinstance(s, str):
        return s
    try:
        return json.loads(s)
    except (json.JSONDecodeError, ValueError):
        return s


def _extract_code_block(text):
    if not text:
        return None
    m = re.search(r'```(?:dax|sql|kql|kusto|tsql)?\s*\n(.*?)```', text, re.DOTALL | re.IGNORECASE)
    return m.group(1).strip() if m else None


def _truncate(text, max_len=200):
    if not text:
        return ""
    text = str(text)
    return text[:max_len] + "..." if len(text) > max_len else text


def _stage_for_tool(tool_name):
    if not tool_name:
        return "unknown"
    if tool_name in STAGE_MAP:
        return STAGE_MAP[tool_name]
    if "fewshots.loading" in tool_name:
        return "Fewshot Loading"
    if "fewshots.matching" in tool_name:
        return "Fewshot Matching"
    if "nl2code" in tool_name:
        return "NL → Query Generation"
    if "execute" in tool_name:
        return "Query Execution"
    if tool_name.startswith("trace."):
        return "Query Execution (trace)"
    return tool_name


def _query_language_for_tool(tool_name, ds_type):
    t = (tool_name or "").lower()
    d = str(ds_type or "").lower()
    if "kusto" in t or "kusto" in d or "kql" in d:
        return "kql"
    if "warehouse" in t or "warehouse" in d:
        return "tsql"
    if "lakehouse" in t or "lakehouse" in d:
        return "sql"
    if "semantic" in t or "semantic" in d:
        return "dax"
    return "query"


# ══════════════════════════════════════════════════════════════
#  STEP PARSER
# ══════════════════════════════════════════════════════════════

def _calc_duration(step):
    c = _normalize_ts(step.get("created_at"))
    d = _normalize_ts(step.get("completed_at"))
    if c is None or d is None:
        return None
    delta = d - c
    if delta < 0 or delta > 24 * 3600:
        return None
    return round(delta, 2)


def parse_step(step):
    """Parse a single run_step into a structured dict."""
    tool_calls = (step.get("step_details") or {}).get("tool_calls", [])
    base = {
        "id": step.get("id"),
        "run_id": step.get("run_id"),
        "stage": "Answer Synthesis",
        "tool": "message_creation",
        "status": step.get("status", "unknown"),
        "created_at": _normalize_ts(step.get("created_at")),
        "completed_at": _normalize_ts(step.get("completed_at")),
        "duration_s": _calc_duration(step),
        "latency_duration_s": None,
        "input": None,
        "output": None,
        "error": step.get("last_error"),
        "datasource_type": None,
        "datasource_name": None,
        "query": None,
        "query_lang": None,
        "query_result": None,
        "diagnostic_details": None,
    }
    if not tool_calls:
        return base

    tc = tool_calls[0]
    fn = tc.get("function", {})
    tool_name = fn.get("name", "unknown")

    args = _safe_json_parse(fn.get("arguments", "{}"))
    output_raw = fn.get("output", "")
    output = _safe_json_parse(output_raw)

    ds_type = None
    ds_name = None
    if isinstance(args, dict):
        ds_type = args.get("datasource_type") or args.get("datasourceType")
        ds_name = args.get("datasource_name") or args.get("datasourceName")

    query = None
    if isinstance(args, dict):
        if "code" in args:
            query = _extract_code_block(args["code"]) or args["code"]
        elif "natural_language_query" in args:
            query = args["natural_language_query"]
        elif "query" in args:
            query = args["query"]

    if "nl2code" in tool_name and isinstance(output, str):
        extracted = _extract_code_block(output)
        if extracted:
            query = extracted

    query_result = None
    if "execute" in tool_name or "trace" in tool_name:
        query_result = output_raw if isinstance(output_raw, str) else str(output)

    base.update({
        "stage": _stage_for_tool(tool_name),
        "tool": tool_name,
        "input": args,
        "output": output,
        "datasource_type": ds_type,
        "datasource_name": ds_name,
        "query": query,
        "query_lang": _query_language_for_tool(tool_name, ds_type),
        "query_result": query_result,
        "diagnostic_details": fn.get("diagnostic_details"),
    })
    return base


# ══════════════════════════════════════════════════════════════
#  DIAGNOSTIC ANALYZER
# ══════════════════════════════════════════════════════════════

def _extract_latency_map(data):
    """Build {step_id: duration_seconds} from top-level latency.tool_calls.

    Fabric Data Agent diagnostic exports include a top-level `latency` section
    with per-tool-call durations measured by the agent runtime — these are more
    accurate than wall-clock `completed_at - created_at` (which include queue time).
    """
    sources = []
    lat = data.get("latency")
    if isinstance(lat, dict):
        sources.append(lat.get("tool_calls") or [])
    thread = data.get("thread")
    if isinstance(thread, dict):
        tlat = thread.get("latency")
        if isinstance(tlat, dict):
            sources.append(tlat.get("tool_calls") or [])
    out = {}
    for items in sources:
        for tc in items or []:
            if not isinstance(tc, dict):
                continue
            sid = tc.get("step_id")
            dur = tc.get("duration_seconds")
            if sid and dur is not None:
                try:
                    out[sid] = float(dur)
                except (TypeError, ValueError):
                    pass
    return out


def _extract_run_timings(data):
    """Return {run_id: {created, completed, total_s, status}} from runs[]."""
    runs = data.get("runs") or (data.get("thread") or {}).get("runs") or []
    out = {}
    for r in runs or []:
        if not isinstance(r, dict):
            continue
        rid = r.get("id")
        if not rid:
            continue
        c = _normalize_ts(r.get("created_at"))
        d = _normalize_ts(r.get("completed_at"))
        total = None
        if c is not None and d is not None and 0 <= (d - c) <= 24 * 3600:
            total = round(d - c, 2)
        out[rid] = {
            "created": c,
            "completed": d,
            "total_s": total,
            "status": r.get("status"),
            "model": r.get("model"),
        }
    return out


def _compute_latency_breakdown(steps, run_timings):
    """Aggregate per-stage and per-tool latency + compute orchestrator overhead.

    Uses `latency_duration_s` when available (true tool exec time),
    otherwise falls back to wall-clock `duration_s`.
    Orchestrator overhead = total run time - sum(tool latencies) → queue + LLM + serialization.
    """
    by_stage = defaultdict(lambda: {"duration_s": 0.0, "count": 0})
    by_tool = defaultdict(lambda: {"duration_s": 0.0, "count": 0})
    tool_total = 0.0
    for s in steps:
        dur = s.get("latency_duration_s")
        if dur is None:
            dur = s.get("duration_s") or 0
        dur = float(dur or 0)
        by_stage[s["stage"]]["duration_s"] += dur
        by_stage[s["stage"]]["count"] += 1
        by_tool[s["tool"]]["duration_s"] += dur
        by_tool[s["tool"]]["count"] += 1
        tool_total += dur
    run_total = sum((rt.get("total_s") or 0) for rt in (run_timings or {}).values())
    orchestrator_s = max(0.0, run_total - tool_total) if run_total else None
    return {
        "by_stage": [
            {"stage": k, "duration_s": round(v["duration_s"], 2), "count": v["count"]}
            for k, v in sorted(by_stage.items(), key=lambda x: -x[1]["duration_s"])
        ],
        "by_tool": [
            {"tool": k, "duration_s": round(v["duration_s"], 2), "count": v["count"]}
            for k, v in sorted(by_tool.items(), key=lambda x: -x[1]["duration_s"])
        ],
        "tool_total_s": round(tool_total, 2),
        "run_total_s": round(run_total, 2) if run_total else None,
        "orchestrator_overhead_s": round(orchestrator_s, 2) if orchestrator_s is not None else None,
        "orchestrator_pct": round(orchestrator_s / run_total * 100, 1) if (orchestrator_s and run_total) else None,
    }


def _detect_cached_response(steps, run_timings, answer):
    """Heuristic: cached response if answer exists but no tool execution / no run."""
    if not answer:
        return False
    if not run_timings:
        return True  # answer with no run record
    has_tool_calls = any(s["tool"] not in ("message_creation",) for s in steps)
    if not has_tool_calls:
        run_total = sum((rt.get("total_s") or 0) for rt in run_timings.values())
        if run_total < 3:
            return True
    return False


def _extract_top_level_datasources(data):
    """Extract datasources declared in config.configuration.dataSources[]."""
    sources = []
    cfg = data.get("config") or {}
    config_obj = cfg.get("configuration") if isinstance(cfg, dict) else None
    if isinstance(config_obj, dict):
        for d in config_obj.get("dataSources") or []:
            if isinstance(d, dict):
                sources.append({
                    "type": d.get("type"),
                    "name": d.get("name") or d.get("displayName"),
                    "id": d.get("id") or d.get("itemId"),
                    "schema_elements": len((d.get("schema") or {}).get("elements") or []),
                })
    return sources


def _extract_usage(data):
    """Sum token usage across runs[] if present."""
    runs = []
    if isinstance(data.get("runs"), list):
        runs = data["runs"]
    elif isinstance(data.get("thread"), dict) and isinstance(data["thread"].get("runs"), list):
        runs = data["thread"]["runs"]
    prompt = completion = 0
    found = False
    for r in runs:
        u = (r or {}).get("usage") or {}
        if u:
            found = True
            prompt += int(u.get("prompt_tokens") or 0)
            completion += int(u.get("completion_tokens") or 0)
    if not found:
        return None
    total = prompt + completion
    cost = (prompt / 1000.0) * _PRICE_PER_1K["prompt"] + (completion / 1000.0) * _PRICE_PER_1K["completion"]
    return {
        "prompt_tokens": prompt,
        "completion_tokens": completion,
        "total_tokens": total,
        "estimated_cost_usd": round(cost, 4),
    }


def _derive_run_status(data, steps):
    rs = data.get("run_status")
    if rs:
        return rs
    if isinstance(data.get("thread"), dict):
        rs = data["thread"].get("run_status")
        if rs:
            return rs
    runs = data.get("runs") or (data.get("thread") or {}).get("runs") or []
    if isinstance(runs, list) and runs:
        last = runs[-1] or {}
        if last.get("status"):
            return last["status"]
    if not steps:
        return "unknown"
    if any(s["status"] == "failed" for s in steps):
        return "failed"
    if all(s["status"] == "completed" for s in steps):
        return "completed"
    return "in_progress"


def analyze_diagnostic(data):
    """Analyze a complete diagnostic JSON and return structured analysis."""
    run_steps_data = []
    if "run_steps" in data:
        raw = data["run_steps"]
        run_steps_data = raw.get("data", []) if isinstance(raw, dict) else raw
    elif isinstance(data.get("thread"), dict) and "run_steps" in data["thread"]:
        raw = data["thread"]["run_steps"]
        run_steps_data = raw.get("data", []) if isinstance(raw, dict) else raw

    run_steps_data = list(run_steps_data or [])
    run_steps_data.sort(key=lambda s: _normalize_ts(s.get("created_at")) or 0)
    latency_map = _extract_latency_map(data)
    run_timings = _extract_run_timings(data)
    steps = []
    for raw_step in run_steps_data:
        parsed = parse_step(raw_step)
        if parsed.get("id") and parsed["id"] in latency_map:
            parsed["latency_duration_s"] = round(latency_map[parsed["id"]], 3)
        steps.append(parsed)

    # Messages
    messages = []
    msg_data = data.get("messages")
    if msg_data is None and isinstance(data.get("thread"), dict):
        msg_data = data["thread"].get("messages", {})
    if isinstance(msg_data, dict):
        msg_data = msg_data.get("data", [])
    for msg in (msg_data or []):
        role = msg.get("role", "unknown")
        content = ""
        for c in (msg.get("content") or []):
            if isinstance(c, dict):
                text = c.get("text", {})
                content = text.get("value", "") if isinstance(text, dict) else str(text)
        messages.append({"role": role, "content": content, "created_at": _normalize_ts(msg.get("created_at"))})

    question = data.get("question", "")
    if not question and isinstance(data.get("thread"), dict):
        question = data["thread"].get("question", "")
    if not question:
        user_msgs = [m for m in messages if m["role"] == "user"]
        if user_msgs:
            question = user_msgs[0]["content"]

    answer = ""
    assistant_msgs = [m for m in messages if m["role"] == "assistant"]
    if assistant_msgs:
        answer = assistant_msgs[-1]["content"]

    # Datasource type detection — merge step-level + top-level config
    ds_types = set()
    ds_names = set()
    for s in steps:
        if s["datasource_type"]:
            ds_types.add(s["datasource_type"])
        if s["datasource_name"]:
            ds_names.add(s["datasource_name"])
    top_sources = _extract_top_level_datasources(data)
    for d in top_sources:
        if d["type"]:
            ds_types.add(d["type"])
        if d["name"]:
            ds_names.add(d["name"])

    primary_ds = next(iter(ds_types), None)

    # Timing — normalized
    all_created = [s["created_at"] for s in steps if s["created_at"]]
    all_completed = [s["completed_at"] for s in steps if s["completed_at"]]
    total_duration = None
    if all_created and all_completed:
        delta = max(all_completed) - min(all_created)
        if 0 <= delta <= 24 * 3600:
            total_duration = round(delta, 2)

    usage = _extract_usage(data)
    latency_breakdown = _compute_latency_breakdown(steps, run_timings)
    is_cached = _detect_cached_response(steps, run_timings, answer)

    # Prefer the precise run-derived total when available
    if latency_breakdown.get("run_total_s"):
        total_duration = latency_breakdown["run_total_s"]

    issues = _detect_issues(steps, answer, data, messages)
    # Latency-specific anomalies
    SLOW_STEP_S = 10.0
    VERY_SLOW_TURN_S = 30.0
    ORCH_OVERHEAD_WARN_PCT = 30.0
    extra_anoms = []
    for s in steps:
        d = s.get("latency_duration_s") or s.get("duration_s") or 0
        if d >= SLOW_STEP_S:
            extra_anoms.append({
                "severity": "warning", "stage": s["stage"],
                "issue": f"Slow tool execution ({d:.1f}s)",
                "detail": f"Tool '{s['tool']}' exceeded {SLOW_STEP_S:.0f}s — review query complexity / schema size.",
            })
    if total_duration and total_duration >= VERY_SLOW_TURN_S:
        extra_anoms.append({
            "severity": "warning", "stage": "Latency",
            "issue": f"Very slow turn ({total_duration:.0f}s)",
            "detail": f"Total response time >= {VERY_SLOW_TURN_S:.0f}s — user-perceived latency is high.",
        })
    op = latency_breakdown.get("orchestrator_pct")
    if op is not None and op >= ORCH_OVERHEAD_WARN_PCT:
        extra_anoms.append({
            "severity": "info", "stage": "Latency",
            "issue": f"High orchestrator overhead ({op:.0f}% of total)",
            "detail": "Time spent outside tool calls (queue, LLM planning, serialization) — possible cold start or model contention.",
        })
    if is_cached:
        extra_anoms.append({
            "severity": "info", "stage": "Latency",
            "issue": "Cached response detected",
            "detail": "Answer returned without executing any tool — served from cache.",
        })
    anomalies = _detect_anomalies(data, steps, messages, top_sources) + extra_anoms
    issues = _aggregate_issues(issues)
    recommendations = _recommend(issues + anomalies, primary_ds, steps)

    run_status = _derive_run_status(data, steps)

    instructions = _extract_instructions(data)
    quality = score_instructions(instructions, primary_ds) if instructions else None

    return {
        "question": question,
        "answer": answer,
        "run_status": run_status,
        "datasource_types": sorted(ds_types),
        "datasource_names": sorted(ds_names),
        "primary_datasource_type": primary_ds,
        "top_level_datasources": top_sources,
        "total_duration_s": total_duration,
        "usage": usage,
        "latency": latency_breakdown,
        "is_cached": is_cached,
        "run_timings": run_timings,
        "steps": steps,
        "messages": messages,
        "issues": issues,
        "anomalies": anomalies,
        "recommendations": recommendations,
        "instruction_quality": quality,
        "metadata": {
            "agent_id": data.get("artifactId") or data.get("assistant_id"),
            "timestamp": data.get("timestamp"),
            "source": data.get("source"),
            "stage": data.get("stage"),
            "profile": data.get("profile"),
            "schema_version": data.get("$schema"),
        },
        "grading": data.get("grading"),
    }


# ══════════════════════════════════════════════════════════════
#  ISSUE DETECTION
# ══════════════════════════════════════════════════════════════

_ERROR_KEYWORDS = ("error", "invalid", "cannot find", "does not exist", "syntax error", "semanticerror")


def _detect_issues(steps, answer, data, messages):
    issues = []

    for s in steps:
        if s["status"] == "failed":
            issues.append({
                "severity": "error",
                "stage": s["stage"],
                "tool": s["tool"],
                "issue": f"Step failed: {s['tool']}",
                "detail": str(s.get("error") or "no error details"),
            })

    for s in steps:
        if s["query_result"] is not None:
            qr = str(s["query_result"]).strip()
            if not qr or qr in ("", "None", "null", "[]", "{}"):
                issues.append({
                    "severity": "warning",
                    "stage": s["stage"],
                    "tool": s["tool"],
                    "issue": "Query returned empty result",
                    "detail": f"Query: {_truncate(s.get('query', ''), 100)}",
                })

    has_query = any(s["query"] for s in steps)
    has_nl2code = any("nl2code" in (s["tool"] or "") for s in steps)
    if not has_query and has_nl2code:
        issues.append({
            "severity": "error",
            "stage": "NL → Query Generation",
            "tool": "nl2code",
            "issue": "No query was generated from the natural language input",
            "detail": "",
        })

    for fs in [s for s in steps if "fewshot" in (s["tool"] or "").lower()]:
        out = str(fs.get("output", ""))
        if "0 fewshots" in out.lower() or "loaded 0" in out.lower():
            issues.append({
                "severity": "info",
                "stage": "Fewshot Loading",
                "tool": fs["tool"],
                "issue": "No fewshots loaded — agent has no examples to learn from",
                "detail": "",
            })

    if not answer or not answer.strip():
        issues.append({
            "severity": "error",
            "stage": "Answer Synthesis",
            "tool": "message_creation",
            "issue": "Agent returned an empty answer",
            "detail": "",
        })

    for s in steps:
        if s["query_result"]:
            qrl = str(s["query_result"]).lower()
            if any(kw in qrl for kw in _ERROR_KEYWORDS):
                issues.append({
                    "severity": "error",
                    "stage": s["stage"],
                    "tool": s["tool"],
                    "issue": "Query execution returned an error",
                    "detail": _truncate(str(s["query_result"]), 200),
                })

    for s in steps:
        if s["duration_s"] and s["duration_s"] > 10:
            issues.append({
                "severity": "info",
                "stage": s["stage"],
                "tool": s["tool"],
                "issue": "Slow step",
                "detail": f"{s['duration_s']:.1f}s on {s['tool']}",
                "_duration": s["duration_s"],
            })

    return issues


def _aggregate_issues(issues):
    """Collapse N identical (stage, issue) rows into one with count + stats."""
    grouped = defaultdict(list)
    for i in issues:
        key = (i["stage"], i["issue"], i.get("severity"))
        grouped[key].append(i)
    aggregated = []
    for (stage, issue, sev), items in grouped.items():
        if len(items) == 1:
            aggregated.append(items[0])
            continue
        durations = [it["_duration"] for it in items if it.get("_duration")]
        agg = {
            "severity": sev,
            "stage": stage,
            "issue": issue,
            "count": len(items),
            "tool": items[0].get("tool"),
        }
        if durations:
            agg["detail"] = f"{len(items)}× steps, avg {sum(durations)/len(durations):.1f}s, max {max(durations):.1f}s"
        else:
            agg["detail"] = f"{len(items)}× occurrences. First: {items[0].get('detail','')}"
        aggregated.append(agg)
    sev_rank = {"error": 0, "warning": 1, "info": 2}
    aggregated.sort(key=lambda x: (sev_rank.get(x.get("severity"), 9), x.get("stage", "")))
    return aggregated


# ══════════════════════════════════════════════════════════════
#  ANOMALY DETECTION (cross-cutting heuristics)
# ══════════════════════════════════════════════════════════════

def _args_signature(args):
    try:
        return hashlib.sha1(json.dumps(args, sort_keys=True, default=str).encode("utf-8")).hexdigest()[:12]
    except (TypeError, ValueError):
        return hashlib.sha1(str(args).encode("utf-8")).hexdigest()[:12]


def _detect_anomalies(data, steps, messages, top_sources):
    anomalies = []

    # A1. Empty top-level dataSources while the pipeline actually queries one
    if not top_sources and any(s["query"] for s in steps):
        anomalies.append({
            "severity": "warning",
            "stage": "Configuration",
            "issue": "config.configuration.dataSources is empty but the agent queried a source",
            "detail": "Agent definition may be missing the dataSources block.",
        })

    # A2. Schema not extracted (elements == 0)
    for d in top_sources:
        if d.get("schema_elements") == 0:
            anomalies.append({
                "severity": "warning",
                "stage": "Configuration",
                "issue": f"Datasource '{d.get('name') or d.get('id')}' has 0 schema elements",
                "detail": "Schema introspection failed — NL2DAX/NL2KQL relies on hints only.",
            })

    # A3. Retry loop — same tool + same args, 3+ consecutive
    last_sig = None
    streak = 1
    fired = False
    for s in steps:
        sig = (s["tool"], _args_signature(s.get("input")))
        if sig == last_sig:
            streak += 1
            if streak == 3 and not fired:
                anomalies.append({
                    "severity": "warning",
                    "stage": s["stage"],
                    "issue": "Retry loop detected",
                    "detail": f"Tool '{s['tool']}' called 3+ times with identical arguments — agent appears stuck.",
                })
                fired = True
        else:
            streak = 1
            fired = False
        last_sig = sig

    # A4. Thread pollution heuristic
    if len(messages) >= 50:
        anomalies.append({
            "severity": "warning",
            "stage": "Thread",
            "issue": f"Thread has {len(messages)} messages (>50 = pollution risk)",
            "detail": "DELETE the thread before next question — Fabric reuses threads per user.",
        })

    # A5. Schema re-discovery
    schema_calls = Counter()
    for s in steps:
        q = (s.get("query") or "").lower()
        if not q:
            continue
        for marker in ("| getschema", "evaluate topn(1, "):
            if marker in q:
                m = re.search(re.escape(marker) + r"\s*([\w\.]+)", q)
                key = m.group(1) if m else marker
                schema_calls[(marker, key)] += 1
    for (marker, target), n in schema_calls.items():
        if n >= 2:
            anomalies.append({
                "severity": "info",
                "stage": "Schema",
                "issue": f"Schema re-discovery: '{target}' inspected {n}× in same run",
                "detail": "Cache schema in agent instructions or fewshots to save tokens & latency.",
            })

    # A6. Fewshot density vs distinct queries
    n_fewshots = 0
    for s in steps:
        if "fewshot" in (s.get("tool") or "").lower():
            out = str(s.get("output", ""))
            m = re.search(r"loaded (\d+)|(\d+) fewshots", out, re.IGNORECASE)
            if m:
                try:
                    n_fewshots = max(n_fewshots, int(m.group(1) or m.group(2) or 0))
                except (TypeError, ValueError):
                    pass
    distinct_queries = len({(s.get("query") or "").strip()[:120] for s in steps if s.get("query")})
    if n_fewshots and distinct_queries and n_fewshots < distinct_queries / 2:
        anomalies.append({
            "severity": "info",
            "stage": "Fewshots",
            "issue": f"Sparse fewshots: {n_fewshots} examples vs {distinct_queries} distinct queries observed",
            "detail": "Aim for ≥1 fewshot per critical query pattern or function.",
        })

    return anomalies


# ══════════════════════════════════════════════════════════════
#  RECOMMENDATION ENGINE
# ══════════════════════════════════════════════════════════════

_RECO_MATRIX = {
    "No fewshots": {
        "semantic_model": "Enable Prep for AI → add Verified Answers in the semantic model.",
        "kusto":          "Add 1 fewshot per critical KQL function (question + canonical query).",
        "lakehouse":      "Add fewshots showing JOIN + WHERE patterns for common questions.",
        "warehouse":      "Add fewshots showing T-SQL idioms (window functions, CTEs).",
        "*":              "Add fewshot examples covering each major question category.",
    },
    "empty result": {
        "*": "Validate data exists and filters are correct — consider relaxing date/scope filters.",
    },
    "Query execution returned an error": {
        "kusto":          "Inspect KQL syntax: check let-statement order and operator precedence.",
        "semantic_model": "Check measure names (case+whitespace sensitive) and table relationships.",
        "lakehouse":      "Verify Delta table exists and Spark/SQL endpoint is reachable.",
        "warehouse":      "Verify object exists and the calling identity has SELECT on the schema.",
        "*":              "Inspect the failing query and validate identifiers exist.",
    },
    "No query was generated": {
        "*": "Strengthen instructions: add explicit 'ALWAYS query the database using <DAX|KQL|SQL>' clause.",
    },
    "Retry loop": {
        "*": "Add a fewshot for the failing intent and clarify expected output format in instructions.",
    },
    "Thread": {
        "*": "DELETE the conversation thread before each new question (Fabric reuses threads per user).",
    },
    "Slow step": {
        "semantic_model": "Pre-compute heavy measures or add aggregation tables; check Direct Lake fallback.",
        "kusto":          "Materialize hot aggregations; add update policies; check function complexity.",
        "*":              "Profile the query and add appropriate indexing / pre-aggregations.",
    },
    "dataSources is empty": {
        "*": "Re-deploy the agent definition with the dataSources[] block populated.",
    },
    "0 schema elements": {
        "*": "Re-run Prep for AI / schema discovery on the datasource.",
    },
    "Schema re-discovery": {
        "kusto": "Cache schema in instructions (function signatures + key tables) to avoid getschema round-trips.",
        "*":     "Hint the schema in the agent's additionalInstructions section.",
    },
    "Sparse fewshots": {
        "*": "Increase fewshot coverage — target ≥1 per critical query pattern.",
    },
}


def _recommend(issues, ds_type, steps):
    ds_key = "*"
    if ds_type:
        low = str(ds_type).lower()
        for k in ("semantic_model", "kusto", "lakehouse", "warehouse"):
            if k in low:
                ds_key = k
                break

    recos = []
    seen = set()
    for it in issues:
        issue_text = it.get("issue", "")
        for keyword, mapping in _RECO_MATRIX.items():
            if keyword.lower() in issue_text.lower():
                advice = mapping.get(ds_key) or mapping.get("*")
                if advice and advice not in seen:
                    recos.append({
                        "trigger": issue_text,
                        "severity": it.get("severity"),
                        "advice": advice,
                    })
                    seen.add(advice)
                break
    return recos


# ══════════════════════════════════════════════════════════════
#  INSTRUCTION QUALITY SCORING
# ══════════════════════════════════════════════════════════════

def _extract_instructions(data):
    """Find the agent additionalInstructions/aiInstructions string in any schema variant."""
    candidates = []
    cfg = data.get("config") or {}
    if isinstance(cfg, dict):
        c = cfg.get("configuration") or {}
        if isinstance(c, dict):
            candidates.append(c.get("additionalInstructions"))
            candidates.append(c.get("aiInstructions"))
            candidates.append(c.get("instructions"))
    for k in ("additionalInstructions", "aiInstructions", "instructions"):
        candidates.append(data.get(k))
    for v in candidates:
        if isinstance(v, str) and v.strip():
            return v
    return None


# 10-point rubric (datasource-aware)
_RUBRIC = {
    "semantic_model": {
        "persona":         ["you are", "agent", "assistant", "expert"],
        "context":         ["domain", "company", "business", "module"],
        "kpi_formulas":    ["measure", "calculated", "= calculate", "divide", "sum"],
        "response_format": ["format", "respond with", "answer in", "language"],
        "attribution":     ["source", "from the model", "verified"],
        "edge_cases":      ["if no data", "if empty", "missing", "null"],
        "disclaimers":     ["estimate", "may not", "approximate"],
        "examples":        ["example", "for instance", "e.g."],
        "actionability":   ["explain", "show", "include", "compare"],
        "tooling_hint":    ["always query", "use the semantic model", "dax"],
    },
    "kusto": {
        "persona":         ["you are", "agent", "expert"],
        "context":         ["operations", "telemetry", "domain"],
        "kpi_formulas":    ["function", "let", "summarize", "calculate"],
        "response_format": ["format", "respond with", "answer in", "language"],
        "attribution":     ["source", "function", "table"],
        "edge_cases":      ["if no rows", "if empty", "missing", "null"],
        "disclaimers":     ["approximate", "estimate"],
        "examples":        ["example", "for instance", "e.g."],
        "actionability":   ["explain", "show", "compare", "trend"],
        "tooling_hint":    ["always query", "kql", "kusto", "use the database"],
    },
    "lakehouse": {
        "persona":         ["you are", "agent"],
        "context":         ["domain", "tables"],
        "kpi_formulas":    ["sum", "avg", "group by", "join"],
        "response_format": ["format", "respond with", "language"],
        "attribution":     ["source", "table"],
        "edge_cases":      ["if empty", "missing", "null"],
        "disclaimers":     ["approximate"],
        "examples":        ["example", "e.g."],
        "actionability":   ["explain", "compare"],
        "tooling_hint":    ["always query", "sql", "use the lakehouse"],
    },
}


def score_instructions(instructions, ds_type):
    if not instructions:
        return None
    ds_key = "semantic_model"
    if ds_type:
        low = str(ds_type).lower()
        for k in ("kusto", "lakehouse", "warehouse"):
            if k in low:
                ds_key = k if k in _RUBRIC else "semantic_model"
                break
    rubric = _RUBRIC.get(ds_key, _RUBRIC["semantic_model"])
    low_text = instructions.lower()
    results = {crit: any(kw in low_text for kw in kws) for crit, kws in rubric.items()}
    score = sum(1 for v in results.values() if v)
    return {
        "datasource_profile": ds_key,
        "score": score,
        "max": len(rubric),
        "criteria": results,
        "length_chars": len(instructions),
    }


# ══════════════════════════════════════════════════════════════
#  REPORT FORMATTER
# ══════════════════════════════════════════════════════════════

def format_report(analysis):
    lines = []
    W = 72
    lines.append("=" * W)
    lines.append("  DATA AGENT DIAGNOSTIC REPORT")
    lines.append("=" * W)

    lines.append(f"  Question : {_truncate(analysis['question'], 60)}")
    lines.append(f"  Status   : {analysis['run_status']}")
    if analysis["datasource_types"]:
        labels = [_label_datasource(t) for t in analysis["datasource_types"]]
        lines.append(f"  Source(s): {', '.join(labels)}")
    if analysis["datasource_names"]:
        lines.append(f"  Dataset  : {', '.join(analysis['datasource_names'])}")
    if analysis.get("is_cached"):
        lines.append(f"  [cached]   served without tool execution")
    if analysis["total_duration_s"]:
        lines.append(f"  Duration : {analysis['total_duration_s']:.1f}s")
    if analysis.get("usage"):
        u = analysis["usage"]
        lines.append(f"  Tokens   : {u['total_tokens']:,} (~${u['estimated_cost_usd']})")
    lines.append("")

    lines.append("-" * W)
    lines.append("  PIPELINE TRACE")
    lines.append("-" * W)
    for i, step in enumerate(analysis["steps"], 1):
        icon = {"completed": "+", "failed": "X", "cancelled": "-"}.get(step["status"], "?")
        # Prefer true latency over wall-clock when available
        dur_val = step.get("latency_duration_s")
        dur_src = "µ"  # latency-measured marker
        if dur_val is None:
            dur_val = step.get("duration_s")
            dur_src = ""
        dur = f" ({dur_val:.1f}s{dur_src})" if dur_val else ""
        lines.append(f"  {icon} Step {i}: {step['stage']}{dur}")
        if step["query"]:
            lines.append(f"      Query: {_truncate(step['query'].replace(chr(10), ' '), 80)}")
        if step["query_result"]:
            lines.append(f"      Result: {_truncate(str(step['query_result']).replace(chr(10), ' | '), 80)}")
        if step["error"]:
            lines.append(f"      Error: {step['error']}")
    lines.append("")

    # Latency breakdown
    lat = analysis.get("latency") or {}
    if lat.get("by_stage") and (lat.get("run_total_s") or lat.get("tool_total_s")):
        lines.append("-" * W)
        lines.append("  LATENCY BREAKDOWN")
        lines.append("-" * W)
        run_total = lat.get("run_total_s") or 0
        tool_total = lat.get("tool_total_s") or 0
        orch = lat.get("orchestrator_overhead_s")
        orch_pct = lat.get("orchestrator_pct")
        if run_total:
            lines.append(f"  Total run        : {run_total:.1f}s")
            lines.append(f"  Tool execution   : {tool_total:.1f}s ({tool_total/run_total*100:.0f}%)")
            if orch is not None:
                lines.append(f"  Orchestrator     : {orch:.1f}s ({orch_pct:.0f}%)  [queue + LLM + serialization]")
        lines.append("  ----")
        for row in lat["by_stage"][:8]:
            if row["duration_s"] <= 0:
                continue
            bar_pct = (row["duration_s"] / run_total * 100) if run_total else 0
            bar = "#" * max(1, int(bar_pct / 4))
            lines.append(f"  {row['stage']:<32} {row['duration_s']:>6.1f}s  {bar_pct:>4.0f}%  {bar}")
        lines.append("  (µ = measured by latency.tool_calls, otherwise wall-clock)")
        lines.append("")

    lines.append("-" * W)
    lines.append("  ANSWER")
    lines.append("-" * W)
    answer = analysis["answer"]
    if answer:
        for ln in answer.split("\n")[:10]:
            lines.append(f"  {ln}")
        if answer.count("\n") > 10:
            lines.append(f"  ... ({answer.count(chr(10)) - 10} more lines)")
    else:
        lines.append("  (empty)")
    lines.append("")

    if analysis["issues"]:
        lines.append("-" * W)
        lines.append("  ISSUES DETECTED")
        lines.append("-" * W)
        for issue in analysis["issues"]:
            icon = {"error": "!!!", "warning": " !!", "info": "  i"}.get(issue.get("severity"), "  ?")
            count = f" (×{issue['count']})" if issue.get("count", 1) > 1 else ""
            lines.append(f"  {icon} [{issue['stage']}] {issue['issue']}{count}")
            if issue.get("detail"):
                lines.append(f"      {issue['detail']}")
        lines.append("")

    if analysis.get("anomalies"):
        lines.append("-" * W)
        lines.append("  ANOMALIES (cross-cutting)")
        lines.append("-" * W)
        for a in analysis["anomalies"]:
            icon = {"error": "!!!", "warning": " !!", "info": "  i"}.get(a.get("severity"), "  ?")
            lines.append(f"  {icon} [{a['stage']}] {a['issue']}")
            if a.get("detail"):
                lines.append(f"      {a['detail']}")
        lines.append("")

    if analysis.get("recommendations"):
        lines.append("-" * W)
        lines.append("  RECOMMENDATIONS")
        lines.append("-" * W)
        for r in analysis["recommendations"]:
            lines.append(f"  -> {r['advice']}")
            lines.append(f"     (triggered by: {r['trigger']})")
        lines.append("")

    q = analysis.get("instruction_quality")
    if q:
        lines.append("-" * W)
        lines.append("  INSTRUCTION QUALITY")
        lines.append("-" * W)
        lines.append(f"  Profile : {q['datasource_profile']}")
        lines.append(f"  Score   : {q['score']}/{q['max']} ({q['length_chars']} chars)")
        for crit, ok in q["criteria"].items():
            mark = "+" if ok else "-"
            lines.append(f"    [{mark}] {crit}")
        lines.append("")

    grading = analysis.get("grading")
    if grading:
        lines.append("-" * W)
        lines.append("  GRADING")
        lines.append("-" * W)
        lines.append(f"  Verdict   : {grading.get('verdict', 'N/A')}")
        if grading.get("expected"):
            lines.append(f"  Expected  : {_truncate(str(grading['expected']), 60)}")
        if grading.get("root_cause"):
            lines.append(f"  Root Cause: {grading['root_cause']}")
        lines.append("")

    lines.append("=" * W)
    return "\n".join(lines)


# ══════════════════════════════════════════════════════════════
#  DIFF MODE
# ══════════════════════════════════════════════════════════════

def diff_diagnostics(a, b):
    """Compare two analyses and return a structured diff."""
    def issue_set(an):
        return {(i["stage"], i["issue"], i.get("severity")) for i in an.get("issues", [])}

    a_iss = issue_set(a)
    b_iss = issue_set(b)
    return {
        "status": {"before": a.get("run_status"), "after": b.get("run_status")},
        "duration_s": {"before": a.get("total_duration_s"), "after": b.get("total_duration_s")},
        "tokens": {
            "before": (a.get("usage") or {}).get("total_tokens"),
            "after":  (b.get("usage") or {}).get("total_tokens"),
        },
        "steps_count": {"before": len(a.get("steps", [])), "after": len(b.get("steps", []))},
        "issues_resolved": sorted(a_iss - b_iss),
        "issues_introduced": sorted(b_iss - a_iss),
        "issues_unchanged": sorted(a_iss & b_iss),
        "quality_score": {
            "before": (a.get("instruction_quality") or {}).get("score"),
            "after":  (b.get("instruction_quality") or {}).get("score"),
        },
    }


def format_diff(diff, name_a="before", name_b="after"):
    lines = []
    W = 72
    lines.append("=" * W)
    lines.append(f"  DIAGNOSTIC DIFF — {name_a} → {name_b}")
    lines.append("=" * W)
    for field in ("status", "duration_s", "tokens", "steps_count", "quality_score"):
        v = diff.get(field, {})
        lines.append(f"  {field:14s}: {v.get('before')!r}  →  {v.get('after')!r}")
    lines.append("")
    if diff["issues_resolved"]:
        lines.append("-" * W)
        lines.append(f"  RESOLVED ({len(diff['issues_resolved'])})")
        lines.append("-" * W)
        for stage, issue, sev in diff["issues_resolved"]:
            lines.append(f"  [{sev}] {stage}: {issue}")
        lines.append("")
    if diff["issues_introduced"]:
        lines.append("-" * W)
        lines.append(f"  NEW ({len(diff['issues_introduced'])})")
        lines.append("-" * W)
        for stage, issue, sev in diff["issues_introduced"]:
            lines.append(f"  [{sev}] {stage}: {issue}")
        lines.append("")
    if diff["issues_unchanged"]:
        lines.append("-" * W)
        lines.append(f"  UNCHANGED ({len(diff['issues_unchanged'])})")
        lines.append("-" * W)
        for stage, issue, sev in diff["issues_unchanged"]:
            lines.append(f"  [{sev}] {stage}: {issue}")
        lines.append("")
    lines.append("=" * W)
    return "\n".join(lines)


# ══════════════════════════════════════════════════════════════
#  BATCH PROCESSOR
# ══════════════════════════════════════════════════════════════

def diagnose_file(path):
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return analyze_diagnostic(data)


def diagnose_folder(folder_path):
    folder = Path(folder_path)
    results = []
    for f in sorted(folder.glob("*.json")):
        try:
            analysis = diagnose_file(f)
            analysis["_source_file"] = str(f.name)
            results.append(analysis)
        except (json.JSONDecodeError, KeyError) as e:
            results.append({"_source_file": str(f.name), "_error": str(e)})
    return results


def format_batch_summary(results):
    lines = []
    W = 72
    lines.append("=" * W)
    lines.append("  DATA AGENT DIAGNOSTIC BATCH SUMMARY")
    lines.append("=" * W)

    valid = [r for r in results if "_error" not in r]
    errors = [r for r in results if "_error" in r]
    lines.append(f"  Files analyzed : {len(results)}")
    lines.append(f"  Successful     : {len(valid)}")
    if errors:
        lines.append(f"  Parse errors   : {len(errors)}")

    if valid:
        statuses = Counter(r.get("run_status", "unknown") for r in valid)
        lines.append("\n  Status distribution:")
        for s, c in statuses.most_common():
            lines.append(f"    {s}: {c}")

        all_ds = set()
        for r in valid:
            all_ds.update(r.get("datasource_types") or [])
        if all_ds:
            labels = [_label_datasource(t) for t in all_ds]
            lines.append(f"\n  Datasource types: {', '.join(labels)}")

        all_issues = []
        for r in valid:
            all_issues.extend(r.get("issues") or [])
        if all_issues:
            counts = Counter(i["issue"] for i in all_issues)
            lines.append("\n  Top issues:")
            for issue, c in counts.most_common(10):
                lines.append(f"    {c}x {issue}")

        durations = [r["total_duration_s"] for r in valid if r.get("total_duration_s")]
        if durations:
            lines.append("\n  Timing:")
            lines.append(f"    Avg: {sum(durations)/len(durations):.1f}s")
            lines.append(f"    Min: {min(durations):.1f}s | Max: {max(durations):.1f}s")

        tokens = [(r.get("usage") or {}).get("total_tokens") for r in valid]
        tokens = [t for t in tokens if t]
        if tokens:
            lines.append(f"\n  Tokens (total across runs): {sum(tokens):,}")

    lines.append("")
    lines.append("=" * W)

    for r in valid:
        lines.append(f"\n  {r.get('_source_file', '?')}")
        lines.append(f"    Q: {_truncate(r.get('question', ''), 55)}")
        lines.append(
            f"    Status: {r.get('run_status')} | "
            f"Steps: {len(r.get('steps') or [])} | "
            f"Issues: {len(r.get('issues') or [])} | "
            f"Anomalies: {len(r.get('anomalies') or [])}"
        )
        for issue in (r.get("issues") or [])[:2]:
            icon = {"error": "!!!", "warning": " !!", "info": "  i"}.get(issue.get("severity"), "  ?")
            lines.append(f"    {icon} {issue['issue']}")

    return "\n".join(lines)
