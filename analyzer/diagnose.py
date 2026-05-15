"""Diagnose — parse and analyze Fabric Data Agent diagnostic JSON files.

Accepts any diagnostic JSON: portal exports, analyzer-generated diagnostics,
or raw run_steps dumps. Works with all datasource types (semantic model, 
lakehouse, warehouse, KQL database, etc.).

Usage:
    python -m analyzer diagnose path/to/diagnostic.json
    python -m analyzer diagnose path/to/folder/   # all .json in folder
    python -m analyzer diagnose path/to/file.json --json  # structured output
"""

import json
import re
from datetime import datetime, timezone
from pathlib import Path


# ── Tool name → human-readable stage ─────────────────────────

STAGE_MAP = {
    # Semantic model pipeline
    "analyze.database.fewshots.loading":  "Fewshot Loading",
    "analyze.database.fewshots.matching": "Fewshot Matching",
    "analyze.database.nl2code":          "NL → Query Generation",
    "trace.analyze_semantic_model":      "Query Execution (trace)",
    "analyze.database.execute":          "Query Execution",
    "generate.filename":                 "Output Naming",
    "message_creation":                  "Answer Synthesis",
    # Legacy / alternate names
    "nl2sa_query":      "NL → Query Generation",
    "nl2sql_query":     "NL → SQL Generation",
    "evaluate_dax":     "DAX Execution",
    "evaluate_sql":     "SQL Execution",
    "evaluate_query":   "Query Execution",
}

# ── Datasource type detection ────────────────────────────────

DATASOURCE_TYPES = {
    "SemanticModel": "Semantic Model (DAX)",
    "Lakehouse":     "Lakehouse (SQL/Spark)",
    "Warehouse":     "Warehouse (T-SQL)",
    "KQLDatabase":   "KQL Database (Kusto)",
    "MirroredDatabase": "Mirrored Database",
}


def _parse_timestamp(ts):
    """Convert epoch timestamp to readable datetime."""
    if ts is None:
        return None
    try:
        return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    except (ValueError, OSError):
        return str(ts)


def _safe_json_parse(s):
    """Try to parse a string as JSON, return dict or original string."""
    if not s or not isinstance(s, str):
        return s
    try:
        return json.loads(s)
    except (json.JSONDecodeError, ValueError):
        return s


def _extract_code_block(text):
    """Extract code from markdown code blocks."""
    if not text:
        return None
    m = re.search(r'```(?:dax|sql|kql|kusto)?\s*\n(.*?)```', text, re.DOTALL | re.IGNORECASE)
    return m.group(1).strip() if m else None


def _truncate(text, max_len=200):
    """Truncate text for display."""
    if not text:
        return ""
    text = str(text)
    return text[:max_len] + "..." if len(text) > max_len else text


# ══════════════════════════════════════════════════════════════
#  STEP PARSER
# ══════════════════════════════════════════════════════════════

def parse_step(step):
    """Parse a single run_step into a structured dict."""
    tool_calls = (step.get("step_details") or {}).get("tool_calls", [])
    if not tool_calls:
        # message_creation step (no tool calls)
        return {
            "stage": "Answer Synthesis",
            "tool": "message_creation",
            "status": step.get("status", "unknown"),
            "created_at": step.get("created_at"),
            "completed_at": step.get("completed_at"),
            "duration_s": _calc_duration(step),
            "input": None,
            "output": None,
            "error": step.get("last_error"),
            "datasource_type": None,
            "datasource_name": None,
            "query": None,
            "query_result": None,
            "diagnostic_details": None,
        }

    tc = tool_calls[0]
    fn = tc.get("function", {})
    tool_name = fn.get("name", "unknown")
    stage = STAGE_MAP.get(tool_name, tool_name)

    # Parse arguments
    args_raw = fn.get("arguments", "{}")
    args = _safe_json_parse(args_raw)

    # Parse output
    output_raw = fn.get("output", "")
    output = _safe_json_parse(output_raw)

    # Extract datasource info from arguments
    ds_type = None
    ds_name = None
    if isinstance(args, dict):
        ds_type = args.get("datasource_type")
        ds_name = args.get("datasource_name")

    # Extract query from arguments or output
    query = None
    if isinstance(args, dict) and "code" in args:
        query = _extract_code_block(args["code"]) or args["code"]
    elif isinstance(args, dict) and "natural_language_query" in args:
        query = args["natural_language_query"]

    # Extract query from output (nl2code step)
    if tool_name == "analyze.database.nl2code" and isinstance(output, str):
        extracted = _extract_code_block(output)
        if extracted:
            query = extracted

    # Query result (execute step)
    query_result = None
    if "execute" in tool_name or "trace" in tool_name:
        query_result = output_raw if isinstance(output_raw, str) else str(output)

    # Diagnostic details (nl2code has the richest payload)
    diag = fn.get("diagnostic_details")

    return {
        "stage": stage,
        "tool": tool_name,
        "status": step.get("status", "unknown"),
        "created_at": step.get("created_at"),
        "completed_at": step.get("completed_at"),
        "duration_s": _calc_duration(step),
        "input": args,
        "output": output,
        "error": step.get("last_error"),
        "datasource_type": ds_type,
        "datasource_name": ds_name,
        "query": query,
        "query_result": query_result,
        "diagnostic_details": diag,
    }


def _calc_duration(step):
    c = step.get("created_at")
    d = step.get("completed_at")
    if c and d and isinstance(c, (int, float)) and isinstance(d, (int, float)):
        return round(d - c, 2)
    return None


# ══════════════════════════════════════════════════════════════
#  DIAGNOSTIC ANALYZER
# ══════════════════════════════════════════════════════════════

def analyze_diagnostic(data):
    """Analyze a complete diagnostic JSON and return structured analysis."""
    # Handle different input formats
    run_steps_data = []
    if "run_steps" in data:
        raw = data["run_steps"]
        run_steps_data = raw.get("data", []) if isinstance(raw, dict) else raw
    elif "thread" in data and "run_steps" in data["thread"]:
        raw = data["thread"]["run_steps"]
        run_steps_data = raw.get("data", []) if isinstance(raw, dict) else raw

    # Sort steps by created_at (ascending = execution order)
    run_steps_data.sort(key=lambda s: s.get("created_at") or 0)

    # Parse all steps
    steps = [parse_step(s) for s in run_steps_data]

    # Extract messages
    messages = []
    msg_data = data.get("messages", data.get("thread", {}).get("messages", {}))
    if isinstance(msg_data, dict):
        msg_data = msg_data.get("data", [])
    for msg in (msg_data or []):
        role = msg.get("role", "unknown")
        content = ""
        for c in (msg.get("content") or []):
            if isinstance(c, dict):
                text = c.get("text", {})
                if isinstance(text, dict):
                    content = text.get("value", "")
                else:
                    content = str(text)
        messages.append({"role": role, "content": content, "created_at": msg.get("created_at")})

    # Extract question
    question = data.get("question", "")
    if not question and "thread" in data:
        question = data["thread"].get("question", "")
    if not question:
        user_msgs = [m for m in messages if m["role"] == "user"]
        if user_msgs:
            question = user_msgs[0]["content"]

    # Extract answer
    answer = ""
    assistant_msgs = [m for m in messages if m["role"] == "assistant"]
    if assistant_msgs:
        answer = assistant_msgs[-1]["content"]

    # Detect datasource type
    ds_types = set()
    ds_names = set()
    for s in steps:
        if s["datasource_type"]:
            ds_types.add(s["datasource_type"])
        if s["datasource_name"]:
            ds_names.add(s["datasource_name"])

    # Detect issues
    issues = _detect_issues(steps, answer, data)

    # Timing
    all_created = [s["created_at"] for s in steps if s["created_at"]]
    all_completed = [s["completed_at"] for s in steps if s["completed_at"]]
    total_duration = (max(all_completed) - min(all_created)) if all_created and all_completed else None

    # Overall status
    run_status = data.get("run_status", "unknown")
    if "thread" in data:
        run_status = data["thread"].get("run_status", run_status)

    return {
        "question": question,
        "answer": answer,
        "run_status": run_status,
        "datasource_types": list(ds_types),
        "datasource_names": list(ds_names),
        "total_duration_s": total_duration,
        "steps": steps,
        "messages": messages,
        "issues": issues,
        "metadata": {
            "agent_id": data.get("artifactId", data.get("run_steps", {}).get("data", [{}])[0].get("assistant_id") if run_steps_data else None),
            "timestamp": data.get("timestamp"),
            "source": data.get("source"),
            "stage": data.get("stage"),
            "profile": data.get("profile"),
        },
        "grading": data.get("grading"),
    }


# ══════════════════════════════════════════════════════════════
#  ISSUE DETECTION
# ══════════════════════════════════════════════════════════════

def _detect_issues(steps, answer, data):
    """Detect common problems from the pipeline trace."""
    issues = []

    # 1. Failed steps
    for s in steps:
        if s["status"] == "failed":
            issues.append({
                "severity": "error",
                "stage": s["stage"],
                "issue": f"Step failed: {s['tool']}",
                "detail": str(s.get("error") or "no error details"),
            })

    # 2. Empty query result
    for s in steps:
        if s["query_result"] is not None:
            qr = str(s["query_result"]).strip()
            if not qr or qr in ("", "None", "null", "[]", "{}"):
                issues.append({
                    "severity": "warning",
                    "stage": s["stage"],
                    "issue": "Query returned empty result",
                    "detail": f"Query: {_truncate(s.get('query', ''), 100)}",
                })

    # 3. No query generated
    has_query = any(s["query"] for s in steps)
    has_nl2code = any("nl2code" in (s["tool"] or "") for s in steps)
    if not has_query and has_nl2code:
        issues.append({
            "severity": "error",
            "stage": "NL → Query Generation",
            "issue": "No query was generated from the natural language input",
            "detail": "",
        })

    # 4. No fewshots loaded
    fewshot_steps = [s for s in steps if "fewshot" in (s["tool"] or "").lower()]
    for fs in fewshot_steps:
        output_str = str(fs.get("output", ""))
        if "0 fewshots" in output_str.lower() or "loaded 0" in output_str.lower():
            issues.append({
                "severity": "info",
                "stage": "Fewshot Loading",
                "issue": "No fewshots loaded — agent has no examples to learn from",
                "detail": "Consider adding fewshot examples to improve accuracy",
            })

    # 5. Empty answer
    if not answer or not answer.strip():
        issues.append({
            "severity": "error",
            "stage": "Answer Synthesis",
            "issue": "Agent returned an empty answer",
            "detail": "",
        })

    # 6. Query error in output
    for s in steps:
        if s["query_result"]:
            qr_lower = str(s["query_result"]).lower()
            if any(kw in qr_lower for kw in ["error", "invalid", "cannot find", "does not exist", "syntax error"]):
                issues.append({
                    "severity": "error",
                    "stage": s["stage"],
                    "issue": "Query execution returned an error",
                    "detail": _truncate(str(s["query_result"]), 200),
                })

    # 7. Slow steps (>10s)
    for s in steps:
        if s["duration_s"] and s["duration_s"] > 10:
            issues.append({
                "severity": "info",
                "stage": s["stage"],
                "issue": f"Slow step: {s['duration_s']:.1f}s",
                "detail": f"Tool: {s['tool']}",
            })

    return issues


# ══════════════════════════════════════════════════════════════
#  REPORT FORMATTER
# ══════════════════════════════════════════════════════════════

def format_report(analysis):
    """Format analysis into a readable terminal report."""
    lines = []
    W = 72
    lines.append("=" * W)
    lines.append("  DATA AGENT DIAGNOSTIC REPORT")
    lines.append("=" * W)

    # Summary
    lines.append(f"  Question : {_truncate(analysis['question'], 60)}")
    lines.append(f"  Status   : {analysis['run_status']}")
    if analysis["datasource_types"]:
        ds_labels = [DATASOURCE_TYPES.get(t, t) for t in analysis["datasource_types"]]
        lines.append(f"  Source(s) : {', '.join(ds_labels)}")
    if analysis["datasource_names"]:
        lines.append(f"  Dataset  : {', '.join(analysis['datasource_names'])}")
    if analysis["total_duration_s"]:
        lines.append(f"  Duration : {analysis['total_duration_s']:.1f}s")
    lines.append("")

    # Pipeline Steps
    lines.append("-" * W)
    lines.append("  PIPELINE TRACE")
    lines.append("-" * W)
    for i, step in enumerate(analysis["steps"], 1):
        status_icon = {"completed": "+", "failed": "X", "cancelled": "-"}.get(step["status"], "?")
        dur = f" ({step['duration_s']:.1f}s)" if step["duration_s"] else ""
        lines.append(f"  {status_icon} Step {i}: {step['stage']}{dur}")

        if step["query"]:
            query_preview = step["query"].replace("\n", " ")[:80]
            lines.append(f"      Query: {query_preview}")

        if step["query_result"]:
            result_preview = str(step["query_result"]).replace("\n", " | ")[:80]
            lines.append(f"      Result: {result_preview}")

        if step["error"]:
            lines.append(f"      Error: {step['error']}")

    lines.append("")

    # Answer
    lines.append("-" * W)
    lines.append("  ANSWER")
    lines.append("-" * W)
    answer = analysis["answer"]
    if answer:
        for line in answer.split("\n")[:10]:
            lines.append(f"  {line}")
        if answer.count("\n") > 10:
            lines.append(f"  ... ({answer.count(chr(10)) - 10} more lines)")
    else:
        lines.append("  (empty)")
    lines.append("")

    # Issues
    if analysis["issues"]:
        lines.append("-" * W)
        lines.append("  ISSUES DETECTED")
        lines.append("-" * W)
        for issue in analysis["issues"]:
            icon = {"error": "!!!", "warning": " !!", "info": "  i"}.get(issue["severity"], "  ?")
            lines.append(f"  {icon} [{issue['stage']}] {issue['issue']}")
            if issue["detail"]:
                lines.append(f"      {issue['detail']}")
        lines.append("")

    # Grading (if present)
    grading = analysis.get("grading")
    if grading:
        lines.append("-" * W)
        lines.append("  GRADING")
        lines.append("-" * W)
        lines.append(f"  Verdict  : {grading.get('verdict', 'N/A')}")
        if grading.get("expected"):
            lines.append(f"  Expected : {_truncate(str(grading['expected']), 60)}")
        if grading.get("root_cause"):
            lines.append(f"  Root Cause: {grading['root_cause']}")
        lines.append("")

    lines.append("=" * W)
    return "\n".join(lines)


# ══════════════════════════════════════════════════════════════
#  BATCH PROCESSOR
# ══════════════════════════════════════════════════════════════

def diagnose_file(path):
    """Load and analyze a single diagnostic JSON file."""
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return analyze_diagnostic(data)


def diagnose_folder(folder_path):
    """Analyze all diagnostic JSON files in a folder."""
    folder = Path(folder_path)
    results = []
    for f in sorted(folder.glob("*.json")):
        try:
            analysis = diagnose_file(f)
            analysis["_source_file"] = str(f.name)
            results.append(analysis)
        except (json.JSONDecodeError, KeyError) as e:
            results.append({
                "_source_file": str(f.name),
                "_error": str(e),
            })
    return results


def format_batch_summary(results):
    """Format a summary of multiple diagnostics."""
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
        # Status distribution
        statuses = {}
        for r in valid:
            s = r.get("run_status", "unknown")
            statuses[s] = statuses.get(s, 0) + 1
        lines.append(f"\n  Status distribution:")
        for s, count in sorted(statuses.items()):
            lines.append(f"    {s}: {count}")

        # Datasource types
        all_ds = set()
        for r in valid:
            all_ds.update(r.get("datasource_types", []))
        if all_ds:
            ds_labels = [DATASOURCE_TYPES.get(t, t) for t in all_ds]
            lines.append(f"\n  Datasource types: {', '.join(ds_labels)}")

        # Issue summary
        all_issues = []
        for r in valid:
            all_issues.extend(r.get("issues", []))
        if all_issues:
            issue_counts = {}
            for i in all_issues:
                key = i["issue"]
                issue_counts[key] = issue_counts.get(key, 0) + 1
            lines.append(f"\n  Top issues:")
            for issue, count in sorted(issue_counts.items(), key=lambda x: -x[1])[:10]:
                lines.append(f"    {count}x {issue}")

        # Timing
        durations = [r["total_duration_s"] for r in valid if r.get("total_duration_s")]
        if durations:
            lines.append(f"\n  Timing:")
            lines.append(f"    Avg: {sum(durations)/len(durations):.1f}s")
            lines.append(f"    Min: {min(durations):.1f}s | Max: {max(durations):.1f}s")

    lines.append("")
    lines.append("=" * W)

    # Per-file detail
    for r in valid:
        lines.append(f"\n  {r.get('_source_file', '?')}")
        lines.append(f"    Q: {_truncate(r.get('question', ''), 55)}")
        lines.append(f"    Status: {r.get('run_status')} | Steps: {len(r.get('steps', []))} | Issues: {len(r.get('issues', []))}")
        if r.get("issues"):
            for issue in r["issues"][:2]:
                icon = {"error": "!!!", "warning": " !!", "info": "  i"}.get(issue["severity"], "  ?")
                lines.append(f"    {icon} {issue['issue']}")

    return "\n".join(lines)
