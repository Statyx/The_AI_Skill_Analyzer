"""Answer comparison, pipeline tracing, and root cause analysis.

Handles grading of agent responses against expected answers,
tracing the agent's internal pipeline steps, and identifying
root causes when answers are wrong.
"""

import re
import json

# ── Pipeline stage mapping ────────────────────────────────────

PIPELINE_STAGES = {
    "nl2sa_query":      "NL_TO_QUERY",
    "nl2sql_query":     "NL_TO_QUERY",
    "evaluate_dax":     "DAX_EXECUTION",
    "evaluate_sql":     "SQL_EXECUTION",
    "evaluate_query":   "QUERY_EXECUTION",
    "message_creation": "ANSWER_SYNTHESIS",
    "analyze.database.nl2code":        "NL_TO_QUERY",
    "analyze.database.execute":        "QUERY_EXECUTION",
    "analyze.database.fewshots.matching": "FEWSHOT_MATCHING",
    "analyze.database.fewshots.loading":  "FEWSHOT_LOADING",
    "trace.analyze_semantic_model":    "QUERY_EXECUTION",
    "generate.filename":               "FILE_GENERATION",
}

# ── Root cause categories ─────────────────────────────────────

RCA_CATEGORIES = {
    "AGENT_ERROR":       "Agent returned an error or non-completed status",
    "QUERY_ERROR":       "Generated query failed to execute (syntax, missing column, etc.)",
    "EMPTY_RESULT":      "Query succeeded but returned no data or empty result",
    "FILTER_CONTEXT":    "Unexpected filter applied (e.g., time intelligence auto-filter)",
    "MEASURE_SELECTION": "Wrong measure referenced in the generated query",
    "RELATIONSHIP":      "Wrong join path or missing relationship traversal",
    "REFORMULATION":     "Agent misunderstood the question -- wrong entities or intent",
    "SYNTHESIS":         "Data was correct but the answer was misinterpreted or truncated",
    "UNKNOWN":           "Cannot determine root cause from available pipeline data",
}


# ── Number extraction ─────────────────────────────────────────

def _extract_numbers(text):
    raw = re.findall(r'-?\d[\d,]*\.?\d*', text)
    nums = []
    for x in raw:
        cleaned = x.replace(",", "")
        if cleaned and cleaned != "-":
            try:
                nums.append(float(cleaned))
            except ValueError:
                pass
    return nums


# ── Answer comparison ─────────────────────────────────────────

def _compare_answer(actual, test_case):
    """Compare agent answer vs expected. Returns (verdict, detail)."""
    expected = test_case.get("expected")
    if expected is None or str(expected).strip() == "":
        return "no_expected", "No expected answer provided -- manual review required"

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
                return "pass", f"Numeric match: {num} ~ {expected_num} (+/-{tolerance})"
        if actual_nums:
            closest = min(actual_nums, key=lambda x: abs(x - expected_num))
            return "fail", f"Expected ~{expected_num} (+/-{tolerance}), closest found: {closest}"
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


# ── Pipeline tracer ───────────────────────────────────────────

def trace_pipeline(run_details):
    """Extract an ordered trace of what the Data Agent did at each step."""
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
                "stage": "ANSWER_SYNTHESIS", "tool": "message_creation",
                "status": status, "arguments": None, "output": None,
                "duration_s": duration, "error": step.get("last_error"),
            })
            continue

        for tc in tool_calls:
            fn = tc.get("function", {})
            tool_name = fn.get("name", "unknown")
            stage = PIPELINE_STAGES.get(tool_name, "TOOL_CALL")

            args_raw = fn.get("arguments", "{}")
            try:
                arguments = json.loads(args_raw) if isinstance(args_raw, str) else args_raw
            except (json.JSONDecodeError, TypeError):
                arguments = {"_raw": str(args_raw)[:500]}

            output_raw = fn.get("output", "")
            try:
                if isinstance(output_raw, str) and output_raw.strip()[:1] in ("{", "["):
                    output = json.loads(output_raw)
                else:
                    # Keep more output for code generation tools (DAX can be long)
                    max_len = 3000 if "nl2code" in tool_name else 1000
                    output = {"_raw": str(output_raw)[:max_len] if output_raw else ""}
            except (json.JSONDecodeError, TypeError):
                max_len = 3000 if "nl2code" in tool_name else 1000
                output = {"_raw": str(output_raw)[:max_len]}

            trace.append({
                "stage": stage, "tool": tool_name, "status": status,
                "arguments": arguments, "output": output,
                "duration_s": duration, "error": step.get("last_error"),
            })

    return trace


# ── Root cause analysis ───────────────────────────────────────

def identify_root_cause(test_case, result, pipeline_trace, verdict):
    """Analyze failed result to identify root cause. Returns (category, detail)."""
    if verdict in ("pass", "no_expected"):
        return None, None

    if result.get("status") == "error" or result.get("error"):
        return "AGENT_ERROR", f"Agent error: {result.get('error', 'unknown')}"
    if result.get("status") not in ("completed",):
        return "AGENT_ERROR", f"Agent status: {result.get('status')}"

    signals = []
    for step in pipeline_trace:
        if step["status"] not in ("completed", "succeeded"):
            signals.append(("QUERY_ERROR", f"Step '{step['tool']}' status='{step['status']}'", step))
            if step.get("error"):
                signals.append(("QUERY_ERROR", f"Error in '{step['tool']}': {step['error']}", step))

        output = step.get("output") or {}
        if isinstance(output, dict):
            err = output.get("error") or output.get("_error") or ""
            if err:
                signals.append(("QUERY_ERROR", f"Tool '{step['tool']}' output error: {err}", step))
            raw = str(output.get("_raw", ""))
            if raw and ("no data" in raw.lower() or "empty" in raw.lower()
                        or "0 rows" in raw.lower()):
                signals.append(("EMPTY_RESULT", f"Tool '{step['tool']}' returned empty/no data", step))

        args = step.get("arguments") or {}
        if isinstance(args, dict):
            query = (args.get("query", "") or args.get("dax", "")
                     or args.get("expression", "") or "")
            if isinstance(query, str) and query:
                if "__PBI_TimeIntelligenceEnabled" in query or "TREATAS" in query.upper():
                    signals.append(("FILTER_CONTEXT",
                                    "Time intelligence auto-filter detected in generated query", step))
                if "CALCULATETABLE" in query.upper() and "FILTER" in query.upper():
                    signals.append(("FILTER_CONTEXT",
                                    "Complex filter context (CALCULATETABLE + FILTER) in DAX", step))

    tool_steps = [s for s in pipeline_trace if s["tool"] != "message_creation"]
    if not tool_steps:
        return "REFORMULATION", "No tool calls made -- agent could not formulate a query"

    if signals:
        priority = ["QUERY_ERROR", "EMPTY_RESULT", "FILTER_CONTEXT",
                     "MEASURE_SELECTION", "RELATIONSHIP", "REFORMULATION"]
        for cat in priority:
            matches = [s for s in signals if s[0] == cat]
            if matches:
                return matches[0][0], matches[0][1]

    if result.get("answer"):
        return "SYNTHESIS", ("Agent returned an answer that doesn't match expected. "
                             "Inspect the generated query and result data below.")

    return "UNKNOWN", "Cannot determine root cause from available pipeline data"


# ── Artifact extractor ────────────────────────────────────────

def extract_artifacts(pipeline_trace):
    reformulated = None
    generated_query = None
    query_result = None
    tool_outputs = []

    for step in pipeline_trace:
        args = step.get("arguments") or {}
        output = step.get("output") or {}

        if isinstance(args, dict):
            # Reformulated question: from trace.analyze_semantic_model or execute args
            if args.get("query") and not reformulated:
                reformulated = args["query"]
            if args.get("natural_language_description") and not reformulated:
                reformulated = args["natural_language_description"]

            # DAX from arguments (legacy tool formats)
            dax = args.get("dax") or args.get("expression") or args.get("query_text") or ""
            if dax and not generated_query:
                generated_query = dax

        # Extract DAX from nl2code output (Fabric agent: markdown fenced code)
        if isinstance(output, dict) and "nl2code" in step.get("tool", ""):
            raw = output.get("_raw", "")
            if raw:
                # Parse ```dax ... ``` fence from nl2code output
                fence_match = re.search(r'```(?:dax|DAX)?\s*\n(.*?)```', raw, re.DOTALL)
                if fence_match and not generated_query:
                    generated_query = fence_match.group(1).strip()

        # Query result: from execute/trace tools (not nl2code)
        if step["tool"] != "message_creation" and isinstance(output, dict):
            raw = output.get("_raw", "")
            tool_name = step.get("tool", "")
            if raw and not query_result and "nl2code" not in tool_name:
                query_result = str(raw)[:500]
            if output and output != {"_raw": ""}:
                tool_outputs.append({"tool": step["tool"],
                                     "output_preview": str(output)[:300]})

    return {
        "reformulated_question": reformulated,
        "generated_query": generated_query,
        "query_result_preview": query_result,
        "tool_outputs": tool_outputs,
    }


# ── Grade single result ──────────────────────────────────────

def grade_result(result, test_case):
    """Grade a single result: compare answer + trace pipeline + identify root cause."""
    verdict, compare_detail = _compare_answer(result.get("answer", ""), test_case)
    pipeline = trace_pipeline(result.get("run_details", {}))
    root_cause, rca_detail = identify_root_cause(test_case, result, pipeline, verdict)
    artifacts = extract_artifacts(pipeline)

    return {
        "verdict": verdict,
        "expected": test_case.get("expected"),
        "match_type": test_case.get("match_type", "contains"),
        "compare_detail": compare_detail,
        "tags": test_case.get("tags", []),
        "pipeline_trace": pipeline,
        "root_cause": root_cause,
        "root_cause_detail": rca_detail,
        "artifacts": artifacts,
    }
