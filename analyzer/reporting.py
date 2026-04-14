"""Reporting — save runs, analyze results, generate HTML reports, diff runs.

This module handles all output: diagnostic JSON files, batch summaries,
terminal analysis output, HTML report generation, and run comparison.
"""

import re
import json
import yaml
from html import escape as h
from pathlib import Path
from datetime import datetime, timezone

from .config import ROOT
from .grading import grade_result, RCA_CATEGORIES


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

    profile_fewshots = _load_profile_fewshots(cfg)

    diag = {
        "downloaded_at": datetime.now(timezone.utc).isoformat(),
        "rolloutEnvironment": "PROD",
        "stage": cfg.get("stage", "sandbox"),
        "profile": cfg.get("profile_name", "default"),
        "artifactId": cfg["agent_id"],
        "workspaceId": cfg["workspace_id"],
        "source": f"analyzer v3 (profile={cfg.get('profile_name', 'default')})",
        "config": agent_data.get("config"),
        "datasources": {
            cfg["semantic_model_id"]: {
                "fewshots": {"fewShots": profile_fewshots,
                             "parentId": cfg["semantic_model_id"],
                             "type": "semantic_model"},
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
    if verdict_data:
        diag["grading"] = {
            "verdict": verdict_data.get("verdict"),
            "expected": verdict_data.get("expected"),
            "actual_answer": question_result.get("answer", ""),
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
#  SAVE RUN
# ══════════════════════════════════════════════════════════════

def save_run(results, agent_data, schema, cfg, total_wall, test_cases, interrupted=False):
    """Grade results, save per-question diagnostics + batch summary.

    Runs are saved under runs/<profile>/<timestamp>/.
    """
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    profile = cfg.get("profile_name", "default")
    out = ROOT / cfg.get("output_dir", "runs") / profile / ts
    diag_dir = out / "diagnostics"
    diag_dir.mkdir(parents=True, exist_ok=True)

    for i, r in enumerate(results):
        tc = test_cases[i] if i < len(test_cases) else {
            "expected": None, "match_type": "contains", "tags": []
        }
        verdict_data = grade_result(r, tc, schema=schema)

        diag = build_diagnostic(agent_data, schema, r, cfg, verdict_data=verdict_data)
        safe_q = re.sub(r"[^a-z0-9]+", "_", r["question"].lower())[:40].strip("_")
        filename = f"Q{i+1}_full_diag_{safe_q}.json"
        with open(diag_dir / filename, "w", encoding="utf-8") as f:
            json.dump(diag, f, indent=2, default=str, ensure_ascii=False)

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
        r.pop("run_details", None)

    n_pass = sum(1 for r in results if r.get("grading", {}).get("verdict") == "pass")
    n_fail = sum(1 for r in results if r.get("grading", {}).get("verdict") == "fail")
    n_ungraded = sum(1 for r in results if r.get("grading", {}).get("verdict") in ("no_expected", None))

    rca_dist = {}
    for r in results:
        rc = r.get("grading", {}).get("root_cause")
        if rc:
            rca_dist[rc] = rca_dist.get(rc, 0) + 1

    # Extract agent display name
    agent_name = (agent_data.get("meta", {}).get("displayName", "")
                  or cfg.get("semantic_model_name", "Agent"))

    summary = {
        "timestamp": ts,
        "profile": profile,
        "agent_id": cfg["agent_id"],
        "agent_name": agent_name,
        "model_id": cfg["semantic_model_id"],
        "model_name": cfg.get("semantic_model_name", "?"),
        "stage": cfg.get("stage", "sandbox"),
        "schema_stats": schema.get("stats"),
        "total_wall_seconds": total_wall,
        "max_workers": cfg.get("max_workers", 1),
        "interrupted": interrupted,
        "total_questions": len(results),
        "passed": sum(1 for r in results if r.get("status") == "completed"),
        "failed": sum(1 for r in results if r.get("status") != "completed"),
        "grading": {
            "pass": n_pass, "fail": n_fail, "ungraded": n_ungraded,
            "score_pct": round(n_pass / max(n_pass + n_fail, 1) * 100) if (n_pass + n_fail) > 0 else None,
            "root_cause_distribution": rca_dist,
        },
        "results": results,
    }
    with open(out / "batch_summary.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, default=str, ensure_ascii=False)

    with open(out / "test_cases.yaml", "w", encoding="utf-8") as f:
        yaml.dump({"test_cases": test_cases}, f, default_flow_style=False, allow_unicode=True)

    return ts, out


# ══════════════════════════════════════════════════════════════
#  DAX & ANSWER QUALITY ASSESSMENT
# ══════════════════════════════════════════════════════════════

def _detect_bpa_violations(query):
    """Detect Best Practice Analyzer violations in a DAX query.

    Based on Tabular Editor BPA rules adapted for agent-generated DAX.
    Returns list of (rule_id, severity, description) tuples.
    Severity: 'error' (deduct 2 stars), 'warning' (deduct 1 star), 'info' (no deduction).
    """
    if not query:
        return []

    violations = []
    upper_q = query.upper()

    # ── PERFORMANCE RULES ────────────────────────────────────

    # BPA-PERF-001: Avoid FILTER() with ALL() inside CALCULATE — use REMOVEFILTERS
    if "CALCULATE" in upper_q and "FILTER" in upper_q and "ALL(" in upper_q:
        # Detect CALCULATE(..., FILTER(ALL(Table), ...)) — use looser match
        # since CALCULATE args can contain nested parens
        if re.search(r'CALCULATE\s*\(.*FILTER\s*\(\s*ALL\s*\(', upper_q, re.DOTALL):
            violations.append(("BPA-PERF-001", "warning",
                "Use REMOVEFILTERS() or KEEPFILTERS() instead of FILTER(ALL(...),...) "
                "inside CALCULATE for better performance"))

    # BPA-PERF-002: Avoid FILTER on large tables — use column filters in CALCULATE
    filter_table_matches = re.findall(
        r'FILTER\s*\(\s*([\'"]?\w+[\'"]?)\s*,', query, re.IGNORECASE)
    for tbl in filter_table_matches:
        tbl_clean = tbl.strip("'\"")
        if not tbl_clean.upper().startswith("ALL") and not tbl_clean.upper().startswith("VALUES"):
            violations.append(("BPA-PERF-002", "warning",
                f"FILTER('{tbl_clean}',...) iterates entire table. "
                f"Prefer column predicates in CALCULATE: "
                f"CALCULATE([Measure], '{tbl_clean}'[Column] = value)"))
            break  # one warning is enough

    # BPA-PERF-003: Avoid nested CALCULATE
    calc_count = len(re.findall(r'\bCALCULATE\s*\(', upper_q))
    if calc_count >= 3:
        violations.append(("BPA-PERF-003", "warning",
            f"Query has {calc_count} nested CALCULATE calls. "
            f"Simplify by combining filter arguments or using variables"))

    # BPA-PERF-004: Use DISTINCTCOUNT instead of COUNTROWS(DISTINCT())
    if re.search(r'COUNTROWS\s*\(\s*DISTINCT\s*\(', upper_q):
        violations.append(("BPA-PERF-004", "info",
            "Use DISTINCTCOUNT() instead of COUNTROWS(DISTINCT()) — "
            "same result, better readability and potential optimization"))

    # BPA-PERF-005: Use DIVIDE instead of / for division (avoids divide-by-zero)
    if re.search(r'[^/]\s*/\s*[^/\*]', query) and "DIVIDE" not in upper_q:
        # Only flag if there's no DIVIDE at all — agent should use safe DIVIDE
        div_count = len(re.findall(r'(?<![/\*])/(?![/\*])', query))
        if div_count > 0:
            violations.append(("BPA-PERF-005", "warning",
                "Use DIVIDE(numerator, denominator) instead of '/' operator — "
                "handles division by zero gracefully"))

    # BPA-PERF-006: Avoid IFERROR / ISERROR — usually masks real problems
    if "IFERROR" in upper_q or "ISERROR" in upper_q:
        violations.append(("BPA-PERF-006", "warning",
            "Avoid IFERROR/ISERROR — they force double evaluation of the expression. "
            "Use DIVIDE for safe division or handle specific conditions explicitly"))

    # BPA-PERF-007: Avoid SELECTEDVALUE — use HASONEVALUE + VALUES
    # Actually: SELECTEDVALUE is fine. The BPA rule is about IF(HASONEVALUE) patterns
    # being replaceable with SELECTEDVALUE. Skip this for agent context.

    # BPA-PERF-008: Avoid SUMMARIZE for adding calculated columns — use ADDCOLUMNS
    if re.search(r'SUMMARIZE\s*\([^,]+,.*,\s*"[^"]+"\s*,', upper_q):
        violations.append(("BPA-PERF-008", "warning",
            "SUMMARIZE with added columns is unreliable. Use "
            "ADDCOLUMNS(SUMMARIZE(Table, GroupByCol), \"Name\", expression) instead"))

    # BPA-PERF-009: Use KEEPFILTERS in CALCULATE to preserve existing filters
    # (info-level for generated queries — agent may legitimately need to override)

    # ── CORRECTNESS RULES ────────────────────────────────────

    # BPA-CORR-001: Avoid comparing with BLANK using = or <>
    if re.search(r'=\s*BLANK\s*\(\s*\)', upper_q) or re.search(r'<>\s*BLANK\s*\(\s*\)', upper_q):
        violations.append(("BPA-CORR-001", "warning",
            "Comparing with BLANK() using = or <> can give unexpected results. "
            "Use ISBLANK() for null checks"))

    # BPA-CORR-002: Avoid using VALUES() where a single value is expected
    if re.search(r'VALUES\s*\([^)]+\)\s*[+\-\*/]', upper_q):
        violations.append(("BPA-CORR-002", "info",
            "VALUES() can return multiple rows. If a scalar is expected, "
            "use SELECTEDVALUE() or wrap in a CALCULATE/MAXX"))

    # BPA-CORR-003: SWITCH TRUE pattern — ensure ELSE clause
    switch_true = re.search(r'SWITCH\s*\(\s*TRUE\s*\(\s*\)', upper_q)
    if switch_true:
        # Check if there's likely a default/else value
        # Hard to parse accurately, so just flag as info
        violations.append(("BPA-CORR-003", "info",
            "SWITCH(TRUE(),...) detected — ensure a default (ELSE) value "
            "is provided to avoid returning BLANK unexpectedly"))

    # BPA-CORR-004: == comparison (DAX uses single =)
    if "==" in query:
        # Check it's not inside a string literal
        code_only = re.sub(r'"[^"]*"', '', query)
        if "==" in code_only:
            violations.append(("BPA-CORR-004", "error",
                "DAX uses single '=' for equality comparison, not '=='. "
                "This will cause a syntax error"))

    # ── TIME INTELLIGENCE RULES ──────────────────────────────

    # BPA-TIME-001: Avoid __PBI_TimeIntelligenceEnabled auto-filters
    if "__PBI_TIMEINTELLIGENCEENABLED" in upper_q:
        violations.append(("BPA-TIME-001", "warning",
            "Auto time-intelligence filter __PBI_TimeIntelligenceEnabled "
            "detected. Use explicit date CALCULATE filters instead"))

    # BPA-TIME-002: TREATAS for date binding — often unneeded in direct queries
    if "TREATAS" in upper_q and ("CALENDAR" in upper_q or "DATE" in upper_q):
        violations.append(("BPA-TIME-002", "warning",
            "TREATAS with date tables detected. Use direct "
            "relationships or explicit date column filters"))

    # BPA-TIME-003: DATESYTD/DATESBETWEEN vs CALCULATE with date filters
    if "DATESYTD" in upper_q or "DATESBETWEEN" in upper_q or "DATESINPERIOD" in upper_q:
        # These are OK but flag as info when combined with CALCULATE complexity
        if calc_count >= 2:
            violations.append(("BPA-TIME-003", "info",
                "Time intelligence functions (DATESYTD/DATESBETWEEN/DATESINPERIOD) "
                "with nested CALCULATE — consider pre-defining as a model measure"))

    # ── READABILITY / MAINTENANCE RULES ──────────────────────

    # BPA-READ-001: Use VAR/RETURN for complex expressions
    lines = [l for l in query.strip().split("\n") if l.strip()]
    if len(lines) > 8 and "VAR" not in upper_q:
        violations.append(("BPA-READ-001", "info",
            "Complex query without VARiables. Use VAR/RETURN to break "
            "down logic for better readability and potential performance gain"))

    # BPA-READ-002: DEFINE MEASURE without clear naming
    define_measures = re.findall(
        r'MEASURE\s+[\'"]?(\w+)[\'"]?\[([^\]]+)\]', query, re.IGNORECASE)
    for tbl, mname in define_measures:
        if mname.startswith("_") or len(mname) < 3:
            violations.append(("BPA-READ-002", "info",
                f"DEFINE MEASURE [{mname}] uses cryptic naming. "
                f"Use descriptive names for inline measures"))
            break

    # BPA-READ-003: Hardcoded values in filters
    hardcoded_years = re.findall(
        r'(?:YEAR|Calendar)\w*\[?\w*\]?\s*=\s*(20\d{2})', query, re.IGNORECASE)
    if hardcoded_years:
        violations.append(("BPA-READ-003", "info",
            f"Hardcoded year values ({', '.join(hardcoded_years[:3])}) detected. "
            f"Consider using dynamic date references like TODAY(), NOW(), or MAX(Date[Year])"))

    # ── MEASURE USAGE RULES ──────────────────────────────────

    # BPA-MEAS-001: Raw column aggregation instead of existing measures
    raw_aggs = re.findall(
        r'\b(SUM|AVERAGE|COUNT|COUNTROWS|MIN|MAX)\s*\(\s*[\'"]?\w+[\'"]?\[[^\]]+\]\s*\)',
        query, re.IGNORECASE)
    if len(raw_aggs) > 2:
        violations.append(("BPA-MEAS-001", "info",
            f"Query aggregates {len(raw_aggs)} raw columns directly. "
            f"If corresponding measures exist, reference them for consistency "
            f"with report calculations and potentially better perf"))

    # BPA-MEAS-002: CALCULATE should reference a measure, not a raw aggregation
    calc_raw = re.findall(
        r'CALCULATE\s*\(\s*(SUM|AVERAGE|COUNT|COUNTROWS|MIN|MAX)\s*\(',
        query, re.IGNORECASE)
    if calc_raw:
        violations.append(("BPA-MEAS-002", "info",
            f"CALCULATE wraps raw {calc_raw[0].upper()}() instead of a measure. "
            f"If a measure exists for this aggregation, reference it directly"))

    return violations


def _assess_dax_quality(result):
    """Rate the quality of the generated DAX query. Returns (score 0-5, label, detail)."""
    artifacts = result.get("grading", {}).get("artifacts", {})
    query = artifacts.get("generated_query", "") or ""
    tools = result.get("tools", [])
    verdict = result.get("grading", {}).get("verdict", "?")
    root_cause = result.get("grading", {}).get("root_cause")

    # No query generated
    if not query:
        if not any(t for t in tools if t != "message_creation"):
            return 0, "No query", "Agent answered without querying the model"
        return 0, "No query", "No DAX captured in pipeline trace"

    score = 5
    notes = []

    # Query errors → poor
    if root_cause == "QUERY_ERROR":
        return 1, "Error", "Query failed to execute"

    # Empty results
    if root_cause == "EMPTY_RESULT":
        score = min(score, 2)
        notes.append("empty result")

    # Complexity check
    lines = [l for l in query.strip().split("\n") if l.strip()]
    if len(lines) > 15:
        score = min(score, 3)
        notes.append(f"complex ({len(lines)} lines)")

    # Problematic patterns
    upper_q = query.upper()
    if "__PBI_TIMEINTELLIGENCEENABLED" in upper_q or "TREATAS" in upper_q:
        score = min(score, 3)
        notes.append("auto-filters detected")

    # ── BPA violations ───────────────────────────────────────
    bpa_violations = _detect_bpa_violations(query)
    n_errors = sum(1 for _, sev, _ in bpa_violations if sev == "error")
    n_warnings = sum(1 for _, sev, _ in bpa_violations if sev == "warning")
    n_info = sum(1 for _, sev, _ in bpa_violations if sev == "info")

    if n_errors > 0:
        score = min(score, 2)
        notes.append(f"BPA: {n_errors} error(s)")
    if n_warnings > 0:
        score = min(score, 3)
        notes.append(f"BPA: {n_warnings} warning(s)")
    if n_info > 0:
        score -= 1
        score = max(score, 0)
        notes.append(f"BPA: {n_info} info")

    # Check if it references measures (good) vs raw columns only
    measure_refs = re.findall(r'\[[A-Z][^\]]*\]', query)
    if measure_refs:
        notes.append(f"refs: {', '.join(measure_refs[:3])}")

    # Verdict influence
    if verdict == "pass":
        notes.append("correct result")
    elif verdict == "fail" and root_cause == "SYNTHESIS":
        score = min(score, 3)
        notes.append("wrong interpretation")

    label = {5: "Excellent", 4: "Good", 3: "Adequate", 2: "Poor", 1: "Bad", 0: "None"}.get(score, "?")
    return score, label, "; ".join(notes) if notes else ""


# ══════════════════════════════════════════════════════════════
#  PER-QUESTION ACTION SUGGESTIONS
# ══════════════════════════════════════════════════════════════

def _suggest_actions(result):
    """Analyze a failed result and return specific remediation actions.

    Combines RCA category + artifact signals + the 3-layer instruction model
    to point the user to the exact fix location:
      Layer 1 — Agent Instructions (orchestrator: decides WHETHER to call DAX)
      Layer 2 — Prep for AI (DAX tool: decides WHAT DAX to generate)
      Layer 3 — Agent Instructions (formatting: decides HOW to present)

    Returns list of (action_type, suggestion) tuples where action_type is one of:
    DESCRIPTION, INSTRUCTION, FEWSHOT, EXPECTED, MEASURE, DATA, PREP_FOR_AI
    """
    g = result.get("grading", {})
    verdict = g.get("verdict", "?")
    if verdict != "fail":
        return []

    root_cause = g.get("root_cause", "UNKNOWN")
    rca_detail = g.get("root_cause_detail", "")
    artifacts = g.get("artifacts", {})
    query = artifacts.get("generated_query", "") or ""
    query_result = artifacts.get("query_result_preview", "") or ""
    reformulated = artifacts.get("reformulated_question", "") or ""
    expected = g.get("expected")
    compare_detail = g.get("compare_detail", "")
    answer = result.get("answer", "") or ""
    question = result.get("question", "")
    query_upper = query.upper()

    actions = []

    # ── AGENT_ERROR ──────────────────────────────────────────
    if root_cause == "AGENT_ERROR":
        if "error" in answer.lower() or not answer.strip():
            actions.append(("DATA",
                "Verify the semantic model is accessible and the agent has "
                "read permissions to the workspace"))
        if "timeout" in rca_detail.lower() or "429" in rca_detail:
            actions.append(("DATA",
                "Agent hit a throttle or timeout. Retry later or reduce "
                "query complexity"))
        else:
            actions.append(("INSTRUCTION",
                "[Layer 1 — Agent Instructions] Check for conflicting rules. "
                "Simplify instructions if >6000 chars"))
        return actions

    # ── QUERY_ERROR ──────────────────────────────────────────
    if root_cause == "QUERY_ERROR":
        # Sub-case: query ran but returned empty → wrong filter values
        if "no data" in query_result.lower() or "empty" in query_result.lower():
            filter_matches = re.findall(r"""['"]([\w\s]+)['"]""", query)
            if filter_matches:
                actions.append(("PREP_FOR_AI",
                    f"[Layer 2 — Prep for AI] Query filtered on "
                    f"{filter_matches[:3]} — open Prep for AI and add "
                    f"column descriptions listing the actual valid values "
                    f"(enum lists) so the DAX tool picks correct filters"))
            else:
                actions.append(("PREP_FOR_AI",
                    "[Layer 2 — Prep for AI] Query returned no data. Add "
                    "descriptions to filter columns with valid value lists "
                    "in Prep for AI → AI Data Schema"))
            actions.append(("FEWSHOT",
                f"[Layer 2 — Verified Answers] Add a verified answer for: "
                f"\"{question}\" with the correct DAX + filter values"))

        # Sub-case: agent couldn't generate query at all
        elif ("unable to generate" in rca_detail.lower()
              or "unable to generate" in query_result.lower()):
            actions.append(("FEWSHOT",
                f"[Layer 2 — Verified Answers] Add a verified answer for: "
                f"\"{question}\" with a working DAX query"))
            actions.append(("PREP_FOR_AI",
                "[Layer 2 — Prep for AI] Add table/column descriptions and "
                "relationship context so the DAX tool understands how to "
                "join the required tables"))
            actions.append(("INSTRUCTION",
                "[Layer 1 — Agent Instructions] Add instruction explaining "
                "the cross-domain query pattern (which tables to join)"))

        # Sub-case: unknown column/measure reference
        elif "unknown identifier" in rca_detail.lower():
            actions.append(("PREP_FOR_AI",
                f"[Layer 2 — Prep for AI] DAX references a non-existent "
                f"column or measure. Check AI Data Schema visibility — "
                f"the needed column may be deselected or hidden"))
        elif "case mismatch" in rca_detail.lower():
            actions.append(("PREP_FOR_AI",
                "[Layer 2 — Prep for AI] Measure name case mismatch. "
                "Check exact casing in the semantic model — DAX is "
                "case-sensitive for measure references"))
        else:
            if query:
                actions.append(("FEWSHOT",
                    f"[Layer 2 — Verified Answers] Add a verified answer "
                    f"with corrected DAX for: \"{question}\""))
            actions.append(("PREP_FOR_AI",
                "[Layer 2 — Prep for AI] Check that queried columns/measures "
                "are visible in AI Data Schema. Add descriptions to clarify "
                "naming conventions"))
        return actions

    # ── EMPTY_RESULT ─────────────────────────────────────────
    if root_cause == "EMPTY_RESULT":
        # Detect time filter → Prep for AI AI Instructions
        has_time_filter = any(kw in query_upper for kw in
                             ("DATEADD", "SAMEPERIODLASTYEAR", "DATESYTD",
                              "DATESBETWEEN", "LASTDATE", "FIRSTDATE",
                              "YEAR", "MONTH", "QUARTER"))
        if has_time_filter:
            actions.append(("PREP_FOR_AI",
                "[Layer 2 — Prep for AI → AI Instructions] Query uses time "
                "filters that returned empty. Add AI Instruction: 'Default "
                "time period is FY2025. Use ALL dates unless the user "
                "specifies a period'"))
        # Detect value filter → column descriptions
        filter_vals = re.findall(r"""['"]([\w\s]+)['"]""", query)
        if filter_vals:
            actions.append(("PREP_FOR_AI",
                f"[Layer 2 — Prep for AI] Query filtered on "
                f"{filter_vals[:3]} and got no results. Add column "
                f"descriptions with the valid enum values (e.g., actual "
                f"category names, region codes)"))
        if not has_time_filter and not filter_vals:
            actions.append(("DATA",
                "Query returned empty with no obvious bad filters. Verify "
                "the underlying data has rows and the model is refreshed"))
        actions.append(("FEWSHOT",
            f"[Layer 2 — Verified Answers] Add a verified answer for: "
            f"\"{question}\" with correct DAX filters"))
        return actions

    # ── FILTER_CONTEXT ───────────────────────────────────────
    if root_cause == "FILTER_CONTEXT":
        if "__PBI_TIMEINTELLIGENCEENABLED" in query_upper:
            actions.append(("PREP_FOR_AI",
                "[Layer 2 — Prep for AI → AI Instructions] Time intelligence "
                "auto-filter detected (__PBI_TimeIntelligenceEnabled). Add "
                "AI Instruction: 'Never inject automatic time filters. "
                "Use explicit CALCULATE with date columns only when the "
                "user requests a specific period'"))
        if "TREATAS" in query_upper:
            actions.append(("PREP_FOR_AI",
                "[Layer 2 — Prep for AI → AI Instructions] TREATAS "
                "auto-filter overrides query context. Add AI Instruction "
                "to avoid TREATAS for date filtering"))
        if "CALCULATETABLE" in query_upper and "FILTER" in query_upper:
            actions.append(("FEWSHOT",
                f"[Layer 2 — Verified Answers] Complex filter context. "
                f"Add a verified answer for \"{question}\" with clean "
                f"CALCULATE (no nested FILTER/CALCULATETABLE)"))
        else:
            actions.append(("FEWSHOT",
                f"[Layer 2 — Verified Answers] Add a verified answer for "
                f"\"{question}\" with clean DAX (no auto-filters)"))
        return actions

    # ── MEASURE_SELECTION ────────────────────────────────────
    if root_cause == "MEASURE_SELECTION":
        if "hidden column" in rca_detail.lower():
            actions.append(("PREP_FOR_AI",
                "[Layer 2 — Prep for AI] Query references a hidden column. "
                "Either unhide it in the model, or create a visible measure "
                "that wraps this column. Check AI Data Schema visibility"))
        elif "case mismatch" in rca_detail.lower():
            actions.append(("PREP_FOR_AI",
                "[Layer 2 — Prep for AI] Measure name casing mismatch. "
                "Add a description or verified answer using the exact "
                "measure name from the model"))
        elif "unknown identifier" in rca_detail.lower():
            actions.append(("MEASURE",
                f"[Model] DAX references a non-existent measure. Either "
                f"create the measure in the semantic model, or add a "
                f"verified answer with the correct existing measure name"))
        else:
            actions.append(("PREP_FOR_AI",
                "[Layer 2 — Prep for AI] Agent picked the wrong measure. "
                "Improve measure descriptions in Prep for AI to clarify "
                "when each should be used (e.g., YTD vs monthly, gross "
                "vs net, with vs without tax)"))
        actions.append(("INSTRUCTION",
            "[Layer 1 — Agent Instructions] Add instruction mapping "
            "common business terms to the correct measure names"))
        return actions

    # ── RELATIONSHIP ─────────────────────────────────────────
    if root_cause == "RELATIONSHIP":
        if "USERELATIONSHIP" in query_upper:
            actions.append(("PREP_FOR_AI",
                "[Layer 2 — Prep for AI] Query uses USERELATIONSHIP for "
                "an inactive relationship. Consider activating this "
                "relationship in the model, or add an AI Instruction "
                "explaining when to use it"))
        elif "CROSSFILTER" in query_upper:
            actions.append(("PREP_FOR_AI",
                "[Layer 2 — Prep for AI] Query overrides cross-filter "
                "direction. Add column descriptions explaining the "
                "many-to-one direction between these tables"))
        else:
            actions.append(("PREP_FOR_AI",
                "[Layer 2 — Prep for AI] Add relationship descriptions "
                "explaining how tables connect. Clarify foreign key "
                "columns in descriptions"))
        actions.append(("FEWSHOT",
            f"[Layer 2 — Verified Answers] Add a verified answer for: "
            f"\"{question}\" showing the correct join path"))
        return actions

    # ── REFORMULATION ────────────────────────────────────────
    if root_cause == "REFORMULATION":
        if not query:
            # No DAX at all → Layer 1 issue
            actions.append(("INSTRUCTION",
                "[Layer 1 — Agent Instructions] Agent did not call the DAX "
                "tool. Add instruction: 'ALWAYS query the semantic model "
                "using DAX. Never answer from your own knowledge.'"))
        else:
            actions.append(("INSTRUCTION",
                f"[Layer 1 — Agent Instructions] Agent misunderstood the "
                f"question. Add instruction explaining what '{question}' "
                f"means in terms of the model"))
        if reformulated:
            actions.append(("FEWSHOT",
                f"[Layer 2 — Verified Answers] Agent reformulated as: "
                f"\"{reformulated[:80]}\" — add a verified answer with "
                f"the correct interpretation"))
        else:
            actions.append(("FEWSHOT",
                f"[Layer 2 — Verified Answers] Add a verified answer for: "
                f"\"{question}\""))
        return actions

    # ── SYNTHESIS ────────────────────────────────────────────
    if root_cause == "SYNTHESIS":
        if expected is not None:
            expected_str = str(expected).lower()

            # Numeric mismatch — the agent might have the correct number
            # but our expected is wrong
            if g.get("match_type") == "numeric":
                from .grading import _extract_numbers
                expected_num = float(str(expected).replace(",", ""))

                # First try abbreviated numbers (711.9M, 1.7B, etc.)
                # — these are more meaningful than raw digits like "2025"
                abbrev_matches = re.findall(
                    r'([\d,.]+)\s*([KMBT])\b', answer, re.IGNORECASE)
                if abbrev_matches:
                    mult_map = {"K": 1e3, "M": 1e6, "B": 1e9, "T": 1e12}
                    expanded = []
                    for num_str, suffix in abbrev_matches:
                        mult = mult_map.get(suffix.upper(), 1)
                        expanded.append(
                            float(num_str.replace(",", "")) * mult)
                    closest = min(expanded,
                                  key=lambda x: abs(x - expected_num))
                    actions.append(("EXPECTED",
                        f"Agent returned ~{closest:,.0f}. If correct, "
                        f"update expected from {expected} to {closest:.0f} "
                        f"in questions.yaml"))
                else:
                    # Fallback: raw numbers from answer
                    answer_nums = _extract_numbers(answer)
                    if answer_nums:
                        closest = min(answer_nums,
                                      key=lambda x: abs(x - expected_num))
                        actions.append(("EXPECTED",
                            f"Agent returned {closest:,.0f}. If correct, "
                            f"update expected from {expected} to {closest:.0f} "
                            f"in questions.yaml"))
                    else:
                        actions.append(("INSTRUCTION",
                            "Agent answer has no extractable numbers. "
                            "Add instruction to always include numeric "
                            "values, not just labels"))

            # Contains mismatch — keyword not found
            elif g.get("match_type") == "contains":
                if not query:
                    actions.append(("INSTRUCTION",
                        f"[Layer 1 — Agent Instructions] Agent answered "
                        f"without querying. Add: 'ALWAYS query the semantic "
                        f"model using DAX. Never answer from knowledge.'"))
                    actions.append(("EXPECTED",
                        f"Review if expected='{expected}' is the right keyword. "
                        f"Agent answered: \"{answer[:80]}\""))
                else:
                    actions.append(("EXPECTED",
                        f"Expected '{expected}' not in answer. "
                        f"Check if the answer is actually correct with "
                        f"different wording and update expected"))
                    actions.append(("INSTRUCTION",
                        f"[Layer 3 — Agent Instructions] Add formatting "
                        f"instruction to include '{expected}' in answers "
                        f"about {', '.join(g.get('tags', []))}"))

        # Check if query was correct but answer interpretation failed
        if query and "correct result" in (
                _assess_dax_quality(result)[2] or ""):
            actions.append(("INSTRUCTION",
                "[Layer 3 — Agent Instructions] The DAX query was correct "
                "but the agent misinterpreted the result. Add formatting "
                "instruction on how to read and present this type of data"))
        elif not query:
            actions.append(("INSTRUCTION",
                "[Layer 1 — Agent Instructions] Agent answered without "
                "generating DAX. Add: 'ALWAYS query the semantic model. "
                "Never answer from your own knowledge.'"))
            actions.append(("FEWSHOT",
                f"[Layer 2 — Verified Answers] Add a verified answer for: "
                f"\"{question}\" to force model querying"))

        # DSO-specific pattern
        if "dso" in question.lower():
            actions.append(("MEASURE",
                "DSO measure may compute per-period. Add a [DSO Annual] "
                "measure or add instruction explaining DSO = "
                "Avg(Accounts Receivable) / Revenue * 365"))

        return actions

    # ── UNKNOWN ──────────────────────────────────────────────
    if not query:
        actions.append(("INSTRUCTION",
            "[Layer 1 — Agent Instructions] No DAX generated and no clear "
            "root cause. Add: 'ALWAYS query the semantic model using DAX'"))
    actions.append(("PREP_FOR_AI",
        "[Layer 2 — Prep for AI] Root cause unclear. Start by adding "
        "descriptions to tables and columns involved in this question"))
    actions.append(("FEWSHOT",
        f"[Layer 2 — Verified Answers] Add a verified answer for: "
        f"\"{question}\""))
    return actions


# ══════════════════════════════════════════════════════════════
#  DAX IMPROVEMENT SUGGESTIONS (works for ALL questions)
# ══════════════════════════════════════════════════════════════

def _load_snapshot_measures(cfg):
    """Load measures from the profile snapshot if available."""
    profile = cfg.get("profile_name", "default")
    schema_path = ROOT / "snapshots" / profile / "schema.json"
    if not schema_path.exists():
        return []
    with open(schema_path, "r", encoding="utf-8") as f:
        schema = json.load(f)
    measures = []
    for table in schema.get("elements", []):
        tname = table.get("display_name", "")
        for child in table.get("children", []):
            if child.get("type") == "semantic_model.measure":
                measures.append({
                    "table": tname,
                    "name": child["display_name"],
                    "description": child.get("description", ""),
                })
    return measures


def _load_profile_fewshots(cfg):
    """Load few-shot examples from the profile's fewshots.json if available."""
    profile = cfg.get("profile_name", "default")
    fs_path = ROOT / "profiles" / profile / "fewshots.json"
    if not fs_path.exists():
        return []
    try:
        with open(fs_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get("fewShots", [])
    except (json.JSONDecodeError, OSError):
        return []


def _question_has_fewshot(question, fewshots, threshold=0.5):
    """Check if a question is already covered by an existing few-shot example.

    Uses word-overlap ratio: if >= threshold of question words appear in a
    fewshot question (or vice versa), it's considered covered.
    """
    if not fewshots:
        return False
    q_words = set(re.sub(r"[^\w\s]", "", question.lower()).split())
    q_words -= {"the", "a", "an", "is", "are", "what", "how", "by", "for",
                "in", "of", "to", "and", "or", "le", "la", "les", "des",
                "du", "de", "un", "une", "est", "par", "quel", "quelle",
                "quels", "quelles", "combien", "pour"}
    if not q_words:
        return False
    for fs in fewshots:
        fs_words = set(re.sub(r"[^\w\s]", "", fs.get("question", "").lower()).split())
        fs_words -= {"the", "a", "an", "is", "are", "what", "how", "by", "for",
                     "in", "of", "to", "and", "or", "le", "la", "les", "des",
                     "du", "de", "un", "une", "est", "par", "quel", "quelle",
                     "quels", "quelles", "combien", "pour"}
        if not fs_words:
            continue
        overlap = len(q_words & fs_words)
        ratio = overlap / min(len(q_words), len(fs_words))
        if ratio >= threshold:
            return True
    return False


def _suggest_dax_improvements(result, dax_stars, dax_note, snapshot_measures=None, fewshots=None):
    """Suggest concrete improvements for any question with imperfect DAX.

    Returns list of (fix_type, suggestion) tuples where fix_type is one of:
    MEASURE, INSTRUCTION, SIMPLIFY, FEWSHOT

    If fewshots is provided, FEWSHOT suggestions are suppressed for questions
    that already have a matching few-shot example.
    """
    artifacts = result.get("grading", {}).get("artifacts", {})
    query = artifacts.get("generated_query", "") or ""
    question = result.get("question", "")
    verdict = result.get("grading", {}).get("verdict", "?")
    root_cause = result.get("grading", {}).get("root_cause")
    suggestions = []

    if not query:
        # No query at all
        if verdict == "fail":
            suggestions.append(("FEWSHOT",
                f"Add a fewshot example for \"{question}\" with a working "
                f"DAX query so the agent learns to query the model"))
        suggestions.append(("INSTRUCTION",
            "Add instruction: 'Always query the semantic model. "
            "Never answer from general knowledge.'"))
        return suggestions

    upper_q = query.upper()
    query_lines = [l for l in query.strip().split("\n") if l.strip()]

    # ── AUTO-FILTERS ─────────────────────────────────────────
    if "__PBI_TIMEINTELLIGENCEENABLED" in upper_q or "TREATAS" in upper_q:
        # BPA-TIME-001/BPA-TIME-002 will add the INSTRUCTION if detected;
        # only add a fewshot here to avoid duplicate instructions.
        suggestions.append(("FEWSHOT",
            f"Add a fewshot for \"{question}\" with clean DAX "
            f"(no auto-filters, explicit date filters)"))

    # ── COMPLEXITY → SUGGEST NEW MEASURE ─────────────────────
    if len(query_lines) > 15:
        # Try to identify what the query computes
        define_measures = re.findall(
            r'MEASURE\s+[\'"]?(\w+)[\'"]?\[([^\]]+)\]', query, re.IGNORECASE)
        if define_measures:
            for tbl, mname in define_measures:
                suggestions.append(("MEASURE",
                    f"The agent had to DEFINE a local measure [{mname}] "
                    f"inline. Create this as a permanent measure in the "
                    f"semantic model to simplify future queries."))
        else:
            # Complex but no DEFINE — agent is using raw aggregations
            raw_aggs = re.findall(
                r'\b(SUM|AVERAGE|COUNT|COUNTROWS|MIN|MAX|DIVIDE)\s*\(',
                query, re.IGNORECASE)
            if raw_aggs:
                agg_list = list(set(a.upper() for a in raw_aggs))
                suggestions.append(("MEASURE",
                    f"Query uses raw aggregations ({', '.join(agg_list[:4])}) "
                    f"across {len(query_lines)} lines. Create a dedicated "
                    f"measure in the model to encapsulate this logic."))
            else:
                suggestions.append(("FEWSHOT",
                    f"Query is {len(query_lines)} lines. Add a fewshot "
                    f"with a simpler DAX approach for this question."))

    # ── RAW COLUMNS vs EXISTING MEASURES ─────────────────────
    if snapshot_measures:
        # Find raw aggregations on columns
        raw_patterns = re.findall(
            r'\b(?:SUM|AVERAGE|COUNT|MIN|MAX)\s*\(\s*[\'"]?(\w+)[\'"]?\[([^\]]+)\]\s*\)',
            query, re.IGNORECASE)
        for tbl, col in raw_patterns:
            # Check if there's an existing measure that covers this
            matching = [m for m in snapshot_measures
                        if (col.lower() in m["name"].lower()
                            or col.lower().replace("_", " ") in m["description"].lower())
                        and tbl.lower() in m["table"].lower()]
            if matching:
                m = matching[0]
                suggestions.append(("INSTRUCTION",
                    f"Agent used raw aggregation on '{tbl}'[{col}] instead "
                    f"of measure [{m['name']}] ('{m['table']}'). "
                    f"Add instruction: 'For {col.replace('_', ' ')}, "
                    f"use the [{m['name']}] measure.'"))

    # ── QUERY ERROR ──────────────────────────────────────────
    if root_cause == "QUERY_ERROR":
        suggestions.append(("FEWSHOT",
            f"Add a fewshot with corrected DAX for \"{question}\". "
            f"The current query has syntax or reference errors."))
        # Check for common issues
        if "CALCULATETABLE" in upper_q and "==" in query:
            suggestions.append(("INSTRUCTION",
                "DAX uses '=' for equality, not '=='. "
                "Add instruction: 'In DAX, use single = for comparison.'"))

    # ── EMPTY RESULT ─────────────────────────────────────────
    if root_cause == "EMPTY_RESULT":
        suggestions.append(("INSTRUCTION",
            "Query returned no rows. Add instruction listing valid filter "
            "values for commonly filtered columns (dates, categories)."))
        suggestions.append(("DESCRIPTION",
            "Add column descriptions with valid enum values so the agent "
            "knows what filter criteria to use."))

    # ── WRONG MEASURE SELECTION ──────────────────────────────
    if root_cause == "MEASURE_SELECTION" and snapshot_measures:
        # List relevant measures the agent should have used
        q_words = set(question.lower().split())
        relevant = [m for m in snapshot_measures
                    if any(w in m["name"].lower() or w in m["description"].lower()
                           for w in q_words if len(w) > 3)]
        if relevant:
            measure_list = ", ".join(f"[{m['name']}]" for m in relevant[:5])
            suggestions.append(("INSTRUCTION",
                f"For \"{question}\", relevant measures are: {measure_list}. "
                f"Add instruction mapping this question type to the correct measure."))

    # ── SYNTHESIS (correct data, wrong interpretation) ───────
    if root_cause == "SYNTHESIS" and dax_stars >= 3:
        suggestions.append(("INSTRUCTION",
            "DAX query was good but the agent misinterpreted the result. "
            "Add instruction on how to read and present this type of data."))

    # ── BPA VIOLATIONS → actionable fixes ────────────────────
    bpa_violations = _detect_bpa_violations(query)
    bpa_fix_map = {
        "BPA-PERF-001": ("INSTRUCTION",
            "Add instruction: 'In CALCULATE, use REMOVEFILTERS() instead of "
            "FILTER(ALL(...),...). Example: CALCULATE([Measure], "
            "REMOVEFILTERS(Table[Column]))'"),
        "BPA-PERF-002": ("FEWSHOT",
            f"Add a fewshot for \"{question}\" using column predicates in "
            f"CALCULATE instead of FILTER(Table,...). E.g., "
            f"CALCULATE([Measure], Table[Col] = \"value\")"),
        "BPA-PERF-003": ("INSTRUCTION",
            "Add instruction: 'Reduce nested CALCULATE calls. Combine filter arguments into "
            "a single CALCULATE or use VAR to store intermediate results'"),
        "BPA-PERF-004": ("INSTRUCTION",
            "Add instruction: 'Use DISTINCTCOUNT(Table[Column]) instead of "
            "COUNTROWS(DISTINCT(Table[Column]))'"),
        "BPA-PERF-005": ("INSTRUCTION",
            "Add instruction: 'Always use DIVIDE(numerator, denominator, 0) "
            "instead of the / operator to handle division by zero'"),
        "BPA-PERF-006": ("INSTRUCTION",
            "Add instruction: 'Avoid IFERROR/ISERROR. Use DIVIDE for safe "
            "division. Handle specific conditions explicitly (IF + ISBLANK)'"),
        "BPA-PERF-008": ("INSTRUCTION",
            "Add instruction: 'Never add calculated columns inside SUMMARIZE. "
            "Use ADDCOLUMNS(SUMMARIZE(Table, GroupBy), \"Name\", Expr)'"),
        "BPA-CORR-001": ("INSTRUCTION",
            "Add instruction: 'Use ISBLANK(expression) instead of comparing "
            "with = BLANK() or <> BLANK()'"),
        "BPA-CORR-002": ("INSTRUCTION",
            "Add instruction: 'When a single value is needed from a column, "
            "use SELECTEDVALUE() instead of VALUES()'"),
        "BPA-CORR-004": ("INSTRUCTION",
            "Add instruction: 'DAX uses single = for equality. Never use ==.'"),
        "BPA-TIME-001": ("INSTRUCTION",
            "Add instruction: 'Do not use __PBI_TimeIntelligenceEnabled. "
            "Use explicit date filters in CALCULATE'"),
        "BPA-TIME-002": ("INSTRUCTION",
            "Add instruction: 'Avoid TREATAS with date tables. Use direct "
            "relationships or explicit date column filters'"),
        "BPA-TIME-003": ("MEASURE",
            "Time intelligence logic (DATESYTD/DATESBETWEEN) is complex inline. "
            "Create a dedicated measure in the semantic model"),
        "BPA-READ-001": ("INSTRUCTION",
            "Add instruction: 'For complex calculations, use VAR/RETURN "
            "to define intermediate variables for clarity and performance'"),
        "BPA-READ-003": ("INSTRUCTION",
            "Add instruction: 'Do not hardcode year values. Use "
            "MAX(DateTable[Year]) or YEAR(TODAY()) for dynamic date logic'"),
        "BPA-MEAS-001": ("MEASURE",
            "Agent aggregates raw columns repeatedly. Create reusable "
            "measures in the semantic model to ensure consistency"),
        "BPA-MEAS-002": ("INSTRUCTION",
            "Add instruction: 'Always reference existing measures inside "
            "CALCULATE instead of wrapping raw SUM/AVERAGE/COUNT'"),
    }

    for rule_id, severity, description in bpa_violations:
        if rule_id in bpa_fix_map:
            fix_type, fix_text = bpa_fix_map[rule_id]
            suggestions.append((fix_type, f"[{rule_id}] {fix_text}"))

    # Deduplicate by suggestion text
    seen = set()
    unique = []
    for fix_type, text in suggestions:
        if text not in seen:
            seen.add(text)
            unique.append((fix_type, text))

    # Filter out FEWSHOT suggestions for questions already covered by existing few-shots
    if fewshots and _question_has_fewshot(question, fewshots):
        unique = [(ft, t) for ft, t in unique if ft != "FEWSHOT"]

    return unique


def _assess_answer_quality(result):
    """Rate the quality of the agent's answer. Returns (score 0-5, label)."""
    answer = result.get("answer", "") or ""
    status = result.get("status", "")

    if status != "completed" or not answer.strip():
        return 0, "Error"

    length = len(answer)
    has_numbers = bool(re.findall(r'\d[\d,]*\.?\d*', answer))
    has_structure = any(c in answer for c in ["\n", ":", "|", "*"])

    score = 1  # Base
    if has_numbers:
        score += 1
    if length > 50:
        score += 1
    if length > 100 and has_structure:
        score += 1
    if length > 200:
        score += 1

    score = min(score, 5)
    label = {5: "Excellent", 4: "Data-rich", 3: "Good", 2: "Adequate", 1: "Thin", 0: "Error"}.get(score, "?")
    return score, label


# ══════════════════════════════════════════════════════════════
#  ACTION PLAN — group fixes by target (what to touch)
# ══════════════════════════════════════════════════════════════

# Maps fix_type → (target_key, target_label, emoji)
_TARGET_MAP = {
    "INSTRUCTION": ("agent_instructions", "Data Agent Instructions",   "Agent"),
    "FEWSHOT":     ("agent_fewshots",     "Data Agent Few-shot Examples", "Agent"),
    "PREP_FOR_AI": ("prep_for_ai",        "Prep for AI (Semantic Model)", "Model"),
    "MEASURE":     ("semantic_model",     "Semantic Model (DAX Measures)", "Model"),
    "DESCRIPTION": ("model_descriptions", "Model Descriptions (Tables/Columns/Measures)", "Model"),
    "SIMPLIFY":    ("agent_instructions", "Data Agent Instructions",   "Agent"),
    "EXPECTED":    ("test_cases",         "Test Cases (questions.yaml)",  "Tests"),
    "DATA":        ("data_source",        "Data Source / Permissions",    "Data"),
}

_TARGET_ORDER = [
    "prep_for_ai",
    "agent_instructions",
    "agent_fewshots",
    "semantic_model",
    "model_descriptions",
    "test_cases",
    "data_source",
]

_TARGET_LABELS = {
    "prep_for_ai":        "PREP FOR AI (AI Data Schema + AI Instructions + Verified Answers)",
    "agent_instructions": "DATA AGENT INSTRUCTIONS",
    "agent_fewshots":     "DATA AGENT FEW-SHOT EXAMPLES",
    "semantic_model":     "SEMANTIC MODEL (DAX MEASURES)",
    "model_descriptions": "MODEL DESCRIPTIONS",
    "test_cases":         "TEST CASES (questions.yaml)",
    "data_source":        "DATA SOURCE / PERMISSIONS",
}

_TARGET_HOW = {
    "prep_for_ai":        "Open Prep for AI in Power BI: AI Data Schema (visibility), AI Instructions (guidance), Verified Answers (example queries)",
    "agent_instructions": "Edit the Data Agent instructions in Fabric portal or push via API",
    "agent_fewshots":     "Add worked examples in the Data Agent's few-shot section",
    "semantic_model":     "Add/modify measures in Tabular Editor, TMDL, or Fabric portal",
    "model_descriptions": "Update table/column/measure descriptions via TMDL or portal",
    "test_cases":         "Update profiles/<profile>/questions.yaml",
    "data_source":        "Check data pipeline, permissions, and freshness",
}


def _build_action_plan(all_fixes):
    """Group fixes by target (what to touch) and deduplicate.

    Returns:
        by_target: dict[str, list[(set_of_qidx, fix_type, text)]]
        stats: dict with counts per target
    """
    by_target = {}
    for qidx, fix_type, text in all_fixes:
        target_key = _TARGET_MAP.get(fix_type, ("other", "Other", "?"))[0]
        by_target.setdefault(target_key, []).append((qidx, fix_type, text))

    # Deduplicate by text within each target, accumulate question refs
    # Then merge semantically overlapping items (same keywords)
    deduped = {}
    for target_key, items in by_target.items():
        seen = {}
        for qidx, fix_type, text in items:
            if text in seen:
                seen[text][0].add(qidx)
            else:
                seen[text] = (set([qidx]), fix_type, text)

        # Semantic merge: collapse items sharing key DAX keywords
        _MERGE_KEYWORDS = [
            "TREATAS", "TimeIntelligenceEnabled", "REMOVEFILTERS",
            "FILTER(ALL", "IFERROR", "ISERROR", "DISTINCTCOUNT",
            "SELECTEDVALUE", "VALUES()", "== ",
        ]
        merged = list(seen.values())
        final = []
        absorbed = set()
        for i, (qs_a, ft_a, txt_a) in enumerate(merged):
            if i in absorbed:
                continue
            for j in range(i + 1, len(merged)):
                if j in absorbed:
                    continue
                _, ft_b, txt_b = merged[j]
                if ft_a != ft_b:
                    continue
                # Check if they share a merge keyword
                for kw in _MERGE_KEYWORDS:
                    if kw.lower() in txt_a.lower() and kw.lower() in txt_b.lower():
                        qs_a.update(merged[j][0])
                        absorbed.add(j)
                        break
            final.append((qs_a, ft_a, txt_a))
        deduped[target_key] = final

    stats = {t: len(deduped.get(t, [])) for t in _TARGET_ORDER if t in deduped}
    return deduped, stats


def _render_action_plan(all_fixes, emit_fn):
    """Render the ACTION PLAN grouped by target. Works with both print and emit."""
    W = 72
    deduped, stats = _build_action_plan(all_fixes)

    if not stats:
        return

    total_actions = sum(stats.values())

    emit_fn(f"\n{'=' * W}")
    emit_fn(f"  ACTION PLAN ({total_actions} actions across {len(stats)} targets)")
    emit_fn(f"{'=' * W}")

    # Overview bar
    emit_fn(f"  Targets to modify:")
    for target_key in _TARGET_ORDER:
        if target_key in stats:
            count = stats[target_key]
            label = _TARGET_LABELS[target_key]
            affected_qs = set()
            for qids, _, _ in deduped[target_key]:
                affected_qs.update(qids)
            q_str = f"Q{','.join(str(q) for q in sorted(affected_qs))}"
            emit_fn(f"    [{count:2d} fix{'es' if count > 1 else ' '}] {label} ({q_str})")

    # Detailed actions per target
    for target_key in _TARGET_ORDER:
        items = deduped.get(target_key)
        if not items:
            continue

        label = _TARGET_LABELS[target_key]
        how = _TARGET_HOW[target_key]

        emit_fn(f"\n  {'-' * (W - 2)}")
        emit_fn(f"  >> {label}")
        emit_fn(f"     How: {how}")

        for i, (qids, fix_type, text) in enumerate(items, 1):
            q_ref = ", ".join(f"Q{q}" for q in sorted(qids))
            # Tag BPA rules distinctly
            bpa_tag = ""
            bpa_match = re.search(r'\[(BPA-\w+-\d+)\]', text)
            if bpa_match:
                bpa_tag = f" {bpa_match.group(1)}"
            emit_fn(f"     {i}. [{fix_type}]{bpa_tag} ({q_ref})")
            # Wrap long text
            clean_text = text
            if bpa_match:
                clean_text = text.replace(f"[{bpa_match.group(1)}] ", "")
            emit_fn(f"        {clean_text}")

    # Handle any unmapped types
    other_items = deduped.get("other")
    if other_items:
        emit_fn(f"\n  {'-' * (W - 2)}")
        emit_fn(f"  >> OTHER")
        for i, (qids, fix_type, text) in enumerate(other_items, 1):
            q_ref = ", ".join(f"Q{q}" for q in sorted(qids))
            emit_fn(f"     {i}. [{fix_type}] ({q_ref})")
            emit_fn(f"        {text}")

    emit_fn(f"{'=' * W}")


def _build_action_plan_json(all_fixes):
    """Build a structured action plan for JSON export."""
    deduped, stats = _build_action_plan(all_fixes)
    targets = []

    for target_key in _TARGET_ORDER:
        items = deduped.get(target_key)
        if not items:
            continue

        actions_list = []
        for qids, fix_type, text in items:
            bpa_match = re.search(r'\[(BPA-\w+-\d+)\]', text)
            actions_list.append({
                "fix_type": fix_type,
                "bpa_rule": bpa_match.group(1) if bpa_match else None,
                "questions": sorted(qids),
                "suggestion": text.replace(f"[{bpa_match.group(1)}] ", "") if bpa_match else text,
            })

        affected_qs = set()
        for qids, _, _ in items:
            affected_qs.update(qids)

        targets.append({
            "target": target_key,
            "label": _TARGET_LABELS[target_key],
            "how": _TARGET_HOW[target_key],
            "action_count": len(actions_list),
            "affected_questions": sorted(affected_qs),
            "actions": actions_list,
        })

    return {
        "total_actions": sum(stats.values()),
        "targets_count": len(stats),
        "targets": targets,
    }


# ══════════════════════════════════════════════════════════════
#  FIND PREVIOUS RUN
# ══════════════════════════════════════════════════════════════

def _find_previous_run_dir(current_ts, cfg):
    """Find the run directory just before the current one (same profile)."""
    runs_root = ROOT / cfg.get("output_dir", "runs")
    profile = cfg.get("profile_name", "default")
    profile_runs = runs_root / profile

    if not profile_runs.exists():
        return None

    previous = sorted(
        d.name for d in profile_runs.iterdir()
        if d.is_dir() and d.name < current_ts
    )
    if not previous:
        return None
    return profile_runs / previous[-1]


# ══════════════════════════════════════════════════════════════
#  SCORE RATING HELPER
# ══════════════════════════════════════════════════════════════

def _score(n):
    return f"{n}/5"


# ══════════════════════════════════════════════════════════════
#  POST-RUN REPORT (auto after each run)
# ══════════════════════════════════════════════════════════════

def print_post_run_report(results, ts, out, cfg, total_wall):
    """Print comprehensive post-run analysis with quality ratings and comparison."""
    lines = []

    def emit(text=""):
        print(text)
        lines.append(text)

    W = 72

    n_pass = sum(1 for r in results if r.get("grading", {}).get("verdict") == "pass")
    n_fail = sum(1 for r in results if r.get("grading", {}).get("verdict") == "fail")
    n_ungraded = sum(1 for r in results
                     if r.get("grading", {}).get("verdict") in ("no_expected", None))
    n_error = sum(1 for r in results if r.get("status") != "completed")
    pct = round(n_pass / max(n_pass + n_fail, 1) * 100) if (n_pass + n_fail) > 0 else None

    # Load schema stats from saved summary
    schema_line = ""
    desc_line = ""
    summary_file = out / "batch_summary.json"
    if summary_file.exists():
        with open(summary_file, "r", encoding="utf-8") as f:
            saved = json.load(f)
        stats = saved.get("schema_stats", {})
        cov = stats.get("description_coverage", {})
        schema_line = (f"{stats.get('tables', '?')}T / {stats.get('columns', '?')}C / "
                       f"{stats.get('measures', '?')}M / {stats.get('relationships', '?')}R")
        desc_line = (f"T={cov.get('tables', '?')} C={cov.get('columns', '?')} "
                     f"M={cov.get('measures', '?')}")

    # Load snapshot measures for DAX improvement suggestions
    snapshot_measures = _load_snapshot_measures(cfg)

    # Load profile few-shots to suppress redundant FEWSHOT suggestions
    profile_fewshots = _load_profile_fewshots(cfg)

    # ═══ HEADER ═══
    emit(f"\n{'=' * W}")
    emit(f"  POST-RUN ANALYSIS")
    emit(f"{'=' * W}")
    emit(f"  Run ID  : {ts}")
    emit(f"  Profile : {cfg.get('profile_name', 'default')}")
    emit(f"  Model   : {cfg.get('semantic_model_name', '?')}")
    if pct is not None:
        emit(f"  Score   : {n_pass}/{n_pass + n_fail} = {pct}%")
    emit(f"  Results : + Pass: {n_pass}  X Fail: {n_fail}  "
         f"? Ungraded: {n_ungraded}  ! Error: {n_error}")
    emit(f"  Wall    : {total_wall}s ({cfg.get('max_workers', 1)} workers)")
    if schema_line:
        emit(f"  Schema  : {schema_line}")
    if desc_line:
        emit(f"  Desc cov: {desc_line}")
    emit(f"{'=' * W}")

    # ═══ COMPARISON WITH PREVIOUS RUN ═══
    prev_dir = _find_previous_run_dir(ts, cfg)
    if prev_dir:
        prev_summary = _load_summary(prev_dir)
        if prev_summary:
            prev_grading = prev_summary.get("grading", {})
            prev_pct = prev_grading.get("score_pct", 0) or 0
            delta = (pct or 0) - prev_pct
            delta_str = f"+{delta}" if delta >= 0 else str(delta)

            emit(f"\n  -- COMPARISON vs {prev_dir.name} --")
            emit(f"  Score : {prev_pct}% -> {pct}% ({delta_str}%)")
            emit(f"  Wall  : {prev_summary.get('total_wall_seconds', '?')}s -> {total_wall}s")

            prev_by_q = {r["question"]: r for r in prev_summary.get("results", [])}
            changes = []
            for r in results:
                q = r["question"]
                prev_r = prev_by_q.get(q)
                if prev_r:
                    v_prev = prev_r.get("grading", {}).get("verdict", "?")
                    v_curr = r.get("grading", {}).get("verdict", "?")
                    if v_prev != v_curr:
                        if v_prev == "fail" and v_curr == "pass":
                            tag = "FIXED"
                        elif v_prev == "pass" and v_curr == "fail":
                            tag = "REGRESSED"
                        else:
                            tag = "CHANGED"
                        changes.append((tag, r.get("index", "?"), q[:50], v_prev, v_curr))

            if changes:
                n_fixed = sum(1 for c in changes if c[0] == "FIXED")
                n_regressed = sum(1 for c in changes if c[0] == "REGRESSED")
                emit(f"  Changes: {len(changes)} "
                     f"({n_fixed} fixed, {n_regressed} regressed)")
                for tag, idx, q, v_prev, v_curr in changes:
                    emit(f"    [{tag:10s}] Q{idx}: {v_prev} -> {v_curr}: {q}")
            else:
                emit(f"  Changes: none (same verdicts)")

    # ═══ QUESTION DETAILS ═══
    emit(f"\n{'-' * W}")
    emit(f"  QUESTION DETAILS")
    emit(f"{'-' * W}")

    all_fixes = []  # Accumulate (question_idx, fix_type, text) for summary

    for r in results:
        g = r.get("grading", {})
        verdict = g.get("verdict", "?")
        idx = r.get("index", "?")
        dur = r.get("duration_wall", "?")

        icon = {"pass": "+", "fail": "X"}.get(verdict, "?")
        rca_tag = f"  [{g.get('root_cause')}]" if g.get("root_cause") else ""

        # Quality ratings
        dax_stars, dax_label, dax_note = _assess_dax_quality(r)
        ans_stars, ans_label = _assess_answer_quality(r)

        emit(f"\n  {icon} Q{idx}  [{dur}s]  {r['question']}{rca_tag}")

        # Answer snippet
        answer = (r.get("answer", "") or "")
        if answer:
            emit(f"    Answer  : {answer}")

        # Expected vs actual
        if g.get("expected") is not None:
            emit(f"    Expected: {g['expected']} ({g.get('match_type', '?')})")
            emit(f"    Verdict : {verdict.upper()} -- {g.get('compare_detail', '')}")
        elif verdict == "no_expected":
            emit(f"    Expected: -- (ungraded)")

        # Quality ratings
        emit(f"    DAX     : {_score(dax_stars)} {dax_label}"
             + (f" -- {dax_note}" if dax_note else ""))
        emit(f"    Quality : {_score(ans_stars)} {ans_label}")

        # Show generated query for every question
        all_artifacts = g.get("artifacts", {})
        gen_query = all_artifacts.get("generated_query", "")
        if gen_query:
            # Preserve full multi-line DAX query with indentation
            query_lines = gen_query.strip().split("\n")
            emit(f"    Query   : {query_lines[0]}")
            for ql in query_lines[1:]:
                emit(f"              {ql}")

        # Root cause detail (for failures)
        if g.get("root_cause"):
            emit(f"    +-- ROOT CAUSE: {g['root_cause']}")
            emit(f"    |   {g.get('root_cause_detail', '')}")
            artifacts = g.get("artifacts", {})
            if artifacts.get("reformulated_question"):
                emit(f"    |   Reformulated: {artifacts['reformulated_question']}")
            if artifacts.get("query_result_preview"):
                emit(f"    |   Result: "
                     f"{artifacts['query_result_preview'][:120]}")
            emit(f"    +--")

        # DAX improvement suggestions (for imperfect DAX or failures)
        fixes = _suggest_dax_improvements(r, dax_stars, dax_note, snapshot_measures, fewshots=profile_fewshots)
        if verdict == "fail":
            fixes.extend(_suggest_actions(r))
        if fixes:
            # Deduplicate across both sources
            seen_texts = set()
            unique_fixes = []
            for fix_type, text in fixes:
                if text not in seen_texts:
                    seen_texts.add(text)
                    unique_fixes.append((fix_type, text))
            emit(f"    >> Fixes:")
            for fix_type, text in unique_fixes[:5]:
                emit(f"       [{fix_type:11s}] {text}")
            all_fixes.extend((idx, fix_type, text) for fix_type, text in unique_fixes)

    # ═══ ROOT CAUSE SUMMARY ═══
    rca_counts = {}
    for r in results:
        rc = r.get("grading", {}).get("root_cause")
        if rc:
            rca_counts[rc] = rca_counts.get(rc, 0) + 1

    if rca_counts:
        emit(f"\n{'-' * W}")
        emit(f"  ROOT CAUSE SUMMARY")
        for cat, count in sorted(rca_counts.items(), key=lambda x: -x[1]):
            desc = RCA_CATEGORIES.get(cat, cat)
            emit(f"    {count}x {cat}: {desc}")

    # ═══ ACTION PLAN (grouped by target) ═══
    if all_fixes:
        _render_action_plan(all_fixes, emit)

    # ═══ RECOMMENDATIONS ═══
    emit(f"\n{'-' * W}")
    emit(f"  NEXT STEPS")

    if n_fail > 0:
        fail_idx = [str(r["index"]) for r in results
                    if r.get("grading", {}).get("verdict") == "fail"]
        emit(f"  -> Re-run failed: python -m analyzer rerun {ts} "
             f"--questions {' '.join(fail_idx)}")
    if "FILTER_CONTEXT" in rca_counts:
        emit("  -> [Prep for AI → AI Instructions] Time intelligence "
             "auto-filters injecting unwanted date context. Add: "
             "'Never use __PBI_TimeIntelligenceEnabled or TREATAS'")
    if "REFORMULATION" in rca_counts:
        no_query_count = sum(1 for r in results
            if r.get("grading", {}).get("root_cause") == "REFORMULATION"
            and not (r.get("grading", {}).get("artifacts", {})
                     .get("generated_query")))
        if no_query_count:
            emit("  -> [Agent Instructions — Layer 1] Agent skipped DAX "
                 f"tool in {no_query_count} question(s). Add: 'ALWAYS "
                 "query the semantic model using DAX'")
        else:
            emit("  -> [Prep for AI] Agent misunderstood questions. "
                 "Add column/measure descriptions or verified answers")
    if "QUERY_ERROR" in rca_counts:
        emit("  -> [Prep for AI → AI Data Schema] Query errors. Check "
             "column visibility and add descriptions with valid values")
    if "SYNTHESIS" in rca_counts:
        emit("  -> [Agent Instructions — Layer 3] Wrong answer formatting. "
             "Inspect DAX results or add formatting instructions")
    if "EMPTY_RESULT" in rca_counts:
        emit("  -> [Prep for AI] Empty results. Check filter defaults "
             "in AI Instructions (default time period, valid enum values)")
    if "MEASURE_SELECTION" in rca_counts:
        emit("  -> [Prep for AI] Wrong measure selected. Improve measure "
             "descriptions to clarify when each applies")
    if "RELATIONSHIP" in rca_counts:
        emit("  -> [Prep for AI / Model] Relationship issues. Check join "
             "paths, add column descriptions for foreign keys")
    if n_ungraded > 0:
        emit(f"  -> {n_ungraded} ungraded. Fill expected answers in "
             f"questions.yaml.")
    if n_pass > 0 and n_fail == 0 and n_error == 0:
        emit("  -> All passed! Consider adding harder test cases.")

    emit(f"\n  Full analysis : python -m analyzer analyze {ts}")
    emit(f"  HTML report   : python -m analyzer analyze {ts} --html")
    try:
        diag_path = out.relative_to(ROOT)
    except ValueError:
        diag_path = out
    emit(f"  Diagnostics   : {diag_path}/diagnostics/")
    emit(f"{'=' * W}")

    # ═══ SAVE REPORT TO FILE ═══
    report_path = out / "report.txt"
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    print(f"  Report saved: {report_path}")

    # ═══ SAVE TO RESULTS FOLDER (by agent name) ═══
    _save_results_export(results, ts, cfg, lines, all_fixes)


def _save_results_export(results, ts, cfg, report_lines, all_fixes):
    """Save a clean results export organized by agent name.

    Structure: results/<AgentName>/<timestamp>/
      - report.txt        (full report)
      - summary.json      (compact: metadata + per-question verdicts + fixes)
    """
    # Load agent name from batch summary or snapshot
    summary_file = ROOT / cfg.get("output_dir", "runs") / cfg.get("profile_name", "default") / ts / "batch_summary.json"
    agent_name = cfg.get("semantic_model_name", "Agent")
    if summary_file.exists():
        with open(summary_file, "r", encoding="utf-8") as f:
            saved = json.load(f)
        agent_name = saved.get("agent_name", agent_name)

    # Sanitize agent name for folder
    safe_name = re.sub(r"[^a-zA-Z0-9_\-]", "_", agent_name)

    results_dir = ROOT / "results" / safe_name / ts
    results_dir.mkdir(parents=True, exist_ok=True)

    # 1. Save report with agent name and timestamp in filename
    report_filename = f"result_{safe_name}_{ts}.txt"
    with open(results_dir / report_filename, "w", encoding="utf-8") as f:
        f.write("\n".join(report_lines) + "\n")

    # 2. Build compact summary
    n_pass = sum(1 for r in results if r.get("grading", {}).get("verdict") == "pass")
    n_fail = sum(1 for r in results if r.get("grading", {}).get("verdict") == "fail")
    n_ungraded = sum(1 for r in results
                     if r.get("grading", {}).get("verdict") in ("no_expected", None))

    questions_summary = []
    for r in results:
        g = r.get("grading", {})
        dax_stars, dax_label, dax_note = _assess_dax_quality(r)
        ans_stars, ans_label = _assess_answer_quality(r)
        artifacts = g.get("artifacts", {})
        questions_summary.append({
            "index": r.get("index"),
            "question": r.get("question"),
            "verdict": g.get("verdict"),
            "expected": g.get("expected"),
            "match_type": g.get("match_type"),
            "answer_snippet": (r.get("answer", "") or "")[:200],
            "duration": r.get("duration_wall"),
            "dax_quality": {"score": dax_stars, "label": dax_label, "note": dax_note},
            "answer_quality": {"score": ans_stars, "label": ans_label},
            "root_cause": g.get("root_cause"),
            "generated_query": artifacts.get("generated_query", ""),
        })

    # Group fixes by type for summary (legacy)
    fix_summary = {}
    for qidx, fix_type, text in all_fixes:
        fix_summary.setdefault(fix_type, []).append({
            "question": f"Q{qidx}", "suggestion": text
        })

    # Build structured action plan grouped by target
    action_plan = _build_action_plan_json(all_fixes)

    export = {
        "agent_name": agent_name,
        "profile": cfg.get("profile_name", "default"),
        "model_name": cfg.get("semantic_model_name", "?"),
        "timestamp": ts,
        "score": {
            "pass": n_pass,
            "fail": n_fail,
            "ungraded": n_ungraded,
            "total_graded": n_pass + n_fail,
            "pct": round(n_pass / max(n_pass + n_fail, 1) * 100) if (n_pass + n_fail) > 0 else None,
        },
        "questions": questions_summary,
        "fixes": fix_summary,
        "action_plan": action_plan,
    }

    with open(results_dir / "summary.json", "w", encoding="utf-8") as f:
        json.dump(export, f, indent=2, default=str, ensure_ascii=False)

    print(f"  Results export: {results_dir}")


# ══════════════════════════════════════════════════════════════
#  ANALYZE (offline — terminal output)
# ══════════════════════════════════════════════════════════════

def analyze_run(run_dir):
    """Print rich analysis of an existing run with grading + root cause analysis."""
    summary_file = run_dir / "batch_summary.json"
    if not summary_file.exists():
        print(f"ERROR: No batch_summary.json in {run_dir}")
        return None

    with open(summary_file, "r", encoding="utf-8") as f:
        summary = json.load(f)

    results = summary.get("results", [])
    total = len(results)

    graded = [r for r in results if r.get("grading")]
    n_pass = sum(1 for r in results if r.get("grading", {}).get("verdict") == "pass")
    n_fail = sum(1 for r in results if r.get("grading", {}).get("verdict") == "fail")
    n_ungraded = sum(1 for r in results if r.get("grading", {}).get("verdict") in ("no_expected", None))
    n_error = sum(1 for r in results if r.get("status") != "completed")

    W = 72
    print(f"\n{'=' * W}")
    print(f"  ANALYSIS: {run_dir.name}")
    print(f"{'=' * W}")
    print(f"  Profile  : {summary.get('profile', 'default')}")
    print(f"  Agent    : {summary.get('agent_id', '?')}")
    print(f"  Model    : {summary.get('model_name', summary.get('model_id', '?'))}")
    print(f"  Stage    : {summary.get('stage', '?')}")
    print(f"  Wall time: {summary.get('total_wall_seconds', '?')}s  "
          f"({summary.get('max_workers', 1)} workers)")
    if summary.get("interrupted"):
        print(f"  WARNING  : Run was interrupted (partial results)")

    stats = summary.get("schema_stats", {})
    cov = stats.get("description_coverage", {})
    print(f"  Schema   : {stats.get('tables', 0)}T / {stats.get('columns', 0)}C / "
          f"{stats.get('measures', 0)}M / {stats.get('relationships', 0)}R")
    print(f"  Desc cov : T={cov.get('tables', '?')} C={cov.get('columns', '?')} "
          f"M={cov.get('measures', '?')}")

    print(f"\n{'-' * W}")
    print(f"  SCOREBOARD: {total} questions")
    if graded:
        print(f"    + Pass: {n_pass}   X Fail: {n_fail}   "
              f"? Ungraded: {n_ungraded}   ! Error: {n_error}")
        pct = round(n_pass / max(n_pass + n_fail, 1) * 100) if (n_pass + n_fail) > 0 else 0
        print(f"    Score: {n_pass}/{n_pass + n_fail} graded = {pct}%")
    else:
        passed = sum(1 for r in results if r.get("status") == "completed")
        print(f"    Completed: {passed}/{total}  (no grading data -- re-run for verdicts)")

    print(f"\n{'-' * W}")

    # Load snapshot measures for suggestions
    _cfg_for_snapshot = {"profile_name": summary.get("profile", "default")}
    _snapshot_measures = _load_snapshot_measures(_cfg_for_snapshot)
    _profile_fewshots = _load_profile_fewshots(_cfg_for_snapshot)
    all_fixes = []

    for r in results:
        g = r.get("grading", {})
        verdict = g.get("verdict", "?")
        idx = r.get("index", "?")
        dur = r.get("duration_wall", "?")

        if verdict == "pass":
            icon = "+"
        elif verdict == "fail":
            icon = "X"
        elif verdict == "no_expected":
            icon = "?"
        else:
            icon = "!" if r.get("status") != "completed" else "?"

        tags_str = f"  [{', '.join(g.get('tags', []))}]" if g.get("tags") else ""

        # Quality ratings
        dax_stars, dax_label, dax_note = _assess_dax_quality(r)
        ans_stars, ans_label = _assess_answer_quality(r)

        print(f"  {icon} Q{idx} [{dur}s]{tags_str}")
        print(f"     Question : {r['question']}")
        print(f"     Tools    : {' -> '.join(r.get('tools', []))}")

        ans = r.get("answer", "")
        print(f"     Answer   : {ans}")

        if g.get("expected") is not None:
            print(f"     Expected : {g['expected']} ({g.get('match_type', '?')})")
            print(f"     Verdict  : {verdict.upper()} -- {g.get('compare_detail', '')}")

        # Quality ratings
        print(f"     DAX      : {_score(dax_stars)} {dax_label}"
              + (f" -- {dax_note}" if dax_note else ""))
        print(f"     Quality  : {_score(ans_stars)} {ans_label}")

        if r.get("error"):
            print(f"     ERROR    : {r['error']}")

        if g.get("root_cause"):
            print(f"     +-- ROOT CAUSE: {g['root_cause']}")
            print(f"     |  {g.get('root_cause_detail', '')}")
            artifacts = g.get("artifacts", {})
            if artifacts.get("reformulated_question"):
                print(f"     |  Reformulated: {artifacts['reformulated_question'][:120]}")
            if artifacts.get("generated_query"):
                query_preview = artifacts["generated_query"][:200].replace("\n", " ")
                print(f"     |  Query: {query_preview}")
            if artifacts.get("query_result_preview"):
                print(f"     |  Result: {artifacts['query_result_preview'][:150]}")
            print(f"     +--")

        # DAX improvement suggestions
        fixes = _suggest_dax_improvements(r, dax_stars, dax_note, _snapshot_measures, fewshots=_profile_fewshots)
        if verdict == "fail":
            fixes.extend(_suggest_actions(r))
        if fixes:
            seen_texts = set()
            unique_fixes = []
            for fix_type, text in fixes:
                if text not in seen_texts:
                    seen_texts.add(text)
                    unique_fixes.append((fix_type, text))
            print(f"     >> Fixes:")
            for fix_type, text in unique_fixes[:5]:
                print(f"        [{fix_type:11s}] {text}")
            all_fixes.extend((idx, fix_type, text) for fix_type, text in unique_fixes)

        print()

    rca_counts = {}
    for r in results:
        rc = r.get("grading", {}).get("root_cause")
        if rc:
            rca_counts[rc] = rca_counts.get(rc, 0) + 1

    if rca_counts:
        print(f"{'-' * W}")
        print("  ROOT CAUSE SUMMARY:")
        for cat, count in sorted(rca_counts.items(), key=lambda x: -x[1]):
            desc = RCA_CATEGORIES.get(cat, cat)
            print(f"    {count}x {cat}: {desc}")

    # ═══ ACTION PLAN (grouped by target) ═══
    if all_fixes:
        _render_action_plan(all_fixes, print)

    print(f"\n{'-' * W}")
    print("  RECOMMENDATIONS:")
    if n_fail > 0 or n_error > 0:
        fail_qs = [str(r.get("index", "?")) for r in results
                   if r.get("grading", {}).get("verdict") == "fail" or r.get("status") != "completed"]
        print(f"  -> Re-run failed: python -m analyzer rerun {run_dir.name} --questions {' '.join(fail_qs)}")
    if "FILTER_CONTEXT" in rca_counts:
        print("  -> Filter issues detected. Check time intelligence settings.")
    if "REFORMULATION" in rca_counts:
        print("  -> Agent failed to understand some questions. Add verified answers or rephrase.")
    if "QUERY_ERROR" in rca_counts:
        print("  -> Query errors found. Check model relationships and column visibility.")
    if "SYNTHESIS" in rca_counts:
        print("  -> Answers returned but wrong. Inspect generated DAX in diagnostic JSON files.")
    if "EMPTY_RESULT" in rca_counts:
        print("  -> Empty results. Check data freshness and filter defaults.")
    if n_ungraded > 0:
        print(f"  -> {n_ungraded} ungraded questions. Fill in expected answers in questions.yaml.")
    if n_pass == total and n_fail == 0:
        print("  -> All passed! Consider adding harder questions.")
    print(f"{'=' * W}")

    return summary


# ══════════════════════════════════════════════════════════════
#  FIND RUN DIRECTORY
# ══════════════════════════════════════════════════════════════

def find_run_dir(run_id, cfg, is_latest=False):
    """Resolve a run ID to a directory path. Checks profile-scoped and legacy paths."""
    runs_root = ROOT / cfg.get("output_dir", "runs")
    profile = cfg.get("profile_name", "default")

    if is_latest:
        # Check profile-scoped first
        profile_runs = runs_root / profile
        if profile_runs.exists():
            candidates = sorted(d for d in profile_runs.iterdir() if d.is_dir())
            if candidates:
                return candidates[-1]
        # Fallback: legacy flat runs/
        candidates = sorted(d for d in runs_root.iterdir()
                            if d.is_dir() and d.name != profile and not (d / "profile.yaml").exists())
        if candidates:
            return candidates[-1]
        return None

    # Explicit run_id
    # Check profile-scoped
    profiled = runs_root / profile / run_id
    if profiled.exists():
        return profiled
    # Fallback: legacy flat
    legacy = runs_root / run_id
    if legacy.exists():
        return legacy
    return None


# ══════════════════════════════════════════════════════════════
#  DIFF RUNS
# ══════════════════════════════════════════════════════════════

def diff_runs(run_dir_a, run_dir_b):
    """Compare two runs and show per-question verdict changes."""
    summary_a = _load_summary(run_dir_a)
    summary_b = _load_summary(run_dir_b)
    if not summary_a or not summary_b:
        return

    results_a = {r["question"]: r for r in summary_a.get("results", [])}
    results_b = {r["question"]: r for r in summary_b.get("results", [])}

    all_qs = list(dict.fromkeys(list(results_a.keys()) + list(results_b.keys())))

    W = 72
    print(f"\n{'=' * W}")
    print(f"  DIFF: {run_dir_a.name}  vs  {run_dir_b.name}")
    print(f"{'=' * W}")

    ga = summary_a.get("grading", {})
    gb = summary_b.get("grading", {})
    score_a = ga.get("score_pct", "?")
    score_b = gb.get("score_pct", "?")
    print(f"  Score: {score_a}% -> {score_b}%")
    print(f"  Wall : {summary_a.get('total_wall_seconds', '?')}s -> {summary_b.get('total_wall_seconds', '?')}s")
    print(f"  Qs   : {len(results_a)} -> {len(results_b)}")

    changes = []
    for q in all_qs:
        ra = results_a.get(q)
        rb = results_b.get(q)
        va = ra.get("grading", {}).get("verdict", "missing") if ra else "missing"
        vb = rb.get("grading", {}).get("verdict", "missing") if rb else "missing"

        if va != vb:
            changes.append((q, va, vb))

    if changes:
        print(f"\n{'-' * W}")
        print(f"  VERDICT CHANGES ({len(changes)}):")
        for q, va, vb in changes:
            arrow = "->"
            if va == "fail" and vb == "pass":
                icon = "FIXED"
            elif va == "pass" and vb == "fail":
                icon = "REGRESSED"
            elif va == "missing":
                icon = "NEW"
            elif vb == "missing":
                icon = "REMOVED"
            else:
                icon = "CHANGED"
            print(f"    [{icon}] {va} -> {vb}: {q[:60]}")
    else:
        print(f"\n  No verdict changes between the two runs.")

    # RCA comparison
    rca_a = ga.get("root_cause_distribution", {})
    rca_b = gb.get("root_cause_distribution", {})
    all_cats = set(list(rca_a.keys()) + list(rca_b.keys()))
    if all_cats:
        print(f"\n{'-' * W}")
        print("  ROOT CAUSE CHANGES:")
        for cat in sorted(all_cats):
            ca = rca_a.get(cat, 0)
            cb = rca_b.get(cat, 0)
            if ca != cb:
                print(f"    {cat}: {ca} -> {cb}")

    print(f"{'=' * W}")


def _load_summary(run_dir):
    sf = run_dir / "batch_summary.json"
    if not sf.exists():
        print(f"ERROR: No batch_summary.json in {run_dir}")
        return None
    with open(sf, "r", encoding="utf-8") as f:
        return json.load(f)


# ══════════════════════════════════════════════════════════════
#  HTML REPORT
# ══════════════════════════════════════════════════════════════

def generate_html_report(run_dir, output_path=None):
    """Generate a self-contained HTML report from a run's batch_summary.json."""
    summary = _load_summary(run_dir)
    if not summary:
        return None

    results = summary.get("results", [])
    grading = summary.get("grading", {})
    stats = summary.get("schema_stats", {})
    cov = stats.get("description_coverage", {})
    rca_dist = grading.get("root_cause_distribution", {})

    n_pass = grading.get("pass", 0)
    n_fail = grading.get("fail", 0)
    n_ungraded = grading.get("ungraded", 0)

    # Build question cards
    cards_html = []
    for r in results:
        g = r.get("grading", {})
        verdict = g.get("verdict", "?")
        css_class = {"pass": "pass", "fail": "fail"}.get(verdict, "ungraded")
        artifacts = g.get("artifacts", {})

        rca_html = ""
        if g.get("root_cause"):
            rca_html = f"""
            <div class="rca">
                <strong>Root Cause:</strong> {h(g['root_cause'])}
                <p>{h(g.get('root_cause_detail', ''))}</p>
                {"<p><em>Query:</em> <code>" + h(str(artifacts.get('generated_query', ''))[:300]) + "</code></p>" if artifacts.get('generated_query') else ""}
            </div>"""

        tools_str = " &rarr; ".join(h(t) for t in r.get("tools", [])) or "none"

        cards_html.append(f"""
        <div class="card {css_class}">
            <div class="card-header">
                <span class="badge {css_class}">{verdict.upper()}</span>
                <span class="q-num">Q{r.get('index', '?')}</span>
                <span class="q-text">{h(r['question'])}</span>
                <span class="duration">{r.get('duration_wall', '?')}s</span>
            </div>
            <div class="card-body">
                <p><strong>Answer:</strong> {h(r.get('answer', '')[:200])}</p>
                {"<p><strong>Expected:</strong> " + h(str(g.get('expected', ''))) + " (" + h(g.get('match_type', '')) + ")</p>" if g.get('expected') is not None else ""}
                {"<p><strong>Detail:</strong> " + h(g.get('compare_detail', '')) + "</p>" if g.get('compare_detail') else ""}
                <p class="tools"><strong>Tools:</strong> {tools_str}</p>
                {rca_html}
            </div>
        </div>""")

    # RCA summary rows
    rca_rows = ""
    for cat, count in sorted(rca_dist.items(), key=lambda x: -x[1]):
        desc = RCA_CATEGORIES.get(cat, cat)
        rca_rows += f"<tr><td>{h(cat)}</td><td>{count}</td><td>{h(desc)}</td></tr>\n"

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>AI Skill Analyzer - {h(run_dir.name)}</title>
<style>
  :root {{ --pass: #22c55e; --fail: #ef4444; --ungraded: #a3a3a3; --bg: #f9fafb; }}
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
         background: var(--bg); color: #1f2937; line-height: 1.5; padding: 2rem; }}
  h1 {{ font-size: 1.5rem; margin-bottom: 0.5rem; }}
  .meta {{ color: #6b7280; font-size: 0.875rem; margin-bottom: 1.5rem; }}
  .meta span {{ margin-right: 1.5rem; }}
  .scoreboard {{ display: flex; gap: 1rem; margin-bottom: 2rem; flex-wrap: wrap; }}
  .score-box {{ padding: 1rem 1.5rem; border-radius: 0.5rem; color: white;
                font-size: 1.25rem; font-weight: 700; min-width: 120px; text-align: center; }}
  .score-box.pass {{ background: var(--pass); }}
  .score-box.fail {{ background: var(--fail); }}
  .score-box.ungraded {{ background: var(--ungraded); }}
  .score-box.total {{ background: #3b82f6; }}
  .score-box small {{ display: block; font-size: 0.75rem; font-weight: 400; opacity: 0.9; }}
  .card {{ background: white; border-radius: 0.5rem; margin-bottom: 1rem;
           border-left: 4px solid var(--ungraded); box-shadow: 0 1px 3px rgba(0,0,0,0.08); }}
  .card.pass {{ border-left-color: var(--pass); }}
  .card.fail {{ border-left-color: var(--fail); }}
  .card-header {{ display: flex; align-items: center; gap: 0.75rem; padding: 0.75rem 1rem;
                  border-bottom: 1px solid #f3f4f6; flex-wrap: wrap; }}
  .badge {{ padding: 0.15rem 0.5rem; border-radius: 0.25rem; font-size: 0.7rem;
            font-weight: 700; color: white; text-transform: uppercase; }}
  .badge.pass {{ background: var(--pass); }}
  .badge.fail {{ background: var(--fail); }}
  .badge.ungraded {{ background: var(--ungraded); }}
  .q-num {{ font-weight: 600; color: #6b7280; }}
  .q-text {{ flex: 1; font-weight: 500; }}
  .duration {{ color: #9ca3af; font-size: 0.8rem; }}
  .card-body {{ padding: 0.75rem 1rem; font-size: 0.875rem; }}
  .card-body p {{ margin-bottom: 0.4rem; }}
  .tools {{ color: #6b7280; }}
  .rca {{ background: #fef2f2; border-radius: 0.25rem; padding: 0.5rem 0.75rem;
          margin-top: 0.5rem; border-left: 3px solid var(--fail); }}
  .rca code {{ font-size: 0.8rem; word-break: break-all; }}
  .actions {{ background: #eff6ff; border-radius: 0.25rem; padding: 0.5rem 0.75rem;
              margin-top: 0.5rem; border-left: 3px solid #3b82f6; }}
  .actions ul {{ margin: 0.3rem 0 0 1.2rem; padding: 0; }}
  .actions li {{ margin-bottom: 0.2rem; font-size: 0.85rem; }}
  table {{ border-collapse: collapse; width: 100%; margin-top: 0.5rem; }}
  th, td {{ text-align: left; padding: 0.5rem 0.75rem; border-bottom: 1px solid #e5e7eb; }}
  th {{ background: #f9fafb; font-weight: 600; font-size: 0.8rem; color: #6b7280; }}
  td {{ font-size: 0.875rem; }}
  .section-title {{ font-size: 1.1rem; font-weight: 600; margin: 1.5rem 0 0.75rem; }}
  footer {{ margin-top: 2rem; color: #9ca3af; font-size: 0.75rem; text-align: center; }}
</style>
</head>
<body>
<h1>AI Skill Analyzer Report</h1>
<div class="meta">
  <span><strong>Run:</strong> {h(run_dir.name)}</span>
  <span><strong>Profile:</strong> {h(summary.get('profile', 'default'))}</span>
  <span><strong>Model:</strong> {h(summary.get('model_name', '?'))}</span>
  <span><strong>Stage:</strong> {h(summary.get('stage', '?'))}</span>
  <span><strong>Wall:</strong> {summary.get('total_wall_seconds', '?')}s</span>
  <span><strong>Workers:</strong> {summary.get('max_workers', 1)}</span>
</div>
<div class="meta">
  <span><strong>Schema:</strong> {stats.get('tables', 0)}T / {stats.get('columns', 0)}C / {stats.get('measures', 0)}M / {stats.get('relationships', 0)}R</span>
  <span><strong>Desc coverage:</strong> T={cov.get('tables', '?')} C={cov.get('columns', '?')} M={cov.get('measures', '?')}</span>
</div>

<div class="scoreboard">
  <div class="score-box total">{grading.get('score_pct', '-')}%<small>Score</small></div>
  <div class="score-box pass">{n_pass}<small>Pass</small></div>
  <div class="score-box fail">{n_fail}<small>Fail</small></div>
  <div class="score-box ungraded">{n_ungraded}<small>Ungraded</small></div>
</div>

<div class="section-title">Questions</div>
{"".join(cards_html)}

{"<div class='section-title'>Root Cause Summary</div><table><tr><th>Category</th><th>Count</th><th>Description</th></tr>" + rca_rows + "</table>" if rca_rows else ""}

<footer>
  Generated by AI Skill Analyzer v3 &mdash; {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}
</footer>
</body>
</html>"""

    if output_path is None:
        output_path = run_dir / "report.html"
    else:
        output_path = Path(output_path)

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"  HTML report: {output_path}")
    return output_path
