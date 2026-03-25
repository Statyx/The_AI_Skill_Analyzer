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
                "fewshots": {"fewShots": [], "parentId": cfg["semantic_model_id"],
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
        verdict_data = grade_result(r, tc)

        diag = build_diagnostic(agent_data, schema, r, cfg, verdict_data=verdict_data)
        safe_q = re.sub(r"[^a-z0-9]+", "_", r["question"].lower())[:40].strip("_")
        filename = f"full_diag_{safe_q}.json"
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

    summary = {
        "timestamp": ts,
        "profile": profile,
        "agent_id": cfg["agent_id"],
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
        print(f"  {icon} Q{idx} [{dur}s]{tags_str}")
        print(f"     Question : {r['question']}")
        print(f"     Tools    : {' -> '.join(r.get('tools', []))}")

        ans = r.get("answer", "")[:150]
        print(f"     Answer   : {ans}")

        if g.get("expected") is not None:
            print(f"     Expected : {g['expected']} ({g.get('match_type', '?')})")
            print(f"     Verdict  : {verdict.upper()} -- {g.get('compare_detail', '')}")

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
