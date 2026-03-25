"""CLI — command-line interface with --profile support.

Usage:
    python -m analyzer --profile marketing360 run
    python -m analyzer --profile marketing360 run --refresh --serial --tag kpi
    python -m analyzer --profile marketing360 rerun 20260325_080433 --questions 3 5
    python -m analyzer --profile marketing360 analyze --latest --html
    python -m analyzer --profile marketing360 snapshot
    python -m analyzer diff RUN_A RUN_B --profile marketing360

If --profile is omitted, uses default_profile from config.yaml,
or falls back to legacy mode (all IDs in config.yaml).
"""

import argparse

from .config import ROOT, resolve_config, load_test_cases, list_profiles
from .auth import FabricSession
from .snapshot import snapshot_is_fresh, load_snapshot, take_snapshot
from .runner import run_questions_parallel, run_questions_serial
from .reporting import save_run, analyze_run, find_run_dir, diff_runs, generate_html_report


def cmd_snapshot(args, cfg):
    print("\n[SNAPSHOT] Fetching agent config + schema from Fabric...\n")
    session = FabricSession(cfg)
    take_snapshot(session, cfg, force=True)
    print("\nDone.")


def cmd_run(args, cfg):
    tag_filter = getattr(args, "tag", None)
    test_cases = load_test_cases(cfg, tag_filter=tag_filter)
    questions = [tc["question"] for tc in test_cases]
    n_graded = sum(1 for tc in test_cases if tc.get("expected") is not None)

    W = 72
    print(f"\n{'=' * W}")
    print("  THE AI SKILL ANALYZER -- BATCH RUN + GRADING")
    print(f"{'=' * W}")
    print(f"  Profile  : {cfg.get('profile_name', 'default')}")
    print(f"  Agent    : {cfg['agent_id']}")
    print(f"  Model    : {cfg['semantic_model_name']}")
    print(f"  Questions: {len(questions)}  ({n_graded} with expected answers)")
    print(f"  Workers  : {cfg.get('max_workers', 4)}")
    print(f"  Stage    : {cfg.get('stage', 'sandbox')}")
    if tag_filter:
        print(f"  Tag      : {tag_filter}")
    if getattr(args, "dry_run", False):
        print(f"\n  [DRY RUN] Config validated, {len(questions)} questions ready. No Fabric calls.")
        return
    print(f"{'=' * W}")

    session = FabricSession(cfg)

    # Snapshot
    refresh = getattr(args, "refresh", False)
    if refresh or not snapshot_is_fresh(cfg):
        print("\n[1/4] Taking snapshot...")
        agent_data, schema = take_snapshot(session, cfg, force=refresh)
    else:
        print(f"\n[1/4] Using cached snapshot (< {cfg['snapshot_ttl_hours']}h old)")
        agent_data, schema = load_snapshot(cfg)

    # Run
    print(f"\n[2/4] Running questions...")
    if cfg.get("max_workers", 4) > 1 and not getattr(args, "serial", False):
        results, total_wall, interrupted = run_questions_parallel(session, questions, cfg)
    else:
        results, total_wall, interrupted = run_questions_serial(session, questions, cfg)

    # Grade + Save
    print(f"\n[3/4] Grading answers + root cause analysis...")
    ts, out = save_run(results, agent_data, schema, cfg, total_wall, test_cases, interrupted)

    # HTML report
    if getattr(args, "html", False):
        generate_html_report(out)

    # Summary
    print(f"\n[4/4] Results\n")
    n_pass = sum(1 for r in results if r.get("grading", {}).get("verdict") == "pass")
    n_fail = sum(1 for r in results if r.get("grading", {}).get("verdict") == "fail")
    n_ungraded = sum(1 for r in results if r.get("grading", {}).get("verdict") in ("no_expected", None))

    print(f"{'=' * W}")
    print(f"  Run ID : {ts}")
    print(f"  Profile: {cfg.get('profile_name', 'default')}")
    print(f"  Output : {out.relative_to(ROOT)}")
    if interrupted:
        print(f"  WARNING: Run was interrupted (partial results saved)")
    if (n_pass + n_fail) > 0:
        pct = round(n_pass / (n_pass + n_fail) * 100)
        print(f"  Score  : {n_pass}/{n_pass + n_fail} = {pct}%")
    print(f"  + Pass: {n_pass}  X Fail: {n_fail}  ? Ungraded: {n_ungraded}  | {total_wall}s")
    print(f"{'=' * W}")

    for r in results:
        g = r.get("grading", {})
        verdict = g.get("verdict", "?")
        icon = {"pass": "+", "fail": "X"}.get(verdict, "?")
        detail = ""
        if verdict == "fail" and g.get("root_cause"):
            detail = f" [{g['root_cause']}]"
        print(f"  {icon} Q{r['index']} [{r['duration_wall']}s] {r['question']}{detail}")

    if n_fail > 0:
        fail_idx = [str(r["index"]) for r in results if r.get("grading", {}).get("verdict") == "fail"]
        print(f"\n  Re-run failed: python -m analyzer rerun {ts} --questions {' '.join(fail_idx)}")

    print(f"\n  Full analysis: python -m analyzer analyze {ts}")
    print(f"  Detail files:  {out.relative_to(ROOT)}/diagnostics/")


def cmd_rerun(args, cfg):
    run_id = args.run_id

    run_dir = find_run_dir(run_id if run_id != "--latest" else None, cfg,
                           is_latest=(run_id == "--latest"))
    if not run_dir:
        print(f"ERROR: Run '{run_id}' not found.")
        return

    import json
    summary_file = run_dir / "batch_summary.json"
    with open(summary_file, "r", encoding="utf-8") as f:
        prev_summary = json.load(f)

    prev_results = prev_summary.get("results", [])

    specific_qs = getattr(args, "questions", None)
    if specific_qs:
        indices = set(int(q) for q in specific_qs)
        to_rerun = [r for r in prev_results if r.get("index") in indices]
    else:
        to_rerun = [r for r in prev_results
                    if r.get("grading", {}).get("verdict") == "fail"
                    or r.get("status") != "completed"]

    if not to_rerun:
        print("Nothing to re-run -- all questions passed!")
        return

    questions = [r["question"] for r in to_rerun]

    all_test_cases = load_test_cases(cfg)
    tc_map = {tc["question"]: tc for tc in all_test_cases}
    rerun_test_cases = [tc_map.get(q, {"question": q, "expected": None,
                        "match_type": "contains", "tolerance": None, "tags": []})
                        for q in questions]

    print(f"\n[RERUN] Re-running {len(questions)} question(s) from run {run_dir.name}...")
    print(f"  Profile: {cfg.get('profile_name', 'default')}\n")

    session = FabricSession(cfg)

    if snapshot_is_fresh(cfg):
        agent_data, schema = load_snapshot(cfg)
    else:
        print("  Snapshot expired -- refreshing...")
        agent_data, schema = take_snapshot(session, cfg, force=True)

    if cfg.get("max_workers", 4) > 1 and len(questions) > 1:
        results, total_wall, interrupted = run_questions_parallel(session, questions, cfg)
    else:
        results, total_wall, interrupted = run_questions_serial(session, questions, cfg)

    ts, out = save_run(results, agent_data, schema, cfg, total_wall, rerun_test_cases, interrupted)

    if getattr(args, "html", False):
        generate_html_report(out)

    n_pass = sum(1 for r in results if r.get("grading", {}).get("verdict") == "pass")
    n_fail = sum(1 for r in results if r.get("grading", {}).get("verdict") == "fail")
    print(f"\n  RERUN: + {n_pass} pass  X {n_fail} fail  |  {total_wall}s  |  Run: {ts}")
    print(f"  Output: {out.relative_to(ROOT)}")
    print(f"  Full analysis: python -m analyzer analyze {ts}")


def cmd_analyze(args, cfg):
    run_id = getattr(args, "run_id", None)
    is_latest = getattr(args, "latest", False)

    run_dir = find_run_dir(run_id, cfg, is_latest=is_latest)
    if not run_dir:
        what = "--latest" if is_latest else run_id
        print(f"ERROR: Run '{what}' not found.")
        return

    analyze_run(run_dir)

    if getattr(args, "html", False):
        generate_html_report(run_dir)


def cmd_diff(args, cfg):
    run_a = find_run_dir(args.run_a, cfg)
    run_b = find_run_dir(args.run_b, cfg)
    if not run_a:
        print(f"ERROR: Run '{args.run_a}' not found.")
        return
    if not run_b:
        print(f"ERROR: Run '{args.run_b}' not found.")
        return
    diff_runs(run_a, run_b)


def cmd_profiles(args, cfg):
    """List available profiles."""
    profiles = list_profiles()
    if not profiles:
        print("No profiles found. Create: profiles/<name>/profile.yaml")
        return
    print("Available profiles:")
    default = cfg.get("default_profile")
    for p in profiles:
        marker = " (default)" if p == default else ""
        print(f"  - {p}{marker}")


def main():
    parser = argparse.ArgumentParser(
        prog="analyzer",
        description="The AI Skill Analyzer - Fabric Data Agent Diagnostic & Grading Tool (v3)",
    )
    parser.add_argument("--profile", "-p", type=str, default=None,
                        help="Profile name (default: from config.yaml)")
    sub = parser.add_subparsers(dest="command")

    # profiles
    sub.add_parser("profiles", help="List available profiles")

    # snapshot
    sub.add_parser("snapshot", help="Fetch & cache agent config + schema")

    # run
    run_p = sub.add_parser("run", help="Run all questions, grade answers, trace pipeline")
    run_p.add_argument("--refresh", action="store_true", help="Force refresh snapshot before run")
    run_p.add_argument("--serial", action="store_true", help="Run questions sequentially")
    run_p.add_argument("--tag", type=str, help="Run only questions with this tag")
    run_p.add_argument("--html", action="store_true", help="Generate HTML report")
    run_p.add_argument("--dry-run", action="store_true", help="Validate config, don't call Fabric")

    # rerun
    rerun_p = sub.add_parser("rerun", help="Re-run failed/specific questions from a previous run")
    rerun_p.add_argument("run_id", help="Run ID (timestamp folder) or --latest")
    rerun_p.add_argument("--questions", nargs="+", help="Specific question indices to re-run")
    rerun_p.add_argument("--html", action="store_true", help="Generate HTML report")

    # analyze
    analyze_p = sub.add_parser("analyze", help="Analyze an existing run with RCA (offline)")
    analyze_p.add_argument("run_id", nargs="?", help="Run ID (timestamp folder)")
    analyze_p.add_argument("--latest", action="store_true", help="Analyze most recent run")
    analyze_p.add_argument("--html", action="store_true", help="Generate HTML report")

    # diff
    diff_p = sub.add_parser("diff", help="Compare two runs side by side")
    diff_p.add_argument("run_a", help="First run ID")
    diff_p.add_argument("run_b", help="Second run ID")

    args = parser.parse_args()

    if args.command == "profiles":
        # profiles doesn't need a resolved config
        from .config import load_global_config
        cfg = load_global_config()
        cmd_profiles(args, cfg)
        return

    cfg = resolve_config(args.profile)

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
    elif args.command == "diff":
        cmd_diff(args, cfg)
    else:
        parser.print_help()
