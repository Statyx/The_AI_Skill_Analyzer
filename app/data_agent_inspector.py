"""Data Agent Inspector — Streamlit app.

Upload Microsoft Fabric Data Agent diagnostic JSON files and get a full
analysis: root-cause diagnosis, pipeline timeline, query inspection,
latency/token breakdown, and aggregate views across many runs.

Reuses the existing analysis engine in `analyzer/diagnose.py` — no logic
is duplicated here; this module is purely the presentation layer.

Run locally:
    streamlit run app/data_agent_inspector.py

Deploy to Fabric: see app/README.md.
"""

import json
import sys
from collections import Counter
from pathlib import Path

import pandas as pd
import streamlit as st

# Make the repo root importable so we can reuse the analysis engine.
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from analyzer.diagnose import analyze_diagnostic  # noqa: E402


# ══════════════════════════════════════════════════════════════
#  CONSTANTS / STYLE
# ══════════════════════════════════════════════════════════════

SEV_ICON = {"error": "🔴", "warning": "🟠", "info": "🔵"}
SEV_COLOR = {"error": "#ff4b4b", "warning": "#ffa421", "info": "#3d9df3"}
STATUS_ICON = {
    "completed": "✅", "failed": "❌", "cancelled": "⚪",
    "expired": "⏰", "in_progress": "🔄", "unknown": "❓",
}
LANG_HINT = {
    "dax": "sql", "kql": "sql", "sql": "sql", "tsql": "sql",
    "kusto": "sql", "spark": "python",
}


# ══════════════════════════════════════════════════════════════
#  DATA HELPERS
# ══════════════════════════════════════════════════════════════

def _load_uploaded(file) -> dict | None:
    """Parse an uploaded file-like into a diagnostic analysis dict."""
    try:
        raw = json.load(file)
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        st.error(f"`{file.name}` is not valid JSON: {e}")
        return None
    try:
        analysis = analyze_diagnostic(raw)
        analysis["_source_file"] = file.name
        return analysis
    except (KeyError, TypeError, ValueError) as e:
        st.error(f"Could not analyze `{file.name}`: {e}")
        return None


def _fmt_duration(seconds) -> str:
    if not seconds:
        return "—"
    if seconds < 60:
        return f"{seconds:.1f}s"
    return f"{int(seconds // 60)}m {seconds % 60:.0f}s"


def _fmt_tokens(usage) -> str:
    if not usage:
        return "—"
    total = usage.get("total_tokens")
    return f"{total:,}" if total else "—"


# ══════════════════════════════════════════════════════════════
#  SINGLE-FILE VIEW
# ══════════════════════════════════════════════════════════════

def render_single(a: dict):
    status = a.get("run_status", "unknown")
    issues = a.get("issues") or []
    anomalies = a.get("anomalies") or []
    errors = [i for i in issues + anomalies if i.get("severity") == "error"]

    # ── Header KPIs ──
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Status", f"{STATUS_ICON.get(status, '❓')} {status}")
    ds = a.get("primary_datasource_type") or "—"
    c2.metric("Datasource", ds)
    c3.metric("Duration", _fmt_duration(a.get("total_duration_s")))
    c4.metric("Tokens", _fmt_tokens(a.get("usage")))
    health = "🔴 Failing" if errors else ("🟠 Warnings" if (issues or anomalies) else "🟢 Healthy")
    c5.metric("Health", health)

    if a.get("is_cached"):
        st.info("⚡ This response was served from cache (no tool executed).")

    # ── Question / Answer ──
    st.markdown("### 💬 Conversation")
    st.markdown(f"**Question**\n\n> {a.get('question') or '_(none)_'}")
    answer = a.get("answer") or "_(empty answer)_"
    st.markdown("**Answer**")
    st.markdown(answer)

    # ── Issues & recommendations ──
    left, right = st.columns(2)
    with left:
        st.markdown("### 🩺 Issues & anomalies")
        all_findings = issues + anomalies
        if not all_findings:
            st.success("No issues detected.")
        else:
            for f in sorted(all_findings, key=lambda x: {"error": 0, "warning": 1, "info": 2}.get(x.get("severity"), 3)):
                sev = f.get("severity", "info")
                with st.container(border=True):
                    st.markdown(f"{SEV_ICON.get(sev, '🔵')} **{f.get('issue', '?')}**  ·  _{f.get('stage', '')}_")
                    detail = f.get("detail")
                    if detail:
                        st.caption(detail)
                    if f.get("count"):
                        st.caption(f"Occurrences: {f['count']}")
    with right:
        st.markdown("### 🛠️ Recommendations")
        recs = a.get("recommendations") or []
        if not recs:
            st.caption("No recommendations — looks good.")
        else:
            for r in recs:
                txt = r if isinstance(r, str) else r.get("text") or r.get("recommendation") or str(r)
                st.markdown(f"- {txt}")

    # ── Pipeline timeline ──
    st.markdown("### 🔬 Pipeline timeline")
    steps = a.get("steps") or []
    if steps:
        rows = []
        for i, s in enumerate(steps, 1):
            dur = s.get("latency_duration_s") or s.get("duration_s")
            rows.append({
                "#": i,
                "Stage": s.get("stage"),
                "Tool": s.get("tool"),
                "Status": f"{STATUS_ICON.get(s.get('status'), '❓')} {s.get('status')}",
                "Duration": _fmt_duration(dur),
                "Datasource": s.get("datasource_name") or "—",
            })
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
    else:
        st.caption("No pipeline steps found.")

    # ── Query inspection ──
    queries = [s for s in steps if s.get("query")]
    if queries:
        st.markdown("### 🧮 Generated queries")
        for s in queries:
            lang = (s.get("query_lang") or "").lower()
            with st.expander(f"{s.get('stage')} · {s.get('tool')} ({s.get('query_lang') or 'query'})"):
                st.code(s["query"], language=LANG_HINT.get(lang, "text"))
                if s.get("query_result"):
                    st.markdown("**Result**")
                    st.code(str(s["query_result"]), language="text")

    # ── Latency breakdown ──
    latency = a.get("latency") or {}
    by_stage = latency.get("by_stage") or []
    if by_stage:
        st.markdown("### ⏱️ Latency breakdown")
        lc1, lc2 = st.columns([2, 1])
        with lc1:
            df = pd.DataFrame(by_stage)
            df = df.rename(columns={"stage": "Stage", "duration_s": "Seconds", "count": "Count"})
            st.bar_chart(df.set_index("Stage")["Seconds"])
        with lc2:
            if latency.get("orchestrator_overhead_s") is not None:
                st.metric("Orchestrator overhead", _fmt_duration(latency["orchestrator_overhead_s"]),
                          delta=f"{latency.get('orchestrator_pct', 0)}% of total", delta_color="off")
            st.metric("Tool execution", _fmt_duration(latency.get("tool_total_s")))
            st.metric("Run total", _fmt_duration(latency.get("run_total_s")))

    # ── Instruction quality (if present) ──
    quality = a.get("instruction_quality")
    if quality:
        st.markdown("### 📋 Instruction quality")
        if isinstance(quality, dict) and "score" in quality:
            st.metric("Score", f"{quality.get('score')}/100")
            for item in quality.get("findings", []) or []:
                st.caption(f"• {item}")
        else:
            st.json(quality)

    # ── Raw JSON ──
    with st.expander("🗂️ Raw analysis (JSON)"):
        st.json({k: v for k, v in a.items() if not k.startswith("_")})


# ══════════════════════════════════════════════════════════════
#  BATCH VIEW
# ══════════════════════════════════════════════════════════════

def render_batch(analyses: list[dict]):
    valid = [a for a in analyses if a]
    if not valid:
        st.warning("No valid diagnostics to display.")
        return

    # ── Aggregate KPIs ──
    statuses = Counter(a.get("run_status", "unknown") for a in valid)
    all_issues = [i for a in valid for i in (a.get("issues") or [])]
    all_anoms = [i for a in valid for i in (a.get("anomalies") or [])]
    durations = [a["total_duration_s"] for a in valid if a.get("total_duration_s")]
    tokens = [(a.get("usage") or {}).get("total_tokens") for a in valid]
    tokens = [t for t in tokens if t]
    failed = statuses.get("failed", 0)

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Runs", len(valid))
    c2.metric("Failed", failed, delta=None if not failed else f"-{failed}", delta_color="inverse")
    c3.metric("Issues", len(all_issues) + len(all_anoms))
    c4.metric("Avg duration", _fmt_duration(sum(durations) / len(durations) if durations else None))
    c5.metric("Total tokens", f"{sum(tokens):,}" if tokens else "—")

    # ── Status distribution + top issues ──
    left, right = st.columns(2)
    with left:
        st.markdown("#### Status distribution")
        sdf = pd.DataFrame(
            [{"Status": s, "Count": c} for s, c in statuses.most_common()]
        )
        st.bar_chart(sdf.set_index("Status")["Count"])
    with right:
        st.markdown("#### Top issues")
        counts = Counter(i["issue"] for i in all_issues + all_anoms)
        if counts:
            idf = pd.DataFrame(
                [{"Issue": k, "Count": v} for k, v in counts.most_common(8)]
            )
            st.dataframe(idf, use_container_width=True, hide_index=True)
        else:
            st.success("No issues across the batch.")

    # ── Per-run table ──
    st.markdown("#### Per-run detail")
    rows = []
    for a in valid:
        iss = a.get("issues") or []
        ano = a.get("anomalies") or []
        worst = "🟢"
        sev_set = {x.get("severity") for x in iss + ano}
        if "error" in sev_set:
            worst = "🔴"
        elif "warning" in sev_set:
            worst = "🟠"
        elif "info" in sev_set:
            worst = "🔵"
        rows.append({
            "Health": worst,
            "File": a.get("_source_file", "?"),
            "Question": (a.get("question") or "")[:60],
            "Status": f"{STATUS_ICON.get(a.get('run_status'), '❓')} {a.get('run_status')}",
            "Datasource": a.get("primary_datasource_type") or "—",
            "Steps": len(a.get("steps") or []),
            "Issues": len(iss) + len(ano),
            "Duration": _fmt_duration(a.get("total_duration_s")),
            "Tokens": (a.get("usage") or {}).get("total_tokens") or 0,
        })
    bdf = pd.DataFrame(rows)
    st.dataframe(bdf, use_container_width=True, hide_index=True)

    # ── Drill-down ──
    st.markdown("#### 🔎 Drill into a run")
    options = {a.get("_source_file", f"run {i}"): a for i, a in enumerate(valid)}
    pick = st.selectbox("Select a diagnostic", list(options.keys()))
    if pick:
        st.divider()
        render_single(options[pick])


# ══════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════

def main():
    st.set_page_config(page_title="Data Agent Inspector", page_icon="🔬", layout="wide")

    st.title("🔬 Data Agent Inspector")
    st.caption(
        "Analyze Microsoft Fabric Data Agent diagnostic exports — "
        "root-cause diagnosis, pipeline timeline, query & latency inspection."
    )

    with st.sidebar:
        st.header("Input")
        uploads = st.file_uploader(
            "Diagnostic JSON file(s)",
            type=["json"],
            accept_multiple_files=True,
            help="Export from the Fabric Data Agent portal, or the files in portal_exports/.",
        )
        st.divider()
        sample = st.checkbox("Load bundled samples (portal_exports/)", value=not uploads)
        st.caption("Single file → detailed view. Multiple files → batch dashboard.")

    analyses: list[dict] = []
    if uploads:
        for f in uploads:
            a = _load_uploaded(f)
            if a:
                analyses.append(a)
    elif sample:
        sample_dir = _REPO_ROOT / "portal_exports"
        for f in sorted(sample_dir.glob("*.json")):
            try:
                with open(f, "r", encoding="utf-8") as fh:
                    a = analyze_diagnostic(json.load(fh))
                a["_source_file"] = f.name
                analyses.append(a)
            except (json.JSONDecodeError, KeyError, TypeError, ValueError):
                continue

    if not analyses:
        st.info("⬅️ Upload one or more Data Agent diagnostic JSON files to begin.")
        return

    if len(analyses) == 1:
        render_single(analyses[0])
    else:
        tab_batch, tab_single = st.tabs(["📊 Batch overview", "📄 Single run"])
        with tab_batch:
            render_batch(analyses)
        with tab_single:
            options = {a.get("_source_file", f"run {i}"): a for i, a in enumerate(analyses)}
            pick = st.selectbox("Run", list(options.keys()), key="single_tab_pick")
            if pick:
                render_single(options[pick])


if __name__ == "__main__":
    main()
