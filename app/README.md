# 🔬 Data Agent Inspector

A Streamlit app that analyzes **Microsoft Fabric Data Agent** diagnostic JSON
exports — root-cause diagnosis, pipeline timeline, query & latency inspection,
and an aggregate dashboard across many runs.

It is the **UI layer** on top of the existing analysis engine in
[`analyzer/diagnose.py`](../analyzer/diagnose.py). No analysis logic is
duplicated — the app calls `analyze_diagnostic()` directly.

## Features

| | Single run | Batch |
|---|:---:|:---:|
| Status / datasource / duration / token KPIs | ✅ | ✅ |
| Question + answer | ✅ | ✅ (drill-down) |
| Issues & anomalies (severity-ranked) | ✅ | ✅ (aggregated) |
| Recommendations / action plan | ✅ | — |
| Pipeline timeline (stages, tools, status) | ✅ | — |
| Generated query inspection (DAX / SQL / KQL) + results | ✅ | — |
| Latency breakdown + orchestrator overhead | ✅ | — |
| Instruction quality score | ✅ | — |
| Status distribution + top-issues chart | — | ✅ |
| Per-run sortable table | — | ✅ |

- **1 file uploaded** → detailed single-run view.
- **Multiple files** → batch overview + per-run drill-down tabs.

## Run locally

```bash
# from the repo root
pip install -r app/requirements.txt
streamlit run app/data_agent_inspector.py
```

Then open http://localhost:8501. By default it loads the bundled samples from
[`portal_exports/`](../portal_exports/); use the sidebar to upload your own.

## Get diagnostic files

Export them from the Fabric Data Agent portal (the **Diagnostics / inspect**
panel of a run), or reuse the JSON files the analyzer already writes under
`runs/<profile>/<timestamp>/diagnostics/`. Both schemas are supported.

## Deploy to Microsoft Fabric

Fabric hosts Streamlit apps via **User Data Functions / Fabric-hosted apps**.
The app is self-contained (`app/` + `analyzer/`), so:

1. **Package** the repo (the app imports `analyzer.diagnose`, so ship both
   `app/` and `analyzer/`).
2. In your Fabric workspace, create a new **Streamlit app** item (or a
   User Data Function hosting Streamlit), point its entrypoint at
   `app/data_agent_inspector.py`, and set the requirements to
   `app/requirements.txt`.
3. Ensure the working directory is the repo root so `analyzer/` resolves on
   `sys.path` (the app already inserts the repo root automatically).
4. Publish. Users upload diagnostic JSON exports directly in the browser — no
   Fabric credentials are required because the app analyzes exported files
   offline.

> For a quick shared demo without Fabric, the same entrypoint deploys as-is to
> Streamlit Community Cloud (set the main file to `app/data_agent_inspector.py`).

## Project layout

```text
app/
  data_agent_inspector.py   # Streamlit entrypoint (presentation only)
  requirements.txt          # streamlit + pandas
  .streamlit/config.toml    # theme (Fabric purple)
  README.md                 # this file
analyzer/diagnose.py        # analysis engine (reused, not modified)
```
