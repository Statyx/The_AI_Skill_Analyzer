# 🔬 Data Agent Inspector — Fabric App

A **native Microsoft Fabric App** (React + Vite + TypeScript, scaffolded with the
Rayfin `blankapp` template) that analyzes **Fabric Data Agent diagnostic JSON**
exports — root-cause diagnosis, pipeline timeline, generated-query inspection,
and an aggregate batch dashboard.

The analysis logic is a TypeScript port of the core of
[`analyzer/diagnose.py`](../analyzer/diagnose.py) (`src/lib/diagnose.ts`), so the
app runs **entirely client-side** — diagnostics are analyzed in the browser, no
backend or Fabric data connection required.

> There is also a Python/Streamlit version of this tool in [`../app/`](../app/)
> for local use. This `fabric_app/` is the version that deploys **into Fabric**.

## What it does

- **1 file** → detailed single-run view: status / datasource / duration KPIs,
  question + answer, issues & anomalies, pipeline timeline, generated queries
  (DAX / SQL / KQL) with results.
- **Multiple files** → batch overview: aggregate KPIs, status distribution,
  top issues, per-run table with click-through drill-down.

## Run locally

```powershell
cd fabric_app
npm install
npm run dev      # http://localhost:5173
```

Drag the sample diagnostics from [`../portal_exports/`](../portal_exports/) onto
the drop zone.

```powershell
npm run build    # type-check + production build to dist/
npm run preview  # serve the production build
```

## Deploy to Microsoft Fabric

This is a Rayfin-managed Fabric App. Deployment is interactive (Entra sign-in)
and targets **your** workspace, so these steps are run by you:

1. **Create a dedicated workspace** in the Fabric portal (e.g. `Data Agent Inspector`).
2. Make sure the **Fabric Apps** workload is enabled in your tenant.
3. From this folder, sign in and deploy:

   ```powershell
   cd fabric_app
   npx rayfin login                       # Entra ID sign-in
   npx rayfin up --workspace "Data Agent Inspector"
   npx rayfin up status                   # verify endpoint health
   ```

   `rayfin up` builds the static app, deploys it as a Fabric App item, and wires
   up Fabric SSO. The live hosting URL is written to `rayfin/.deployments.json`
   and appended to `allowedRedirectUris` in `rayfin/rayfin.yml`.

4. Open the **Data App** item in your workspace to use it.

> Note: Fabric SSO only works inside the Fabric portal — local `npm run dev`
> runs without authentication (it only reads uploaded files, so no auth needed).

## Project layout

```text
fabric_app/
  src/
    lib/diagnose.ts        # TS port of the analysis engine (client-side)
    components/
      SingleView.tsx       # detailed single-run view
      BatchView.tsx        # aggregate batch dashboard
    App.tsx                # upload + routing (single vs batch)
    main.tsx               # entry
    styles.css             # Fabric-purple dark theme
  rayfin/rayfin.yml        # Rayfin/Fabric App config
  AGENTS.md / .agents/     # Rayfin agent context (from scaffold)
  package.json, vite.config.ts, tsconfig.json, index.html
```

## Parity with the Python engine

`src/lib/diagnose.ts` implements the MVP subset of `analyzer/diagnose.py`:
step parsing, stage mapping, datasource detection, question/answer extraction,
issue detection (failed steps, empty results, missing query, 0 fewshots, empty
answer, query errors, slow steps), and run timing. Latency breakdown, token
usage, anomaly heuristics, and instruction-quality scoring from the Python
engine are **not yet ported** — candidates for the next iteration.
