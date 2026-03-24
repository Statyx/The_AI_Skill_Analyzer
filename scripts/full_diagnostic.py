#!/usr/bin/env python3
"""
Full Diagnostic Batch Runner for Fabric Data Agents.

Produces portal-equivalent diagnostics by combining:
  1. Fabric REST API  → agent config, additionalInstructions
  2. Fabric REST API  → semantic model schema (tables, columns, measures, relationships, annotations)
  3. Python Client SDK → run_steps, messages, tool calls, NL2SA details

Usage:
    python full_diagnostic.py

Questions are loaded from questions.txt (one per line) if it exists,
otherwise the QUESTIONS list below is used.
"""

import sys
import os
import json
import time
import base64
import requests
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.environ["TEMP"], "fabric_data_agent_client"))
from fabric_data_agent_client import FabricDataAgentClient

# ── Configuration ─────────────────────────────────────────────
TENANT_ID = "92701a21-ddea-4028-ba85-4c1f91fab881"
WORKSPACE_ID = "5fa6b81d-fabe-4363-ad3d-b09ef82d16f2"
AGENT_ID = "e92e5867-213a-4a7d-8fac-af1711046527"
SEMANTIC_MODEL_ID = "3d00aeaa-91b9-4567-9166-fa3fc8249e6f"
DATA_AGENT_URL = (
    f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}"
    f"/aiskills/{AGENT_ID}/aiassistant/openai"
)
FABRIC_API = "https://api.fabric.microsoft.com/v1"
OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))

# Default questions (overridden by questions.txt if present)
QUESTIONS = [
    "what is the churn rate",
]


# ── REST API helpers ──────────────────────────────────────────

def fabric_get(token, path):
    r = requests.get(
        f"{FABRIC_API}{path}",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
    )
    return r.json() if r.ok else {"_error": r.status_code, "_body": r.text[:500]}


def fabric_post(token, path, body=None):
    r = requests.post(
        f"{FABRIC_API}{path}",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json=body or {},
    )
    if r.status_code == 202:
        return _poll_lro(token, r)
    if r.ok:
        return r.json()
    return {"_error": r.status_code, "_body": r.text[:500]}


def _poll_lro(token, initial_response):
    """Poll a Fabric long-running operation until done."""
    loc = initial_response.headers.get("Location") or initial_response.headers.get("Operation-Location")
    if not loc:
        return {"_error": 202, "_note": "LRO with no Location header"}
    for _ in range(30):
        time.sleep(2)
        r = requests.get(loc, headers={"Authorization": f"Bearer {token}"})
        if not r.ok:
            continue
        data = r.json()
        status = data.get("status", "")
        if status in ("Succeeded", "Completed"):
            # Try resourceLocation first
            result_url = data.get("resourceLocation")
            if result_url:
                rr = requests.get(result_url, headers={"Authorization": f"Bearer {token}"})
                return rr.json() if rr.ok else data
            # Fabric pattern: append /result to the operation URL
            result_url = loc.rstrip("/") + "/result"
            rr = requests.get(result_url, headers={"Authorization": f"Bearer {token}"})
            if rr.ok:
                return rr.json()
            return data
        if status in ("Failed", "Cancelled"):
            return {"_error": "lro_failed", "_data": data}
    return {"_error": "lro_timeout"}


# ── Agent config ──────────────────────────────────────────────

def fetch_agent_config(token):
    """Return (item_metadata, parsed_config_dict)."""
    meta = fabric_get(token, f"/workspaces/{WORKSPACE_ID}/items/{AGENT_ID}")
    defn = fabric_post(token, f"/workspaces/{WORKSPACE_ID}/items/{AGENT_ID}/getDefinition")

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
    return meta, config or defn


# ── Schema via Semantic Model getDefinition (TMDL) ───────────

def _parse_tmdl_tables(parts):
    """Parse TMDL definition parts into a hierarchical schema.

    TMDL format uses /// doc comments on the line BEFORE the object:
        /// Table description here.
        table 'my_table'
            /// Measure description.
            measure 'My Measure' = SUM(...)
            /// Column description.
            column my_column
    """
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

        # Parse table TMDL files
        if path.startswith("definition/tables/") and path.endswith(".tmdl"):
            table_name = None
            table_desc = ""
            columns = []
            measures = []
            pending_desc = []  # Accumulates /// lines

            for line in payload.split("\n"):
                stripped = line.strip()

                # Collect /// doc comments
                if stripped.startswith("///"):
                    pending_desc.append(stripped[3:].strip())
                    continue

                # Table header
                if stripped.startswith("table "):
                    table_name = stripped.split("table ", 1)[1].strip().strip("'")
                    table_desc = " ".join(pending_desc) if pending_desc else ""
                    pending_desc = []

                # Measure line
                elif stripped.startswith("measure "):
                    rest = stripped.split("measure ", 1)[1].strip()
                    # Name may be quoted and may contain = expression
                    if " = " in rest:
                        meas_name = rest.split(" = ")[0].strip().strip("'")
                    else:
                        meas_name = rest.strip().strip("'")
                    desc = " ".join(pending_desc) if pending_desc else ""
                    pending_desc = []
                    measures.append({
                        "name": meas_name,
                        "description": desc,
                        "expression": "",
                        "format_string": "",
                    })

                # Column line
                elif stripped.startswith("column "):
                    col_name = stripped.split("column ", 1)[1].strip().strip("'")
                    desc = " ".join(pending_desc) if pending_desc else ""
                    pending_desc = []
                    columns.append({
                        "name": col_name,
                        "description": desc,
                        "data_type": "",
                        "is_hidden": False,
                    })

                # dataType for last column
                elif stripped.startswith("dataType:") and columns:
                    columns[-1]["data_type"] = stripped.split(":", 1)[1].strip()

                # isHidden for last column
                elif stripped == "isHidden" and columns:
                    columns[-1]["is_hidden"] = True

                # formatString for last measure
                elif stripped.startswith("formatString:") and measures:
                    measures[-1]["format_string"] = stripped.split(":", 1)[1].strip().strip('"')

                # Any non-/// non-object line resets pending_desc
                else:
                    if not stripped.startswith("lineageTag:") and not stripped.startswith("sourceLineageTag:"):
                        pending_desc = []

            if table_name:
                tables[table_name] = {
                    "name": table_name,
                    "description": table_desc,
                    "columns": columns,
                    "measures": measures,
                }

        # Parse relationships
        elif "relationships" in path and path.endswith(".tmdl"):
            for line in payload.split("\n"):
                stripped = line.strip()
                if stripped.startswith("relationship "):
                    relationships.append(stripped)

    return tables, relationships


def fetch_schema(token):
    """Build a portal-equivalent schema from the semantic model TMDL definition."""
    print("  Fetching model definition (TMDL)...")
    defn = fabric_post(token,
        f"/workspaces/{WORKSPACE_ID}/semanticModels/{SEMANTIC_MODEL_ID}/getDefinition?format=TMDL")

    parts = []
    if defn and isinstance(defn, dict) and "definition" in defn:
        parts = defn["definition"].get("parts", [])
    elif defn and isinstance(defn, dict) and "_error" not in defn:
        # Might be the definition directly
        parts = defn.get("parts", [])

    if not parts:
        print("  ⚠ Could not retrieve TMDL definition, schema will be empty")
        print(f"    Response: {json.dumps(defn, default=str)[:300]}")
        return _empty_schema()

    print(f"  Parsing {len(parts)} TMDL parts...")
    tmap, rels = _parse_tmdl_tables(parts)

    # ── Portal-style elements array ──
    elements = []
    for tname, tinfo in tmap.items():
        children = []
        for col in tinfo["columns"]:
            children.append({
                "display_name": col["name"],
                "type": "semantic_model.column",
                "description": col["description"],
                "data_type": col.get("data_type", ""),
                "is_hidden": col.get("is_hidden", False),
            })
        for meas in tinfo["measures"]:
            children.append({
                "display_name": meas["name"],
                "type": "semantic_model.measure",
                "description": meas["description"],
                "expression": meas.get("expression", ""),
                "format_string": meas.get("format_string", ""),
            })
        elements.append({
            "display_name": tinfo["name"],
            "type": "semantic_model.table",
            "description": tinfo["description"],
            "is_selected": True,
            "children": children,
        })

    # ── Stats ──
    n_tables = len(tmap)
    n_cols = sum(len(t["columns"]) for t in tmap.values())
    n_measures = sum(len(t["measures"]) for t in tmap.values())
    desc_t = sum(1 for t in tmap.values() if t["description"])
    desc_c = sum(1 for t in tmap.values() for c in t["columns"] if c["description"])
    desc_m = sum(1 for t in tmap.values() for m in t["measures"] if m["description"])

    print(f"  → {n_tables} tables, {n_cols} columns, {n_measures} measures, {len(rels)} relationships")
    print(f"  → Descriptions: {desc_t}/{n_tables} tables, {desc_c}/{n_cols} cols, {desc_m}/{n_measures} measures")

    return {
        "dataSourceInfo": {
            "type": "semantic_model",
            "semantic_model_id": SEMANTIC_MODEL_ID,
            "semantic_model_name": "Marketing360_Model",
            "semantic_model_workspace_id": WORKSPACE_ID,
        },
        "elements": elements,
        "relationships": rels,
        "stats": {
            "tables": n_tables,
            "columns": n_cols,
            "measures": n_measures,
            "relationships": len(rels),
            "description_coverage": {
                "tables": f"{desc_t}/{n_tables}",
                "columns": f"{desc_c}/{n_cols}",
                "measures": f"{desc_m}/{n_measures}",
            },
        },
    }


def _empty_schema():
    return {
        "dataSourceInfo": {
            "type": "semantic_model",
            "semantic_model_id": SEMANTIC_MODEL_ID,
            "semantic_model_name": "Marketing360_Model",
            "semantic_model_workspace_id": WORKSPACE_ID,
        },
        "elements": [],
        "relationships": [],
        "stats": {"tables": 0, "columns": 0, "measures": 0, "relationships": 0,
                  "description_coverage": {"tables": "0/0", "columns": "0/0", "measures": "0/0"}},
    }


# ── Build diagnostic ─────────────────────────────────────────

def build_diagnostic(agent_meta, agent_config, schema, question, run_details):
    """Assemble a portal-equivalent diagnostic JSON."""

    # Extract timing from run_steps
    steps = run_details.get("run_steps", {}).get("data", [])
    step_times = []
    for s in steps:
        tc = (s.get("step_details") or {}).get("tool_calls", [])
        fn_name = tc[0]["function"]["name"] if tc else "message_creation"
        step_times.append({
            "tool": fn_name,
            "status": s.get("status"),
            "created_at": s.get("created_at"),
            "completed_at": s.get("completed_at"),
        })

    # Compute total run duration
    all_created = [s.get("created_at") or 0 for s in steps if s.get("created_at")]
    all_completed = [s.get("completed_at") or 0 for s in steps if s.get("completed_at")]
    total_duration = (max(all_completed) - min(all_created)) if all_created and all_completed else None

    return {
        "downloaded_at": datetime.now(timezone.utc).isoformat(),
        "rolloutEnvironment": "PROD",
        "stage": "sandbox",
        "artifactId": AGENT_ID,
        "workspaceId": WORKSPACE_ID,
        "source": "full_diagnostic.py (Fabric REST API + Python Client SDK)",
        "config": agent_config,
        "datasources": {
            SEMANTIC_MODEL_ID: {
                "fewshots": {
                    "fewShots": [],
                    "parentId": SEMANTIC_MODEL_ID,
                    "type": "semantic_model",
                },
                "schema": schema,
            }
        },
        "thread": {
            "question": question,
            "messages": run_details.get("messages"),
            "run_status": run_details.get("run_status"),
            "run_steps": run_details.get("run_steps"),
        },
        "timing": {
            "total_seconds": total_duration,
            "steps": step_times,
        },
    }


# ── Main ──────────────────────────────────────────────────────

def main():
    global QUESTIONS

    # Load questions from file if present
    qfile = os.path.join(OUTPUT_DIR, "questions.txt")
    if os.path.exists(qfile):
        with open(qfile, "r", encoding="utf-8") as f:
            loaded = [ln.strip() for ln in f if ln.strip() and not ln.startswith("#")]
        if loaded:
            QUESTIONS = loaded
            print(f"Loaded {len(QUESTIONS)} questions from questions.txt")

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    print("=" * 60)
    print("  FULL DIAGNOSTIC BATCH RUNNER")
    print("=" * 60)
    print(f"  Agent   : {AGENT_ID}")
    print(f"  Model   : {SEMANTIC_MODEL_ID}")
    print(f"  Questions: {len(QUESTIONS)}")
    print("=" * 60)

    # ── 1. Auth (once) via SDK client ──
    print("\n[1/4] Authenticating...")
    client = FabricDataAgentClient(tenant_id=TENANT_ID, data_agent_url=DATA_AGENT_URL)
    token = client.token.token  # reuse for REST calls → single browser popup

    # ── 2. Agent config ──
    print("\n[2/4] Fetching agent config via REST API...")
    agent_meta, agent_config = fetch_agent_config(token)
    print(f"  Agent name: {agent_meta.get('displayName', '?')}")

    # ── 3. Schema ──
    print("\n[3/4] Fetching semantic model schema (TMDL)...")
    schema = fetch_schema(token)

    # ── 4. Run questions ──
    print(f"\n[4/4] Running {len(QUESTIONS)} question(s)...\n")
    results = []

    for i, question in enumerate(QUESTIONS, 1):
        print(f"{'─' * 50}")
        print(f"  [{i}/{len(QUESTIONS)}] {question}")
        print(f"{'─' * 50}")

        run_details = client.get_run_details(question)
        diag = build_diagnostic(agent_meta, agent_config, schema, question, run_details)

        # Save
        safe_q = "".join(c if c.isalnum() or c in " _-" else "" for c in question)[:40].strip().replace(" ", "_")
        filename = f"full_diag_{safe_q}_{ts}.json"
        filepath = os.path.join(OUTPUT_DIR, filename)
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(diag, f, indent=2, default=str, ensure_ascii=False)

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

        results.append({
            "question": question,
            "status": run_details.get("run_status"),
            "answer": answer[:200],
            "tools": tools,
            "file": filename,
            "duration": diag["timing"]["total_seconds"],
        })

        print(f"  Status  : {run_details.get('run_status')}")
        print(f"  Tools   : {' → '.join(tools)}")
        print(f"  Duration: {diag['timing']['total_seconds']}s")
        print(f"  Answer  : {answer[:120]}...")
        print(f"  File    : {filename}")

    # ── Summary ──
    print(f"\n{'=' * 60}")
    print("  BATCH SUMMARY")
    print(f"{'=' * 60}")
    for r in results:
        icon = "✅" if r["status"] == "completed" else "❌"
        print(f"  {icon} [{r['duration']}s] {r['question']}")
        print(f"    → {r['answer'][:100]}...")
        print(f"    → {r['file']}")

    # Save summary
    summary_file = os.path.join(OUTPUT_DIR, f"batch_summary_{ts}.json")
    with open(summary_file, "w", encoding="utf-8") as f:
        json.dump({
            "timestamp": ts,
            "agent_id": AGENT_ID,
            "model_id": SEMANTIC_MODEL_ID,
            "schema_stats": schema.get("stats"),
            "results": results,
        }, f, indent=2, default=str, ensure_ascii=False)

    print(f"\n  Summary : {os.path.basename(summary_file)}")
    print(f"  Output  : {OUTPUT_DIR}")
    print(f"  Total   : {len(results)} diagnostic(s)")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
