"""
Deploy updated Marketing360 Data Agent with:
  - Updated instructions (5 new DAX best-practice rules from BPA analysis)
  - 3 new few-shot examples (churn rate, top campaigns, churn risk by segment)
  - Existing datasource + elements preserved from current definition

Usage:
    python deploy_marketing360_agent.py
"""
import base64
import json
import os
import subprocess
import sys
import time

import requests

# ═══════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════
API = "https://api.fabric.microsoft.com/v1"
WORKSPACE_ID = "5fa6b81d-fabe-4363-ad3d-b09ef82d16f2"
AGENT_ID = "e92e5867-213a-4a7d-8fac-af1711046527"
AGENT_NAME = "Marketing360_Agent"

SEMANTIC_MODEL_ID = "3d00aeaa-91b9-4567-9166-fa3fc8249e6f"
SEMANTIC_MODEL_NAME = "Marketing360_Model"
DATASOURCE_FOLDER = f"semantic-model-{SEMANTIC_MODEL_NAME}"

PROFILE_DIR = os.path.dirname(os.path.abspath(__file__))
if os.path.basename(PROFILE_DIR) == "scripts":
    PROFILE_DIR = os.path.join(os.path.dirname(PROFILE_DIR), "profiles", "marketing360")
INSTRUCTIONS_PATH = os.path.join(PROFILE_DIR, "profiles", "marketing360", "instructions.md")
FEWSHOTS_PATH = os.path.join(PROFILE_DIR, "profiles", "marketing360", "fewshots.json")
RAW_DEF_PATH = os.path.join(PROFILE_DIR, "_agent_def_raw.json")


# ═══════════════════════════════════════════════════════════════
# AUTH
# ═══════════════════════════════════════════════════════════════
def get_token():
    result = subprocess.run(
        "az account get-access-token --resource https://api.fabric.microsoft.com --query accessToken -o tsv",
        capture_output=True, text=True, shell=True,
    )
    token = result.stdout.strip()
    if not token:
        print("ERROR: az login required")
        sys.exit(1)
    return token


def headers(token):
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def b64(obj):
    return base64.b64encode(json.dumps(obj, ensure_ascii=False).encode("utf-8")).decode("ascii")


def poll(token, resp, max_wait=120):
    h = headers(token)
    op_id = resp.headers.get("x-ms-operation-id")
    retry = int(resp.headers.get("Retry-After", "5"))
    print(f"  Async: {op_id}")
    for _ in range(max_wait // retry):
        time.sleep(retry)
        op = requests.get(f"{API}/operations/{op_id}", headers=h).json()
        status = op.get("status", "?")
        print(f"  ... {status}")
        if status == "Succeeded":
            return True
        if status in ("Failed", "Cancelled"):
            print(f"  ERROR: {json.dumps(op.get('error', {}), indent=2)}")
            return False
    print("  TIMEOUT")
    return False


# ═══════════════════════════════════════════════════════════════
# LOAD FILES
# ═══════════════════════════════════════════════════════════════
def load_instructions():
    # Try multiple paths
    for path in [INSTRUCTIONS_PATH,
                 os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "profiles", "marketing360", "instructions.md"),
                 os.path.join(os.path.dirname(os.path.abspath(__file__)), "profiles", "marketing360", "instructions.md")]:
        p = os.path.normpath(path)
        if os.path.exists(p):
            with open(p, "r", encoding="utf-8") as f:
                text = f.read()
            print(f"  Instructions: {len(text)} chars from {p}")
            return text
    print("ERROR: instructions.md not found")
    sys.exit(1)


def load_fewshots():
    for path in [FEWSHOTS_PATH,
                 os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "profiles", "marketing360", "fewshots.json"),
                 os.path.join(os.path.dirname(os.path.abspath(__file__)), "profiles", "marketing360", "fewshots.json")]:
        p = os.path.normpath(path)
        if os.path.exists(p):
            with open(p, "r", encoding="utf-8") as f:
                data = json.load(f)
            shots = data.get("fewShots", [])
            print(f"  Fewshots: {len(shots)} examples from {p}")
            return data
    print("  WARNING: fewshots.json not found, using empty")
    return {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/dataAgent/definition/fewShots/1.0.0/schema.json",
        "fewShots": [],
    }


def load_existing_datasource():
    """Extract the existing datasource.json (with elements) from _agent_def_raw.json."""
    for path in [RAW_DEF_PATH,
                 os.path.join(os.path.dirname(os.path.abspath(__file__)), "_agent_def_raw.json"),
                 os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "_agent_def_raw.json")]:
        p = os.path.normpath(path)
        if os.path.exists(p):
            with open(p, "r", encoding="utf-8") as f:
                parts = json.load(f)
            # Find draft datasource.json
            for part in parts:
                if "draft" in part["path"] and "datasource.json" in part["path"]:
                    ds = json.loads(base64.b64decode(part["payload"]).decode("utf-8"))
                    elem_count = len(ds.get("elements", []))
                    child_count = sum(len(e.get("children", [])) for e in ds.get("elements", []))
                    print(f"  Datasource: {elem_count} tables, {child_count} cols/measures from existing def")
                    return ds
    # Fallback: build minimal datasource without elements
    print("  WARNING: No existing datasource found, using minimal config")
    return {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/dataAgent/definition/dataSource/1.0.0/schema.json",
        "artifactId": SEMANTIC_MODEL_ID,
        "workspaceId": WORKSPACE_ID,
        "displayName": SEMANTIC_MODEL_NAME,
        "type": "semantic_model",
    }


# ═══════════════════════════════════════════════════════════════
# BUILD DEFINITION
# ═══════════════════════════════════════════════════════════════
def build_parts(instructions_text, fewshots, datasource):
    data_agent_json = {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/dataAgent/definition/dataAgent/2.1.0/schema.json"
    }
    stage_config = {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/dataAgent/definition/stageConfiguration/1.0.0/schema.json",
        "aiInstructions": instructions_text,
    }
    publish_info = {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/dataAgent/definition/publishInfo/1.0.0/schema.json",
        "description": f"{AGENT_NAME} — Updated {time.strftime('%Y-%m-%d')} with BPA fixes",
    }

    # Add new measures to datasource elements if not already present
    new_measures = [
        {"display_name": "Return Rate 2025", "type": "semantic_model.measure"},
        {"display_name": "Return Rate vs Benchmark", "type": "semantic_model.measure"},
        {"display_name": "Attributed Revenue", "type": "semantic_model.measure"},
    ]
    if "elements" in datasource:
        for elem in datasource["elements"]:
            existing_names = {c["display_name"] for c in elem.get("children", [])}
            # Add new measures to their host tables
            if elem["display_name"] == "returns":
                for m in new_measures[:2]:  # Return Rate 2025 + vs Benchmark
                    if m["display_name"] not in existing_names:
                        elem["children"].append({
                            "id": None, "display_name": m["display_name"],
                            "type": "semantic_model.measure",
                            "is_selected": True, "description": None, "children": [],
                        })
            if elem["display_name"] == "orders":
                for m in new_measures[2:]:  # Attributed Revenue
                    if m["display_name"] not in existing_names:
                        elem["children"].append({
                            "id": None, "display_name": m["display_name"],
                            "type": "semantic_model.measure",
                            "is_selected": True, "description": None, "children": [],
                        })

    sc = b64(stage_config)
    ds = b64(datasource)
    fs = b64(fewshots)

    parts = [
        {"path": "Files/Config/data_agent.json", "payload": b64(data_agent_json), "payloadType": "InlineBase64"},
        {"path": "Files/Config/publish_info.json", "payload": b64(publish_info), "payloadType": "InlineBase64"},
        # Draft
        {"path": "Files/Config/draft/stage_config.json", "payload": sc, "payloadType": "InlineBase64"},
        {"path": f"Files/Config/draft/{DATASOURCE_FOLDER}/datasource.json", "payload": ds, "payloadType": "InlineBase64"},
        {"path": f"Files/Config/draft/{DATASOURCE_FOLDER}/fewshots.json", "payload": fs, "payloadType": "InlineBase64"},
        # Published
        {"path": "Files/Config/published/stage_config.json", "payload": sc, "payloadType": "InlineBase64"},
        {"path": f"Files/Config/published/{DATASOURCE_FOLDER}/datasource.json", "payload": ds, "payloadType": "InlineBase64"},
        {"path": f"Files/Config/published/{DATASOURCE_FOLDER}/fewshots.json", "payload": fs, "payloadType": "InlineBase64"},
    ]
    return parts


# ═══════════════════════════════════════════════════════════════
# DEPLOY
# ═══════════════════════════════════════════════════════════════
def main():
    print("=" * 60)
    print("  Marketing360 Data Agent — Update Deployment")
    print("=" * 60)

    token = get_token()
    h = headers(token)

    # Load files
    print("\n📄 Loading files...")
    instructions = load_instructions()
    fewshots = load_fewshots()
    datasource = load_existing_datasource()

    # Build parts
    print("\n🔨 Building definition...")
    parts = build_parts(instructions, fewshots, datasource)
    print(f"  Parts: {len(parts)}")

    # Update agent
    print(f"\n🚀 Updating agent {AGENT_NAME} ({AGENT_ID})...")
    resp = requests.post(
        f"{API}/workspaces/{WORKSPACE_ID}/items/{AGENT_ID}/updateDefinition",
        headers=h,
        json={"definition": {"parts": parts}},
        timeout=60,
    )

    if resp.status_code == 200:
        print("  ✅ Updated (sync)")
    elif resp.status_code == 202:
        ok = poll(token, resp)
        if not ok:
            sys.exit(1)
        print("  ✅ Updated (async)")
    else:
        print(f"  ❌ FAILED ({resp.status_code}): {resp.text[:400]}")
        sys.exit(1)

    # Summary
    print(f"\n{'=' * 60}")
    print(f"  ✅ Agent updated & published: {AGENT_NAME}")
    print(f"  ID: {AGENT_ID}")
    print(f"  Changes:")
    print(f"    - 5 new DAX best-practice rules in instructions")
    print(f"    - 3 new few-shot examples (churn rate, top campaigns, churn risk)")
    print(f"    - 3 new measures added to semantic model (via MCP)")
    print(f"      [Return Rate 2025], [Return Rate vs Benchmark], [Attributed Revenue]")
    print(f"    - Updated measure list in instructions")
    print(f"\n  Portal: https://app.fabric.microsoft.com/groups/{WORKSPACE_ID}/dataAgents/{AGENT_ID}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
