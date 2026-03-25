"""Snapshot management — cache agent config + schema to disk.

Snapshots avoid hitting Fabric API on every run. They are scoped per profile
and refreshed when older than snapshot_ttl_hours.
"""

import json
import base64
from pathlib import Path
from datetime import datetime, timezone, timedelta

from .config import ROOT
from .api import fabric_get, fabric_post
from .tmdl import build_schema, empty_schema


def snapshot_path(cfg):
    """Snapshot directory: snapshots/<profile_name>/."""
    return ROOT / "snapshots" / cfg["profile_name"]


def snapshot_is_fresh(cfg):
    meta_file = snapshot_path(cfg) / "snapshot_meta.json"
    if not meta_file.exists():
        return False
    ttl = cfg.get("snapshot_ttl_hours", 24)
    if ttl <= 0:
        return False
    with open(meta_file, "r") as f:
        meta = json.load(f)
    taken_at = datetime.fromisoformat(meta["taken_at"])
    return datetime.now(timezone.utc) - taken_at < timedelta(hours=ttl)


def load_snapshot(cfg):
    sp = snapshot_path(cfg)
    with open(sp / "agent_config.json", "r", encoding="utf-8") as f:
        agent_data = json.load(f)
    with open(sp / "schema.json", "r", encoding="utf-8") as f:
        schema = json.load(f)
    return agent_data, schema


def take_snapshot(session, cfg, force=False):
    """Fetch agent config + TMDL schema from Fabric and cache to disk."""
    sp = snapshot_path(cfg)
    if not force and snapshot_is_fresh(cfg):
        print(f"  Snapshot is fresh (< {cfg['snapshot_ttl_hours']}h). Use --refresh to force.")
        return load_snapshot(cfg)

    sp.mkdir(parents=True, exist_ok=True)
    ws = cfg["workspace_id"]
    agent = cfg["agent_id"]
    model = cfg["semantic_model_id"]

    # Agent config
    print("  Fetching agent config...")
    meta = fabric_get(session, f"/workspaces/{ws}/items/{agent}")
    defn = fabric_post(session, f"/workspaces/{ws}/items/{agent}/getDefinition")
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
    agent_data = {"meta": meta, "config": config or defn}
    with open(sp / "agent_config.json", "w", encoding="utf-8") as f:
        json.dump(agent_data, f, indent=2, default=str, ensure_ascii=False)
    print(f"  -> Agent: {meta.get('displayName', '?')}")

    # Schema (TMDL)
    print("  Fetching semantic model schema (TMDL)...")
    defn = fabric_post(session, f"/workspaces/{ws}/semanticModels/{model}/getDefinition?format=TMDL")
    parts = []
    if defn and isinstance(defn, dict) and "definition" in defn:
        parts = defn["definition"].get("parts", [])
    elif defn and isinstance(defn, dict) and "_error" not in defn:
        parts = defn.get("parts", [])

    schema = build_schema(parts, cfg) if parts else empty_schema(cfg)
    with open(sp / "schema.json", "w", encoding="utf-8") as f:
        json.dump(schema, f, indent=2, default=str, ensure_ascii=False)

    stats = schema.get("stats", {})
    print(f"  -> {stats.get('tables', 0)} tables, {stats.get('columns', 0)} columns, "
          f"{stats.get('measures', 0)} measures, {stats.get('relationships', 0)} relationships")

    # Meta
    snap_meta = {
        "taken_at": datetime.now(timezone.utc).isoformat(),
        "agent_id": agent,
        "model_id": model,
        "agent_name": meta.get("displayName", "?"),
        "model_name": cfg["semantic_model_name"],
        "profile": cfg["profile_name"],
        "stats": stats,
    }
    with open(sp / "snapshot_meta.json", "w", encoding="utf-8") as f:
        json.dump(snap_meta, f, indent=2)

    print(f"  Snapshot saved to: {sp.relative_to(ROOT)}")
    return agent_data, schema
