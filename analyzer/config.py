"""Configuration loading — global config + per-profile config.

Supports two modes:
  1. Profile mode:  config.yaml (global) + profiles/<name>/profile.yaml
  2. Legacy mode:   config.yaml with all IDs (backward-compatible)
"""

import sys
import yaml
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def load_global_config():
    cfg_file = ROOT / "config.yaml"
    if not cfg_file.exists():
        print(f"ERROR: {cfg_file} not found. Copy config.yaml.example and fill in your IDs.")
        sys.exit(1)
    with open(cfg_file, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    cfg.setdefault("stage", "sandbox")
    cfg.setdefault("snapshot_ttl_hours", 24)
    cfg.setdefault("max_workers", 4)
    cfg.setdefault("output_dir", "runs")
    cfg.setdefault("default_profile", None)
    return cfg


def list_profiles():
    profiles_dir = ROOT / "profiles"
    if not profiles_dir.exists():
        return []
    return sorted(
        p.name
        for p in profiles_dir.iterdir()
        if p.is_dir() and (p / "profile.yaml").exists()
    )


def load_profile(profile_name, global_cfg):
    profile_dir = ROOT / "profiles" / profile_name
    profile_file = profile_dir / "profile.yaml"
    if not profile_file.exists():
        avail = list_profiles()
        print(f"ERROR: Profile '{profile_name}' not found at {profile_file}")
        if avail:
            print(f"  Available profiles: {', '.join(avail)}")
        sys.exit(1)
    with open(profile_file, "r", encoding="utf-8") as f:
        profile = yaml.safe_load(f) or {}
    # Merge: profile values override global
    cfg = {**global_cfg, **profile}
    cfg["profile_name"] = profile_name
    cfg["profile_dir"] = str(profile_dir)
    # Build data agent URL
    cfg["data_agent_url"] = (
        f"https://api.fabric.microsoft.com/v1/workspaces/{cfg['workspace_id']}"
        f"/aiskills/{cfg['agent_id']}/aiassistant/openai"
    )
    return cfg


def resolve_config(profile_name=None):
    """Load configuration, resolving the profile (explicit, default, or legacy).

    Priority:
      1. Explicit --profile flag
      2. default_profile in config.yaml
      3. Legacy mode: all IDs directly in config.yaml
    """
    global_cfg = load_global_config()

    profile = profile_name or global_cfg.get("default_profile")

    if profile:
        return load_profile(profile, global_cfg)

    # Legacy mode: agent_id directly in config.yaml
    if "agent_id" in global_cfg:
        cfg = global_cfg
        cfg["profile_name"] = "default"
        cfg["profile_dir"] = str(ROOT / "scripts")  # legacy questions location
        cfg["data_agent_url"] = (
            f"https://api.fabric.microsoft.com/v1/workspaces/{cfg['workspace_id']}"
            f"/aiskills/{cfg['agent_id']}/aiassistant/openai"
        )
        return cfg

    avail = list_profiles()
    print("ERROR: No profile specified and no agent_id in config.yaml.")
    if avail:
        print(f"  Available profiles: {', '.join(avail)}")
        print(f"  Usage: python -m analyzer --profile {avail[0]} run")
    else:
        print("  Create a profile: profiles/<name>/profile.yaml")
    sys.exit(1)


def load_test_cases(cfg, tag_filter=None):
    """Load test cases from the profile's questions.yaml.

    Falls back to scripts/questions.yaml or scripts/questions.txt for legacy.
    """
    profile_dir = Path(cfg["profile_dir"])
    questions_yaml = profile_dir / "questions.yaml"

    # Fallback: legacy questions.yaml in scripts/
    if not questions_yaml.exists():
        questions_yaml = ROOT / "scripts" / "questions.yaml"
    questions_txt = ROOT / "scripts" / "questions.txt"

    cases = []
    if questions_yaml.exists():
        with open(questions_yaml, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
        for tc in data.get("test_cases", []):
            tc.setdefault("match_type", "contains")
            tc.setdefault("expected", None)
            tc.setdefault("tolerance", None)
            tc.setdefault("tags", [])
            cases.append(tc)
    elif questions_txt.exists():
        with open(questions_txt, "r", encoding="utf-8") as f:
            for ln in f:
                ln = ln.strip()
                if ln and not ln.startswith("#"):
                    cases.append({"question": ln, "expected": None,
                                  "match_type": "contains", "tolerance": None, "tags": []})
    else:
        print(f"WARNING: No questions.yaml found in {profile_dir}")
        cases = [{"question": "what is the churn rate", "expected": None,
                  "match_type": "contains", "tolerance": None, "tags": []}]

    if tag_filter:
        cases = [tc for tc in cases if tag_filter in tc.get("tags", [])]
    return cases
