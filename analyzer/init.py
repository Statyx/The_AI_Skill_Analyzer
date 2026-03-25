"""Scaffold a new profile with template files."""

import sys
from pathlib import Path

from .config import ROOT

PROFILE_TEMPLATE = """\
# ── Profile: {name} ─────────────────────────────────────
# Connection details for the {name} Data Agent.
# These override global settings from config.yaml.
#
# HOW TO FIND THESE IDs:
#
# 1. workspace_id
#    Fabric portal → open your workspace → look at the URL:
#    https://app.fabric.microsoft.com/groups/<workspace_id>/...
#
# 2. agent_id (= AI Skill / Data Agent)
#    Fabric portal → open the Data Agent item → URL:
#    https://app.fabric.microsoft.com/groups/<ws>/aiskills/<agent_id>
#    OR: Fabric REST → GET /v1/workspaces/<ws>/items?type=AISkill
#
# 3. semantic_model_id
#    Fabric portal → open the semantic model → Settings → look at the URL
#    OR: Fabric REST → GET /v1/workspaces/<ws>/items?type=SemanticModel
#
# 4. semantic_model_name
#    The display name of the semantic model (human-readable, used in reports).

workspace_id: "REPLACE_ME"
agent_id: "REPLACE_ME"
semantic_model_id: "REPLACE_ME"
semantic_model_name: "{name}"

# Optional overrides (uncomment to override config.yaml):
# stage: "sandbox"           # or "production"
# max_workers: 4             # parallel threads for this profile
# snapshot_ttl_hours: 24     # cache TTL for this profile
"""

QUESTIONS_TEMPLATE = """\
# ── Test Cases for {name} ──────────────────────────────────
#
# Start with generic questions, then refine expected answers after your first run.
#
# Workflow:
#   1. Run with no expected values:  python -m analyzer -p {slug} run
#   2. Review answers:               python -m analyzer -p {slug} analyze --latest
#   3. Fill in expected values below for answers you trust
#   4. Re-run to get grading:        python -m analyzer -p {slug} run
#
# match_type options: contains, numeric, exact, regex, any_of
# expected: null (~) means "no grading, manual review"
# tolerance: for numeric only (absolute value)
# tags: labels for --tag filtering and report grouping

test_cases:

  # ── KPI / Measures ──
  # Start with high-level questions the agent SHOULD answer well

  - question: "what are the main KPIs"
    expected: ~
    match_type: "contains"
    tags: ["overview"]

  - question: "what is the total revenue"
    expected: ~
    match_type: "numeric"
    tolerance: 100
    tags: ["kpi", "revenue"]

  # ── Counts ──
  # These are easy to verify and catch relationship/filter bugs

  - question: "how many customers do we have"
    expected: ~
    match_type: "numeric"
    tolerance: 10
    tags: ["counts"]

  # ── Time Intelligence ──
  # Tests whether the agent applies date filters correctly

  - question: "what is the revenue trend over time"
    expected: ~
    match_type: "contains"
    tags: ["time_intelligence"]

  # ── Rankings ──
  # Tests table generation and sorting

  - question: "top 5 products by revenue"
    expected: ~
    match_type: "contains"
    tags: ["ranking"]

  # Add your own questions below:
  # - question: "your question here"
  #   expected: "expected answer"
  #   match_type: "numeric"
  #   tolerance: 0
  #   tags: ["your_tag"]
"""


def scaffold_profile(name):
    """Create a new profile directory with template files."""
    slug = name.lower().replace(" ", "_").replace("-", "_")
    profile_dir = ROOT / "profiles" / slug

    if profile_dir.exists():
        print(f"ERROR: Profile '{slug}' already exists at {profile_dir}")
        sys.exit(1)

    profile_dir.mkdir(parents=True, exist_ok=True)

    profile_yaml = profile_dir / "profile.yaml"
    profile_yaml.write_text(
        PROFILE_TEMPLATE.format(name=name),
        encoding="utf-8",
    )

    questions_yaml = profile_dir / "questions.yaml"
    questions_yaml.write_text(
        QUESTIONS_TEMPLATE.format(name=name, slug=slug),
        encoding="utf-8",
    )

    W = 60
    print(f"\n{'=' * W}")
    print(f"  NEW PROFILE: {slug}")
    print(f"{'=' * W}")
    print(f"\n  Created:")
    print(f"    {profile_yaml.relative_to(ROOT)}")
    print(f"    {questions_yaml.relative_to(ROOT)}")
    print(f"\n  Next steps:")
    print(f"    1. Edit profile.yaml → fill in your 4 IDs")
    print(f"       (see comments for where to find them)")
    print(f"    2. Edit questions.yaml → customize test questions")
    print(f"    3. Validate: python -m analyzer -p {slug} validate")
    print(f"    4. First run: python -m analyzer -p {slug} run")
    print(f"    5. Review:    python -m analyzer -p {slug} analyze --latest")
    print(f"    6. Fill in expected answers in questions.yaml")
    print(f"    7. Re-run:    python -m analyzer -p {slug} run")
    print(f"\n{'=' * W}\n")
