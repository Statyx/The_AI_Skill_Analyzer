"""Auto-generate starter test questions from a semantic model snapshot.

Scans the cached schema.json (tables, columns, measures) and produces
a questions_generated.yaml file in the profile directory. This gives
users a head start when writing test cases for a new profile.

Usage:
    python -m analyzer -p my_profile generate
    python -m analyzer -p my_profile generate --out questions.yaml  # overwrite
"""

import json
from pathlib import Path

from .config import ROOT
from .snapshot import snapshot_path


# ── Question templates keyed by pattern ───────────────────────

def _measure_questions(measure_name, table_name, tags):
    """Generate questions for a single measure."""
    clean = measure_name.replace("_", " ").strip()
    return [
        {
            "question": f"what is the {clean}",
            "expected": None,
            "match_type": "contains",
            "tags": tags,
        },
    ]


def _ranking_questions(column_name, measure_name, table_name, tags):
    """Generate a top-N ranking question combining a column and a measure."""
    col_clean = column_name.replace("_", " ").strip()
    meas_clean = measure_name.replace("_", " ").strip()
    return [
        {
            "question": f"top 5 {col_clean} by {meas_clean}",
            "expected": None,
            "match_type": "contains",
            "tags": tags + ["ranking"],
        },
    ]


def _overview_questions(table_name, tags):
    """Generate overview/summary questions for a table."""
    clean = table_name.replace("_", " ").strip()
    return [
        {
            "question": f"show a summary of {clean}",
            "expected": None,
            "match_type": "contains",
            "tags": tags + ["overview"],
        },
    ]


# ── Main generator ────────────────────────────────────────────

def generate_questions(cfg, max_per_table=5, max_total=40):
    """Read cached schema and produce starter test cases.

    Returns (test_cases_list, stats_dict).
    """
    sp = snapshot_path(cfg)
    schema_file = sp / "schema.json"
    if not schema_file.exists():
        raise FileNotFoundError(
            f"No schema snapshot found at {schema_file}. "
            "Run 'python -m analyzer -p <profile> snapshot' first."
        )

    with open(schema_file, "r", encoding="utf-8") as f:
        schema = json.load(f)

    elements = schema.get("elements", [])
    test_cases = []

    for elem in elements:
        table_name = elem.get("display_name", "")
        if not table_name:
            continue

        children = elem.get("children", [])
        measures = [c for c in children if c.get("type") == "semantic_model.measure"]
        columns = [c for c in children
                   if c.get("type") == "semantic_model.column" and not c.get("is_hidden")]

        base_tag = table_name.lower().replace(" ", "_")[:20]
        table_cases = []

        # 1) One overview question per table with measures
        if measures:
            table_cases.extend(_overview_questions(table_name, [base_tag]))

        # 2) One KPI question per measure (up to limit)
        for meas in measures[:max_per_table]:
            table_cases.extend(_measure_questions(
                meas["display_name"], table_name, [base_tag, "kpi"]
            ))

        # 3) One ranking question per (first visible column × first measure)
        if columns and measures:
            table_cases.extend(_ranking_questions(
                columns[0]["display_name"], measures[0]["display_name"],
                table_name, [base_tag]
            ))

        test_cases.extend(table_cases)

    # Trim to max_total
    test_cases = test_cases[:max_total]

    stats = {
        "tables_scanned": len(elements),
        "questions_generated": len(test_cases),
        "max_total": max_total,
    }
    return test_cases, stats


def write_questions_yaml(test_cases, output_path, cfg, stats):
    """Write generated test cases to a YAML file."""
    import yaml

    header = (
        f"# ── Auto-generated test cases for {cfg.get('profile_name', 'unknown')} ──\n"
        f"# Model: {cfg.get('semantic_model_name', '?')}\n"
        f"# Tables scanned: {stats['tables_scanned']}\n"
        f"# Questions generated: {stats['questions_generated']}\n"
        f"#\n"
        f"# Review and fill in expected answers, then adjust match_type/tolerance.\n"
        f"# match_type options: contains, numeric, numeric_pct, exact, regex, any_of, list_contains\n"
        f"#\n\n"
    )

    content = {"test_cases": test_cases}
    yaml_str = yaml.dump(content, default_flow_style=False, allow_unicode=True, sort_keys=False)

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(header)
        f.write(yaml_str)

    return output_path
