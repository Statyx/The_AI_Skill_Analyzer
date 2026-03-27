"""Quick integration test for the action plan rendering."""
from analyzer.reporting import _render_action_plan, _build_action_plan_json
import json

fixes = [
    (1, "INSTRUCTION", "Add instruction: Always use DIVIDE instead of /"),
    (1, "FEWSHOT", 'Add a fewshot for "total revenue by region" with clean DAX'),
    (2, "MEASURE", "Create a [Gross Margin Pct] measure in semantic model"),
    (2, "INSTRUCTION", "[BPA-PERF-005] Add instruction: Always use DIVIDE(num, denom, 0)"),
    (3, "DESCRIPTION", "Add descriptions to filter columns with valid enum values"),
    (3, "FEWSHOT", 'Add a fewshot for "DSO by customer" with correct filters'),
    (4, "EXPECTED", "Agent returned 1234567. Update expected from 1200000"),
    (5, "MEASURE", "Agent DEFINED inline measure [YTD Revenue]. Make it permanent"),
    (5, "INSTRUCTION", "[BPA-READ-001] Use VAR/RETURN for complex calculations"),
    (6, "DATA", "Verify the semantic model is accessible and has read permissions"),
    (7, "DESCRIPTION", "Add descriptions to filter columns with valid enum values"),
    (7, "INSTRUCTION", "[BPA-PERF-001] Use REMOVEFILTERS() instead of FILTER(ALL())"),
    (8, "INSTRUCTION", "[BPA-CORR-004] DAX uses single = not =="),
]

print("=== TERMINAL REPORT ===")
_render_action_plan(fixes, print)

print("\n\n=== JSON EXPORT ===")
plan = _build_action_plan_json(fixes)
print(json.dumps(plan, indent=2))
