import json, pathlib

p = pathlib.Path(r"runs/finance_controller/20260325_111918/diagnostics/full_diag_what_is_the_total_expenses.json")
d = json.loads(p.read_text(encoding="utf-8"))

# Extract run steps to find the nl2code output
thread = d.get("thread", {})
run_steps = thread.get("run_steps", {}).get("data", [])

for step in run_steps:
    details = step.get("step_details", {})
    tool_calls = details.get("tool_calls", [])
    for tc in tool_calls:
        fn = tc.get("function", {})
        name = fn.get("name", "")
        output = fn.get("output", "")
        if name in ("analyze.database.nl2code", "trace.analyze_semantic_model", "analyze.database.execute"):
            print(f"=== TOOL: {name} ===")
            # Try to parse output as JSON
            try:
                out_obj = json.loads(output) if output.startswith("{") else output
                if isinstance(out_obj, dict):
                    raw = out_obj.get("_raw", output)
                else:
                    raw = output
            except:
                raw = output
            print(raw[:3000])
            print()


