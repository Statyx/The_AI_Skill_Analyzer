import json

data = json.load(open("runs/cce_validation/20260409_082828/batch_summary.json", encoding="utf-8"))
for r in data["results"]:
    g = r.get("grading", {})
    verdict = g.get("verdict", "?")
    if verdict != "pass":
        idx = r["index"]
        ans = (r.get("answer") or "")[:300]
        exp = g.get("expected", "?")
        reason = g.get("reason", "?")
        print(f"Q{idx}: {reason}")
        print(f"  expected: {exp}")
        print(f"  answer: {ans}")
        print()
