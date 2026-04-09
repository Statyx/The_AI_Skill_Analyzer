import json, pathlib

p = pathlib.Path(r"c:\Users\cdroinat\OneDrive - Microsoft\1 - Microsoft\01 - Architecture\-- 004 - Demo\02 - Fabric Démo\The_AI_Skill_Analyzer\runs\cce_validation\20260409_080610\batch_summary.json")
d = json.loads(p.read_text(encoding="utf-8"))

print(f"Score: {d.get('score', '?')}")
print(f"Total: {len(d['results'])} questions")
print()

pass_count = 0
fail_count = 0
error_count = 0

for r in d["results"]:
    idx = r.get("index", "?")
    status = r.get("status", "?")
    q = r.get("question", "")[:55]
    err = r.get("error", "")
    grade = r.get("grading", {}) or {}
    verdict = grade.get("verdict", "") if isinstance(grade, dict) else ""
    
    if "pass" in verdict.lower():
        icon = "+"
        pass_count += 1
    elif err:
        icon = "!"
        error_count += 1
    else:
        icon = "X"
        fail_count += 1
    
    err_short = ""
    if err:
        if "404" in err:
            err_short = " [404]"
        elif "400" in err:
            err_short = " [400]"
        elif "timeout" in err.lower():
            err_short = " [TIMEOUT]"
        else:
            err_short = f" [{err[:30]}]"
    
    print(f"  {icon} Q{idx:2d}  {q}{err_short}")

print(f"\nSummary: {pass_count} PASS, {fail_count} FAIL, {error_count} ERROR")
print(f"Real score (excl errors): {pass_count}/{pass_count + fail_count} = {100*pass_count/(pass_count+fail_count):.0f}%")
