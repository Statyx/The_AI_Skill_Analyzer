import json
s = json.load(open("snapshots/finance_controller/schema.json"))
st = s["stats"]
print(f"Tables: {st['tables']}, Cols: {st['columns']}, Measures: {st['measures']}")
dc = st.get("description_coverage", {})
print(f"Description coverage: T={dc.get('tables','?')}, C={dc.get('columns','?')}, M={dc.get('measures','?')}")
# Check one table
for t in s.get("elements", []):
    if t["display_name"] == "dim_chart_of_accounts":
        print(f"\ndim_chart_of_accounts desc: {t.get('description','NONE')[:80]}")
        for c in t.get("children", [])[:3]:
            print(f"  {c['display_name']}: {c.get('description','NONE')[:60]}")
        break
