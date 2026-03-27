import json, base64

with open('_agent_def_raw.json', 'r') as f:
    parts = json.load(f)

payload = json.loads(base64.b64decode(parts[2]['payload']).decode('utf-8'))

for elem in payload.get('elements', []):
    tbl = elem.get('display_name', '?')
    cols = [c['display_name'] for c in elem.get('children', []) if c.get('type') == 'semantic_model.column']
    measures = [c['display_name'] for c in elem.get('children', []) if c.get('type') == 'semantic_model.measure']
    m_str = ', '.join(measures) if measures else '-'
    print(f"Table: {tbl}  | Cols: {len(cols)} | Measures: {m_str}")
