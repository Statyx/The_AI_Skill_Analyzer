"""Temporary script to discover workspace + items for Finance_Controller."""
import sys, os, yaml
sys.path.insert(0, '.')
from analyzer.auth import _get_persistent_credential, FABRIC_SCOPE
from analyzer.api import fabric_get

cfg = yaml.safe_load(open('config.yaml'))
cred = _get_persistent_credential(cfg['tenant_id'])
token = cred.get_token(FABRIC_SCOPE)

class SimpleSession:
    @property
    def headers(self):
        return {'Authorization': f'Bearer {token.token}'}

s = SimpleSession()

# 1. Find workspace
print("=== WORKSPACES (matching 'finance' or 'cdr') ===")
ws = fabric_get(s, '/workspaces')
target_ws = None
for w in ws.get('value', []):
    name_lower = w['displayName'].lower()
    if 'finance' in name_lower or 'cdr' in name_lower:
        print(f"  {w['displayName']}  ->  {w['id']}")
        if 'finance' in name_lower and 'cdr' in name_lower:
            target_ws = w

if not target_ws:
    print("No matching workspace found!")
    sys.exit(1)

ws_id = target_ws['id']
print(f"\nTarget workspace: {target_ws['displayName']} ({ws_id})")

# 2. List items in workspace
print("\n=== ITEMS IN WORKSPACE ===")
items = fabric_get(s, f'/workspaces/{ws_id}/items')
for item in sorted(items.get('value', []), key=lambda x: x.get('type', '')):
    print(f"  [{item.get('type', '?'):25s}] {item['displayName']:40s} id={item['id']}")
