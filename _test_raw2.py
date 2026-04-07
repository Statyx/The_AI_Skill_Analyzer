import sys, time, os, json, traceback

sys.path.insert(0, os.path.join(os.environ.get('TEMP', '/tmp'), 'fabric_data_agent_client'))
from fabric_data_agent_client import FabricDataAgentClient
from azure.identity import AzureCliCredential

LOG = open("_test_raw2_log.txt", "w", encoding="utf-8")
def p(msg):
    LOG.write(msg + "\n")
    LOG.flush()

cred = AzureCliCredential(tenant_id='92701a21-ddea-4028-ba85-4c1f91fab881')
token = cred.get_token('https://api.fabric.microsoft.com/.default')
p('Token OK')

client = FabricDataAgentClient.__new__(FabricDataAgentClient)
client.tenant_id = '92701a21-ddea-4028-ba85-4c1f91fab881'
client.data_agent_url = 'https://api.fabric.microsoft.com/v1/workspaces/a1dce412-7b2d-4406-838d-61c94cad8acf/dataAgents/9e866649-625e-4f52-95af-cb688be9455d/aiassistant/openai'
client.stage = 'sandbox'
client.api_version = '2024-02-15-preview'
client.credential = cred
client.token = token

p('Testing...')
t0 = time.time()
try:
    result = client.get_raw_run_response('How many sensor readings do we have?', timeout=120)
    dt = time.time() - t0
    p(f'Done in {dt:.1f}s, status={result["run_status"]}')
    p(f'Messages: {len(result["messages"]["data"])}')
    p(f'Steps: {len(result["run_steps"]["data"])}')
    for msg in result['messages']['data']:
        if msg['role'] == 'assistant':
            for c in msg['content']:
                p(f'Answer: {c.get("text", {}).get("value", "?")[:300]}')
    for step in result['run_steps']['data']:
        for tc in (step.get('step_details') or {}).get('tool_calls', []):
            p(f'Tool: {tc.get("function", {}).get("name", "?")}')
    p(json.dumps(result, indent=2, default=str)[:3000])
except Exception as e:
    p(f'ERROR after {time.time()-t0:.1f}s: {e}')
    p(traceback.format_exc())

p('DONE')
LOG.close()
print("See _test_raw2_log.txt")
