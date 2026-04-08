"""Quick test: ask a minimal question and inspect tool calls."""
import sys, os, json, time
sys.path.insert(0, os.path.join(os.environ.get("TEMP", "/tmp"), "fabric_data_agent_client"))
from fabric_data_agent_client import FabricDataAgentClient
from azure.identity import AzureCliCredential

cred = AzureCliCredential()
url = "https://api.fabric.microsoft.com/v1/workspaces/acf3556a-b23c-4ea1-b706-4ea9c9a1c181/dataAgents/b90bd88c-0714-4d4a-8c99-05da816dc125/aiassistant/openai"
client = FabricDataAgentClient.__new__(FabricDataAgentClient)
client.tenant_id = "4b2583b7-f29d-43dd-a97c-a37434ec71ee"
client.data_agent_url = url
client.stage = "production"
client.api_version = FabricDataAgentClient.DEFAULT_API_VERSION
client.credential = cred
client.token = cred.get_token("https://api.fabric.microsoft.com/.default")

result = client.get_raw_run_response("Combien de lignes de benchmark ?")
print("status:", result["run_status"])
print("steps:", len(result["run_steps"]["data"]))
for s in result["run_steps"]["data"]:
    sd = s.get("step_details", {})
    tc = sd.get("tool_calls", [])
    if tc:
        for t in tc:
            print(f"  step: {s['type']} | tool={t['function']['name']}")
    else:
        print(f"  step: {s['type']} | {sd}")
msgs = result["messages"]["data"]
if msgs:
    txt = msgs[0].get("content", [{}])[0].get("text", {}).get("value", "")
    print("answer:", txt[:300])
