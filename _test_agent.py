"""Quick test of the CCE_Advisor Data Agent."""
import yaml
from analyzer.auth import FabricSession
from analyzer.config import load_global_config, load_profile

cfg = load_profile("cce_validation", load_global_config())
ws = cfg["workspace_id"]
agent = cfg["agent_id"]
stage = cfg.get("stage", "production")
tenant = cfg["tenant_id"]
url = cfg["data_agent_url"]
session_cfg = {"tenant_id": tenant, "data_agent_url": url, "stage": stage}
session = FabricSession(session_cfg)

question = "Combien de lignes de benchmark avons-nous ?"
print(f"\n>>> Question: {question}")
result = session.client.get_raw_run_response(question, timeout=120)

print(f"\nRun status: {result['run_status']}")
print(f"Messages count: {len(result['messages']['data'])}")
print(f"Steps count: {len(result['run_steps']['data'])}")

for m in result["messages"]["data"]:
    for c in m.get("content", []):
        val = c.get("text", {}).get("value", "")
        print(f"\n--- ANSWER ---\n{val[:800]}")

for i, s in enumerate(result["run_steps"]["data"]):
    sd = s.get("step_details", {})
    print(f"\nStep {i}: type={s.get('type')} status={s.get('status')}")
    if "tool_calls" in sd:
        for tc in sd["tool_calls"]:
            fn = tc.get("function", {})
            print(f"  Tool: {fn.get('name')}")
            print(f"  Args: {str(fn.get('arguments', ''))[:300]}")
            output = fn.get("output")
            if output:
                print(f"  Output: {str(output)[:300]}")
