#!/usr/bin/env python3
"""
Query the Marketing360 Data Agent and save full diagnostic (run_details).
Uses the fabric_data_agent_client from GitHub.
"""

import sys
import json
import time
import os

# Add the cloned client SDK to path
sys.path.insert(0, os.path.join(os.environ["TEMP"], "fabric_data_agent_client"))

from fabric_data_agent_client import FabricDataAgentClient

# --- Configuration ---
TENANT_ID = "92701a21-ddea-4028-ba85-4c1f91fab881"  # Fabric tenant
WORKSPACE_ID = "5fa6b81d-fabe-4363-ad3d-b09ef82d16f2"
AGENT_ID = "e92e5867-213a-4a7d-8fac-af1711046527"
DATA_AGENT_URL = f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/aiskills/{AGENT_ID}/aiassistant/openai"

QUESTION = "what is the churn rate"
OUTPUT_FILE = os.path.join(os.path.dirname(__file__), f"diagnostic_{AGENT_ID}_{int(time.time())}.json")

def main():
    print(f"Data Agent URL: {DATA_AGENT_URL}")
    print(f"Question: {QUESTION}")
    print()

    # Initialize client (opens browser for auth)
    client = FabricDataAgentClient(
        tenant_id=TENANT_ID,
        data_agent_url=DATA_AGENT_URL
    )

    # Get full run details (includes steps, messages, tool calls)
    run_details = client.get_run_details(QUESTION)

    # Save full diagnostic to JSON
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(run_details, f, indent=2, default=str, ensure_ascii=False)

    print(f"\nDiagnostic saved to: {OUTPUT_FILE}")
    print(f"Run status: {run_details.get('run_status')}")

    # Print the answer
    messages = run_details.get("messages", {}).get("data", [])
    for msg in messages:
        if msg.get("role") == "assistant":
            content = msg.get("content", [])
            if content and isinstance(content[0], dict):
                text = content[0].get("text", {})
                if isinstance(text, dict):
                    print(f"\nAnswer:\n{text.get('value', '')}")
                else:
                    print(f"\nAnswer:\n{text}")

if __name__ == "__main__":
    main()
