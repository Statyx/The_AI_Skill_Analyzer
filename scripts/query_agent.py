#!/usr/bin/env python3
"""
Query the Marketing360 Data Agent and save full diagnostic (run_details).
Uses the fabric_data_agent_client from GitHub.
"""

import sys
import json
import time
import os
import pathlib
import yaml

# Add the cloned client SDK to path
sys.path.insert(0, os.path.join(os.environ["TEMP"], "fabric_data_agent_client"))

from fabric_data_agent_client import FabricDataAgentClient

# --- Configuration ---
# Load IDs from local config.yaml (gitignored, see config.yaml.example)
_cfg = yaml.safe_load((pathlib.Path(__file__).resolve().parents[1] / "config.yaml").read_text(encoding="utf-8"))
TENANT_ID = _cfg["tenant_id"]
WORKSPACE_ID = _cfg["workspace_id"]
AGENT_ID = _cfg["agent_id"]
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
