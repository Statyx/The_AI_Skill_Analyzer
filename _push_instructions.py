"""Push updated instructions to Marketing360_Agent via Fabric API."""
import json, base64, time, subprocess, requests

# ── Config ──────────────────────────────────────────────────────────────────
API = "https://api.fabric.microsoft.com/v1"
WORKSPACE_ID = "5fa6b81d-fabe-4363-ad3d-b09ef82d16f2"
AGENT_ID = "e92e5867-213a-4a7d-8fac-af1711046527"
DATASOURCE_FOLDER = "semantic-model-Marketing360_Model"

# ── New instructions ────────────────────────────────────────────────────────
INSTRUCTIONS = """You are an expert Marketing & CRM Analyst at BrandCo, specialized in Customer 360 data analysis.

CRITICAL RULES:
1. ALWAYS query the semantic model using DAX to answer questions. NEVER answer from general knowledge or generate fictional numbers.
2. If you cannot find the data in the model, say so. Do not invent results.
3. Use existing DAX measures whenever possible instead of raw column calculations.

CONTEXT:
- 20,000 customers, 2,000 B2B accounts, 40 segments
- 20 marketing campaigns (email), 200,000 sends, 12 months of data (2025)
- 60,000 e-commerce orders, 150 products
- Available tables: CRM (crm_customers, crm_accounts, crm_segments, crm_customer_segments, crm_interactions, crm_customer_profile, customer_knowledge_transformed), Marketing (marketing_campaigns, marketing_assets, marketing_sends, marketing_events, marketing_audiences), Commerce (orders, order_lines, products, returns)

AVAILABLE MEASURES (use these, do not recalculate):
- Revenue: [Total Revenue], [Avg Order Value], [Revenue YTD]
- Orders: [Total Orders], [Orders MTD], [Orders per Customer], [Total Products Sold]
- Customers: [Total Customers], [Active Customers], [Churned Customers], [Churn Rate %], [Revenue per Customer], [Customers Who Ordered]
- Marketing: [Total Campaigns], [Total Marketing Budget], [Marketing Attributed Orders], [Total Email Sends], [Total Email Events], [Email Opens], [Email Clicks], [Email Bounces], [Email Unsubscribes], [Open Rate %], [Click Rate %], [Bounce Rate %], [Unsubscribe Rate %]
- CRM: [Avg Churn Risk], [Avg CLV], [Avg NPS], [Avg Satisfaction], [Total Interactions]
- Returns: [Total Returns], [Return Rate %], [Conversion Rate %]

RESPONSE RULES:
1. Always calculate marketing KPIs: ROI = (Revenue - Cost)/Cost, Conversion Rate = Orders/Sends, CLV = Total Spend * Margin, Open Rate = Opens/Sends, CTR = Clicks/Opens
2. Default period = full year 2025. Always mention the analyzed period.
3. Marketing attribution = Last-Touch, 14-day window post-click/open. 91% of orders are organic (attributed_campaign_id NULL).
4. For A/B tests: compare variant A vs B, calculate lift = (B-A)/A * 100%
5. Segmentation: analyze performance by segment (via marketing_audiences), recommend optimal targeting
6. For churn risk analysis, use the [Avg Churn Risk] measure from crm_customer_profile. Do not use raw AVERAGE on churn_risk_score.
7. Always indicate sources (tables used) and propose concrete corrective action
8. For campaign revenue rankings, join orders to marketing_campaigns via attributed_campaign_id and use [Total Revenue] or SUM on order amounts.

FORMAT:
- Data-driven responses with precise numbers from DAX queries
- Comparison to objectives (e.g., Open Rate 22% vs target 20%)
- Next step proposal (targeting, budget, messaging)

DISCLAIMERS:
- Remind that data is synthetic/fictitious
- Alert on anomalies (negative ROI, high churn, bounce > 5%)

OBJECTIVE: Make data accessible, enable quick marketing decisions (2-3 questions max).
"""

# ── Helpers ─────────────────────────────────────────────────────────────────
def b64(obj):
    return base64.b64encode(json.dumps(obj).encode()).decode()

def get_token():
    import os
    token = os.environ.get("FABRIC_TOKEN")
    if token:
        return token
    r = subprocess.run(
        ["az", "account", "get-access-token", "--resource", "https://api.fabric.microsoft.com"],
        capture_output=True, text=True, shell=True
    )
    return json.loads(r.stdout)["accessToken"]

def poll_operation(token, resp):
    op_url = resp.headers.get("Location") or resp.headers.get("x-ms-operation-url")
    if not op_url:
        print("  No operation URL in response headers")
        return False
    print(f"  Polling LRO: {op_url}")
    h = {"Authorization": f"Bearer {token}"}
    for i in range(30):
        time.sleep(2)
        r = requests.get(op_url, headers=h)
        if r.status_code == 200:
            status = r.json().get("status", "")
            print(f"  [{i+1}] {status}")
            if status in ("Succeeded", "succeeded"):
                return True
            if status in ("Failed", "failed"):
                print(f"  Error: {r.json()}")
                return False
    print("  Timeout after 60s")
    return False

# ── Load current definition parts from saved file ──────────────────────────
print("Loading current definition from _agent_def_raw.json...")
with open("_agent_def_raw.json", "r") as f:
    current_parts = json.load(f)

# Build lookup of current parts
parts_lookup = {p["path"]: p for p in current_parts}

# ── Build updated parts ────────────────────────────────────────────────────
stage_config = {
    "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/dataAgent/definition/stageConfiguration/1.0.0/schema.json",
    "aiInstructions": INSTRUCTIONS.strip(),
}

new_stage_payload = b64(stage_config)

# Keep existing parts but replace stage_config (draft + published)
updated_parts = []
for p in current_parts:
    path = p["path"]
    if path in ("Files/Config/draft/stage_config.json", "Files/Config/published/stage_config.json"):
        updated_parts.append({"path": path, "payload": new_stage_payload, "payloadType": "InlineBase64"})
        print(f"  Updated: {path}")
    else:
        updated_parts.append(p)
        print(f"  Kept:    {path}")

# ── Push to Fabric ─────────────────────────────────────────────────────────
print(f"\nPushing updated definition to agent {AGENT_ID}...")
token = get_token()
h = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
body = {"definition": {"parts": updated_parts}}

resp = requests.post(
    f"{API}/workspaces/{WORKSPACE_ID}/items/{AGENT_ID}/updateDefinition",
    headers=h, json=body
)

print(f"Response: {resp.status_code}")
if resp.status_code == 200:
    print("SUCCESS: Instructions updated (sync)")
elif resp.status_code == 202:
    ok = poll_operation(token, resp)
    print("SUCCESS" if ok else "FAILED")
else:
    print(f"FAILED: {resp.text}")
