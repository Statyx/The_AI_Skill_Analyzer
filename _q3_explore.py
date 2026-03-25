"""Explore more data for comprehensive descriptions."""
import json
from azure.identity import AzureCliCredential
import requests

WORKSPACE_ID = "133c6c70-2e26-4d97-aac1-8ed423dbbf34"
MODEL_ID = "236080b8-3bea-4c14-86df-d1f9a14ac7a8"

cred = AzureCliCredential()
token = cred.get_token("https://analysis.windows.net/powerbi/api/.default").token

url = f"https://api.powerbi.com/v1.0/myorg/groups/{WORKSPACE_ID}/datasets/{MODEL_ID}/executeQueries"
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
session = requests.Session()
session.headers.update(headers)

queries = [
    ("Sub-categories", "EVALUATE DISTINCT(SELECTCOLUMNS('dim_chart_of_accounts', \"cat\", 'dim_chart_of_accounts'[category], \"sub\", 'dim_chart_of_accounts'[sub_category], \"type\", 'dim_chart_of_accounts'[account_type]))"),
    ("Cost center types", "EVALUATE DISTINCT('dim_cost_centers'[cost_center_type])"),
    ("Cost center names", "EVALUATE DISTINCT('dim_cost_centers'[cost_center_name])"),
    ("Product categories", "EVALUATE DISTINCT('dim_products'[category])"),
    ("Customer segments", "EVALUATE DISTINCT('dim_customers'[segment])"),
    ("Entry types", "EVALUATE DISTINCT('fact_general_ledger'[entry_type])"),
    ("Budget types", "EVALUATE DISTINCT('fact_budgets'[budget_type])"),
    ("Forecast types", "EVALUATE DISTINCT('fact_forecasts'[forecast_type])"),
    ("Invoice statuses", "EVALUATE DISTINCT('fact_invoices'[status])"),
    ("Gross Profit", "EVALUATE ROW(\"GP\", [Gross Profit], \"GM%\", [Gross Margin %])"),
    ("EBITDA", "EVALUATE ROW(\"EBITDA\", [EBITDA], \"EBITDA%\", [EBITDA Margin %])"),
    ("Net Income", "EVALUATE ROW(\"NI\", [Net Income])"),
    ("Budget vs Actual", "EVALUATE ROW(\"Budget\", [Budget Amount], \"Actual\", [Actual Amount], \"Var\", [Variance Amount], \"Var%\", [Variance %])"),
    ("DSO", "EVALUATE ROW(\"DSO\", [DSO], \"TotalAR\", [Total AR], \"Overdue\", [Overdue Invoices Amount])"),
]

for label, q in queries:
    print(f"\n=== {label} ===")
    body = {"queries": [{"query": q}], "serializerSettings": {"includeNulls": True}}
    try:
        resp = session.post(url, json=body, timeout=30)
        if resp.status_code == 200:
            result = resp.json()
            err = result.get("results", [{}])[0].get("error")
            if err:
                print(f"  DAX Error: {json.dumps(err)[:300]}")
            else:
                rows = result.get("results", [{}])[0].get("tables", [{}])[0].get("rows", [])
                for r in rows[:30]:
                    print(f"  {r}")
        else:
            print(f"  HTTP {resp.status_code}: {resp.text[:300]}")
    except Exception as e:
        print(f"  Exception: {e}")


