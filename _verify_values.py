import json, requests
from azure.identity import AzureCliCredential

cred = AzureCliCredential()
token = cred.get_token("https://analysis.windows.net/powerbi/api/.default").token
url = "https://api.powerbi.com/v1.0/myorg/groups/acf3556a-b23c-4ea1-b706-4ea9c9a1c181/datasets/fa2333c4-b003-440b-b6f5-25b6e2454d6c/executeQueries"
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

queries = {
    "Q17 StdDev": 'EVALUATE ROW("StdDev", [Rate Std Dev])',
    "Q18 MinMax": 'EVALUATE ROW("MinRate", [Min Benchmark Rate], "MaxRate", [Max Benchmark Rate])',
    "Q14 TopWBS": 'EVALUATE TOPN(1, SUMMARIZE(fact_benchmarks, fact_benchmarks[wbs_code], "AvgRate", AVERAGE(fact_benchmarks[unit_rate_eur])), [AvgRate], DESC)',
    "Q8 WBS count": 'EVALUATE ROW("WBS", [Total WBS Codes])',
}

for name, q in queries.items():
    r = requests.post(url, headers=headers, json={"queries": [{"query": q}], "serializerSettings": {"includeNulls": True}})
    data = r.json()
    rows = data.get("results", [{}])[0].get("tables", [{}])[0].get("rows", [])
    print(f"{name}: {rows[:5]}")
