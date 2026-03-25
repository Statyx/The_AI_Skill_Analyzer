"""
Step 2: Add descriptions to TMDL files and push back.
Reads from _tmdl_raw/, adds /// descriptions, pushes to Fabric API.
"""
import base64, json, time, sys, os, re, requests
from azure.identity import AzureCliCredential

WORKSPACE_ID = "133c6c70-2e26-4d97-aac1-8ed423dbbf34"
MODEL_ID = "236080b8-3bea-4c14-86df-d1f9a14ac7a8"
API = "https://api.fabric.microsoft.com/v1"

# ── Descriptions ───────────────────────────────────────────────
TABLE_DESCS = {
    "dim_chart_of_accounts": "Chart of accounts (French PCG). Categories: '6 - Charges'=Expenses, '7 - Produits'=Revenue, '1 - Actif'=Asset, '2 - Passif'=Liability, '4 - Tresorerie'=Cash. Use account_type (English: Revenue/Expense/Asset/Liability/Cash) for filtering.",
    "dim_cost_centers": "Cost centers. 13 centers by type: Revenue (Sales France/EMEA/AMER), R&D (Product Development), Delivery (Professional Services), Support (Customer Success, IT Infrastructure), Admin (Marketing, HR, Legal, Finance, Facilities, Executive).",
    "dim_customers": "Customer master. Segments: enterprise, mid_market, smb. Has payment_terms, credit_limits, country, industry.",
    "dim_products": "Product catalog. Categories: software_licenses (margin 85%), maintenance (70%), training (50%), professional_services (40%).",
    "fact_general_ledger": "General ledger entries with debit/credit in EUR. entry_type: Revenue, COGS, Expense. Joins to dim_chart_of_accounts and dim_cost_centers. Central fact for P&L.",
    "fact_budgets": "Budget amounts by cost center, account, month. budget_type=Operating. Has Budget Amount, Actual Amount, Variance measures.",
    "fact_forecasts": "Rolling forecast amounts by cost center, account, month. Has Forecast Amount and Forecast Accuracy measures.",
    "fact_allocations": "Indirect cost allocations between cost centers using drivers (headcount, revenue_share, sqm).",
    "fact_invoices": "Invoice headers, one per customer invoice. status=Issued. Has DSO, AR, overdue measures. Joins to dim_customers.",
    "fact_invoice_lines": "Invoice line items with quantity, price, discount, total, cogs. Joins to dim_products and fact_invoices.",
    "fact_payments": "Payments against invoices with amount, date, method, days_overdue.",
}

COL_DESCS = {
    "dim_chart_of_accounts": {
        "account_id": "PK. FK for GL, budgets, forecasts.",
        "account_number": "French PCG number (e.g. 601, 707).",
        "account_name": "Human-readable account name.",
        "account_type": "English type: Revenue, Expense, Asset, Liability, Cash. USE THIS to filter revenue vs expenses.",
        "category": "French PCG: '6 - Charges'=Expenses, '7 - Produits'=Revenue, '1 - Actif', '2 - Passif', '4 - Tresorerie'.",
        "sub_category": "French sub-cat. Expenses: '60 - Achats'(COGS), '61 - Salaires', '62 - Autres charges externes'(OpEx), '63 - Impots et taxes', '64 - Charges financieres'. Revenue: '70 - Ventes', '76 - Produits financiers'.",
        "is_active": "Whether account is active.",
        "currency": "Currency (EUR).",
    },
    "dim_cost_centers": {
        "cost_center_id": "PK. FK for GL, budgets, forecasts, allocations.",
        "cost_center_name": "Values: Sales France, Sales EMEA, Sales AMER, Professional Services, Product Development, Customer Success, IT Infrastructure, Marketing, HR, Legal, Finance, Facilities, Executive.",
        "cost_center_type": "Type: Revenue, R&D, Support, Delivery, Admin.",
        "region": "Geographic region.",
        "manager": "Manager name.",
        "budget_allocation_pct": "Budget allocation percentage (0-1).",
        "is_active": "Active flag.",
    },
    "dim_customers": {
        "customer_id": "PK. FK for fact_invoices.",
        "company_name": "Company legal name.",
        "segment": "Segment: enterprise, mid_market, smb.",
        "industry": "Industry vertical.",
        "country": "Country.",
        "payment_terms_days": "Payment terms in days (30/60).",
        "credit_limit_eur": "Max credit in EUR.",
        "account_manager": "Account manager.",
        "created_date": "Creation date.",
        "is_active": "Active flag.",
    },
    "dim_products": {
        "product_id": "PK. FK for fact_invoice_lines.",
        "product_name": "Product name.",
        "category": "Type: software_licenses, maintenance, training, professional_services.",
        "unit_price_eur": "List price per unit EUR.",
        "cogs_eur": "COGS per unit EUR.",
        "gross_margin_pct": "Expected margin (0-1). software_licenses~0.85, maintenance~0.70, training~0.50, professional_services~0.40.",
        "is_active": "Active flag.",
    },
    "fact_general_ledger": {
        "entry_id": "PK.",
        "entry_date": "Entry date.",
        "period_month": "Month (1-12).",
        "fiscal_year": "Fiscal year (2025).",
        "account_id": "FK to dim_chart_of_accounts.",
        "cost_center_id": "FK to dim_cost_centers.",
        "debit_amount_eur": "Debit EUR.",
        "credit_amount_eur": "Credit EUR.",
        "description": "Entry description text.",
        "reference": "Reference number.",
        "entry_type": "Type: Revenue, COGS, Expense. Use for filtering.",
    },
    "fact_budgets": {
        "budget_id": "PK.",
        "fiscal_year": "Budget year.",
        "period_month": "Month (1-12).",
        "period_date": "Period date.",
        "cost_center_id": "FK to dim_cost_centers.",
        "account_id": "FK to dim_chart_of_accounts.",
        "budget_amount_eur": "Budget EUR.",
        "budget_type": "Type: Operating.",
        "version": "Budget version.",
    },
    "fact_forecasts": {
        "forecast_id": "PK.",
        "fiscal_year": "Forecast year.",
        "period_month": "Month (1-12).",
        "period_date": "Period date.",
        "cost_center_id": "FK to dim_cost_centers.",
        "account_id": "FK to dim_chart_of_accounts.",
        "forecast_amount_eur": "Forecast EUR.",
        "forecast_type": "Type: Rolling.",
        "version": "Forecast version.",
        "created_date": "Creation date.",
    },
    "fact_allocations": {
        "allocation_id": "PK.",
        "fiscal_year": "Year.",
        "from_cost_center": "Source cost center (text name).",
        "to_cost_center_id": "FK to dim_cost_centers (target).",
        "allocation_driver": "Driver: headcount, revenue_share, sqm.",
        "driver_units": "Driver units.",
        "allocated_amount_eur": "Allocated EUR.",
        "allocation_month": "Month (1-12).",
    },
    "fact_invoices": {
        "invoice_id": "PK. FK for lines and payments.",
        "invoice_number": "Invoice number.",
        "customer_id": "FK to dim_customers.",
        "invoice_date": "Issue date.",
        "due_date": "Due date for aging calculations.",
        "total_amount_eur": "Total EUR.",
        "status": "Status: Issued.",
        "payment_terms_days": "Payment terms days.",
    },
    "fact_invoice_lines": {
        "line_id": "PK.",
        "invoice_id": "FK to fact_invoices.",
        "product_id": "FK to dim_products.",
        "quantity": "Units sold.",
        "unit_price_eur": "Unit price EUR.",
        "discount_pct": "Discount (0-1).",
        "line_total_eur": "Line total after discount EUR.",
        "cogs_eur": "Line COGS EUR.",
    },
    "fact_payments": {
        "payment_id": "PK.",
        "invoice_id": "FK to fact_invoices.",
        "payment_date": "Payment date.",
        "payment_amount_eur": "Payment EUR.",
        "payment_method": "Payment method.",
        "days_overdue": "Days late (0=on time).",
    },
}

MEASURE_DESCS = {
    "Total Revenue": "Total revenue. SUM of credit_amount where account_type=Revenue.",
    "Total COGS": "Cost of goods sold. SUM of debit where account_type=Expense AND sub_category='60 - Achats'.",
    "Gross Profit": "[Total Revenue] - [Total COGS].",
    "Gross Margin %": "[Gross Profit] / [Total Revenue].",
    "Operating Expenses": "All expenses. SUM debit where account_type=Expense. For total expenses, USE THIS.",
    "EBITDA": "[Total Revenue] - [Operating Expenses].",
    "EBITDA Margin %": "[EBITDA] / [Total Revenue].",
    "Net Income": "Net income: total credits minus total debits.",
    "YTD Revenue": "Year-to-date revenue.",
    "Budget Amount": "Total approved budget EUR.",
    "Actual Amount": "Actual spend from GL for budget comparison.",
    "Variance Amount": "Actual - Budget. Positive = over budget.",
    "Variance %": "Variance / Budget. Positive = unfavorable.",
    "Material Variance": "Flag for exceeding materiality threshold.",
    "Forecast Amount": "Total forecast EUR.",
    "Forecast Accuracy": "Forecast vs actual accuracy %.",
    "Total Invoices": "Count of invoices.",
    "Paid Invoices": "Count of paid invoices.",
    "Unpaid Invoices": "Count of unpaid invoices.",
    "Total AR": "Accounts Receivable EUR.",
    "DSO": "Days Sales Outstanding: (AR/Revenue)*365. Target: 45 days.",
    "Overdue Invoices Amount": "EUR past due date.",
    "Overdue Invoices Count": "Count past due date.",
    "Total Payments": "Total payments received EUR.",
    "Collection Rate": "Payments / Invoiced amount %.",
    "Avg Days to Pay": "Average days invoice to payment.",
}


def add_descriptions_to_tmdl(tmdl_text, table_name):
    """Insert /// doc comments before table/column/measure declarations."""
    lines = tmdl_text.split("\n")
    result = []
    col_descs = COL_DESCS.get(table_name, {})
    stats = {"t": 0, "c": 0, "m": 0}

    i = 0
    while i < len(lines):
        line = lines[i]
        stripped = line.strip()

        # Detect leading tabs
        tab_count = 0
        for ch in line:
            if ch == "\t":
                tab_count += 1
            else:
                break
        prefix = "\t" * tab_count

        # Table declaration (no leading tabs)
        if tab_count == 0 and stripped.startswith("table "):
            if table_name in TABLE_DESCS:
                # Check not already described
                if not result or not result[-1].strip().startswith("///"):
                    result.append(f"/// {TABLE_DESCS[table_name]}")
                    stats["t"] += 1
            result.append(line)

        # Column (1 tab)
        elif stripped.startswith("column "):
            m = re.match(r"column\s+'?([^'\s]+)'?", stripped)
            if m:
                col_name = m.group(1)
                if col_name in col_descs and (not result or not result[-1].strip().startswith("///")):
                    result.append(f"{prefix}/// {col_descs[col_name]}")
                    stats["c"] += 1
            result.append(line)

        # Measure (1 tab)
        elif stripped.startswith("measure "):
            m = re.match(r"measure\s+'([^']+)'", stripped)
            if not m:
                m = re.match(r"measure\s+(\S+)", stripped)
            if m:
                meas_name = m.group(1)
                if meas_name in MEASURE_DESCS and (not result or not result[-1].strip().startswith("///")):
                    result.append(f"{prefix}/// {MEASURE_DESCS[meas_name]}")
                    stats["m"] += 1
            result.append(line)

        else:
            result.append(line)

        i += 1

    return "\n".join(result), stats


def poll_lro(session, resp, label=""):
    op_id = resp.headers.get("x-ms-operation-id")
    if not op_id:
        print(f"  [{label}] No op ID", flush=True)
        return None
    poll_url = f"{API}/operations/{op_id}"
    retry_after = int(resp.headers.get("Retry-After", "10"))
    print(f"  [{label}] op={op_id} retry={retry_after}s", flush=True)
    for i in range(40):
        time.sleep(max(retry_after, 5))
        print(f"  [{label}] #{i+1}...", end=" ", flush=True)
        try:
            r = session.get(poll_url, timeout=30)
        except Exception as e:
            print(f"Err: {e}", flush=True)
            continue
        if not r.ok:
            print(f"HTTP {r.status_code}", flush=True)
            continue
        data = r.json()
        st = data.get("status", "")
        print(st, flush=True)
        if st in ("Succeeded", "Completed"):
            try:
                rr = session.get(f"{API}/operations/{op_id}/result", timeout=30)
                return rr.json() if rr.ok else data
            except:
                return data
        if st in ("Failed", "Cancelled"):
            print(f"  DETAIL: {json.dumps(data)[:600]}", flush=True)
            return None
    return None


def main():
    cred = AzureCliCredential()
    token = cred.get_token("https://api.fabric.microsoft.com/.default").token
    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {token}", "Content-Type": "application/json"})

    # ── Step 1: Get TMDL ──
    print("=== Get TMDL ===", flush=True)
    resp = session.post(f"{API}/workspaces/{WORKSPACE_ID}/semanticModels/{MODEL_ID}/getDefinition", timeout=30)
    print(f"  Status: {resp.status_code}", flush=True)
    if resp.status_code == 202:
        defn = poll_lro(session, resp, "GET")
    elif resp.status_code == 200:
        defn = resp.json()
    else:
        print(f"ERROR: {resp.text[:500]}", flush=True)
        return

    parts = defn["definition"]["parts"]
    print(f"  Parts: {len(parts)}", flush=True)

    # ── Step 2: Add descriptions ──
    print("\n=== Add descriptions ===", flush=True)
    total = {"t": 0, "c": 0, "m": 0}
    updated_parts = []

    for p in parts:
        path = p["path"]
        if path.startswith("definition/tables/") and path.endswith(".tmdl"):
            tmdl_text = base64.b64decode(p["payload"]).decode("utf-8")
            tname = path.split("/")[-1].replace(".tmdl", "")
            new_tmdl, stats = add_descriptions_to_tmdl(tmdl_text, tname)
            total["t"] += stats["t"]
            total["c"] += stats["c"]
            total["m"] += stats["m"]
            if stats["t"] + stats["c"] + stats["m"] > 0:
                print(f"  {tname}: +{stats['t']}T +{stats['c']}C +{stats['m']}M", flush=True)
            updated_parts.append({
                "path": path,
                "payload": base64.b64encode(new_tmdl.encode("utf-8")).decode("ascii"),
                "payloadType": "InlineBase64",
            })
        else:
            updated_parts.append(p)

    print(f"  TOTAL: {total['t']}T {total['c']}C {total['m']}M", flush=True)

    # Verify dim_chart_of_accounts first 10 lines
    for p in updated_parts:
        if "dim_chart_of_accounts" in p["path"]:
            content = base64.b64decode(p["payload"]).decode("utf-8")
            print(f"\n  Preview dim_chart_of_accounts:", flush=True)
            for j, ln in enumerate(content.split("\n")[:10], 1):
                print(f"    {j}| {repr(ln)}", flush=True)
            break

    # ── Step 3: Push ──
    print("\n=== Push updated TMDL ===", flush=True)
    body = {"definition": {"parts": updated_parts}}
    resp = session.post(
        f"{API}/workspaces/{WORKSPACE_ID}/semanticModels/{MODEL_ID}/updateDefinition",
        json=body, timeout=60
    )
    print(f"  Status: {resp.status_code}", flush=True)
    if resp.status_code == 200:
        print("  SUCCESS", flush=True)
    elif resp.status_code == 202:
        result = poll_lro(session, resp, "PUSH")
        print("  SUCCESS" if result else "  FAILED", flush=True)
    else:
        print(f"  ERROR: {resp.text[:500]}", flush=True)


if __name__ == "__main__":
    main()
