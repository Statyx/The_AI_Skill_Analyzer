"""
Update SM_Finance semantic model descriptions.
Gets model.bim, adds descriptions to all tables/columns/measures, pushes back.
"""
import base64, json, time, requests
from azure.identity import AzureCliCredential

WORKSPACE_ID = "133c6c70-2e26-4d97-aac1-8ed423dbbf34"
MODEL_ID = "236080b8-3bea-4c14-86df-d1f9a14ac7a8"
API = "https://api.fabric.microsoft.com/v1"

# ── Descriptions ────────────────────────────────────────────────────────
# Structured as { table_name: { "desc": ..., "columns": {...}, "measures": {...} } }

DESCRIPTIONS = {
    "dim_chart_of_accounts": {
        "desc": "Chart of accounts dimension. French accounting standard (PCG). Maps account_id to account name, type, category, and sub-category. Used to classify GL entries. Categories are French (e.g. '6 - Charges' = Expenses, '7 - Produits' = Revenue). account_type has English values: Revenue, Expense, Asset, Liability, Cash.",
        "columns": {
            "account_id": "Unique account identifier (PK). Joins to fact_general_ledger, fact_budgets, fact_forecasts.",
            "account_number": "French PCG account number (e.g. 601, 707).",
            "account_name": "Human-readable account name.",
            "account_type": "English account classification. Values: Revenue, Expense, Asset, Liability, Cash. USE THIS for filtering revenue vs expenses.",
            "category": "French PCG category. Values: '1 - Actif' (Asset), '2 - Passif' (Liability), '4 - Trésorerie' (Cash), '6 - Charges' (Expenses), '7 - Produits' (Revenue). For expense queries, filter on '6 - Charges' or use account_type='Expense'.",
            "sub_category": "French PCG sub-category. Expense sub-cats: '60 - Achats' (COGS/Purchases), '61 - Salaires' (Salaries), '62 - Autres charges externes' (External charges/OpEx), '63 - Impôts et taxes' (Taxes), '64 - Charges financières' (Financial charges). Revenue sub-cats: '70 - Ventes' (Sales), '71 - Production stockée', '76 - Produits financiers'.",
            "is_active": "Whether the account is currently active.",
            "currency": "Currency code (EUR).",
        },
        "measures": {},
    },
    "dim_cost_centers": {
        "desc": "Cost center dimension. 13 cost centers grouped by type. Types: Revenue (Sales France, Sales EMEA, Sales AMER), R&D (Product Development), Support (Customer Success, IT Infrastructure), Delivery (Professional Services), Admin (Marketing, HR, Legal, Finance, Facilities, Executive).",
        "columns": {
            "cost_center_id": "Unique cost center ID (PK). Joins to fact_general_ledger, fact_budgets, fact_forecasts, fact_allocations.",
            "cost_center_name": "Human-readable name. Values: Sales France, Sales EMEA, Sales AMER, Professional Services, Product Development, Customer Success, IT Infrastructure, Marketing, HR, Legal, Finance, Facilities, Executive.",
            "cost_center_type": "Classification. Values: Revenue, R&D, Support, Delivery, Admin.",
            "region": "Geographic region of the cost center.",
            "manager": "Name of the cost center manager.",
            "budget_allocation_pct": "Percentage of total budget allocated to this cost center (0-1 scale).",
            "is_active": "Whether the cost center is currently active.",
        },
        "measures": {},
    },
    "dim_customers": {
        "desc": "Customer master data. Contains company info, segment, country, payment terms, and credit limits. Joins to fact_invoices via customer_id.",
        "columns": {
            "customer_id": "Unique customer ID (PK). Joins to fact_invoices.",
            "company_name": "Legal company name (e.g. 'Company 0042 SA').",
            "segment": "Customer segment. Values: enterprise, mid_market, smb. Enterprise = large accounts, mid_market = medium, smb = small business.",
            "industry": "Industry vertical of the customer.",
            "country": "Customer country.",
            "payment_terms_days": "Standard payment terms in days (e.g. 30, 60).",
            "credit_limit_eur": "Maximum credit allowed in EUR before payment required.",
            "account_manager": "Assigned account manager name.",
            "created_date": "Date the customer was created in the system.",
            "is_active": "Whether the customer account is active.",
        },
        "measures": {},
    },
    "dim_products": {
        "desc": "Product catalog dimension. 4 categories: software_licenses (highest margin ~85%), maintenance (~70%), training (~50%), professional_services (~40%). Each product has unit price, COGS, and gross margin percentage.",
        "columns": {
            "product_id": "Unique product ID (PK). Joins to fact_invoice_lines.",
            "product_name": "Product name.",
            "category": "Product category. Values: software_licenses, maintenance, training, professional_services. Use for revenue/margin analysis by product type.",
            "unit_price_eur": "List price per unit in EUR.",
            "cogs_eur": "Cost of goods sold per unit in EUR.",
            "gross_margin_pct": "Expected gross margin percentage (0-1 scale). software_licenses≈0.85, maintenance≈0.70, training≈0.50, professional_services≈0.40.",
            "is_active": "Whether the product is currently sold.",
        },
        "measures": {},
    },
    "fact_general_ledger": {
        "desc": "General ledger fact table. Contains all accounting entries with debit/credit amounts in EUR. Central fact for P&L, expenses, and budget analysis. Joins to dim_chart_of_accounts (via account_id) and dim_cost_centers (via cost_center_id). entry_type values: 'Revenue', 'COGS', 'Expense'. Use entry_type or the related dim_chart_of_accounts[account_type] to distinguish revenue from expenses.",
        "columns": {
            "entry_id": "Unique GL entry ID (PK).",
            "entry_date": "Date of the accounting entry.",
            "period_month": "Month number (1-12).",
            "fiscal_year": "Fiscal year (e.g. 2025). Use for time filtering.",
            "account_id": "FK to dim_chart_of_accounts. Determines the account type (Revenue/Expense/Asset/Liability/Cash).",
            "cost_center_id": "FK to dim_cost_centers. Determines which department incurred the cost.",
            "debit_amount_eur": "Debit amount in EUR. For expenses, this is typically the cost amount.",
            "credit_amount_eur": "Credit amount in EUR. For revenue, this is typically the income amount.",
            "description": "Free-text description of the GL entry.",
            "reference": "Reference document number.",
            "entry_type": "Type of GL entry. Values: 'Revenue', 'COGS', 'Expense'. CRITICAL for filtering. Use entry_type='Expense' for OpEx queries, entry_type='COGS' for cost of goods sold.",
        },
        "measures": {
            "Total Revenue": "Total revenue from Revenue-type accounts. Uses SUM of amounts where account_type='Revenue'. Responds to date, cost center, and product filters.",
            "Total COGS": "Total Cost of Goods Sold. Uses SUM of amounts where account_type='COGS'. Represents direct costs of products/services sold.",
            "Gross Profit": "Revenue minus COGS. Formula: [Total Revenue] - [Total COGS].",
            "Gross Margin %": "Gross profit as percentage of revenue. Formula: [Gross Profit] / [Total Revenue]. Format: percentage.",
            "Operating Expenses": "Total operating expenses (OpEx). Sum of amounts where account_type='Expense'. Includes salaries, external charges, taxes, financial charges. For total expenses query, USE THIS MEASURE directly.",
            "EBITDA": "Earnings Before Interest, Taxes, Depreciation, Amortization. Formula: [Gross Profit] - [Operating Expenses].",
            "EBITDA Margin %": "EBITDA as percentage of revenue. Formula: [EBITDA] / [Total Revenue]. Format: percentage.",
            "Net Income": "Net income after all charges. Similar to EBITDA in this model (no separate D&A/interest).",
            "YTD Revenue": "Year-to-date revenue accumulation.",
        },
    },
    "fact_budgets": {
        "desc": "Budget fact table. Contains approved budget amounts by cost center, account, month, and fiscal year. budget_type is 'Operating'. Compare with fact_general_ledger actuals using the Budget Amount, Actual Amount, Variance Amount, and Variance % measures.",
        "columns": {
            "budget_id": "Unique budget line ID (PK).",
            "fiscal_year": "Fiscal year of the budget (e.g. 2025).",
            "period_month": "Month number (1-12).",
            "period_date": "Date representation of the budget period.",
            "cost_center_id": "FK to dim_cost_centers.",
            "account_id": "FK to dim_chart_of_accounts.",
            "budget_amount_eur": "Approved budget amount in EUR.",
            "budget_type": "Type of budget. Values: 'Operating'.",
            "version": "Budget version identifier.",
        },
        "measures": {
            "Budget Amount": "Total approved budget amount in EUR. SUM of budget_amount_eur.",
            "Actual Amount": "Actual spend amount from GL for budget comparison. Compare with [Budget Amount] for variance analysis.",
            "Variance Amount": "Budget variance in EUR. Formula: [Actual Amount] - [Budget Amount]. Positive = over budget (unfavorable).",
            "Variance %": "Budget variance as percentage. Formula: [Variance Amount] / [Budget Amount]. Positive = over budget. A 'Material Variance' flag indicates significant deviations.",
            "Material Variance": "Flag indicating whether variance exceeds materiality threshold.",
        },
    },
    "fact_forecasts": {
        "desc": "Forecast fact table. Contains rolling forecast amounts by cost center, account, and month. forecast_type is 'Rolling'. Compare with actuals and budgets for accuracy analysis.",
        "columns": {
            "forecast_id": "Unique forecast line ID (PK).",
            "fiscal_year": "Fiscal year of the forecast.",
            "period_month": "Month number (1-12).",
            "period_date": "Date representation of the forecast period.",
            "cost_center_id": "FK to dim_cost_centers.",
            "account_id": "FK to dim_chart_of_accounts.",
            "forecast_amount_eur": "Forecasted amount in EUR.",
            "forecast_type": "Type of forecast. Values: 'Rolling'.",
            "version": "Forecast version identifier.",
            "created_date": "Date the forecast was created.",
        },
        "measures": {
            "Forecast Amount": "Total forecast amount in EUR. SUM of forecast_amount_eur.",
            "Forecast Accuracy": "Accuracy of forecast vs actuals. Format: percentage. 100% = perfect forecast.",
        },
    },
    "fact_allocations": {
        "desc": "Cost allocation fact table. Records indirect cost allocations between cost centers. Shows how shared costs (IT, admin) are distributed to revenue/delivery centers using allocation drivers (headcount, revenue, sqm).",
        "columns": {
            "allocation_id": "Unique allocation ID (PK).",
            "fiscal_year": "Fiscal year of the allocation.",
            "from_cost_center": "Source cost center performing the allocation (text name, not ID).",
            "to_cost_center_id": "FK to dim_cost_centers. Target cost center receiving allocated cost.",
            "allocation_driver": "Basis for allocation. Examples: headcount, revenue_share, sqm (square meters).",
            "driver_units": "Number of driver units used in this allocation.",
            "allocated_amount_eur": "Amount allocated in EUR.",
            "allocation_month": "Month number (1-12) of the allocation.",
        },
        "measures": {},
    },
    "fact_invoices": {
        "desc": "Invoice header fact table. One row per customer invoice. Contains invoice dates, amounts, status, and payment terms. Joins to dim_customers (via customer_id) and fact_invoice_lines (via invoice_id). All invoices have status='Issued'.",
        "columns": {
            "invoice_id": "Unique invoice ID (PK). Joins to fact_invoice_lines and fact_payments.",
            "invoice_number": "Human-readable invoice number.",
            "customer_id": "FK to dim_customers.",
            "invoice_date": "Date the invoice was issued.",
            "due_date": "Payment due date. Used for overdue/aging calculations.",
            "total_amount_eur": "Total invoice amount in EUR.",
            "status": "Invoice status. Values: 'Issued'. All invoices are in Issued state.",
            "payment_terms_days": "Payment terms in days for this specific invoice.",
        },
        "measures": {
            "Total Invoices": "Count of all invoices.",
            "Paid Invoices": "Count of invoices that have been fully paid.",
            "Unpaid Invoices": "Count of invoices not yet fully paid.",
            "Total AR": "Total Accounts Receivable in EUR. Sum of outstanding invoice amounts.",
            "DSO": "Days Sales Outstanding. Formula: (Total AR / Total Revenue) × 365. Measures average collection period in days. Target: 45 days.",
            "Overdue Invoices Amount": "Total EUR amount of invoices past their due date.",
            "Overdue Invoices Count": "Number of invoices past their due date.",
        },
    },
    "fact_invoice_lines": {
        "desc": "Invoice line items fact table. Detail of each product/service sold on an invoice. Contains quantity, unit price, discount, line total, and COGS per line. Joins to fact_invoices (via invoice_id) and dim_products (via product_id). Use for revenue by product, margin by product, and COGS analysis.",
        "columns": {
            "line_id": "Unique line item ID (PK).",
            "invoice_id": "FK to fact_invoices.",
            "product_id": "FK to dim_products.",
            "quantity": "Number of units sold.",
            "unit_price_eur": "Price per unit in EUR (may differ from list price due to negotiation).",
            "discount_pct": "Discount percentage applied (0-1 scale). Average ~5-8%.",
            "line_total_eur": "Total line amount in EUR after discount. Formula: quantity × unit_price × (1 - discount).",
            "cogs_eur": "Cost of goods sold for this line in EUR.",
        },
        "measures": {},
    },
    "fact_payments": {
        "desc": "Payment fact table. Records payments received against invoices. Joins to fact_invoices (via invoice_id). Used for cash collection, DSO, and aging analysis.",
        "columns": {
            "payment_id": "Unique payment ID (PK).",
            "invoice_id": "FK to fact_invoices.",
            "payment_date": "Date the payment was received.",
            "payment_amount_eur": "Payment amount in EUR.",
            "payment_method": "Method of payment (e.g. wire transfer, check).",
            "days_overdue": "Number of days the payment was late (0 = on time, positive = late).",
        },
        "measures": {
            "Total Payments": "Total payments received in EUR.",
            "Collection Rate": "Percentage of invoiced amount collected. Formula: Total Payments / Total Invoices Amount.",
            "Avg Days to Pay": "Average number of days from invoice date to payment date.",
        },
    },
}

# ── Helper ──────────────────────────────────────────────────────────────

def b64(obj):
    return base64.b64encode(json.dumps(obj).encode()).decode()


def poll_lro(session, resp):
    loc = resp.headers.get("Location") or resp.headers.get("Operation-Location")
    op_id = resp.headers.get("x-ms-operation-id")
    print(f"  LRO Location: {loc}")
    print(f"  Operation ID: {op_id}")

    # Use Fabric API base URL (avoids redirect domain SSL issues)
    if op_id:
        poll_url = f"{API}/operations/{op_id}"
    elif loc:
        poll_url = loc
    else:
        print("  No LRO location or operation ID")
        return None

    print(f"  Polling URL: {poll_url}")
    retry_after = int(resp.headers.get("Retry-After", "10"))

    for i in range(40):
        time.sleep(max(retry_after, 5))
        print(f"  Polling #{i+1}...")
        try:
            r = session.get(poll_url, timeout=30)
        except Exception as e:
            print(f"  Poll exception: {e}")
            continue
        if not r.ok:
            print(f"  Poll #{i+1} HTTP {r.status_code}")
            continue
        data = r.json()
        status = data.get("status", "")
        print(f"  Poll #{i+1}: {status}")
        if status in ("Succeeded", "Completed"):
            # Try /result endpoint on the Fabric API
            result_url = f"{API}/operations/{op_id}/result" if op_id else poll_url.rstrip("/") + "/result"
            print(f"  Getting result from: {result_url}")
            try:
                rr = session.get(result_url, timeout=30)
                return rr.json() if rr.ok else data
            except Exception as e:
                print(f"  Result fetch exception: {e}")
                return data
        if status in ("Failed", "Cancelled"):
            print(f"  FAILED: {json.dumps(data)[:500]}")
            return None
    print("  LRO timeout")
    return None


def main():
    cred = AzureCliCredential()
    token = cred.get_token("https://api.fabric.microsoft.com/.default").token
    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {token}", "Content-Type": "application/json"})

    # ── Step 1: Get current model.bim ──
    print("=== Getting model definition ===")
    resp = session.post(f"{API}/workspaces/{WORKSPACE_ID}/semanticModels/{MODEL_ID}/getDefinition", timeout=30)
    print(f"  Status: {resp.status_code}")
    if resp.status_code == 200:
        definition = resp.json()
    elif resp.status_code == 202:
        definition = poll_lro(session, resp)
    else:
        print(f"ERROR: HTTP {resp.status_code}: {resp.text[:500]}")
        return

    if not definition:
        print("Failed to get definition")
        return

    parts = definition.get("definition", {}).get("parts", [])
    print(f"  Got {len(parts)} parts: {[p['path'] for p in parts]}")

    # Find model.bim
    model_bim_part = None
    other_parts = []
    for p in parts:
        if p["path"] == "model.bim":
            model_bim_part = p
        else:
            other_parts.append(p)

    if not model_bim_part:
        print("ERROR: model.bim not found in parts")
        return

    model_bim = json.loads(base64.b64decode(model_bim_part["payload"]))
    print(f"  model.bim loaded: {len(model_bim.get('model', {}).get('tables', []))} tables")

    # Save backup
    with open("_model_bim_backup.json", "w", encoding="utf-8") as f:
        json.dump(model_bim, f, indent=2, ensure_ascii=False)
    print("  Backup saved to _model_bim_backup.json")

    # ── Step 2: Add descriptions ──
    print("\n=== Adding descriptions ===")
    stats = {"tables": 0, "columns": 0, "measures": 0}

    for table in model_bim.get("model", {}).get("tables", []):
        tname = table.get("name", "")
        if tname not in DESCRIPTIONS:
            print(f"  SKIP table: {tname} (not in descriptions dict)")
            continue

        desc_info = DESCRIPTIONS[tname]

        # Table description
        table["description"] = desc_info["desc"]
        stats["tables"] += 1

        # Column descriptions
        for col in table.get("columns", []):
            cname = col.get("name", "")
            if cname in desc_info["columns"]:
                col["description"] = desc_info["columns"][cname]
                stats["columns"] += 1

        # Measure descriptions
        for meas in table.get("measures", []):
            mname = meas.get("name", "")
            if mname in desc_info["measures"]:
                meas["description"] = desc_info["measures"][mname]
                stats["measures"] += 1

    print(f"  Updated: {stats['tables']} tables, {stats['columns']} columns, {stats['measures']} measures")

    # ── Step 3: Push back ──
    print("\n=== Pushing updated model ===")
    updated_parts = other_parts + [
        {
            "path": "model.bim",
            "payload": base64.b64encode(json.dumps(model_bim).encode()).decode(),
            "payloadType": "InlineBase64",
        }
    ]

    body = {"definition": {"parts": updated_parts}}
    resp = session.post(
        f"{API}/workspaces/{WORKSPACE_ID}/semanticModels/{MODEL_ID}/updateDefinition",
        json=body, timeout=30
    )

    if resp.status_code == 200:
        print("  SUCCESS (200)")
    elif resp.status_code == 202:
        result = poll_lro(session, resp)
        if result:
            print("  SUCCESS (LRO completed)")
        else:
            print("  FAILED (LRO)")
    else:
        print(f"  ERROR: HTTP {resp.status_code}: {resp.text[:500]}")


if __name__ == "__main__":
    main()
