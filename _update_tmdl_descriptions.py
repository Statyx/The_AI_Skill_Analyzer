"""
Update SM_Finance semantic model descriptions via TMDL format.
Gets TMDL parts, adds /// doc comments to tables/columns/measures, pushes back.
"""
import base64, json, time, re, sys, requests
from azure.identity import AzureCliCredential

WORKSPACE_ID = "133c6c70-2e26-4d97-aac1-8ed423dbbf34"
MODEL_ID = "236080b8-3bea-4c14-86df-d1f9a14ac7a8"
API = "https://api.fabric.microsoft.com/v1"

# ── Table descriptions ──────────────────────────────────────────────────
TABLE_DESCS = {
    "dim_chart_of_accounts": "Chart of accounts dimension (French PCG standard). Maps account_id to account name, type, category, sub-category. Categories are French: '6 - Charges'=Expenses, '7 - Produits'=Revenue, '1 - Actif'=Asset, '2 - Passif'=Liability, '4 - Trésorerie'=Cash. The account_type column has English values: Revenue, Expense, Asset, Liability, Cash.",
    "dim_cost_centers": "Cost center dimension. 13 cost centers by type: Revenue (Sales France/EMEA/AMER), R&D (Product Development), Delivery (Professional Services), Support (Customer Success, IT Infrastructure), Admin (Marketing, HR, Legal, Finance, Facilities, Executive).",
    "dim_customers": "Customer master data. Segments: enterprise, mid_market, smb. Contains payment terms, credit limits, country, industry.",
    "dim_products": "Product catalog. Categories: software_licenses (margin ~85%), maintenance (~70%), training (~50%), professional_services (~40%). Each has unit_price, cogs, gross_margin_pct.",
    "fact_general_ledger": "General ledger fact. All accounting entries with debit/credit amounts in EUR. entry_type values: 'Revenue', 'COGS', 'Expense'. Joins to dim_chart_of_accounts (account_id) and dim_cost_centers (cost_center_id). Central fact for P&L analysis.",
    "fact_budgets": "Budget fact. Approved budget amounts by cost center, account, month. budget_type='Operating'. Use Budget Amount, Actual Amount, Variance measures for budget vs actual analysis.",
    "fact_forecasts": "Forecast fact. Rolling forecast amounts by cost center, account, month. forecast_type='Rolling'. Compare with budgets and actuals.",
    "fact_allocations": "Cost allocation fact. Indirect cost allocations between cost centers using drivers (headcount, revenue_share, sqm).",
    "fact_invoices": "Invoice headers. One row per customer invoice. status='Issued'. Joins to dim_customers (customer_id). Contains DSO, AR, overdue measures.",
    "fact_invoice_lines": "Invoice line items. Product/service detail per invoice. Has quantity, unit_price, discount_pct, line_total_eur, cogs_eur. Joins to dim_products and fact_invoices.",
    "fact_payments": "Payment records against invoices. Contains payment_date, amount, method, days_overdue. Joins to fact_invoices (invoice_id).",
}

# ── Column descriptions ─────────────────────────────────────────────────
COL_DESCS = {
    "dim_chart_of_accounts": {
        "account_id": "Unique account ID (PK). FK for fact_general_ledger, fact_budgets, fact_forecasts.",
        "account_number": "French PCG account number (e.g. 601, 707).",
        "account_name": "Human-readable account name.",
        "account_type": "English classification: Revenue, Expense, Asset, Liability, Cash. USE THIS to filter revenue vs expenses.",
        "category": "French PCG category: '1 - Actif', '2 - Passif', '4 - Trésorerie', '6 - Charges' (=Expenses), '7 - Produits' (=Revenue).",
        "sub_category": "French PCG sub-category. Expenses: '60 - Achats'(COGS), '61 - Salaires', '62 - Autres charges externes'(OpEx), '63 - Impôts et taxes', '64 - Charges financières'. Revenue: '70 - Ventes', '71 - Production stockée', '76 - Produits financiers'.",
        "is_active": "Whether account is active.",
        "currency": "Currency code (EUR).",
    },
    "dim_cost_centers": {
        "cost_center_id": "Unique cost center ID (PK). FK for GL, budgets, forecasts, allocations.",
        "cost_center_name": "Name: Sales France, Sales EMEA, Sales AMER, Professional Services, Product Development, Customer Success, IT Infrastructure, Marketing, HR, Legal, Finance, Facilities, Executive.",
        "cost_center_type": "Classification: Revenue, R&D, Support, Delivery, Admin.",
        "region": "Geographic region.",
        "manager": "Cost center manager name.",
        "budget_allocation_pct": "Percentage of total budget allocated (0-1 scale).",
        "is_active": "Whether cost center is active.",
    },
    "dim_customers": {
        "customer_id": "Unique customer ID (PK). FK for fact_invoices.",
        "company_name": "Legal company name.",
        "segment": "Customer segment: enterprise, mid_market, smb.",
        "industry": "Industry vertical.",
        "country": "Customer country.",
        "payment_terms_days": "Standard payment terms in days (30/60).",
        "credit_limit_eur": "Max credit allowed in EUR.",
        "account_manager": "Assigned account manager.",
        "created_date": "Customer creation date.",
        "is_active": "Whether customer is active.",
    },
    "dim_products": {
        "product_id": "Unique product ID (PK). FK for fact_invoice_lines.",
        "product_name": "Product name.",
        "category": "Product type: software_licenses, maintenance, training, professional_services.",
        "unit_price_eur": "List price per unit in EUR.",
        "cogs_eur": "COGS per unit in EUR.",
        "gross_margin_pct": "Expected gross margin (0-1). software_licenses~0.85, maintenance~0.70, training~0.50, professional_services~0.40.",
        "is_active": "Whether product is sold.",
    },
    "fact_general_ledger": {
        "entry_id": "Unique GL entry ID (PK).",
        "entry_date": "Accounting entry date.",
        "period_month": "Month number (1-12).",
        "fiscal_year": "Fiscal year (2025). Use for time filtering.",
        "account_id": "FK to dim_chart_of_accounts.",
        "cost_center_id": "FK to dim_cost_centers.",
        "debit_amount_eur": "Debit amount in EUR.",
        "credit_amount_eur": "Credit amount in EUR.",
        "description": "GL entry description.",
        "reference": "Reference document number.",
        "entry_type": "GL entry type: 'Revenue', 'COGS', 'Expense'. CRITICAL for filtering expenses vs revenue.",
    },
    "fact_budgets": {
        "budget_id": "Unique budget line ID (PK).",
        "fiscal_year": "Budget fiscal year.",
        "period_month": "Month (1-12).",
        "period_date": "Budget period date.",
        "cost_center_id": "FK to dim_cost_centers.",
        "account_id": "FK to dim_chart_of_accounts.",
        "budget_amount_eur": "Approved budget in EUR.",
        "budget_type": "Budget type: 'Operating'.",
        "version": "Budget version.",
    },
    "fact_forecasts": {
        "forecast_id": "Unique forecast ID (PK).",
        "fiscal_year": "Forecast fiscal year.",
        "period_month": "Month (1-12).",
        "period_date": "Forecast period date.",
        "cost_center_id": "FK to dim_cost_centers.",
        "account_id": "FK to dim_chart_of_accounts.",
        "forecast_amount_eur": "Forecast amount in EUR.",
        "forecast_type": "Forecast type: 'Rolling'.",
        "version": "Forecast version.",
        "created_date": "Forecast creation date.",
    },
    "fact_allocations": {
        "allocation_id": "Unique allocation ID (PK).",
        "fiscal_year": "Allocation fiscal year.",
        "from_cost_center": "Source cost center name (text).",
        "to_cost_center_id": "FK to dim_cost_centers (target).",
        "allocation_driver": "Allocation basis: headcount, revenue_share, sqm.",
        "driver_units": "Driver units for allocation.",
        "allocated_amount_eur": "Allocated amount in EUR.",
        "allocation_month": "Allocation month (1-12).",
    },
    "fact_invoices": {
        "invoice_id": "Unique invoice ID (PK). FK for invoice_lines and payments.",
        "invoice_number": "Human-readable invoice number.",
        "customer_id": "FK to dim_customers.",
        "invoice_date": "Invoice issue date.",
        "due_date": "Payment due date for aging/overdue calculations.",
        "total_amount_eur": "Total invoice amount in EUR.",
        "status": "Invoice status: 'Issued'.",
        "payment_terms_days": "Payment terms for this invoice.",
    },
    "fact_invoice_lines": {
        "line_id": "Unique line ID (PK).",
        "invoice_id": "FK to fact_invoices.",
        "product_id": "FK to dim_products.",
        "quantity": "Units sold.",
        "unit_price_eur": "Unit price in EUR.",
        "discount_pct": "Discount applied (0-1 scale).",
        "line_total_eur": "Line total after discount in EUR.",
        "cogs_eur": "COGS for this line in EUR.",
    },
    "fact_payments": {
        "payment_id": "Unique payment ID (PK).",
        "invoice_id": "FK to fact_invoices.",
        "payment_date": "Payment received date.",
        "payment_amount_eur": "Payment amount in EUR.",
        "payment_method": "Payment method.",
        "days_overdue": "Days late (0=on time, positive=late).",
    },
}

# ── Measure descriptions ────────────────────────────────────────────────
MEASURE_DESCS = {
    "Total Revenue": "Total revenue from Revenue-type GL accounts. Responds to date, cost center, product filters.",
    "Total COGS": "Total Cost of Goods Sold from COGS-type GL accounts.",
    "Gross Profit": "Revenue minus COGS: [Total Revenue] - [Total COGS].",
    "Gross Margin %": "Gross profit as % of revenue: [Gross Profit]/[Total Revenue].",
    "Operating Expenses": "Total operating expenses from Expense-type GL accounts. For 'total expenses' queries, USE THIS MEASURE.",
    "EBITDA": "Gross Profit minus Operating Expenses: [Gross Profit] - [Operating Expenses].",
    "EBITDA Margin %": "EBITDA as % of revenue: [EBITDA]/[Total Revenue].",
    "Net Income": "Net income after all charges.",
    "YTD Revenue": "Year-to-date revenue accumulation.",
    "Budget Amount": "Total approved budget amount in EUR.",
    "Actual Amount": "Actual spend from GL for budget comparison.",
    "Variance Amount": "Budget variance in EUR: [Actual Amount] - [Budget Amount]. Positive = over budget.",
    "Variance %": "Budget variance as %: [Variance Amount]/[Budget Amount]. Positive = unfavorable.",
    "Material Variance": "Flag for variance exceeding materiality threshold.",
    "Forecast Amount": "Total forecast amount in EUR.",
    "Forecast Accuracy": "Forecast vs actual accuracy as %.",
    "Total Invoices": "Count of all invoices.",
    "Paid Invoices": "Count of fully paid invoices.",
    "Unpaid Invoices": "Count of unpaid invoices.",
    "Total AR": "Total Accounts Receivable in EUR.",
    "DSO": "Days Sales Outstanding: (Total AR / Total Revenue) x 365. Target: 45 days.",
    "Overdue Invoices Amount": "EUR amount of invoices past due date.",
    "Overdue Invoices Count": "Number of invoices past due date.",
    "Total Payments": "Total payments received in EUR.",
    "Collection Rate": "% of invoiced amount collected.",
    "Avg Days to Pay": "Average days from invoice to payment.",
}


def add_tmdl_description(tmdl_text, table_name):
    """Add /// doc comments before table, column, and measure declarations in TMDL."""
    lines = tmdl_text.split("\n")
    new_lines = []
    stats = {"table": 0, "columns": 0, "measures": 0}
    col_descs = COL_DESCS.get(table_name, {})

    i = 0
    while i < len(lines):
        line = lines[i]
        stripped = line.strip()

        # Table declaration
        if stripped.startswith("table ") and table_name in TABLE_DESCS:
            desc = TABLE_DESCS[table_name]
            indent = len(line) - len(line.lstrip())
            prefix = " " * indent
            # Only add if not already has ///
            if i == 0 or not lines[i-1].strip().startswith("///"):
                new_lines.append(f"{prefix}/// {desc}")
                stats["table"] += 1
            new_lines.append(line)

        # Column declaration
        elif stripped.startswith("column "):
            # Extract column name (may be quoted with single quotes)
            m = re.match(r"\s*column\s+'?([^'=\s]+)'?\s*", stripped)
            if m:
                col_name = m.group(1)
                if col_name in col_descs:
                    indent = len(line) - len(line.lstrip())
                    prefix = " " * indent
                    if i == 0 or not lines[i-1].strip().startswith("///"):
                        new_lines.append(f"{prefix}/// {col_descs[col_name]}")
                        stats["columns"] += 1
            new_lines.append(line)

        # Measure declaration
        elif stripped.startswith("measure "):
            # Extract measure name (may be quoted)
            m = re.match(r"\s*measure\s+'([^']+)'", stripped)
            if not m:
                m = re.match(r"\s*measure\s+(\S+)", stripped)
            if m:
                meas_name = m.group(1)
                # Clean up trailing ' = etc
                meas_name = meas_name.rstrip("'").rstrip()
                if meas_name in MEASURE_DESCS:
                    indent = len(line) - len(line.lstrip())
                    prefix = " " * indent
                    if i == 0 or not lines[i-1].strip().startswith("///"):
                        new_lines.append(f"{prefix}/// {MEASURE_DESCS[meas_name]}")
                        stats["measures"] += 1
            new_lines.append(line)
        else:
            new_lines.append(line)

        i += 1

    return "\n".join(new_lines), stats


def b64_encode(text):
    return base64.b64encode(text.encode("utf-8")).decode("ascii")


def poll_lro(session, resp):
    op_id = resp.headers.get("x-ms-operation-id")
    if not op_id:
        print("  No operation ID", flush=True)
        return None
    poll_url = f"{API}/operations/{op_id}"
    retry_after = int(resp.headers.get("Retry-After", "10"))
    print(f"  Operation: {op_id}, retry={retry_after}s", flush=True)

    for i in range(40):
        time.sleep(max(retry_after, 5))
        sys.stdout.write(f"  Poll #{i+1}... ")
        sys.stdout.flush()
        try:
            r = session.get(poll_url, timeout=30)
        except Exception as e:
            print(f"Exception: {e}", flush=True)
            continue
        if not r.ok:
            print(f"HTTP {r.status_code}", flush=True)
            continue
        data = r.json()
        status = data.get("status", "")
        print(f"status={status}", flush=True)
        if status in ("Succeeded", "Completed"):
            result_url = f"{API}/operations/{op_id}/result"
            try:
                rr = session.get(result_url, timeout=30)
                return rr.json() if rr.ok else data
            except:
                return data
        if status in ("Failed", "Cancelled"):
            print(f"  FAILED: {json.dumps(data)[:500]}", flush=True)
            return None
    print("  Timeout", flush=True)
    return None


def main():
    cred = AzureCliCredential()
    token = cred.get_token("https://api.fabric.microsoft.com/.default").token
    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {token}", "Content-Type": "application/json"})

    # ── Step 1: Get current TMDL definition ──
    print("=== Getting model definition (TMDL) ===")
    resp = session.post(f"{API}/workspaces/{WORKSPACE_ID}/semanticModels/{MODEL_ID}/getDefinition", timeout=30)
    print(f"  Status: {resp.status_code}")

    if resp.status_code == 200:
        definition = resp.json()
    elif resp.status_code == 202:
        definition = poll_lro(session, resp)
    else:
        print(f"  ERROR: {resp.text[:500]}")
        return

    if not definition:
        print("Failed to get definition")
        return

    parts = definition.get("definition", {}).get("parts", [])
    print(f"  Got {len(parts)} parts")

    # ── Step 2: Add descriptions to TMDL table files ──
    print("\n=== Adding descriptions to TMDL ===")
    total_stats = {"table": 0, "columns": 0, "measures": 0}
    updated_parts = []

    for part in parts:
        path = part["path"]
        if path.startswith("definition/tables/") and path.endswith(".tmdl"):
            # Decode TMDL content
            tmdl_text = base64.b64decode(part["payload"]).decode("utf-8")
            # Extract table name from filename
            table_name = path.split("/")[-1].replace(".tmdl", "")

            # Add descriptions
            new_tmdl, stats = add_tmdl_description(tmdl_text, table_name)

            total_stats["table"] += stats["table"]
            total_stats["columns"] += stats["columns"]
            total_stats["measures"] += stats["measures"]

            if stats["table"] + stats["columns"] + stats["measures"] > 0:
                print(f"  {table_name}: +{stats['table']}T +{stats['columns']}C +{stats['measures']}M")

            updated_parts.append({
                "path": path,
                "payload": b64_encode(new_tmdl),
                "payloadType": "InlineBase64",
            })
        else:
            # Keep other parts as-is
            updated_parts.append(part)

    print(f"\n  Total: +{total_stats['table']} tables, +{total_stats['columns']} columns, +{total_stats['measures']} measures")

    # ── Step 3: Push updated definition ──
    print("\n=== Pushing updated model ===")
    body = {"definition": {"parts": updated_parts}}
    resp = session.post(
        f"{API}/workspaces/{WORKSPACE_ID}/semanticModels/{MODEL_ID}/updateDefinition",
        json=body, timeout=30
    )
    print(f"  Status: {resp.status_code}")

    if resp.status_code == 200:
        print("  SUCCESS")
    elif resp.status_code == 202:
        result = poll_lro(session, resp)
        if result:
            print("  SUCCESS (LRO)")
        else:
            print("  FAILED (LRO)")
    else:
        print(f"  ERROR: {resp.text[:500]}")


if __name__ == "__main__":
    main()
