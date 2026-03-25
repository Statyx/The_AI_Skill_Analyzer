# The AI Skill Analyzer

End-to-end diagnostic and evaluation toolkit for **Fabric Data Agents** backed by Power BI semantic models.

**v3** — Multi-profile architecture: test multiple agents without changing code.

---

## 5-Minute Quick Start

```bash
# 1. Install
cd The_AI_Skill_Analyzer/
pip install -r requirements.txt

# 2. Edit config.yaml with your tenant ID
#    (look in Azure portal → Entra ID → Overview → Tenant ID)

# 3. Create a new profile (scaffolds template files for you)
python -m analyzer init my_agent

# 4. Edit the generated files:
#    profiles/my_agent/profile.yaml  → fill in your 4 IDs (see "Where to find your IDs" below)
#    profiles/my_agent/questions.yaml → customize questions for your model

# 5. Validate connectivity (checks auth, workspace, agent, model)
python -m analyzer -p my_agent validate

# 6. First run (no expected answers yet — just check the agent works)
python -m analyzer -p my_agent run

# 7. Review results
python -m analyzer -p my_agent analyze --latest

# 8. Fill in expected answers in questions.yaml for answers you trust
# 9. Re-run to get grading scores
python -m analyzer -p my_agent run
```

### Where to Find Your IDs

All 4 IDs are visible in the Fabric portal URL when you open each item:

| ID | Where to find it |
|----|------------------|
| **workspace_id** | Open your workspace → URL: `app.fabric.microsoft.com/groups/`**`<workspace_id>`**`/...` |
| **agent_id** | Open the Data Agent (AI Skill) → URL: `...aiskills/`**`<agent_id>`** |
| **semantic_model_id** | Open the semantic model → Settings → URL: `...datasets/`**`<model_id>`** |
| **semantic_model_name** | The display name of the model (e.g., `Sales_Model`) — used in reports only |
| **tenant_id** | Azure portal → Entra ID → Overview → **Tenant ID** |

> **Tip:** You can also get workspace items via REST: `GET https://api.fabric.microsoft.com/v1/workspaces/{ws}/items`

---

## Table of Contents

- [Installation](#installation)
- [Folder Structure](#folder-structure)
- [Configuration](#configuration)
  - [Global Config (config.yaml)](#global-config-configyaml)
  - [Profile Config (profile.yaml)](#profile-config-profileyaml)
  - [Config Resolution Order](#config-resolution-order)
- [Profiles](#profiles)
  - [Creating a New Profile](#creating-a-new-profile)
  - [Listing Profiles](#listing-profiles)
- [Commands](#commands)
  - [Global Options](#global-options)
  - [init — Scaffold a New Profile](#init--scaffold-a-new-profile)
  - [validate — Check Connectivity](#validate--check-connectivity)
  - [profiles — List Profiles](#profiles--list-profiles)
  - [snapshot — Cache Agent Config + Schema](#snapshot--cache-agent-config--schema)
  - [run — Batch Run + Grading](#run--batch-run--grading)
  - [rerun — Re-run Failed Questions](#rerun--re-run-failed-questions)
  - [analyze — Offline Analysis + RCA](#analyze--offline-analysis--rca)
  - [diff — Compare Two Runs](#diff--compare-two-runs)
- [Test Cases (questions.yaml)](#test-cases-questionsyaml)
  - [Match Types](#match-types)
  - [Tags](#tags)
- [Grading & Root Cause Analysis](#grading--root-cause-analysis)
  - [Verdicts](#verdicts)
  - [Root Cause Categories](#root-cause-categories)
  - [Pipeline Trace](#pipeline-trace)
- [Output Files](#output-files)
  - [Run Directory Structure](#run-directory-structure)
  - [batch_summary.json](#batch_summaryjson)
  - [Diagnostic JSON](#diagnostic-json)
  - [HTML Report](#html-report)
- [Authentication](#authentication)
- [Architecture](#architecture)
- [Module Reference](#module-reference)
- [Troubleshooting](#troubleshooting)
- [Legacy (v2) Support](#legacy-v2-support)
- [Run History (Marketing360_Agent)](#run-history-marketing360_agent)

---

## Installation

```bash
cd The_AI_Skill_Analyzer/
pip install -r requirements.txt
```

**Dependencies:**
- `azure-identity` >= 1.15.0 — Entra ID / browser authentication
- `requests` >= 2.31.0 — Fabric REST API calls
- `pyyaml` >= 6.0 — Config and test case parsing
- `fabric-data-agent-client` — Fabric SDK (installed separately to `%TEMP%`)

**Prerequisites:**
- Python >= 3.10
- Fabric capacity F2+ with Data Agent tenant settings enabled
- XMLA endpoints enabled on the workspace
- A published (or sandbox) Data Agent backed by a Power BI semantic model

---

## Folder Structure

```
The_AI_Skill_Analyzer/
├── config.yaml                    # Global settings (tenant, defaults, workers)
├── requirements.txt               # Python dependencies
├── .gitignore                     # Ignore runs/, snapshots/, debug/, *.txt
│
├── profiles/                      # One folder per Data Agent ← add new agents here
│   └── marketing360/
│       ├── profile.yaml           # Agent/workspace/model IDs + stage
│       └── questions.yaml         # Test cases + expected answers for this agent
│
├── analyzer/                      # Python package (run: python -m analyzer)
│   ├── __init__.py                # Package init (version = 3.0.0)
│   ├── __main__.py                # Entry point for python -m analyzer
│   ├── cli.py                     # CLI + argparse + command dispatch
│   ├── config.py                  # Config loading + profile resolution
│   ├── auth.py                    # Persistent Fabric auth (MSAL token cache)
│   ├── api.py                     # Fabric REST API helpers (GET, POST, LRO)
│   ├── init.py                    # Profile scaffolding (init command)
│   ├── validate.py                # Connectivity checks (validate command)
│   ├── snapshot.py                # Agent config + TMDL schema caching
│   ├── tmdl.py                    # TMDL definition parser
│   ├── grading.py                 # Answer comparison + pipeline trace + RCA
│   ├── runner.py                  # Parallel/serial execution + retry + Ctrl+C
│   └── reporting.py               # Save runs, HTML reports, diff, terminal output
│
├── knowledge_base/                # Reference documentation for analysis
│   ├── instructions.md            # Analysis rules, workflows, rubric
│   ├── known_issues.md            # Known issues catalog (DI/EV/PC/GP series)
│   ├── diagnostic_schema.md       # Diagnostic JSON schema reference
│   ├── evaluation_sdk.md          # Fabric evaluation SDK usage
│   ├── python_client_sdk.md       # Python client SDK + batch automation
│   └── semantic_model_best_practices.md  # Prep for AI guide
│
├── snapshots/<profile>/           # Cached agent config + schema (auto-managed)
│   ├── agent_config.json          # Agent definition + instructions
│   ├── schema.json                # TMDL-parsed tables, columns, measures
│   └── snapshot_meta.json         # When snapshot was taken + stats
│
├── runs/<profile>/<timestamp>/    # Batch run outputs with grading + RCA
│   ├── batch_summary.json         # Aggregated results + scores + RCA distribution
│   ├── test_cases.yaml            # Frozen test cases for reproducibility
│   ├── report.html                # Self-contained HTML report (if --html)
│   └── diagnostics/               # One JSON per question with full pipeline trace
│
├── scripts/                       # Legacy v2 scripts (still functional)
│   ├── analyzer.py                # Original monolithic CLI (backward compat)
│   ├── run_test.py                # Subprocess wrapper with tee-to-file
│   └── questions.yaml             # Legacy test cases
│
└── portal_exports/                # Raw diagnostic exports from Fabric portal
```

---

## Configuration

### Global Config (config.yaml)

Located at project root. Contains tenant-wide settings shared across all profiles.

```yaml
# Tenant ID (required — shared across all profiles)
tenant_id: "92701a21-ddea-4028-ba85-4c1f91fab881"

# Default profile (folder name under profiles/).
# Override per-run with: python -m analyzer --profile <name> run
# Set to null or remove to require explicit --profile flag.
default_profile: "marketing360"

# Snapshot cache TTL in hours.
# 0 = always refresh. Cached snapshots avoid ~15-30s Fabric API calls.
snapshot_ttl_hours: 24

# Max parallel questions per batch.
# Each question = 1 SDK call (~5-15s). Fabric may throttle above 4-5.
max_workers: 4

# Output directory for runs (relative to project root).
# Runs are saved under: <output_dir>/<profile>/<timestamp>/
output_dir: "runs"

# Stage: "sandbox" or "production"
stage: "sandbox"
```

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `tenant_id` | string | required | Microsoft Entra tenant ID |
| `default_profile` | string | `null` | Profile to use when `--profile` is not specified |
| `snapshot_ttl_hours` | int | `24` | Hours before snapshot is considered stale. `0` = always refresh |
| `max_workers` | int | `4` | Max parallel threads. Overridden to `1` by `--serial` |
| `output_dir` | string | `"runs"` | Root directory for run outputs |
| `stage` | string | `"sandbox"` | Agent stage: `"sandbox"` or `"production"` |

### Profile Config (profile.yaml)

Located at `profiles/<name>/profile.yaml`. Agent-specific connection details.

```yaml
# Required — identifies which Fabric agent to test
workspace_id: "5fa6b81d-fabe-4363-ad3d-b09ef82d16f2"
agent_id: "e92e5867-213a-4a7d-8fac-af1711046527"
semantic_model_id: "3d00aeaa-91b9-4567-9166-fa3fc8249e6f"
semantic_model_name: "Marketing360_Model"

# Optional — overrides global config.yaml value
stage: "sandbox"
```

| Key | Type | Required | Description |
|-----|------|----------|-------------|
| `workspace_id` | string | yes | Fabric workspace GUID |
| `agent_id` | string | yes | Data Agent (AI Skill) artifact GUID |
| `semantic_model_id` | string | yes | Backing Power BI semantic model GUID |
| `semantic_model_name` | string | yes | Human-readable model name (used for snapshot folder) |
| `stage` | string | no | Overrides global `stage`. `"sandbox"` or `"production"` |

Profile values **override** global values when both are set (e.g., a profile can set its own `max_workers`).

### Config Resolution Order

1. **`--profile` flag** → loads `profiles/<name>/profile.yaml` merged with `config.yaml`
2. **`default_profile` in config.yaml** → same as above
3. **Legacy mode** → uses `agent_id`, `workspace_id`, etc. directly from `config.yaml`

---

## Profiles

A **profile** = one Data Agent you want to test. Each profile is a folder under `profiles/` containing two files.

### Creating a New Profile

The fastest way is to use the `init` command:

```bash
python -m analyzer init sales_agent
```

This creates:
```
profiles/sales_agent/
├── profile.yaml     # Template with placeholder IDs + instructions
└── questions.yaml   # Starter questions (KPIs, counts, rankings)
```

Then:

**Step 1:** Edit `profiles/sales_agent/profile.yaml` — fill in your 4 IDs (comments explain where to find each one):

```yaml
workspace_id: "your-workspace-id"
agent_id: "your-agent-id"
semantic_model_id: "your-model-id"
semantic_model_name: "Sales_Model"
stage: "sandbox"
```

**Step 2:** Edit `profiles/sales_agent/questions.yaml` — customize questions for your model.

**Step 3:** Validate + run:

```bash
# Check connectivity (no questions sent)
python -m analyzer -p sales_agent validate

# First run (questions without expected → just check agent works)
python -m analyzer -p sales_agent run

# Review results
python -m analyzer -p sales_agent analyze --latest

# Fill in expected values in questions.yaml for answers you trust, then re-run
python -m analyzer -p sales_agent run
```

Or set it as default in `config.yaml`:
```yaml
default_profile: "sales_agent"
```

### Listing Profiles

```bash
python -m analyzer profiles
```

Output:
```
Available profiles:
  - marketing360 (default)
  - sales_agent
```

---

## Commands

### Invocation

```bash
python -m analyzer [--profile PROFILE] <command> [options]
```

Or via the wrapper script (captures output to file + terminal):

```bash
python scripts/run_test.py [--profile PROFILE] <command> [options]
```

### Global Options

| Option | Short | Description |
|--------|-------|-------------|
| `--profile NAME` | `-p NAME` | Profile to use. Overrides `default_profile` from config.yaml |
| `--help` | `-h` | Show help for any command |

---

### `init` — Scaffold a New Profile

Create a new profile directory with template `profile.yaml` and `questions.yaml` files.

```bash
python -m analyzer init <NAME>
```

| Argument | Description |
|----------|-------------|
| `NAME` | Profile name (e.g., `sales_agent`). Converted to lowercase with underscores |

**Does not require Fabric authentication.** Creates files only.

The generated `profile.yaml` includes inline comments explaining where to find each ID in the Fabric portal. The generated `questions.yaml` includes starter questions that work with most models (KPIs, counts, rankings, time intelligence).

**Example:**

```bash
python -m analyzer init hr_analytics
# → profiles/hr_analytics/profile.yaml   (template with REPLACE_ME placeholders)
# → profiles/hr_analytics/questions.yaml (5 starter questions)
```

---

### `validate` — Check Connectivity

Run pre-flight checks on a profile: config completeness, authentication, workspace access, agent existence, and semantic model existence.

```bash
python -m analyzer [-p PROFILE] validate
```

**No options.** Requires Fabric authentication (browser popup on first use).

**Checks performed:**

| Check | What it tests |
|-------|---------------|
| `config` | All required fields present and not `REPLACE_ME` |
| `questions` | `questions.yaml` loads correctly, shows question count |
| `auth` | Entra ID token acquisition works |
| `workspace` | `GET /workspaces/{id}` returns 200 + name |
| `agent` | `GET /workspaces/{ws}/items/{agent_id}` returns 200 |
| `model` | `GET /workspaces/{ws}/items/{model_id}` returns 200 |
| `sdk` | SDK client initializes successfully |

**Example output:**

```
============================================================
  VALIDATE: sales_agent
============================================================
  + config        All required fields present
  + questions     8 questions loaded (2 with expected answers)
  + auth          Token acquired (len=1847)
  + workspace     'Sales Workspace' (capacity: a1b2c3d4...)
  + agent         'Sales Agent' (type: AISkill)
  + model         'Sales_Model' (type: SemanticModel)
  + sdk           SDK client initialized
============================================================
  ALL CHECKS PASSED (7/7)

  Ready to run: python -m analyzer -p sales_agent run
============================================================
```

---

### `profiles` — List Profiles

List all available profiles found under `profiles/`.

```bash
python -m analyzer profiles
```

**No options.** Does not require Fabric authentication.

---

### `snapshot` — Cache Agent Config + Schema

Fetches the agent definition and semantic model schema (via TMDL) from Fabric and caches them locally. Subsequent `run` commands reuse the cache until it expires.

```bash
python -m analyzer snapshot
```

**No options.** Always forces a full refresh (ignores TTL).

**What it fetches:**
1. Agent metadata (`GET /workspaces/{ws}/items/{agent}`)
2. Agent definition (`POST .../getDefinition`) — instructions, system prompt, few-shots
3. Semantic model TMDL (`POST .../getDefinition?format=TMDL`) — tables, columns, measures, relationships, descriptions

**Saved to:** `snapshots/<profile>/` (3 files: `agent_config.json`, `schema.json`, `snapshot_meta.json`)

**When you need this:** After changing agent instructions, model schema, or measure definitions. The `run` command auto-refreshes if snapshot is older than `snapshot_ttl_hours`.

---

### `run` — Batch Run + Grading

Run all questions from the profile's `questions.yaml`, grade each answer, trace the agent pipeline, identify root causes for failures.

```bash
python -m analyzer run [--refresh] [--serial] [--tag TAG] [--html] [--dry-run]
```

| Option | Description |
|--------|-------------|
| `--refresh` | Force-refresh the snapshot before running (even if cached) |
| `--serial` | Run questions one at a time instead of parallel. Sets `max_workers=1` |
| `--tag TAG` | Run only questions that have this tag in their `tags` list |
| `--html` | Generate a self-contained HTML report alongside the JSON output |
| `--dry-run` | Validate config + loaded questions, print summary, but **do not call Fabric** |

**Workflow:**
1. Load profile config + test cases
2. Check/refresh snapshot (agent config + schema)
3. Send each question to the agent via SDK (`get_run_details`)
4. Grade each answer against expected values
5. Trace the agent's tool call pipeline
6. Identify root cause for failures
7. Save results to `runs/<profile>/<timestamp>/`

**Examples:**

```bash
# Default: parallel run, all questions, default profile
python -m analyzer run

# Specific profile, sequential, only KPI questions, with HTML
python -m analyzer -p marketing360 run --serial --tag kpi --html

# Check everything is wired correctly without hitting Fabric
python -m analyzer run --dry-run

# Force snapshot refresh (after changing model/instructions)
python -m analyzer run --refresh
```

**Output (terminal):**
```
========================================================================
  THE AI SKILL ANALYZER -- BATCH RUN + GRADING
========================================================================
  Profile  : marketing360
  Agent    : e92e5867-213a-4a7d-8fac-af1711046527
  Model    : Marketing360_Model
  Questions: 8  (2 with expected answers)
  Workers  : 4
  Stage    : sandbox
========================================================================

[1/4] Using cached snapshot (< 24h old)
[2/4] Running questions...
  + [1/8] (12.3s) what is the churn rate
  + [2/8] (8.7s) what is the total revenue for 2025
  ...

[3/4] Grading answers + root cause analysis...

[4/4] Results

========================================================================
  Run ID : 20260325_143022
  Profile: marketing360
  Score  : 2/2 = 100%
  + Pass: 2  X Fail: 0  ? Ungraded: 6  | 24.5s
========================================================================
  + Q1 [12.3s] what is the churn rate
  ? Q2 [8.7s] what is the total revenue for 2025
  ...

  Full analysis: python -m analyzer analyze 20260325_143022
```

**Resilience features:**
- **Retry with backoff:** Transient errors (HTTP 429, 503, timeouts) are retried up to 2 times with exponential delay
- **Graceful Ctrl+C:** Keyboard interrupt saves all completed results instead of losing the run

---

### `rerun` — Re-run Failed Questions

Re-run specific questions (or all failures) from a previous run. Creates a new run with its own timestamp.

```bash
python -m analyzer rerun <RUN_ID> [--questions Q1 Q2 ...] [--html]
```

| Argument / Option | Description |
|-------------------|-------------|
| `RUN_ID` | Timestamp folder name (e.g., `20260325_143022`) or `--latest` |
| `--questions N ...` | Space-separated question indices (1-based) to re-run. If omitted, re-runs all failed questions + errors |
| `--html` | Generate HTML report for the rerun |

**Examples:**

```bash
# Re-run all failures from the latest run
python -m analyzer rerun --latest

# Re-run questions 3 and 5 from a specific run
python -m analyzer rerun 20260325_143022 --questions 3 5

# Re-run Q1 with HTML report
python -m analyzer rerun 20260325_143022 --questions 1 --html
```

**Behavior:**
- Loads `batch_summary.json` from the specified run
- If `--questions` is not specified, selects questions with verdict `fail` or status `error`
- Uses current `questions.yaml` for grading (so updated expected values are applied)
- Creates a completely new run directory with fresh diagnostics

---

### `analyze` — Offline Analysis + RCA

Analyze an existing run with detailed grading breakdown and root cause analysis. **Does not connect to Fabric** — works entirely from saved JSON files.

```bash
python -m analyzer analyze [RUN_ID] [--latest] [--html]
```

| Argument / Option | Description |
|-------------------|-------------|
| `RUN_ID` | Timestamp folder name. Optional if `--latest` is used |
| `--latest` | Analyze the most recent run for the current profile |
| `--html` | Generate a self-contained HTML report |

**Examples:**

```bash
# Analyze the latest run
python -m analyzer analyze --latest

# Analyze a specific run + generate HTML
python -m analyzer analyze 20260325_143022 --html

# Analyze latest for a different profile
python -m analyzer -p sales_agent analyze --latest
```

**Terminal output includes:**
- Header: profile, agent, model, stage, wall time, schema stats
- Scoreboard: pass / fail / ungraded counts + score percentage
- Per-question detail: question, tools used, answer, expected, verdict
- Root cause detail for failures: category, explanation, generated query, result preview
- Root cause summary: distribution of failure categories
- Recommendations: suggested next steps based on failures

---

### `diff` — Compare Two Runs

Compare two runs side-by-side showing verdict changes, score deltas, and root cause shifts. Useful for regression testing after model or instruction changes.

```bash
python -m analyzer diff <RUN_A> <RUN_B>
```

| Argument | Description |
|----------|-------------|
| `RUN_A` | First run ID (timestamp) |
| `RUN_B` | Second run ID (timestamp) |

**Example:**

```bash
python -m analyzer diff 20260324_202817 20260325_143022
```

**Output:**
```
========================================================================
  DIFF: 20260324_202817  vs  20260325_143022
========================================================================
  Score: 50% -> 100%
  Wall : 24.5s -> 22.1s
  Qs   : 8 -> 8

------------------------------------------------------------------------
  VERDICT CHANGES (1):
    [FIXED] fail -> pass: how many active customers do we have

------------------------------------------------------------------------
  ROOT CAUSE CHANGES:
    SYNTHESIS: 1 -> 0
========================================================================
```

**Change labels:**
- `FIXED` — was fail, now pass
- `REGRESSED` — was pass, now fail
- `CHANGED` — verdict changed (e.g., `no_expected` → `pass`)
- `NEW` — question only in run B
- `REMOVED` — question only in run A

---

## Test Cases (questions.yaml)

Located at `profiles/<profile>/questions.yaml`. Defines what questions to ask the agent and how to grade the answers.

```yaml
test_cases:
  - question: "how many active customers do we have"
    expected: "18016"
    match_type: "numeric"
    tolerance: 50
    tags: ["customers", "time_intelligence"]

  - question: "what is the churn rate"
    expected: ~            # null = no grading, manual review
    match_type: "contains"
    tags: ["kpi", "measures"]
```

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `question` | string | yes | — | Natural language question sent to the agent |
| `expected` | string/number/list/null | no | `null` | Expected answer. `null` or `~` = no grading |
| `match_type` | string | no | `"contains"` | How to compare actual vs expected |
| `tolerance` | number | no | `0` | Absolute tolerance for `numeric` match type |
| `tags` | list[string] | no | `[]` | Labels for filtering (`--tag`) and grouping |

### Match Types

| Type | Comparison Logic | Example |
|------|-----------------|---------|
| `contains` | `expected` appears anywhere in the answer (case-insensitive) | expected: `"churn"` matches `"The churn rate is 5%"` |
| `numeric` | Extracts all numbers from the answer, checks if any is within `±tolerance` of `expected` | expected: `18016`, tolerance: `50` matches `"We have 18,016 active customers"` |
| `exact` | Exact string match after lowercasing and trimming | expected: `"yes"` matches `"Yes"` but not `"Yes, we do"` |
| `regex` | Python regex pattern match (case-insensitive) | expected: `"\\d+\\.\\d+%"` matches `"The rate is 5.2%"` |
| `any_of` | `expected` is a list; any item found in the answer counts as pass | expected: `["high", "elevated"]` matches `"Risk is high"` |

### Tags

Tags are free-form labels. Use them to:
- **Filter runs:** `python -m analyzer run --tag kpi` runs only questions tagged `"kpi"`
- **Group in reports:** tags appear in the HTML report and analysis output
- **Organize:** common tags: `kpi`, `measures`, `time_intelligence`, `ranking`, `segmentation`

---

## Grading & Root Cause Analysis

### Verdicts

Every question receives one of three verdicts:

| Verdict | Icon | Meaning |
|---------|------|---------|
| `pass` | `+` | Agent answer matches the expected value within the match rules |
| `fail` | `X` | Answer does not match — root cause analysis is generated |
| `no_expected` | `?` | No expected answer defined (`expected: ~`) — manual review needed |

### Root Cause Categories

When a question **fails**, the analyzer traces the agent's internal pipeline and classifies the root cause:

| Category | Description | Suggested Fix |
|----------|-------------|---------------|
| `AGENT_ERROR` | Agent returned an error or non-completed status | Check agent health, retry |
| `QUERY_ERROR` | Generated DAX/SQL failed to execute (syntax, missing column) | Fix model relationships or column visibility |
| `EMPTY_RESULT` | Query succeeded but returned no data or empty result | Check data freshness, filter defaults |
| `FILTER_CONTEXT` | Unexpected filter applied (time intelligence auto-filter, TREATAS) | Disable `__PBI_TimeIntelligenceEnabled` or add `REMOVEFILTERS` |
| `MEASURE_SELECTION` | Wrong measure referenced in the generated query | Improve measure descriptions or add verified answers |
| `RELATIONSHIP` | Wrong join path or missing relationship traversal | Fix relationship direction (Many→One), refresh model |
| `REFORMULATION` | Agent misunderstood the question — wrong entities or intent | Add verified answers, rephrase question |
| `SYNTHESIS` | Data was correct but the answer was misinterpreted or truncated | Inspect generated DAX in diagnostic JSON |
| `UNKNOWN` | Cannot determine root cause from available pipeline data | Open diagnostic JSON for manual inspection |

### Pipeline Trace

For every question, the analyzer records the full tool call chain:

```
NL_TO_QUERY → QUERY_EXECUTION → ANSWER_SYNTHESIS
```

Each step includes:
- **Stage:** `NL_TO_QUERY`, `DAX_EXECUTION`, `SQL_EXECUTION`, `QUERY_EXECUTION`, `ANSWER_SYNTHESIS`, `TOOL_CALL`
- **Tool name:** `nl2sa_query`, `evaluate_dax`, `evaluate_sql`, `message_creation`, etc.
- **Arguments:** reformulated question, generated DAX/SQL query
- **Output:** query result preview, error messages
- **Duration:** per-step timing in seconds
- **Error:** any error returned by the step

---

## Output Files

### Run Directory Structure

Each run creates a timestamped folder under `runs/<profile>/`:

```
runs/marketing360/20260325_143022/
├── batch_summary.json              # Aggregated results + grading stats
├── test_cases.yaml                 # Frozen copy of questions used in this run
├── report.html                     # Self-contained HTML report (if --html)
└── diagnostics/
    ├── full_diag_what_is_the_churn_rate.json
    ├── full_diag_how_many_active_customers_do_we_have.json
    └── ...                         # One file per question
```

### batch_summary.json

Top-level fields:

| Field | Description |
|-------|-------------|
| `timestamp` | Run ID (YYYYMMDD_HHMMSS) |
| `profile` | Profile name |
| `agent_id` | Data Agent GUID |
| `model_id` | Semantic model GUID |
| `model_name` | Human-readable model name |
| `stage` | `"sandbox"` or `"production"` |
| `schema_stats` | Table/column/measure/relationship counts + description coverage |
| `total_wall_seconds` | End-to-end wall clock time |
| `max_workers` | Parallelism used |
| `interrupted` | `true` if run was interrupted by Ctrl+C |
| `total_questions` | Number of questions attempted |
| `passed` | Number with agent status `completed` |
| `failed` | Number with agent status not `completed` |
| `grading.pass` | Number of questions with verdict `pass` |
| `grading.fail` | Number with verdict `fail` |
| `grading.ungraded` | Number with verdict `no_expected` |
| `grading.score_pct` | Pass rate: `pass / (pass + fail) * 100` (null if no graded questions) |
| `grading.root_cause_distribution` | Map of RCA category → count (e.g., `{"SYNTHESIS": 1}`) |
| `results` | Array of per-question result objects |

### Diagnostic JSON

Each question produces a `full_diag_<question_slug>.json` file containing:

| Section | Content |
|---------|---------|
| `config` | Agent definition (instructions, system prompt) |
| `datasources` | Schema snapshot (tables, columns, measures, relationships) |
| `thread.question` | Original question text |
| `thread.messages` | Full message exchange (user + assistant) |
| `thread.run_steps` | Raw SDK run steps with tool calls |
| `timing` | Per-step timing breakdown |
| `grading.verdict` | `pass` / `fail` / `no_expected` |
| `grading.expected` | Expected answer from questions.yaml |
| `grading.actual_answer` | Agent's answer (first 300 chars) |
| `grading.compare_detail` | Explanation of comparison result |
| `grading.root_cause` | RCA category (if failed) |
| `grading.root_cause_detail` | Human-readable explanation |
| `grading.artifacts` | Reformulated question, generated DAX, result preview |
| `grading.pipeline_trace` | Ordered array of pipeline steps |

### HTML Report

Generated when `--html` is passed to `run`, `rerun`, or `analyze`. Saved as `report.html` in the run directory. Self-contained (no external CSS/JS) — can be shared via email or Teams.

Features:
- Score badges (pass / fail / ungraded)
- Per-question cards with verdict, answer, expected, tools, and root cause
- Root cause summary table
- Schema coverage stats

---

## Authentication

The analyzer uses a **persistent MSAL token cache** to avoid repeated browser popups.

- **First run:** A browser window opens for interactive Entra ID login. The token is cached to disk under the name `"ai_skill_analyzer"`.
- **Subsequent runs:** The cached token is reused silently. No browser popup.
- **Token lifetime:** Access tokens last ~1 hour, refresh tokens ~24 hours. After expiry, the browser popup reappears once.
- **Fallback:** If the OS keyring is unavailable, the cache falls back to unencrypted file storage (`allow_unencrypted_storage=True`).

The SDK client is initialized by injecting the pre-authenticated credential, bypassing the SDK's own browser authentication. This ensures a single token is shared across all REST API calls and SDK operations.

---

## Architecture

```
┌─────────────────────────────────────────────────────┐
│  PROFILES (config — no code changes)                │
│  profiles/<name>/profile.yaml   → agent IDs         │
│  profiles/<name>/questions.yaml → test cases        │
└──────────────────────┬──────────────────────────────┘
                       ↓
┌─────────────────────────────────────────────────────┐
│  SNAPSHOT (once / on change)                        │
│  ┌─────────────┐  ┌──────────────────┐              │
│  │ Agent Config │  │ Schema (TMDL)    │  → disk cache│
│  │ (REST API)   │  │ (REST + LRO)     │  snapshots/  │
│  └─────────────┘  └──────────────────┘              │
└──────────────────────┬──────────────────────────────┘
                       ↓ read from cache
┌─────────────────────────────────────────────────────┐
│  RUN (parallel or serial)                           │
│  ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐                   │
│  │ Q1  │ │ Q2  │ │ Q3  │ │ Q4  │  ThreadPoolExecutor│
│  │ SDK │ │ SDK │ │ SDK │ │ SDK │  max_workers=4     │
│  └──┬──┘ └──┬──┘ └──┬──┘ └──┬──┘  + retry + Ctrl+C │
│     └───────┴───────┴───────┘                       │
│              ↓                                      │
│  GRADE each answer vs questions.yaml                │
│  ├─ Compare: exact / contains / numeric / regex     │
│  ├─ Trace pipeline: NL→Query→Execute→Synthesize     │
│  └─ RCA: identify WHY the answer was wrong          │
│              ↓ combine with cached snapshot          │
│  ┌──────────────────────────────────┐               │
│  │ diagnostics/ + batch_summary.json│  → runs/<p>/  │
│  │ report.html (if --html)          │               │
│  └──────────────────────────────────┘               │
└──────────────────────┬──────────────────────────────┘
                       ↓ offline
┌─────────────────────────────────────────────────────┐
│  ANALYZE (no Fabric connection needed)              │
│  Read batch_summary.json → terminal report + HTML   │
│  RERUN → selective re-run of failed Qs              │
│  DIFF → compare two runs side-by-side               │
└─────────────────────────────────────────────────────┘
```

**Performance:**

| Approach | 8 Questions | Config/Schema Fetch |
|----------|-------------|---------------------|
| v1 (`full_diagnostic.py`) | ~80-120s serial | Every run (~15-30s) |
| v2 (`scripts/analyzer.py`) | ~20-30s parallel (4 workers) | Cached (0s if fresh) |
| **v3** (`python -m analyzer`) | ~20-30s parallel + retry | Cached + per-profile |

---

## Module Reference

| Module | Purpose |
|--------|---------|
| `analyzer/config.py` | Load `config.yaml`, resolve profiles, load test cases from `questions.yaml` |
| `analyzer/auth.py` | `FabricSession` class: persistent MSAL cache, SDK client init, token refresh |
| `analyzer/api.py` | `fabric_get()`, `fabric_post()`, LRO polling (30 attempts, 2s interval) |
| `analyzer/init.py` | `scaffold_profile()` — creates template `profile.yaml` + `questions.yaml` |
| `analyzer/validate.py` | `validate_profile()` — 7 connectivity checks (config, auth, workspace, agent, model, SDK) |
| `analyzer/tmdl.py` | Parse TMDL definition parts into tables, columns, measures, relationships with `///` doc comments |
| `analyzer/snapshot.py` | Take / load / check freshness of cached agent config + schema |
| `analyzer/grading.py` | `_compare_answer()` (5 match types), `trace_pipeline()`, `identify_root_cause()` (8 categories), `grade_result()` |
| `analyzer/runner.py` | `run_questions_parallel()`, `run_questions_serial()`, retry on 429/503/timeout, Ctrl+C saves partial |
| `analyzer/reporting.py` | `save_run()`, `analyze_run()`, `generate_html_report()`, `diff_runs()`, `find_run_dir()` |
| `analyzer/cli.py` | argparse setup, 8 commands: `init`, `validate`, `profiles`, `snapshot`, `run`, `rerun`, `analyze`, `diff` |

---

## Troubleshooting

### Browser popup doesn't appear / Authentication fails

- Check that `tenant_id` in `config.yaml` is correct (Azure portal → Entra ID → Overview)
- Try clearing the MSAL cache: delete `%LOCALAPPDATA%\.IdentityService\` or `~/.IdentityService/`
- Ensure your account has access to the Fabric workspace

### "Workspace not found" (404)

- Run `python -m analyzer -p <profile> validate` — the workspace check will show the exact error
- Verify `workspace_id` is a GUID, not the workspace name
- Check you have at least Viewer role on the workspace

### "Agent not found" (404)

- The `agent_id` is the **item ID**, not the AI Skill name
- Open the Data Agent in Fabric portal and copy the GUID from the URL
- Ensure the Data Agent is created (not just the semantic model)

### "XMLA endpoint not enabled"

- Fabric Admin Portal → Tenant Settings → Integration Settings → "Allow XMLA endpoints..."
- Requires F2+ capacity (PPU doesn't support XMLA)

### "Capacity paused" / Slow responses

- Fabric capacity may be paused or throttled — check app.fabric.microsoft.com
- Try `--serial` to reduce parallel load: `python -m analyzer run --serial`

### Questions return wrong answers

1. Run `python -m analyzer analyze --latest` — check the root cause category
2. Most common fixes by root cause:
   - **MEASURE_SELECTION**: Improve measure descriptions in the semantic model
   - **FILTER_CONTEXT**: Disable `__PBI_TimeIntelligenceEnabled` or add `REMOVEFILTERS`
   - **RELATIONSHIP**: Check Many-to-One direction, run Calculate refresh
   - **REFORMULATION**: Add verified answers to the Data Agent
3. After fixing, run `python -m analyzer run --refresh` to pick up schema changes

### First run — what to expect

1. **All questions will be "ungraded"** (no expected values) — this is normal
2. Review the answers: `python -m analyzer analyze --latest`
3. For answers that look correct, copy the value into `questions.yaml` as `expected`
4. Re-run: now you get pass/fail grading + root cause analysis for failures

---

## Legacy (v2) Support

The original monolithic `scripts/analyzer.py` (v2) still works and is independent of the `analyzer/` package. It reads `config.yaml` directly (no profiles).

```bash
# Legacy invocation (still works)
python scripts/analyzer.py run
python scripts/analyzer.py analyze --latest

# Via wrapper (captures output to run_output.txt)
python scripts/run_test.py run --serial
```

The `run_test.py` wrapper now calls the v3 package (`python -m analyzer`) by default. Legacy runs saved in `runs/<timestamp>/` (flat) are also found by the `analyze` and `diff` commands.

---

## Run History (Marketing360_Agent)

| Run | Timestamp | Questions | Passed | Notes |
|-----|-----------|-----------|--------|-------|
| **7** | `20260324_202817` | 8 | **8/8** | Final — all fixes applied, Calculate refresh done |
| 6 | `20260324_201734` | 8 | 8/8 | Intermediate — relationship hydrated |
| 5 | `20260324_201601` | 8 | 6/8 | Q4+Q5 fail — relationship not yet hydrated |
| 4 | `20260324_201519` | 8 | 6/8 | Post-relationship fix, pre-Calculate refresh |
| 3 | `20260324_201325` | 2 | 2/2 | Debug: churn_rate + revenue only |
| 2 | `20260324_195159` | 1 | 1/1 | Debug: churn_rate only |
| 1b | `20260324_194852` | 1 | 1/1 | Debug: churn_rate only |
| **1** | `20260324_193250` | 8 | **6/8** | First full run — exposed relationship bug |

### Key Fixes Applied Between Run 1 and Run 7

1. **Reversed relationship**: `marketing_events[send_id] → marketing_sends[send_id]` (was backwards)
2. **Calculate refresh**: Hydrated new relationship metadata on DirectLake model
3. **15 verified answers** (was 12): added active customers, top campaigns, segment churn
4. **CopilotInstructions**: Added critical disambiguation rules
5. **Measure descriptions**: Updated `[Active Customers]` to explicitly say no date filter

---

## Related Resources

| Resource | Location |
|----------|----------|
| Github Brain (full agent KB) | `../Github_Brain/agents/ai-skills-analysis-agent/` |
| Fabric RTI Demo (infra scripts) | `../Fabric RTI Demo/` |
| Semantic Model Agent KB | `../Github_Brain/agents/semantic-model-agent/` |
