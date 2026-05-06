# The AI Skill Analyzer

Test and grade **Microsoft Fabric Data Agents** automatically. Ask questions, compare answers to expected values, get a score and actionable fixes.

![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue?style=flat-square)
![Fabric Data Agents](https://img.shields.io/badge/Fabric-Data_Agents-742774?style=flat-square)
![License MIT](https://img.shields.io/badge/license-MIT-green?style=flat-square)

## What It Does

1. **Sends questions** to your Fabric Data Agent (via the Assistants API)
2. **Grades answers** against expected values (5 match types: exact, numeric, contains, regex, semantic)
3. **Diagnoses failures** with root cause analysis (wrong measure, query error, empty result, synthesis error...)
4. **Generates an action plan** (fix instructions, add few-shots, create measures)

## Quick Start

```bash
# Install
git clone https://github.com/Statyx/The_AI_Skill_Analyzer.git
cd The_AI_Skill_Analyzer
pip install -r requirements.txt

# Configure
cp config.yaml.example config.yaml
# Edit config.yaml → set your tenant_id

# Create your first profile
python -m analyzer init my_agent
# Edit profiles/my_agent/profile.yaml → set workspace_id, agent_id, semantic_model_id
# Edit profiles/my_agent/questions.yaml → write your test questions

# Validate connectivity
python -m analyzer -p my_agent validate

# Run
python -m analyzer -p my_agent run
```

## Where to Find Your IDs

| ID | Where |
|----|-------|
| `tenant_id` | Azure portal → Entra ID → Overview |
| `workspace_id` | Fabric URL: `app.fabric.microsoft.com/groups/<this>/...` |
| `agent_id` | Open the Data Agent → URL contains the ID |
| `semantic_model_id` | Open the semantic model → Settings → URL |

## Profile Structure

Each agent gets a folder under `profiles/`:

```text
profiles/my_agent/
  profile.yaml      # Connection: workspace_id, agent_id, model_id
  questions.yaml    # Test cases with expected answers
  instructions.md   # (optional) Agent instructions for reference
  fewshots.json     # (optional) Few-shot examples for reference
```

### questions.yaml format

```yaml
test_cases:
  - question: "What is the total revenue?"
    expected: "31200000"
    match_type: "numeric"     # exact | numeric | contains | regex | semantic
    tolerance: 5              # % tolerance (numeric only)
    tags: ["kpi", "revenue"]
```

## Commands

| Command | What it does |
|---------|-------------|
| `init <name>` | Create a new profile with template files |
| `validate` | Check auth + workspace + agent + model connectivity |
| `snapshot` | Cache agent config and model schema locally |
| `run` | Run all questions, grade, produce diagnostics |
| `rerun` | Re-run only failed questions from a previous run |
| `analyze` | Offline analysis of an existing run (RCA + action plan) |
| `diff` | Compare two runs side by side |
| `profiles` | List available profiles |

All commands accept `-p <profile>` to select which agent to test.

## Output

Each run creates a timestamped folder:

```text
runs/my_agent/20260506_143022/
  batch_summary.json          # Score, pass/fail counts, timing
  diagnostics/                # Per-question JSON with full trace
    Q1_full_diag_*.json
    Q2_full_diag_*.json
```

## Authentication

The analyzer uses **Azure CLI credentials** (`az login`). Make sure you're logged into the right tenant:

```bash
az login --tenant <your-tenant-id>
```

The Data Agent API requires `Item.ReadWrite.All` scope and at least Contributor role on the workspace.

## Typical Workflow

```text
1. Create profile → init
2. Write 5-10 questions → questions.yaml
3. First run (no expected answers) → run → see what the agent returns
4. Fill in expected values for correct answers → questions.yaml
5. Re-run → get a score (e.g. 8/10 = 80%)
6. Read the action plan → fix instructions / add measures / add few-shots
7. Redeploy agent → re-run → compare with diff
8. Iterate until score is stable
```

## Configuration

### config.yaml

```yaml
tenant_id: "<your-entra-tenant-id>"
default_profile: "my_agent"
snapshot_ttl_hours: 24
max_workers: 4
output_dir: "runs"
```

### profile.yaml

```yaml
workspace_id: "<fabric-workspace-id>"
agent_id: "<data-agent-id>"
semantic_model_id: "<semantic-model-id>"
semantic_model_name: "My_Model"
stage: "production"           # or "sandbox" for draft agents
```

## Troubleshooting

| Problem | Fix |
|---------|-----|
| Browser popup doesn't appear | Run `az login` manually first |
| "Workspace not found" (404) | Check workspace_id, ensure Contributor access |
| "Agent not found" (404) | Check agent_id, ensure agent is published (not draft-only) |
| Slow responses / timeouts | Fabric capacity may be paused — resume it in the portal |
| Wrong answers | Check agent instructions, add few-shot examples, verify measure names |

## Contributing

1. Fork the repo
2. Create a profile for your agent
3. Run tests, iterate on quality
4. PRs welcome for core analyzer improvements

## License

MIT
