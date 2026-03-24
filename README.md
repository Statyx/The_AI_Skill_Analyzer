# The AI Skill Analyzer

End-to-end diagnostic and evaluation toolkit for **Fabric Data Agents** backed by Power BI semantic models.

---

## Folder Structure

```
The_AI_Skill_Analyzer/
├── config.yaml                    # Connection details + settings (edit this)
├── knowledge_base/                # Agent knowledge base (reference docs)
│   ├── instructions.md            # Analysis rules, workflows, rubric
│   ├── known_issues.md            # All known issues (DI/EV/PC/GP series)
│   ├── diagnostic_schema.md       # Diagnostic JSON schema reference
│   ├── evaluation_sdk.md          # Fabric evaluation SDK usage
│   ├── python_client_sdk.md       # Python client SDK + batch automation
│   ├── semantic_model_best_practices.md  # Prep for AI guide + annotations
│   └── README.md                  # Knowledge base overview
│
├── scripts/                       # Automation scripts
│   ├── analyzer.py                # Main CLI tool (snapshot/run/rerun/analyze + grading + RCA)
│   ├── questions.yaml             # Test cases: questions + expected answers + tags
│   ├── questions.txt              # Legacy question list (fallback if no YAML)
│   ├── full_diagnostic.py         # Legacy single-file runner (deprecated)
│   └── query_agent.py             # Single-question query tool
│
├── snapshots/                     # Cached agent config + schema (auto-managed)
│   └── Marketing360_Model/
│       ├── agent_config.json      # Agent definition + instructions
│       ├── schema.json            # TMDL-parsed tables, columns, measures
│       └── snapshot_meta.json     # When snapshot was taken
│
├── runs/                          # Batch run outputs (by timestamp)
│   └── <YYYYMMDD_HHMMSS>/ + grading + RCA
│       ├── test_cases.yaml        # Frozen test cases for reproducibility
│       └── diagnostics/           # Per-question diagnostic JSONs (with verdicts)oducibility
│       └── diagnostics/           # Per-question diagnostic JSONs
│
├── portal_exports/                # Raw diagnostic exports from Fabric portal
│
└── debug/                         # Scratch/debug artifacts
```

---

## Quick Start

### Prerequisites

- Python >= 3.10
- Fabric capacity F2+ with Data Agent tenant settings enabled
- XMLA endpoints enabled
- A published (or sandbox) Data Agent backed by a Power BI semantic model

### Run a Batch Diagnostic

```bash
cd The_AI_Skill_Analyzer/

# 1. Edit config.yaml with your IDs (tenant, workspace, agent, model)

# 2. Define expected answers in scripts/questions.yaml
#    (questions without expected answers will still run but won't be graded)

# 3. Install dependencies
pip install azure-identity fabric-data-agent-client requests pyyaml

# 4. Take a snapshot (fetches agent config + schema, caches locally)
python scripts/analyzer.py snapshot

# 5. Run all questions — grade answers — trace pipeline — identify root causes
python scripts/analyzer.py run

# 6. Run only questions tagged 'kpi'
python scripts/analyzer.py run --tag kpi

# 7. Force-refresh snapshot + run
python scripts/analyzer.py run --refresh

# 8. Re-run only failed questions from a previous batch
python scripts/analyzer.py rerun 20260324_202817

# 9. Re-run specific questions by index
python scripts/analyzer.py rerun 20260324_202817 --questions 3 5

# 10. Analyze an existing run with RCA (OFFLINE — no Fabric connection needed)
python scripts/analyzer.py analyze 20260324_202817
python scripts/analyzer.py analyze --latest
```

### Grading & Root Cause Analysis

Every question is graded against an expected answer defined in `questions.yaml`:

| Verdict | Meaning |
|---------|---------|
| **pass** | Agent answer matches expected value |
| **fail** | Answer doesn't match — root cause analysis provided |
| **no_expected** | No expected answer defined — manual review needed |

When a question **fails**, the analyzer traces the agent's internal pipeline and identifies the root cause:

| Root Cause | Description | Fix |
|------------|-------------|-----|
| `AGENT_ERROR` | Agent itself errored or timed out | Check agent health, retry |
| `QUERY_ERROR` | Generated DAX/SQL failed to execute | Fix model relationships or column visibility |
| `EMPTY_RESULT` | Query ran but returned no data | Check data freshness, filter defaults |
| `FILTER_CONTEXT` | Unexpected filter applied (time intelligence, etc.) | Disable `__PBI_TimeIntelligenceEnabled` or add REMOVEFILTERS |
| `MEASURE_SELECTION` | Wrong measure used in the query | Improve measure descriptions or verified answers |
| `RELATIONSHIP` | Wrong join path / missing relationship | Fix relationship direction (Many→One) |
| `REFORMULATION` | Agent couldn't understand the question | Add verified answers, rephrase question |
| `SYNTHESIS` | Data correct but answer formatted wrong | Inspect generated DAX in diagnostic JSON |

### Performance Comparison

| Approach | 8 Questions | Config/Schema Fetch |
|----------|-------------|---------------------|
| **Old** (`full_diagnostic.py`) | ~80-120s serial | Every run (~15-30s) |
| **New** (`analyzer.py`) | ~20-30s parallel (4 workers) | Cached (0s if fresh) |

### Architecture

```
┌─────────────────────────────────────────────────────┐
│  SNAPSHOT (once / on change)                        │
│  ┌─────────────┐  ┌──────────────────┐              │
│  │ Agent Config │  │ Schema (TMDL)    │  → disk cache│
│  │ (REST API)   │  │ (REST + LRO)     │  snapshots/  │
│  └─────────────┘  └──────────────────┘              │
└─────────────────────────────────────────────────────┘
                    ↓ read from cache
┌─────────────────────────────────────────────────────┐
│  RUN (parallel)                                     │
│  ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐                   │
│  │ Q1  │ │ Q2  │ │ Q3  │ │ Q4  │  ThreadPoolExecutor│
│  │ SDK │ │ SDK │ │ SDK │ │ SDK │  max_workers=4     │
│  └──┬──┘ └──┬──┘ └──┬──┘ └──┬──┘                   │
│     └───────┴───────┴───────┘                       │
│              ↓                                      │
│  GRADE each answer vs questions.yaml                │
│  ├─ Compare: exact / contains / numeric / regex     │
│  ├─ Trace pipeline: reformulation → query → result  │
│  └─ RCA: identify WHY the answer was wrong          │
│              ↓ combine with cached snapshot          │
│  ┌──────────────────────────────────┐               │
│  │ diagnostics/ + batch_summary.json│  → runs/ts/   │
│  └──────────────────────────────────┘               │
└─────────────────────────────────────────────────────┘
                    ↓ offline
┌─────────────────────────────────────────────────────┐
│  ANALYZE (no connection)                            │
│  Read batch_summary.json → print graded report      │
│  Show per-question: expected vs actual + root cause │
│  RERUN (selective) → re-run only failed questions   │
└─────────────────────────────────────────────────────┘
```

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
