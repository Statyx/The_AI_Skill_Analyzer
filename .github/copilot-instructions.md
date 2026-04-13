# Copilot Instructions — The AI Skill Analyzer

## Mandatory Testing Gate

Before running ANY analysis, grading, or generation command:
```bash
python -m pytest tests/ -v --tb=short
```
If ANY test fails → **STOP. Fix the code first. Do not proceed.**

## Project Context

- Python 3.12, Windows
- CLI tool: `python -m analyzer`
- 81 existing tests (test_grading.py + test_generate.py)
- Grading pipeline: question → Data Agent → DAX → answer → grade
- Profiles in `profiles/`, results in `results/`, snapshots in `snapshots/`

## Known Pitfalls

- Data Agent thread pollution: DELETE thread before each question
- executeQueries: use `api.powerbi.com` NOT `api.fabric.microsoft.com`
- Measure names are case+whitespace sensitive in DAX
