# DAX Best Practice Analyzer (BPA) Rules

Rules integrated into the AI Skill Analyzer, adapted from Tabular Editor's Best Practice Analyzer for agent-generated DAX queries.

## Rule Reference

### Performance Rules

| Rule ID | Severity | Pattern | Fix |
|---------|----------|---------|-----|
| **BPA-PERF-001** | Warning | `CALCULATE(..., FILTER(ALL(Table), ...))` | Use `REMOVEFILTERS()` or `KEEPFILTERS()` |
| **BPA-PERF-002** | Warning | `FILTER(Table, ...)` on full table | Use column predicates in CALCULATE: `CALCULATE([M], T[Col] = val)` |
| **BPA-PERF-003** | Warning | 3+ nested `CALCULATE` calls | Combine filter args or use VAR for intermediates |
| **BPA-PERF-004** | Info | `COUNTROWS(DISTINCT(...))` | Use `DISTINCTCOUNT()` |
| **BPA-PERF-005** | Warning | `/` operator without `DIVIDE` | Use `DIVIDE(num, denom, 0)` for safe division |
| **BPA-PERF-006** | Warning | `IFERROR` / `ISERROR` | Forces double evaluation. Use `DIVIDE` or explicit `IF + ISBLANK` |
| **BPA-PERF-008** | Warning | `SUMMARIZE(Table, Col, "Name", expr)` | Use `ADDCOLUMNS(SUMMARIZE(T, Col), "Name", expr)` |

### Correctness Rules

| Rule ID | Severity | Pattern | Fix |
|---------|----------|---------|-----|
| **BPA-CORR-001** | Warning | `= BLANK()` or `<> BLANK()` comparison | Use `ISBLANK()` |
| **BPA-CORR-002** | Info | `VALUES()` used where scalar expected | Use `SELECTEDVALUE()` or `MAXX` |
| **BPA-CORR-003** | Info | `SWITCH(TRUE(), ...)` without default | Add an ELSE clause |
| **BPA-CORR-004** | Error | `==` for equality | DAX uses single `=` for comparison |

### Time Intelligence Rules

| Rule ID | Severity | Pattern | Fix |
|---------|----------|---------|-----|
| **BPA-TIME-001** | Warning | `__PBI_TimeIntelligenceEnabled` | Use explicit date CALCULATE filters |
| **BPA-TIME-002** | Warning | `TREATAS` with date/calendar tables | Use direct relationships or explicit date filters |
| **BPA-TIME-003** | Info | `DATESYTD`/`DATESBETWEEN` with nested CALCULATE | Create a dedicated model measure |

### Readability / Maintenance Rules

| Rule ID | Severity | Pattern | Fix |
|---------|----------|---------|-----|
| **BPA-READ-001** | Info | 8+ lines without `VAR` | Use VAR/RETURN for clarity and perf |
| **BPA-READ-002** | Info | `DEFINE MEASURE` with cryptic name (`_x`, `a`) | Use descriptive names |
| **BPA-READ-003** | Info | Hardcoded year values (`2024`, `2025`) | Use `MAX(Date[Year])` or `YEAR(TODAY())` |

### Measure Usage Rules

| Rule ID | Severity | Pattern | Fix |
|---------|----------|---------|-----|
| **BPA-MEAS-001** | Info | 3+ raw column aggregations (`SUM(T[Col])`) | Create reusable measures |
| **BPA-MEAS-002** | Info | `CALCULATE(SUM(T[Col]), ...)` wrapping raw agg | Reference existing measures inside CALCULATE |

## Impact on Quality Rating

Violations affect the DAX quality star rating (0-3 stars):

- **Error** severity → caps at 1 star (Poor)
- **Warning** severity → caps at 2 stars (Adequate)
- **Info** severity → no star deduction, reported in notes

## Example Output

```
  + Q3  [4.2s]  What is the total revenue by region?
    DAX     : ★★☆ Adequate -- BPA: 1 warning(s); BPA: 1 info; refs: [Total Revenue]; correct result
    Quality : ★★★ Data-rich
    >> Fixes:
       [INSTRUCTION ] [BPA-PERF-005] Add instruction: 'Always use DIVIDE(numerator, denominator, 0)...'
       [SIMPLIFY    ] [BPA-READ-001] Add instruction: 'For complex calculations, use VAR/RETURN...'
```

## Sources

- [Tabular Editor BPA Rules](https://docs.tabulareditor.com/te3/features/best-practice-analyzer.html)
- [DAX Best Practices (SQLBI)](https://www.sqlbi.com/articles/best-practices-using-summarize-and-addcolumns/)
- [DIVIDE vs / operator (Microsoft)](https://learn.microsoft.com/en-us/dax/best-practices/dax-divide-function-operator)
- [Avoid IFERROR (SQLBI)](https://www.sqlbi.com/articles/from-sql-to-dax-implementing-nullif-and-coalesce-in-dax/)
