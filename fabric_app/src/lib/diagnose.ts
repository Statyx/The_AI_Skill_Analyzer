// ──────────────────────────────────────────────────────────────
//  diagnose.ts — TypeScript port (MVP) of analyzer/diagnose.py
//
//  Parses Microsoft Fabric Data Agent diagnostic JSON exports into a
//  structured analysis: question/answer, run status, datasource,
//  pipeline steps, generated queries, issues, and timing.
//
//  Client-side only — no backend. Mirrors the Python engine's core
//  contract so the UI stays consistent with the CLI analyzer.
// ──────────────────────────────────────────────────────────────

export type Severity = "error" | "warning" | "info";

export interface Issue {
  severity: Severity;
  stage: string;
  tool?: string;
  issue: string;
  detail?: string;
}

export interface DataSource {
  type?: string;
  name?: string;
  id?: string;
  schemaElements: number;
}

export interface Recommendation {
  trigger: string;
  severity?: Severity;
  advice: string;
}

export interface InstructionQuality {
  score: number;
  max: number;
  criteria: Record<string, boolean>;
  lengthChars: number;
}

export interface Usage {
  promptTokens: number;
  completionTokens: number;
  totalTokens: number;
}

export interface Step {
  id?: string;
  stage: string;
  tool: string;
  status: string;
  createdAt?: number;
  completedAt?: number;
  durationS?: number;
  datasourceType?: string;
  datasourceName?: string;
  query?: string;
  queryLang?: string;
  queryResult?: string;
  error?: unknown;
}

export interface Analysis {
  sourceFile?: string;
  question: string;
  answer: string;
  runStatus: string;
  datasourceTypes: string[];
  primaryDatasourceType?: string;
  totalDurationS?: number;
  steps: Step[];
  issues: Issue[];
  anomalies: Issue[];
  recommendations: Recommendation[];
  dataSources: DataSource[];
  instructions?: string;
  instructionQuality?: InstructionQuality;
  fewshotCount: number;
  usage?: Usage;
  narrative: string[];
  parseError?: string;
}

// ── Tool name → human-readable stage ──────────────────────────
const STAGE_MAP: Record<string, string> = {
  "analyze.database.fewshots.loading": "Fewshot Loading",
  "analyze.database.fewshots.matching": "Fewshot Matching",
  "analyze.database.nl2code": "NL → Query Generation",
  "analyze.database.execute": "Query Execution",
  "trace.analyze_semantic_model": "Query Execution (DAX trace)",
  "trace.analyze_kusto_database": "Query Execution (KQL trace)",
  "trace.analyze_lakehouse": "Query Execution (Lakehouse trace)",
  "trace.analyze_warehouse": "Query Execution (Warehouse trace)",
  "analyze.kusto_database.nl2code": "NL → KQL Generation",
  "analyze.kusto_database.execute": "KQL Execution",
  "analyze.lakehouse.nl2code": "NL → SQL Generation",
  "analyze.lakehouse.execute": "SQL Execution",
  "analyze.warehouse.nl2code": "NL → T-SQL Generation",
  "analyze.warehouse.execute": "T-SQL Execution",
  "analyze.semantic_model.nl2code": "NL → DAX Generation",
  "analyze.semantic_model.execute": "DAX Execution",
  "generate.filename": "Output Naming",
  "message_creation": "Answer Synthesis",
};

const DATASOURCE_TYPES: Record<string, string> = {
  semantic_model: "Semantic Model (DAX)",
  semanticmodel: "Semantic Model (DAX)",
  kusto: "KQL Database (Kusto)",
  kql_database: "KQL Database (Kusto)",
  kqldatabase: "KQL Database (Kusto)",
  lakehouse: "Lakehouse (SQL/Spark)",
  warehouse: "Warehouse (T-SQL)",
  mirrored_database: "Mirrored Database",
};

const ERROR_KEYWORDS = [
  "error",
  "invalid",
  "cannot find",
  "does not exist",
  "syntax error",
  "semanticerror",
];

// ── helpers ───────────────────────────────────────────────────

function labelDatasource(t?: string): string | undefined {
  if (!t) return undefined;
  return DATASOURCE_TYPES[t] ?? DATASOURCE_TYPES[t.toLowerCase()] ?? t;
}

function stageForTool(tool: string): string {
  if (STAGE_MAP[tool]) return STAGE_MAP[tool];
  if (tool.includes("nl2code")) return "NL → Query Generation";
  if (tool.includes("execute") || tool.includes("trace")) return "Query Execution";
  if (tool.includes("fewshot")) return "Fewshot Loading";
  return "Answer Synthesis";
}

function queryLangForTool(tool: string, dsType?: string): string | undefined {
  const t = (dsType ?? "").toLowerCase();
  if (t.includes("semantic")) return "DAX";
  if (t.includes("kusto") || t.includes("kql")) return "KQL";
  if (t.includes("warehouse")) return "T-SQL";
  if (t.includes("lakehouse")) return "SQL";
  if (tool.includes("semantic_model")) return "DAX";
  if (tool.includes("kusto")) return "KQL";
  if (tool.includes("warehouse")) return "T-SQL";
  if (tool.includes("lakehouse")) return "SQL";
  return undefined;
}

function normalizeTs(ts: unknown): number | undefined {
  if (ts == null) return undefined;
  if (typeof ts === "string") {
    const parsed = Date.parse(ts);
    return Number.isNaN(parsed) ? undefined : parsed / 1000;
  }
  const v = Number(ts);
  if (Number.isNaN(v)) return undefined;
  if (v > 1e14) return v / 1_000_000;
  if (v > 1e11) return v / 1_000;
  return v;
}

function safeJsonParse(s: unknown): unknown {
  if (typeof s !== "string") return s;
  try {
    return JSON.parse(s);
  } catch {
    return s;
  }
}

function extractCodeBlock(text: string): string | undefined {
  const m = text.match(/```(?:dax|sql|kql|kusto|tsql)?\s*\n([\s\S]*?)```/i);
  return m ? m[1].trim() : undefined;
}

function truncate(s: string, n: number): string {
  return s.length > n ? s.slice(0, n) + "…" : s;
}

// ── step parsing ──────────────────────────────────────────────

function parseStep(raw: any): Step {
  const toolCalls = raw?.step_details?.tool_calls ?? [];
  const base: Step = {
    id: raw?.id,
    stage: "Answer Synthesis",
    tool: "message_creation",
    status: raw?.status ?? "unknown",
    createdAt: normalizeTs(raw?.created_at),
    completedAt: normalizeTs(raw?.completed_at),
    error: raw?.last_error ?? undefined,
  };
  if (base.createdAt != null && base.completedAt != null) {
    const d = base.completedAt - base.createdAt;
    if (d >= 0 && d <= 24 * 3600) base.durationS = Math.round(d * 100) / 100;
  }
  if (!toolCalls.length) return base;

  const tc = toolCalls[0];
  const fn = tc?.function ?? {};
  const toolName: string = fn.name ?? "unknown";
  const args = safeJsonParse(fn.arguments ?? "{}");
  const outputRaw = fn.output ?? "";
  const output = safeJsonParse(outputRaw);

  let dsType: string | undefined;
  let dsName: string | undefined;
  if (args && typeof args === "object") {
    const a = args as Record<string, unknown>;
    dsType = (a.datasource_type as string) ?? (a.datasourceType as string);
    dsName = (a.datasource_name as string) ?? (a.datasourceName as string);
  }

  let query: string | undefined;
  if (args && typeof args === "object") {
    const a = args as Record<string, unknown>;
    if (typeof a.code === "string") query = extractCodeBlock(a.code) ?? a.code;
    else if (typeof a.natural_language_query === "string") query = a.natural_language_query;
    else if (typeof a.query === "string") query = a.query;
  }
  if (toolName.includes("nl2code") && typeof output === "string") {
    const extracted = extractCodeBlock(output);
    if (extracted) query = extracted;
  }

  let queryResult: string | undefined;
  if (toolName.includes("execute") || toolName.includes("trace")) {
    queryResult = typeof outputRaw === "string" ? outputRaw : String(output);
  }

  return {
    ...base,
    stage: stageForTool(toolName),
    tool: toolName,
    datasourceType: dsType,
    datasourceName: dsName,
    query,
    queryLang: queryLangForTool(toolName, dsType),
    queryResult,
  };
}

// ── issue detection (MVP subset) ──────────────────────────────

function detectIssues(steps: Step[], answer: string): Issue[] {
  const issues: Issue[] = [];

  for (const s of steps) {
    if (s.status === "failed") {
      issues.push({
        severity: "error",
        stage: s.stage,
        tool: s.tool,
        issue: `Step failed: ${s.tool}`,
        detail: String(s.error ?? "no error details"),
      });
    }
  }

  for (const s of steps) {
    if (s.queryResult != null) {
      const qr = s.queryResult.trim();
      if (!qr || ["None", "null", "[]", "{}"].includes(qr)) {
        issues.push({
          severity: "warning",
          stage: s.stage,
          tool: s.tool,
          issue: "Query returned empty result",
          detail: `Query: ${truncate(s.query ?? "", 100)}`,
        });
      }
    }
  }

  const hasQuery = steps.some((s) => s.query);
  const hasNl2code = steps.some((s) => (s.tool ?? "").includes("nl2code"));
  if (!hasQuery && hasNl2code) {
    issues.push({
      severity: "error",
      stage: "NL → Query Generation",
      tool: "nl2code",
      issue: "No query was generated from the natural language input",
    });
  }

  for (const s of steps) {
    if ((s.tool ?? "").toLowerCase().includes("fewshot")) {
      const out = String(s.queryResult ?? "").toLowerCase();
      if (out.includes("0 fewshots") || out.includes("loaded 0")) {
        issues.push({
          severity: "info",
          stage: "Fewshot Loading",
          tool: s.tool,
          issue: "No fewshots loaded — agent has no examples to learn from",
        });
      }
    }
  }

  if (!answer || !answer.trim()) {
    issues.push({
      severity: "error",
      stage: "Answer Synthesis",
      tool: "message_creation",
      issue: "Agent returned an empty answer",
    });
  }

  for (const s of steps) {
    if (s.queryResult) {
      const qrl = s.queryResult.toLowerCase();
      if (ERROR_KEYWORDS.some((kw) => qrl.includes(kw))) {
        issues.push({
          severity: "error",
          stage: s.stage,
          tool: s.tool,
          issue: "Query execution returned an error",
          detail: truncate(s.queryResult, 200),
        });
      }
    }
  }

  for (const s of steps) {
    if (s.durationS && s.durationS > 10) {
      issues.push({
        severity: "info",
        stage: s.stage,
        tool: s.tool,
        issue: "Slow step",
        detail: `${s.durationS.toFixed(1)}s on ${s.tool}`,
      });
    }
  }

  return issues;
}

// ── config / instructions / usage extraction ─────────────────

function extractTopLevelDatasources(data: any): DataSource[] {
  const out: DataSource[] = [];
  const config = data?.config?.configuration;
  if (config && typeof config === "object") {
    for (const d of config.dataSources ?? []) {
      if (d && typeof d === "object") {
        out.push({
          type: d.type,
          name: d.name ?? d.displayName,
          id: d.id ?? d.itemId,
          schemaElements: (d.schema?.elements ?? []).length,
        });
      }
    }
  }
  return out;
}

function extractInstructions(data: any): string | undefined {
  const c = data?.config?.configuration ?? {};
  const candidates = [
    c.additionalInstructions,
    c.aiInstructions,
    c.instructions,
    data?.additionalInstructions,
    data?.aiInstructions,
    data?.instructions,
  ];
  for (const v of candidates) {
    if (typeof v === "string" && v.trim()) return v;
  }
  return undefined;
}

const RUBRIC: Record<string, Record<string, string[]>> = {
  semantic_model: {
    Persona: ["you are", "agent", "assistant", "expert"],
    Context: ["domain", "company", "business", "module"],
    "KPI formulas": ["measure", "calculated", "calculate", "divide", "sum"],
    "Response format": ["format", "respond with", "answer in", "language"],
    Attribution: ["source", "from the model", "verified"],
    "Edge cases": ["if no data", "if empty", "missing", "null"],
    Disclaimers: ["estimate", "may not", "approximate"],
    Examples: ["example", "for instance", "e.g."],
    Actionability: ["explain", "show", "include", "compare"],
    "Tooling hint": ["always query", "use the semantic model", "dax"],
  },
};

function scoreInstructions(instructions?: string): InstructionQuality | undefined {
  if (!instructions) return undefined;
  const rubric = RUBRIC.semantic_model;
  const low = instructions.toLowerCase();
  const criteria: Record<string, boolean> = {};
  for (const [crit, kws] of Object.entries(rubric)) {
    criteria[crit] = kws.some((kw) => low.includes(kw));
  }
  const score = Object.values(criteria).filter(Boolean).length;
  return { score, max: Object.keys(rubric).length, criteria, lengthChars: instructions.length };
}

function extractUsage(data: any): Usage | undefined {
  const runs: any[] = data?.runs ?? data?.thread?.runs ?? [];
  let prompt = 0;
  let completion = 0;
  let found = false;
  for (const r of runs) {
    const u = r?.usage;
    if (u) {
      found = true;
      prompt += Number(u.prompt_tokens ?? 0);
      completion += Number(u.completion_tokens ?? 0);
    }
  }
  if (!found) return undefined;
  return { promptTokens: prompt, completionTokens: completion, totalTokens: prompt + completion };
}

function extractFewshotCount(steps: Step[]): number {
  let n = 0;
  for (const s of steps) {
    if ((s.tool ?? "").toLowerCase().includes("fewshot")) {
      const out = String(s.queryResult ?? "");
      const m = out.match(/loaded (\d+)|(\d+) fewshots/i);
      if (m) n = Math.max(n, Number(m[1] ?? m[2] ?? 0));
    }
  }
  return n;
}

// ── recommendations ───────────────────────────────────────────

const RECO_MATRIX: Record<string, Record<string, string>> = {
  "No fewshots": {
    semantic_model: "Enable Prep for AI → add Verified Answers in the semantic model.",
    kusto: "Add 1 fewshot per critical KQL function (question + canonical query).",
    "*": "Add fewshot examples covering each major question category.",
  },
  "empty result": {
    "*": "Validate data exists and filters are correct — consider relaxing date/scope filters.",
  },
  "Query execution returned an error": {
    semantic_model: "Check measure names (case + whitespace sensitive) and table relationships.",
    kusto: "Inspect KQL syntax: check let-statement order and operator precedence.",
    "*": "Inspect the failing query and validate identifiers exist.",
  },
  "No query was generated": {
    "*": "Strengthen instructions: add an explicit 'ALWAYS query the database using DAX/KQL/SQL' clause.",
  },
  "empty answer": {
    "*": "Add a response-format instruction so the agent always synthesises a textual answer.",
  },
  "Slow step": {
    semantic_model: "Pre-compute heavy measures or add aggregation tables; check Direct Lake fallback.",
    "*": "Profile the query and add appropriate indexing / pre-aggregations.",
  },
};

function dsKey(dsType?: string): string {
  const low = (dsType ?? "").toLowerCase();
  for (const k of ["semantic_model", "kusto", "lakehouse", "warehouse"]) {
    if (low.includes(k.split("_")[0])) return k;
  }
  return "*";
}

function recommend(issues: Issue[], dsType?: string): Recommendation[] {
  const key = dsKey(dsType);
  const out: Recommendation[] = [];
  const seen = new Set<string>();
  for (const it of issues) {
    for (const [keyword, mapping] of Object.entries(RECO_MATRIX)) {
      if (it.issue.toLowerCase().includes(keyword.toLowerCase())) {
        const advice = mapping[key] ?? mapping["*"];
        if (advice && !seen.has(advice)) {
          out.push({ trigger: it.issue, severity: it.severity, advice });
          seen.add(advice);
        }
        break;
      }
    }
  }
  return out;
}

// ── anomalies (subset) ────────────────────────────────────────

function detectAnomalies(steps: Step[], sources: DataSource[], messageCount: number): Issue[] {
  const out: Issue[] = [];

  if (!sources.length && steps.some((s) => s.query)) {
    out.push({
      severity: "warning",
      stage: "Configuration",
      issue: "config.dataSources is empty but the agent queried a source",
      detail: "Agent definition may be missing the dataSources block.",
    });
  }
  for (const d of sources) {
    if (d.schemaElements === 0) {
      out.push({
        severity: "warning",
        stage: "Configuration",
        issue: `Datasource '${d.name ?? d.id}' has 0 schema elements`,
        detail: "Schema introspection failed — NL2DAX/NL2KQL relies on hints only.",
      });
    }
  }
  if (messageCount >= 50) {
    out.push({
      severity: "warning",
      stage: "Thread",
      issue: `Thread has ${messageCount} messages (>50 = pollution risk)`,
      detail: "DELETE the thread before next question — Fabric reuses threads per user.",
    });
  }
  return out;
}

// ── narrative generator ───────────────────────────────────────

function fmtDur(s?: number): string {
  if (!s) return "an unmeasured time";
  return s < 60 ? `${s.toFixed(1)}s` : `${Math.floor(s / 60)}m ${(s % 60).toFixed(0)}s`;
}

function generateNarrative(a: {
  question: string;
  answer: string;
  runStatus: string;
  steps: Step[];
  issues: Issue[];
  anomalies: Issue[];
  primaryDatasourceType?: string;
  totalDurationS?: number;
  fewshotCount: number;
}): string[] {
  const lines: string[] = [];
  const errors = [...a.issues, ...a.anomalies].filter((i) => i.severity === "error");
  const warnings = [...a.issues, ...a.anomalies].filter((i) => i.severity === "warning");

  // 1. Opening — what was asked and the verdict.
  const verdict =
    a.runStatus === "failed" || errors.length
      ? "but the run did not fully succeed"
      : warnings.length
        ? "and the run completed, though with some warnings"
        : "and the run completed cleanly";
  lines.push(
    `The user asked **"${a.question || "(no question captured)"}"** ${verdict}. ` +
      `The agent ran **${a.steps.length} step${a.steps.length === 1 ? "" : "s"}** against ` +
      `**${a.primaryDatasourceType ?? "an unidentified datasource"}** in ${fmtDur(a.totalDurationS)}.`,
  );

  // 2. The pipeline walk-through.
  const gen = a.steps.find((s) => (s.tool ?? "").includes("nl2code"));
  const exec = a.steps.find((s) => (s.tool ?? "").includes("execute") || (s.tool ?? "").includes("trace"));
  if (gen?.query) {
    lines.push(
      `It translated the question into a **${gen.queryLang ?? "query"}** statement` +
        (exec?.queryResult
          ? ` and executed it, returning: \`${truncate(exec.queryResult.replace(/\s+/g, " ").trim(), 120)}\`.`
          : ", but no execution result was captured."),
    );
  } else if (a.steps.some((s) => (s.tool ?? "").includes("nl2code"))) {
    lines.push("It reached the query-generation stage but **no query was produced** — the agent could not translate the question.");
  }

  // 3. Few-shots / grounding.
  if (a.fewshotCount === 0) {
    lines.push("**No few-shot examples** were loaded, so the agent had no curated patterns to ground its query generation.");
  } else {
    lines.push(`**${a.fewshotCount} few-shot example${a.fewshotCount === 1 ? "" : "s"}** were available to guide query generation.`);
  }

  // 4. The verdict on answer + problems.
  if (!a.answer || !a.answer.trim()) {
    lines.push("Critically, the agent **returned an empty answer** to the user.");
  }
  if (errors.length) {
    lines.push(`**${errors.length} error-level issue${errors.length === 1 ? "" : "s"}** were detected: ${errors.map((e) => e.issue).join("; ")}.`);
  }
  if (warnings.length) {
    lines.push(`There ${warnings.length === 1 ? "was" : "were"} also **${warnings.length} warning${warnings.length === 1 ? "" : "s"}** worth reviewing.`);
  }
  if (!errors.length && !warnings.length) {
    lines.push("No issues were detected — this run is a healthy reference example.");
  }

  return lines;
}

// ── main entrypoint ───────────────────────────────────────────

export function analyzeDiagnostic(data: any): Analysis {
  let rawSteps: any[] = [];
  if (data?.run_steps) {
    rawSteps = Array.isArray(data.run_steps) ? data.run_steps : data.run_steps.data ?? [];
  } else if (data?.thread?.run_steps) {
    const r = data.thread.run_steps;
    rawSteps = Array.isArray(r) ? r : r.data ?? [];
  }

  const steps = [...rawSteps]
    .sort((a, b) => (normalizeTs(a?.created_at) ?? 0) - (normalizeTs(b?.created_at) ?? 0))
    .map(parseStep);

  // messages
  let msgData = data?.messages ?? data?.thread?.messages ?? [];
  if (msgData && !Array.isArray(msgData)) msgData = msgData.data ?? [];
  const messages = (msgData as any[]).map((m) => {
    let content = "";
    for (const c of m?.content ?? []) {
      if (c && typeof c === "object") {
        const text = c.text;
        content = text && typeof text === "object" ? text.value ?? "" : String(text ?? "");
      }
    }
    return { role: m?.role ?? "unknown", content };
  });

  let question: string = data?.question ?? data?.thread?.question ?? "";
  if (!question) {
    const u = messages.find((m) => m.role === "user");
    if (u) question = u.content;
  }
  const assistantMsgs = messages.filter((m) => m.role === "assistant");
  const answer = assistantMsgs.length ? assistantMsgs[assistantMsgs.length - 1].content : "";

  const dsTypes = new Set<string>();
  for (const s of steps) {
    if (s.datasourceType) dsTypes.add(labelDatasource(s.datasourceType)!);
  }
  const primary = dsTypes.size ? [...dsTypes][0] : undefined;

  const created = steps.map((s) => s.createdAt).filter((x): x is number => x != null);
  const completed = steps.map((s) => s.completedAt).filter((x): x is number => x != null);
  let totalDuration: number | undefined;
  if (created.length && completed.length) {
    const d = Math.max(...completed) - Math.min(...created);
    if (d >= 0 && d <= 24 * 3600) totalDuration = Math.round(d * 100) / 100;
  }

  const issues = detectIssues(steps, answer);

  const runStatus: string =
    data?.run_status ??
    (steps.some((s) => s.status === "failed") ? "failed" : steps.length ? "completed" : "unknown");

  const dataSources = extractTopLevelDatasources(data);
  const anomalies = detectAnomalies(steps, dataSources, messages.length);
  const instructions = extractInstructions(data);
  const instructionQuality = scoreInstructions(instructions);
  const fewshotCount = extractFewshotCount(steps);
  const usage = extractUsage(data);
  const recommendations = recommend([...issues, ...anomalies], primary);
  const narrative = generateNarrative({
    question,
    answer,
    runStatus,
    steps,
    issues,
    anomalies,
    primaryDatasourceType: primary,
    totalDurationS: totalDuration,
    fewshotCount,
  });

  return {
    question,
    answer,
    runStatus,
    datasourceTypes: [...dsTypes].sort(),
    primaryDatasourceType: primary,
    totalDurationS: totalDuration,
    steps,
    issues,
    anomalies,
    recommendations,
    dataSources,
    instructions,
    instructionQuality,
    fewshotCount,
    usage,
    narrative,
  };
}

export function analyzeText(text: string, fileName?: string): Analysis {
  try {
    const data = JSON.parse(text);
    const a = analyzeDiagnostic(data);
    a.sourceFile = fileName;
    return a;
  } catch (e) {
    return {
      sourceFile: fileName,
      question: "",
      answer: "",
      runStatus: "unknown",
      datasourceTypes: [],
      steps: [],
      issues: [],
      anomalies: [],
      recommendations: [],
      dataSources: [],
      fewshotCount: 0,
      narrative: [],
      parseError: e instanceof Error ? e.message : String(e),
    };
  }
}
