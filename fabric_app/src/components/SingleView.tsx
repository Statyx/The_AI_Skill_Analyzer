import type { Analysis, Issue, Severity } from "../lib/diagnose";

const SEV_ICON: Record<Severity, string> = {
  error: "🔴",
  warning: "🟠",
  info: "🔵",
};

const STATUS_ICON: Record<string, string> = {
  completed: "✅",
  failed: "❌",
  cancelled: "⚪",
  expired: "⏰",
  in_progress: "🔄",
  unknown: "❓",
};

const LANG_CLASS: Record<string, string> = {
  dax: "sql",
  kql: "sql",
  sql: "sql",
  tsql: "sql",
};

function fmtDuration(s?: number): string {
  if (!s) return "—";
  if (s < 60) return `${s.toFixed(1)}s`;
  return `${Math.floor(s / 60)}m ${(s % 60).toFixed(0)}s`;
}

const SEV_ORDER: Record<Severity, number> = { error: 0, warning: 1, info: 2 };

// Minimal **bold** + `code` renderer for narrative lines.
function RichText({ text }: { text: string }) {
  const parts = text.split(/(\*\*[^*]+\*\*|`[^`]+`)/g);
  return (
    <>
      {parts.map((p, i) => {
        if (p.startsWith("**") && p.endsWith("**")) return <strong key={i}>{p.slice(2, -2)}</strong>;
        if (p.startsWith("`") && p.endsWith("`")) return <code key={i}>{p.slice(1, -1)}</code>;
        return <span key={i}>{p}</span>;
      })}
    </>
  );
}

export function SingleView({ a }: { a: Analysis }) {
  if (a.parseError) {
    return <div className="card">⚠️ Could not parse <code>{a.sourceFile}</code>: {a.parseError}</div>;
  }

  const allFindings = [...a.issues, ...a.anomalies];
  const errors = allFindings.filter((i) => i.severity === "error");
  const health = errors.length ? "🔴 Failing" : allFindings.length ? "🟠 Warnings" : "🟢 Healthy";
  const status = a.runStatus;
  const queries = a.steps.filter((s) => s.query);

  return (
    <div>
      <div className="kpis">
        <Kpi label="Status" value={`${STATUS_ICON[status] ?? "❓"} ${status}`} />
        <Kpi label="Datasource" value={a.primaryDatasourceType ?? "—"} />
        <Kpi label="Duration" value={fmtDuration(a.totalDurationS)} />
        <Kpi label="Steps" value={String(a.steps.length)} />
        <Kpi label="Few-shots" value={String(a.fewshotCount)} />
        <Kpi label="Health" value={health} />
      </div>

      {/* What happened — narrative summary */}
      <div className="card narrative">
        <h3>📖 What happened</h3>
        {a.narrative.map((line, i) => (
          <p key={i} style={{ margin: "0 0 8px" }}><RichText text={line} /></p>
        ))}
      </div>

      <div className="cols">
        <div className="card">
          <h3>💬 Conversation</h3>
          <div className="muted" style={{ fontSize: 12, marginBottom: 4 }}>Question</div>
          <div className="qa">{a.question || "—"}</div>
          <div className="muted" style={{ fontSize: 12, marginBottom: 4 }}>Answer</div>
          <div className="qa">{a.answer || "(empty answer)"}</div>
        </div>

        <div className="card">
          <h3>🧭 Explorer</h3>
          <div className="muted" style={{ fontSize: 12, marginBottom: 4 }}>Data sources</div>
          {a.dataSources.length === 0 ? (
            <div className="muted" style={{ marginBottom: 10 }}>
              {a.primaryDatasourceType
                ? `Queried ${a.primaryDatasourceType} (no config block in export)`
                : "None declared in the diagnostic export."}
            </div>
          ) : (
            <table style={{ marginBottom: 10 }}>
              <thead>
                <tr><th>Name</th><th>Type</th><th>Schema</th></tr>
              </thead>
              <tbody>
                {a.dataSources.map((d, i) => (
                  <tr key={i}>
                    <td>{d.name ?? d.id ?? "—"}</td>
                    <td>{d.type ?? "—"}</td>
                    <td>{d.schemaElements} elements</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}

          <div className="muted" style={{ fontSize: 12, marginBottom: 4 }}>Few-shots</div>
          <div style={{ marginBottom: 10 }}>
            <span className="badge" style={a.fewshotCount === 0 ? { borderColor: "var(--warning)" } : undefined}>
              {a.fewshotCount} loaded
            </span>
          </div>

          <div className="muted" style={{ fontSize: 12, marginBottom: 4 }}>Agent instructions</div>
          {a.instructionQuality ? (
            <div>
              <span className="badge">
                Quality {a.instructionQuality.score}/{a.instructionQuality.max} · {a.instructionQuality.lengthChars} chars
              </span>
              <div style={{ display: "flex", flexWrap: "wrap", gap: 6, marginTop: 8 }}>
                {Object.entries(a.instructionQuality.criteria).map(([k, ok]) => (
                  <span key={k} className="badge" style={{ opacity: ok ? 1 : 0.4 }}>
                    {ok ? "✓" : "✕"} {k}
                  </span>
                ))}
              </div>
            </div>
          ) : (
            <div className="muted">No instructions found in this export.</div>
          )}
          {a.instructions && (
            <details style={{ marginTop: 8 }}>
              <summary>View instructions</summary>
              <pre>{a.instructions}</pre>
            </details>
          )}
        </div>
      </div>

      <div className="cols">
        <div className="card">
          <h3>🩺 Issues &amp; anomalies</h3>
          {allFindings.length === 0 ? (
            <div className="muted">No issues detected.</div>
          ) : (
            [...allFindings]
              .sort((x, y) => SEV_ORDER[x.severity] - SEV_ORDER[y.severity])
              .map((i, idx) => <IssueRow key={idx} i={i} />)
          )}
        </div>

        <div className="card">
          <h3>🛠️ Recommendations</h3>
          {a.recommendations.length === 0 ? (
            <div className="muted">No recommendations — looks good.</div>
          ) : (
            a.recommendations.map((r, i) => (
              <div key={i} className="issue info" style={{ borderLeftColor: "var(--accent)" }}>
                <div><RichText text={r.advice} /></div>
                <div className="detail">Triggered by: {r.trigger}</div>
              </div>
            ))
          )}
        </div>
      </div>

      <div className="card">
        <h3>🔬 Pipeline timeline</h3>
        {a.steps.length === 0 ? (
          <div className="muted">No pipeline steps found.</div>
        ) : (
          <table>
            <thead>
              <tr>
                <th>#</th>
                <th>Stage</th>
                <th>Tool</th>
                <th>Status</th>
                <th>Duration</th>
                <th>Datasource</th>
              </tr>
            </thead>
            <tbody>
              {a.steps.map((s, i) => (
                <tr key={i}>
                  <td>{i + 1}</td>
                  <td>{s.stage}</td>
                  <td><code>{s.tool}</code></td>
                  <td>{STATUS_ICON[s.status] ?? "❓"} {s.status}</td>
                  <td>{fmtDuration(s.durationS)}</td>
                  <td>{s.datasourceName ?? "—"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>

      {queries.length > 0 && (
        <div className="card">
          <h3>🧮 Generated queries</h3>
          {queries.map((s, i) => (
            <details key={i} style={{ marginBottom: 10 }}>
              <summary>
                {s.stage} · <code>{s.tool}</code>{" "}
                <span className="badge">{s.queryLang ?? "query"}</span>
              </summary>
              <pre className={LANG_CLASS[(s.queryLang ?? "").toLowerCase()] ?? ""}>{s.query}</pre>
              {s.queryResult && (
                <>
                  <div className="muted" style={{ fontSize: 12 }}>Result</div>
                  <pre>{s.queryResult}</pre>
                </>
              )}
            </details>
          ))}
        </div>
      )}
    </div>
  );
}

function Kpi({ label, value }: { label: string; value: string }) {
  return (
    <div className="kpi">
      <div className="label">{label}</div>
      <div className="value">{value}</div>
    </div>
  );
}

function IssueRow({ i }: { i: Issue }) {
  return (
    <div className={`issue ${i.severity}`}>
      <div>
        {SEV_ICON[i.severity]} <strong>{i.issue}</strong>{" "}
        <span className="stage">· {i.stage}</span>
      </div>
      {i.detail && <div className="detail">{i.detail}</div>}
    </div>
  );
}
