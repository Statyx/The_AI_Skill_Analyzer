import { useMemo, useState } from "react";
import type { Analysis } from "../lib/diagnose";
import { SingleView } from "./SingleView";

const STATUS_ICON: Record<string, string> = {
  completed: "✅",
  failed: "❌",
  cancelled: "⚪",
  expired: "⏰",
  in_progress: "🔄",
  unknown: "❓",
};

function fmtDuration(s?: number): string {
  if (!s) return "—";
  if (s < 60) return `${s.toFixed(1)}s`;
  return `${Math.floor(s / 60)}m ${(s % 60).toFixed(0)}s`;
}

function worstHealth(a: Analysis): string {
  const sev = new Set(a.issues.map((i) => i.severity));
  if (sev.has("error")) return "🔴";
  if (sev.has("warning")) return "🟠";
  if (sev.has("info")) return "🔵";
  return "🟢";
}

export function BatchView({ analyses }: { analyses: Analysis[] }) {
  const valid = analyses.filter((a) => !a.parseError);
  const [selected, setSelected] = useState<string | null>(null);

  const agg = useMemo(() => {
    const statuses = new Map<string, number>();
    let issueCount = 0;
    const issueTypes = new Map<string, number>();
    const durations: number[] = [];
    for (const a of valid) {
      statuses.set(a.runStatus, (statuses.get(a.runStatus) ?? 0) + 1);
      issueCount += a.issues.length;
      for (const i of a.issues) issueTypes.set(i.issue, (issueTypes.get(i.issue) ?? 0) + 1);
      if (a.totalDurationS) durations.push(a.totalDurationS);
    }
    return {
      statuses: [...statuses.entries()].sort((x, y) => y[1] - x[1]),
      issueCount,
      topIssues: [...issueTypes.entries()].sort((x, y) => y[1] - x[1]).slice(0, 8),
      avgDuration: durations.length ? durations.reduce((s, d) => s + d, 0) / durations.length : undefined,
      failed: statuses.get("failed") ?? 0,
    };
  }, [valid]);

  if (!valid.length) return <div className="empty">No valid diagnostics to display.</div>;

  const picked = selected ? valid.find((a) => a.sourceFile === selected) : undefined;

  return (
    <div>
      <div className="kpis">
        <div className="kpi"><div className="label">Runs</div><div className="value">{valid.length}</div></div>
        <div className="kpi"><div className="label">Failed</div><div className="value">{agg.failed}</div></div>
        <div className="kpi"><div className="label">Issues</div><div className="value">{agg.issueCount}</div></div>
        <div className="kpi"><div className="label">Avg duration</div><div className="value">{fmtDuration(agg.avgDuration)}</div></div>
      </div>

      <div className="cols">
        <div className="card">
          <h3>Status distribution</h3>
          <table>
            <tbody>
              {agg.statuses.map(([s, c]) => (
                <tr key={s}>
                  <td>{STATUS_ICON[s] ?? "❓"} {s}</td>
                  <td style={{ textAlign: "right" }}>{c}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <div className="card">
          <h3>Top issues</h3>
          {agg.topIssues.length === 0 ? (
            <div className="muted">No issues across the batch.</div>
          ) : (
            <table>
              <tbody>
                {agg.topIssues.map(([issue, c]) => (
                  <tr key={issue}>
                    <td>{issue}</td>
                    <td style={{ textAlign: "right" }}>{c}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      </div>

      <div className="card">
        <h3>Per-run detail</h3>
        <table>
          <thead>
            <tr>
              <th>Health</th>
              <th>File</th>
              <th>Question</th>
              <th>Status</th>
              <th>Datasource</th>
              <th>Steps</th>
              <th>Issues</th>
              <th>Duration</th>
            </tr>
          </thead>
          <tbody>
            {valid.map((a) => (
              <tr key={a.sourceFile} className="clickable" onClick={() => setSelected(a.sourceFile ?? null)}>
                <td>{worstHealth(a)}</td>
                <td>{a.sourceFile}</td>
                <td>{(a.question || "").slice(0, 50)}</td>
                <td>{STATUS_ICON[a.runStatus] ?? "❓"} {a.runStatus}</td>
                <td>{a.primaryDatasourceType ?? "—"}</td>
                <td>{a.steps.length}</td>
                <td>{a.issues.length}</td>
                <td>{fmtDuration(a.totalDurationS)}</td>
              </tr>
            ))}
          </tbody>
        </table>
        <div className="muted" style={{ fontSize: 12, marginTop: 8 }}>
          Click a row to drill into the full run analysis below.
        </div>
      </div>

      {picked && (
        <div className="card">
          <h3>🔎 {picked.sourceFile}</h3>
          <SingleView a={picked} />
        </div>
      )}
    </div>
  );
}
