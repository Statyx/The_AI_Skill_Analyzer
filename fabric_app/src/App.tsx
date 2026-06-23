import { useCallback, useState } from "react";
import { analyzeText, type Analysis } from "./lib/diagnose";
import { SingleView } from "./components/SingleView";
import { BatchView } from "./components/BatchView";

export default function App() {
  const [analyses, setAnalyses] = useState<Analysis[]>([]);
  const [drag, setDrag] = useState(false);
  const [tab, setTab] = useState<"batch" | "single">("batch");
  const [singlePick, setSinglePick] = useState<string | null>(null);

  const ingest = useCallback(async (files: FileList | File[]) => {
    const out: Analysis[] = [];
    for (const f of Array.from(files)) {
      const text = await f.text();
      out.push(analyzeText(text, f.name));
    }
    setAnalyses(out);
    setSinglePick(out[0]?.sourceFile ?? null);
  }, []);

  const onDrop = useCallback(
    (e: React.DragEvent) => {
      e.preventDefault();
      setDrag(false);
      if (e.dataTransfer.files?.length) ingest(e.dataTransfer.files);
    },
    [ingest],
  );

  const single = analyses.length === 1;
  const picked = analyses.find((a) => a.sourceFile === singlePick) ?? analyses[0];

  return (
    <div className="app">
      <aside className="sidebar">
        <h1>🔬 Data Agent Inspector</h1>
        <div className="sub">
          Analyze Microsoft Fabric Data Agent diagnostic exports — RCA, pipeline timeline,
          query &amp; latency inspection.
        </div>

        <label
          className={`dropzone ${drag ? "drag" : ""}`}
          onDragOver={(e) => {
            e.preventDefault();
            setDrag(true);
          }}
          onDragLeave={() => setDrag(false)}
          onDrop={onDrop}
        >
          <input
            type="file"
            accept="application/json,.json"
            multiple
            style={{ display: "none" }}
            onChange={(e) => e.target.files && ingest(e.target.files)}
          />
          <div style={{ fontSize: 28 }}>📂</div>
          <div>Drop diagnostic JSON file(s) here</div>
          <div style={{ fontSize: 12, marginTop: 4 }}>or click to browse</div>
        </label>

        {analyses.length > 0 && (
          <div style={{ marginTop: 16, fontSize: 13 }} className="muted">
            {analyses.length} file{analyses.length > 1 ? "s" : ""} loaded
            {single ? " · single view" : " · batch view"}
          </div>
        )}
      </aside>

      <main className="main">
        {analyses.length === 0 ? (
          <div className="empty">
            ⬅️ Upload one or more Data Agent diagnostic JSON files to begin.
          </div>
        ) : single ? (
          <SingleView a={analyses[0]} />
        ) : (
          <>
            <div className="tabs">
              <div className={`tab ${tab === "batch" ? "active" : ""}`} onClick={() => setTab("batch")}>
                📊 Batch overview
              </div>
              <div className={`tab ${tab === "single" ? "active" : ""}`} onClick={() => setTab("single")}>
                📄 Single run
              </div>
            </div>
            {tab === "batch" ? (
              <BatchView analyses={analyses} />
            ) : (
              <>
                <select value={singlePick ?? ""} onChange={(e) => setSinglePick(e.target.value)}>
                  {analyses.map((a) => (
                    <option key={a.sourceFile} value={a.sourceFile}>
                      {a.sourceFile}
                    </option>
                  ))}
                </select>
                {picked && <SingleView a={picked} />}
              </>
            )}
          </>
        )}
      </main>
    </div>
  );
}
