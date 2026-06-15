import { useState, useEffect, useCallback, useRef } from "react";
import {
  LineChart, Line, BarChart, Bar, XAxis, YAxis, CartesianGrid,
  Tooltip, ResponsiveContainer, Legend
} from "recharts";

// ── Config ──────────────────────────────────────────────────────────────────
const API_BASE = import.meta?.env?.VITE_QUERY_API_URL ?? "/api/query";

// ── Types ───────────────────────────────────────────────────────────────────
const WINDOWS = [
  { label: "1h", hours: 1 },
  { label: "6h", hours: 6 },
  { label: "24h", hours: 24 },
  { label: "7d", hours: 168 },
  { label: "30d", hours: 720 },
];

const GRANULARITIES = [
  { label: "Hourly", value: "hour" },
  { label: "Daily", value: "day" },
];

// ── Helpers ──────────────────────────────────────────────────────────────────
function timeWindow(hours) {
  const end = new Date();
  const start = new Date(Date.now() - hours * 3600_000);
  return {
    start: start.toISOString(),
    end: end.toISOString(),
  };
}

function fmt$(n) {
  if (n === null || n === undefined) return "—";
  if (n >= 1) return `$${n.toFixed(4)}`;
  return `$${n.toFixed(6)}`;
}

function fmtTokens(n) {
  if (n === null || n === undefined) return "—";
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(2)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}k`;
  return String(n);
}

function fmtPct(n) {
  if (n === null || n === undefined) return "—";
  return `${Number(n).toFixed(1)}%`;
}

function shortTs(ts, granularity) {
  if (!ts) return "";
  if (granularity === "day") return ts.slice(0, 10);
  // hour: "2025-08-01T14" → "14:00"
  const parts = ts.split("T");
  return parts[1] ? `${parts[1]}:00` : ts;
}

// ── API client ───────────────────────────────────────────────────────────────
async function fetchQ(path, params = {}) {
  const url = new URL(`${API_BASE}/${path}`, window.location.origin);
  Object.entries(params).forEach(([k, v]) => url.searchParams.set(k, v));
  const resp = await fetch(url.toString());
  if (!resp.ok) throw new Error(`${resp.status} ${resp.statusText}`);
  return resp.json();
}

// ── Reusable components ───────────────────────────────────────────────────────
function Chip({ children, active, onClick }) {
  return (
    <button
      onClick={onClick}
      style={{
        padding: "4px 12px",
        borderRadius: "6px",
        border: active
          ? "1.5px solid var(--color-border-primary)"
          : "1px solid var(--color-border-tertiary)",
        background: active
          ? "var(--color-background-secondary)"
          : "transparent",
        color: active
          ? "var(--color-text-primary)"
          : "var(--color-text-secondary)",
        fontSize: "13px",
        cursor: "pointer",
        fontWeight: active ? 500 : 400,
        transition: "all 0.1s",
      }}
    >
      {children}
    </button>
  );
}

function Card({ children, style = {} }) {
  return (
    <div
      style={{
        background: "var(--color-background-secondary)",
        border: "1px solid var(--color-border-tertiary)",
        borderRadius: "12px",
        padding: "20px",
        ...style,
      }}
    >
      {children}
    </div>
  );
}

function StatBox({ label, value, sub, accent = false }) {
  return (
    <Card>
      <div style={{ fontSize: "12px", color: "var(--color-text-tertiary)", marginBottom: "6px" }}>
        {label}
      </div>
      <div
        style={{
          fontSize: "24px",
          fontWeight: 500,
          color: accent ? "var(--color-text-success)" : "var(--color-text-primary)",
          letterSpacing: "-0.5px",
        }}
      >
        {value}
      </div>
      {sub && (
        <div style={{ fontSize: "12px", color: "var(--color-text-secondary)", marginTop: "4px" }}>
          {sub}
        </div>
      )}
    </Card>
  );
}

function Spinner() {
  return (
    <span
      style={{
        display: "inline-block",
        width: "14px",
        height: "14px",
        border: "2px solid var(--color-border-secondary)",
        borderTop: "2px solid var(--color-text-secondary)",
        borderRadius: "50%",
        animation: "spin 0.7s linear infinite",
      }}
    />
  );
}

function SectionHeader({ title, children }) {
  return (
    <div
      style={{
        display: "flex",
        alignItems: "center",
        justifyContent: "space-between",
        marginBottom: "14px",
      }}
    >
      <h2
        style={{
          fontSize: "15px",
          fontWeight: 500,
          color: "var(--color-text-primary)",
          margin: 0,
        }}
      >
        {title}
      </h2>
      {children}
    </div>
  );
}

function BreakdownTable({ data, dimensionKey, dimensionLabel, loading }) {
  if (loading) return <div style={{ padding: "20px 0", textAlign: "center" }}><Spinner /></div>;
  if (!data?.items?.length)
    return (
      <div style={{ color: "var(--color-text-tertiary)", fontSize: "13px", padding: "16px 0" }}>
        No data for this window.
      </div>
    );

  const maxCost = Math.max(...data.items.map((r) => r.total_cost_usd ?? 0), 0.0000001);

  return (
    <div style={{ overflowX: "auto" }}>
      <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "13px" }}>
        <thead>
          <tr style={{ borderBottom: "1px solid var(--color-border-tertiary)" }}>
            <th style={{ textAlign: "left", padding: "6px 8px", color: "var(--color-text-tertiary)", fontWeight: 400 }}>
              {dimensionLabel}
            </th>
            <th style={{ textAlign: "right", padding: "6px 8px", color: "var(--color-text-tertiary)", fontWeight: 400 }}>
              Cost
            </th>
            <th style={{ textAlign: "right", padding: "6px 8px", color: "var(--color-text-tertiary)", fontWeight: 400 }}>
              Input tokens
            </th>
            <th style={{ textAlign: "right", padding: "6px 8px", color: "var(--color-text-tertiary)", fontWeight: 400 }}>
              Output tokens
            </th>
            <th style={{ textAlign: "right", padding: "6px 8px", color: "var(--color-text-tertiary)", fontWeight: 400 }}>
              Calls
            </th>
            <th style={{ padding: "6px 8px", width: "120px" }}></th>
          </tr>
        </thead>
        <tbody>
          {data.items.map((row, i) => {
            const pct = ((row.total_cost_usd ?? 0) / maxCost) * 100;
            return (
              <tr
                key={i}
                style={{ borderBottom: "1px solid var(--color-border-tertiary)" }}
              >
                <td style={{ padding: "8px", color: "var(--color-text-primary)", maxWidth: "220px", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                  {row[dimensionKey] || "(unattributed)"}
                </td>
                <td style={{ padding: "8px", textAlign: "right", fontVariantNumeric: "tabular-nums", color: "var(--color-text-primary)" }}>
                  {fmt$(row.total_cost_usd)}
                </td>
                <td style={{ padding: "8px", textAlign: "right", color: "var(--color-text-secondary)", fontVariantNumeric: "tabular-nums" }}>
                  {fmtTokens(row.input_tokens)}
                </td>
                <td style={{ padding: "8px", textAlign: "right", color: "var(--color-text-secondary)", fontVariantNumeric: "tabular-nums" }}>
                  {fmtTokens(row.output_tokens)}
                </td>
                <td style={{ padding: "8px", textAlign: "right", color: "var(--color-text-secondary)", fontVariantNumeric: "tabular-nums" }}>
                  {row.event_count?.toLocaleString()}
                </td>
                <td style={{ padding: "8px" }}>
                  <div style={{ height: "6px", background: "var(--color-border-tertiary)", borderRadius: "3px", overflow: "hidden" }}>
                    <div
                      style={{
                        height: "100%",
                        width: `${pct}%`,
                        background: "var(--color-text-secondary)",
                        borderRadius: "3px",
                        transition: "width 0.4s ease",
                      }}
                    />
                  </div>
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

// ── Main dashboard ────────────────────────────────────────────────────────────
export default function Dashboard() {
  const [windowHours, setWindowHours] = useState(24);
  const [granularity, setGranularity] = useState("hour");
  const [activeTab, setActiveTab] = useState("agent");

  const [summary, setSummary] = useState(null);
  const [timeseries, setTimeseries] = useState(null);
  const [breakdown, setBreakdown] = useState(null);
  const [reconciliation, setReconciliation] = useState(null);
  const [coverage, setCoverage] = useState(null);

  const [loadingSummary, setLoadingSummary] = useState(true);
  const [loadingTimeseries, setLoadingTimeseries] = useState(true);
  const [loadingBreakdown, setLoadingBreakdown] = useState(true);
  const [loadingRecon, setLoadingRecon] = useState(true);
  const [loadingCoverage, setLoadingCoverage] = useState(true);

  const [error, setError] = useState(null);
  const refreshTimer = useRef(null);

  const DIMENSION_MAP = {
    agent: { path: "by-agent", key: "agent_id", label: "Agent" },
    user: { path: "by-user", key: "user_id", label: "User" },
    app: { path: "by-app", key: "application_id", label: "Application" },
    model: { path: "by-model", key: "model_id", label: "Model" },
  };

  const loadAll = useCallback(async () => {
    const { start, end } = timeWindow(windowHours);
    const params = { start, end };
    const tsParams = { start, end, granularity };
    const dim = DIMENSION_MAP[activeTab];

    setLoadingSummary(true);
    setLoadingTimeseries(true);
    setLoadingBreakdown(true);

    try {
      const [s, ts, bd, recon, cov] = await Promise.allSettled([
        fetchQ("summary", params),
        fetchQ("timeseries", tsParams),
        fetchQ(dim.path, params),
        fetchQ("reconciliation", { limit: "10" }),
        fetchQ("coverage"),
      ]);

      if (s.status === "fulfilled") setSummary(s.value);
      if (ts.status === "fulfilled") setTimeseries(ts.value);
      if (bd.status === "fulfilled") setBreakdown(bd.value);
      if (recon.status === "fulfilled") setReconciliation(recon.value);
      if (cov.status === "fulfilled") setCoverage(cov.value);

      setError(null);
    } catch (e) {
      setError(String(e));
    } finally {
      setLoadingSummary(false);
      setLoadingTimeseries(false);
      setLoadingBreakdown(false);
      setLoadingRecon(false);
      setLoadingCoverage(false);
    }
  }, [windowHours, granularity, activeTab]);

  useEffect(() => {
    loadAll();
    clearInterval(refreshTimer.current);
    refreshTimer.current = setInterval(loadAll, 60_000); // refresh every 60s
    return () => clearInterval(refreshTimer.current);
  }, [loadAll]);

  // Reload breakdown when tab changes
  useEffect(() => {
    const { start, end } = timeWindow(windowHours);
    const dim = DIMENSION_MAP[activeTab];
    setLoadingBreakdown(true);
    fetchQ(dim.path, { start, end })
      .then(setBreakdown)
      .finally(() => setLoadingBreakdown(false));
  }, [activeTab]);

  const coveragePct = coverage?.coverage_pct ?? 100;
  const coverageColor =
    coveragePct >= 95
      ? "var(--color-text-success)"
      : coveragePct >= 80
      ? "var(--color-text-warning)"
      : "var(--color-text-danger)";

  return (
    <>
      <style>{`
        @keyframes spin { to { transform: rotate(360deg); } }
        * { box-sizing: border-box; }
        body { margin: 0; }
      `}</style>

      <div style={{ padding: "24px 28px", maxWidth: "1280px", margin: "0 auto" }}>

        {/* Header */}
        <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: "24px" }}>
          <div>
            <h1 style={{ margin: 0, fontSize: "20px", fontWeight: 500, color: "var(--color-text-primary)" }}>
              Bedrock cost explorer
            </h1>
            <p style={{ margin: "4px 0 0", fontSize: "13px", color: "var(--color-text-tertiary)" }}>
              Real-time · computed from token counts · not from billing data
            </p>
          </div>
          <div style={{ display: "flex", alignItems: "center", gap: "8px" }}>
            {(loadingSummary || loadingTimeseries) && <Spinner />}
            <button
              onClick={loadAll}
              style={{
                padding: "6px 14px",
                borderRadius: "8px",
                border: "1px solid var(--color-border-secondary)",
                background: "transparent",
                color: "var(--color-text-secondary)",
                fontSize: "13px",
                cursor: "pointer",
              }}
            >
              Refresh
            </button>
          </div>
        </div>

        {error && (
          <div style={{ padding: "12px 16px", marginBottom: "16px", borderRadius: "8px", background: "var(--color-background-danger)", color: "var(--color-text-danger)", fontSize: "13px" }}>
            {error}
          </div>
        )}

        {/* Window + granularity controls */}
        <div style={{ display: "flex", gap: "8px", marginBottom: "20px", flexWrap: "wrap" }}>
          {WINDOWS.map((w) => (
            <Chip key={w.hours} active={windowHours === w.hours} onClick={() => setWindowHours(w.hours)}>
              {w.label}
            </Chip>
          ))}
          <div style={{ flex: 1 }} />
          {GRANULARITIES.map((g) => (
            <Chip key={g.value} active={granularity === g.value} onClick={() => setGranularity(g.value)}>
              {g.label}
            </Chip>
          ))}
        </div>

        {/* KPI row */}
        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(160px, 1fr))", gap: "12px", marginBottom: "20px" }}>
          <StatBox
            label="Total cost"
            value={loadingSummary ? "—" : fmt$(summary?.total_cost_usd)}
            sub={`${summary?.total_events?.toLocaleString() ?? "—"} calls`}
          />
          <StatBox
            label="Input tokens"
            value={loadingSummary ? "—" : fmtTokens(summary?.total_input_tokens)}
          />
          <StatBox
            label="Output tokens"
            value={loadingSummary ? "—" : fmtTokens(summary?.total_output_tokens)}
          />
          <StatBox
            label="Error rate"
            value={
              loadingSummary || !summary
                ? "—"
                : fmtPct(
                    summary.total_events
                      ? (summary.error_count / summary.total_events) * 100
                      : 0
                  )
            }
            sub={`${summary?.error_count ?? "—"} errors`}
          />
          <StatBox
            label="Wrapper coverage"
            value={
              loadingCoverage ? "—" : fmtPct(coveragePct)
            }
            sub="last 7 days"
          />
        </div>

        {/* Time-series chart */}
        <Card style={{ marginBottom: "20px" }}>
          <SectionHeader title="Cost over time" />
          {loadingTimeseries ? (
            <div style={{ height: "200px", display: "flex", alignItems: "center", justifyContent: "center" }}>
              <Spinner />
            </div>
          ) : timeseries?.series?.length ? (
            <ResponsiveContainer width="100%" height={200}>
              <LineChart data={timeseries.series} margin={{ top: 4, right: 8, left: -10, bottom: 0 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="var(--color-border-tertiary)" />
                <XAxis
                  dataKey="period"
                  tickFormatter={(v) => shortTs(v, granularity)}
                  tick={{ fontSize: 11, fill: "var(--color-text-tertiary)" }}
                  tickLine={false}
                  axisLine={false}
                />
                <YAxis
                  tick={{ fontSize: 11, fill: "var(--color-text-tertiary)" }}
                  tickLine={false}
                  axisLine={false}
                  tickFormatter={(v) => `$${v.toFixed(4)}`}
                />
                <Tooltip
                  formatter={(v) => [fmt$(v), "Cost"]}
                  labelFormatter={(l) => shortTs(l, granularity)}
                  contentStyle={{
                    background: "var(--color-background-primary)",
                    border: "1px solid var(--color-border-secondary)",
                    borderRadius: "8px",
                    fontSize: "12px",
                  }}
                />
                <Line
                  type="monotone"
                  dataKey="total_cost_usd"
                  stroke="var(--color-text-secondary)"
                  strokeWidth={1.5}
                  dot={false}
                  activeDot={{ r: 3 }}
                />
              </LineChart>
            </ResponsiveContainer>
          ) : (
            <div style={{ color: "var(--color-text-tertiary)", fontSize: "13px", padding: "20px 0", textAlign: "center" }}>
              No data for this window.
            </div>
          )}
        </Card>

        {/* Token breakdown chart */}
        <Card style={{ marginBottom: "20px" }}>
          <SectionHeader title="Token consumption" />
          {loadingTimeseries || !timeseries?.series?.length ? null : (
            <ResponsiveContainer width="100%" height={160}>
              <BarChart data={timeseries.series} margin={{ top: 4, right: 8, left: -10, bottom: 0 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="var(--color-border-tertiary)" />
                <XAxis dataKey="period" tickFormatter={(v) => shortTs(v, granularity)} tick={{ fontSize: 11, fill: "var(--color-text-tertiary)" }} tickLine={false} axisLine={false} />
                <YAxis tick={{ fontSize: 11, fill: "var(--color-text-tertiary)" }} tickLine={false} axisLine={false} tickFormatter={fmtTokens} />
                <Tooltip
                  formatter={(v, name) => [fmtTokens(v), name === "input_tokens" ? "Input" : "Output"]}
                  labelFormatter={(l) => shortTs(l, granularity)}
                  contentStyle={{ background: "var(--color-background-primary)", border: "1px solid var(--color-border-secondary)", borderRadius: "8px", fontSize: "12px" }}
                />
                <Legend wrapperStyle={{ fontSize: "12px" }} />
                <Bar dataKey="input_tokens" name="Input tokens" stackId="a" fill="var(--color-border-secondary)" radius={[0, 0, 0, 0]} />
                <Bar dataKey="output_tokens" name="Output tokens" stackId="a" fill="var(--color-text-tertiary)" radius={[3, 3, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          )}
        </Card>

        {/* Breakdown table with tabs */}
        <Card style={{ marginBottom: "20px" }}>
          <SectionHeader title="Cost breakdown">
            <div style={{ display: "flex", gap: "6px" }}>
              {["agent", "user", "app", "model"].map((tab) => (
                <Chip key={tab} active={activeTab === tab} onClick={() => setActiveTab(tab)}>
                  By {tab}
                </Chip>
              ))}
            </div>
          </SectionHeader>
          <BreakdownTable
            data={breakdown}
            dimensionKey={DIMENSION_MAP[activeTab].key}
            dimensionLabel={DIMENSION_MAP[activeTab].label}
            loading={loadingBreakdown}
          />
        </Card>

        {/* Reconciliation + Coverage */}
        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "12px", marginBottom: "20px" }}>
          <Card>
            <SectionHeader title="Reconciliation runs" />
            {loadingRecon ? (
              <Spinner />
            ) : reconciliation?.runs?.length ? (
              <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "12px" }}>
                <thead>
                  <tr style={{ borderBottom: "1px solid var(--color-border-tertiary)" }}>
                    <th style={{ textAlign: "left", padding: "4px 6px", color: "var(--color-text-tertiary)", fontWeight: 400 }}>Date</th>
                    <th style={{ textAlign: "right", padding: "4px 6px", color: "var(--color-text-tertiary)", fontWeight: 400 }}>Computed</th>
                    <th style={{ textAlign: "right", padding: "4px 6px", color: "var(--color-text-tertiary)", fontWeight: 400 }}>Billed</th>
                    <th style={{ textAlign: "right", padding: "4px 6px", color: "var(--color-text-tertiary)", fontWeight: 400 }}>Variance</th>
                  </tr>
                </thead>
                <tbody>
                  {reconciliation.runs.map((run, i) => {
                    const vPct = parseFloat(run.variance_pct ?? 0);
                    const vColor =
                      Math.abs(vPct) < 5
                        ? "var(--color-text-secondary)"
                        : Math.abs(vPct) < 10
                        ? "var(--color-text-warning)"
                        : "var(--color-text-danger)";
                    return (
                      <tr key={i} style={{ borderBottom: "1px solid var(--color-border-tertiary)" }}>
                        <td style={{ padding: "6px", color: "var(--color-text-primary)" }}>{run.run_date}</td>
                        <td style={{ padding: "6px", textAlign: "right", color: "var(--color-text-secondary)", fontVariantNumeric: "tabular-nums" }}>
                          {fmt$(parseFloat(run.computed_cost_usd ?? 0))}
                        </td>
                        <td style={{ padding: "6px", textAlign: "right", color: "var(--color-text-secondary)", fontVariantNumeric: "tabular-nums" }}>
                          {run.cur_available ? fmt$(parseFloat(run.billed_cost_usd ?? 0)) : "—"}
                        </td>
                        <td style={{ padding: "6px", textAlign: "right", color: vColor, fontVariantNumeric: "tabular-nums" }}>
                          {run.cur_available ? `${vPct > 0 ? "+" : ""}${vPct.toFixed(1)}%` : "CUR pending"}
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            ) : (
              <div style={{ color: "var(--color-text-tertiary)", fontSize: "13px" }}>No reconciliation runs yet.</div>
            )}
          </Card>

          <Card>
            <SectionHeader title="Wrapper coverage" />
            {loadingCoverage ? <Spinner /> : (
              <>
                <div style={{ display: "flex", alignItems: "baseline", gap: "8px", marginBottom: "12px" }}>
                  <span style={{ fontSize: "36px", fontWeight: 500, color: coverageColor }}>
                    {fmtPct(coveragePct)}
                  </span>
                  <span style={{ fontSize: "13px", color: "var(--color-text-tertiary)" }}>last 7 days</span>
                </div>
                <div style={{ fontSize: "13px", color: "var(--color-text-secondary)", lineHeight: "1.7" }}>
                  <div style={{ display: "flex", justifyContent: "space-between" }}>
                    <span>Wrapper events</span>
                    <span style={{ fontVariantNumeric: "tabular-nums" }}>{coverage?.wrapper_events?.toLocaleString()}</span>
                  </div>
                  <div style={{ display: "flex", justifyContent: "space-between" }}>
                    <span>Backfill events</span>
                    <span style={{ fontVariantNumeric: "tabular-nums" }}>{coverage?.backfill_events?.toLocaleString()}</span>
                  </div>
                  <div style={{ display: "flex", justifyContent: "space-between", borderTop: "1px solid var(--color-border-tertiary)", marginTop: "6px", paddingTop: "6px" }}>
                    <span>Total</span>
                    <span style={{ fontVariantNumeric: "tabular-nums" }}>{coverage?.total_events?.toLocaleString()}</span>
                  </div>
                </div>
                {coveragePct < 90 && (
                  <div style={{ marginTop: "12px", padding: "10px 12px", borderRadius: "8px", background: "var(--color-background-warning)", color: "var(--color-text-warning)", fontSize: "12px" }}>
                    Coverage below 90% — check for Bedrock calls bypassing the wrapper.
                  </div>
                )}
              </>
            )}
          </Card>
        </div>

        <div style={{ fontSize: "12px", color: "var(--color-text-tertiary)", textAlign: "center", paddingBottom: "20px" }}>
          Costs computed from token counts × versioned price table. Auto-refreshes every 60s.
          Reconciliation against AWS CUR runs daily at 06:00 UTC.
        </div>
      </div>
    </>
  );
}
