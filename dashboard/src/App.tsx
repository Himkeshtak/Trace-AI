import type { ReactNode } from "react";
import type {
  AgentMetric,
  Execution,
  MemoryMetric,
  Summary,
  TokenMetric,
  ToolMetric,
} from "./types";
import { useDashboard } from "./useDashboard";

const compact = new Intl.NumberFormat("en", { notation: "compact", maximumFractionDigits: 1 });
const money = new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", maximumFractionDigits: 4 });
const time = new Intl.DateTimeFormat("en", { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" });
const day = new Intl.DateTimeFormat("en", { month: "short", day: "numeric" });

function duration(value: number): string {
  return value >= 1000 ? `${(value / 1000).toFixed(2)} s` : `${Math.round(value)} ms`;
}

function Panel({ title, note, children, wide = false, full = false }: {
  title: string; note: string; children: ReactNode; wide?: boolean; full?: boolean;
}) {
  return (
    <section className={`panel${wide ? " panel--wide" : ""}${full ? " panel--full" : ""}`}>
      <header><h2>{title}</h2><p>{note}</p></header>
      <div className="panel__body">{children}</div>
    </section>
  );
}

function Empty({ children }: { children: ReactNode }) {
  return <div className="empty">{children}</div>;
}

function MetricStrip({ summary }: { summary: Summary }) {
  const metrics = [
    ["Executions", compact.format(summary.total_executions), `${summary.failed_executions} failed`],
    ["Success rate", `${summary.success_rate.toFixed(1)}%`, `${summary.successful_executions} completed`],
    ["Average latency", duration(summary.average_latency_ms), "completed executions"],
    ["LLM tokens", compact.format(summary.input_tokens + summary.output_tokens), `${compact.format(summary.input_tokens)} input`],
    ["Estimated cost", money.format(summary.estimated_cost_usd), `${summary.memory_accesses} memory accesses`],
  ];
  return (
    <section className="metrics" aria-label="Execution summary">
      {metrics.map(([label, value, detail]) => (
        <article key={label}><span>{label}</span><strong>{value}</strong><small>{detail}</small></article>
      ))}
    </section>
  );
}

function ToolPerformance({ tools }: { tools: ToolMetric[] }) {
  if (!tools.length) return <Empty>Tool calls will appear after the simulator runs.</Empty>;
  const maximum = Math.max(...tools.map((tool) => tool.average_response_time_ms), 1);
  return (
    <div className="tool-list">
      {tools.slice(0, 6).map((tool) => (
        <div className="tool" key={tool.tool_name}>
          <div className="tool__heading"><b>{tool.tool_name.replaceAll("_", " ")}</b><span>{duration(tool.average_response_time_ms)}</span></div>
          <div className="track" role="progressbar" aria-label={`${tool.tool_name} average latency`} aria-valuenow={tool.average_response_time_ms} aria-valuemin={0} aria-valuemax={maximum}>
            <i style={{ width: `${tool.average_response_time_ms / maximum * 100}%` }} />
          </div>
          <div className="tool__meta"><span>{tool.call_count} calls</span><span>p95 {duration(tool.p95_response_time_ms)}</span><span>{tool.success_rate.toFixed(1)}% successful</span></div>
        </div>
      ))}
    </div>
  );
}

function AgentReliability({ agents }: { agents: AgentMetric[] }) {
  if (!agents.length) return <Empty>Agent reliability appears after the first execution.</Empty>;
  return (
    <div className="table-wrap"><table><thead><tr><th>Agent</th><th className="num">Runs</th><th className="num">Failures</th><th className="num">Latency</th></tr></thead>
      <tbody>{agents.map((agent) => <tr key={agent.agent_id}><td><b>{agent.agent_name}</b><small>v{agent.version}</small></td><td className="num">{agent.execution_count}</td><td className={`num${agent.failure_count ? " danger" : ""}`}>{agent.failure_count}</td><td className="num">{duration(agent.average_latency_ms)}</td></tr>)}</tbody>
    </table></div>
  );
}

function TokenUsage({ tokens }: { tokens: TokenMetric[] }) {
  if (!tokens.length) return <Empty>Token history appears after LLM usage events arrive.</Empty>;
  const maximum = Math.max(...tokens.map((item) => item.input_tokens + item.output_tokens), 1);
  return (
    <div className="token-chart" role="img" aria-label="Input and output tokens by day">
      <div className="token-chart__plot">
        {tokens.map((item) => {
          const inputHeight = item.input_tokens / maximum * 100;
          const outputHeight = item.output_tokens / maximum * 100;
          return (
            <div className="token-day" key={item.usage_date} title={`${item.input_tokens.toLocaleString()} input, ${item.output_tokens.toLocaleString()} output`}>
              <div className="bars"><i className="bar bar--input" style={{ height: `${inputHeight}%` }} /><i className="bar bar--output" style={{ height: `${outputHeight}%` }} /></div>
              <span>{day.format(new Date(`${item.usage_date}T00:00:00Z`))}</span>
            </div>
          );
        })}
      </div>
      <div className="legend"><span><i className="key key--input" />Input</span><span><i className="key key--output" />Output</span></div>
    </div>
  );
}

function MemoryActivity({ memory }: { memory: MemoryMetric[] }) {
  if (!memory.length) return <Empty>Memory activity has not arrived yet.</Empty>;
  return <ul className="memory-list">{memory.slice(0, 6).map((item) => <li key={`${item.memory_type}-${item.access_type}`}><span><b>{item.memory_type}</b><small>{item.access_type}</small></span><span className="num"><b>{item.access_count}</b><small>{duration(item.average_retrieval_time_ms)} avg</small></span></li>)}</ul>;
}

function Status({ value }: { value: Execution["status"] }) {
  return <span className={`status status--${value}`}><i />{value}</span>;
}

function RecentExecutions({ executions }: { executions: Execution[] }) {
  if (!executions.length) return <Empty>No executions have been recorded.</Empty>;
  return (
    <div className="table-wrap"><table><thead><tr><th>Execution</th><th>Agent</th><th>Task</th><th>Status</th><th className="num">Latency</th><th className="num">Started</th></tr></thead>
      <tbody>{executions.map((execution) => <tr key={execution.execution_id}><td className="mono">{execution.execution_id.slice(0, 8)}</td><td>{execution.agent_name}</td><td>{execution.task_name}</td><td><Status value={execution.status} /></td><td className="num">{execution.latency_ms === null ? "—" : duration(execution.latency_ms)}</td><td className="num subtle">{time.format(new Date(execution.started_at))}</td></tr>)}</tbody>
    </table></div>
  );
}

export function App() {
  const { data, error, loading, updatedAt, refresh } = useDashboard();
  return (
    <div className="app">
      <header className="topbar"><div className="brand"><i>TA</i><b>TraceAI</b><span />Observability</div><div className={`connection${error ? " connection--error" : ""}`}><i />{error ? "Disconnected" : "Pipeline connected"}</div></header>
      <main>
        <div className="heading"><div><p className="eyebrow">Agent execution telemetry</p><h1>Execution overview</h1><p>Operational latency, reliability, usage, and memory activity.</p></div><div className="actions"><span>{updatedAt ? `Updated ${updatedAt.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })} · Redis hot layer` : "Waiting for data"}</span><button type="button" onClick={refresh} disabled={loading}>{loading ? "Refreshing…" : "Refresh"}</button></div></div>
        {error ? <div className="error" role="alert"><div><b>Telemetry is unavailable</b><p>{error} Confirm that the API is running on port 8000.</p></div><button type="button" onClick={refresh}>Retry</button></div> : null}
        {loading && !data ? <div className="loading" aria-label="Loading telemetry"><span /><span /><span /></div> : null}
        {data ? <><MetricStrip summary={data.summary} /><div className="grid"><Panel title="Tool performance" note="Average and p95 response times by integration" wide><ToolPerformance tools={data.tools} /></Panel><Panel title="Agent reliability" note="Failures and latency by deployed agent"><AgentReliability agents={data.agents} /></Panel><Panel title="Token usage" note="Input and output volume across the last 14 days" wide><TokenUsage tokens={data.tokens} /></Panel><Panel title="Memory activity" note="Read and write frequency by memory type"><MemoryActivity memory={data.memory} /></Panel><Panel title="Recent executions" note="Latest runs received by the event pipeline" full><RecentExecutions executions={data.executions} /></Panel></div></> : null}
      </main>
    </div>
  );
}

