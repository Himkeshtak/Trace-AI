import type { DashboardData, Envelope } from "./types";

const baseUrl = import.meta.env.VITE_API_BASE_URL ?? "http://localhost:8000/api/v1";

async function get<T>(path: string, signal: AbortSignal): Promise<T> {
  const response = await fetch(`${baseUrl}${path}`, { headers: { Accept: "application/json" }, signal });
  if (!response.ok) throw new Error(`The API returned status ${response.status}.`);
  return ((await response.json()) as Envelope<T>).data;
}

export async function fetchDashboard(signal: AbortSignal): Promise<DashboardData> {
  if (import.meta.env.VITE_DEMO_MODE === "true") {
    return (await import("./demo")).demoData;
  }
  const [summary, tools, agents, tokens, memory, executions] = await Promise.all([
    get<DashboardData["summary"]>("/analytics/summary", signal),
    get<DashboardData["tools"]>("/analytics/tools", signal),
    get<DashboardData["agents"]>("/analytics/agents", signal),
    get<DashboardData["tokens"]>("/analytics/tokens", signal),
    get<DashboardData["memory"]>("/analytics/memory", signal),
    get<DashboardData["executions"]>("/executions/recent?limit=12", signal),
  ]);
  return { summary, tools, agents, tokens, memory, executions };
}
