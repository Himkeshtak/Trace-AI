export interface Summary {
  total_executions: number;
  successful_executions: number;
  failed_executions: number;
  success_rate: number;
  average_latency_ms: number;
  input_tokens: number;
  output_tokens: number;
  estimated_cost_usd: number;
  memory_accesses: number;
}

export interface ToolMetric {
  tool_name: string;
  call_count: number;
  average_response_time_ms: number;
  p95_response_time_ms: number;
  success_rate: number;
}

export interface AgentMetric {
  agent_id: string;
  agent_name: string;
  version: string;
  execution_count: number;
  failure_count: number;
  average_latency_ms: number;
}

export interface TokenMetric {
  usage_date: string;
  input_tokens: number;
  output_tokens: number;
  estimated_cost_usd: number;
}

export interface MemoryMetric {
  memory_type: string;
  access_type: "read" | "write";
  access_count: number;
  average_retrieval_time_ms: number;
}

export interface Execution {
  execution_id: string;
  agent_name: string;
  task_name: string;
  status: "running" | "success" | "failed";
  started_at: string;
  latency_ms: number | null;
  error_message: string | null;
}

export interface Envelope<T> { data: T; meta: { cached: boolean; source?: string } }
export interface DashboardData {
  summary: Summary;
  tools: ToolMetric[];
  agents: AgentMetric[];
  tokens: TokenMetric[];
  memory: MemoryMetric[];
  executions: Execution[];
}

