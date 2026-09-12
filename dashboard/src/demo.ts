import type { DashboardData } from "./types";

export const demoData: DashboardData = {
  summary: {
    total_executions: 1248,
    successful_executions: 1126,
    failed_executions: 122,
    success_rate: 90.2,
    average_latency_ms: 3842,
    input_tokens: 1642000,
    output_tokens: 683000,
    estimated_cost_usd: 0.6564,
    memory_accesses: 716,
  },
  tools: [
    { tool_name: "web_search", call_count: 634, average_response_time_ms: 1840, p95_response_time_ms: 3040, success_rate: 91.6 },
    { tool_name: "api_request", call_count: 521, average_response_time_ms: 1435, p95_response_time_ms: 2870, success_rate: 88.9 },
    { tool_name: "database_query", call_count: 608, average_response_time_ms: 960, p95_response_time_ms: 2110, success_rate: 95.2 },
    { tool_name: "file_reader", call_count: 487, average_response_time_ms: 440, p95_response_time_ms: 970, success_rate: 98.1 },
    { tool_name: "calculator", call_count: 402, average_response_time_ms: 125, p95_response_time_ms: 310, success_rate: 99.5 },
  ],
  agents: [
    { agent_id: "1", agent_name: "Support Agent", version: "2.1.1", execution_count: 273, failure_count: 36, average_latency_ms: 4210 },
    { agent_id: "2", agent_name: "Research Agent", version: "1.4.0", execution_count: 256, failure_count: 29, average_latency_ms: 4980 },
    { agent_id: "3", agent_name: "Workflow Agent", version: "0.9.5", execution_count: 241, failure_count: 24, average_latency_ms: 3650 },
    { agent_id: "4", agent_name: "Data Analyst", version: "1.2.3", execution_count: 252, failure_count: 19, average_latency_ms: 3320 },
    { agent_id: "5", agent_name: "Document Agent", version: "1.1.0", execution_count: 226, failure_count: 14, average_latency_ms: 2780 },
  ],
  tokens: Array.from({ length: 14 }, (_, index) => ({
    usage_date: new Date(Date.UTC(2026, 8, index + 1)).toISOString().slice(0, 10),
    input_tokens: 82000 + ((index * 31741) % 72000),
    output_tokens: 31000 + ((index * 15403) % 39000),
    estimated_cost_usd: 0.03 + index * 0.002,
  })),
  memory: [
    { memory_type: "working", access_type: "read", access_count: 286, average_retrieval_time_ms: 34 },
    { memory_type: "episodic", access_type: "read", access_count: 194, average_retrieval_time_ms: 82 },
    { memory_type: "profile", access_type: "read", access_count: 117, average_retrieval_time_ms: 41 },
    { memory_type: "working", access_type: "write", access_count: 75, average_retrieval_time_ms: 29 },
    { memory_type: "episodic", access_type: "write", access_count: 44, average_retrieval_time_ms: 96 },
  ],
  executions: [
    { execution_id: "c31f0a9b-a5aa-4be0-9b48-a4e8cd683743", agent_name: "Research Agent", task_name: "Research a customer question", status: "success", started_at: "2026-09-13T09:42:00Z", latency_ms: 4260, error_message: null },
    { execution_id: "a807e122-3d85-4e8c-ad40-f52dcb77490c", agent_name: "Support Agent", task_name: "Prepare a support response", status: "failed", started_at: "2026-09-13T09:39:00Z", latency_ms: 5170, error_message: "Dependency timeout" },
    { execution_id: "d31dd799-c6de-4c1b-9363-f74df8a2d11c", agent_name: "Data Analyst", task_name: "Analyze operational metrics", status: "success", started_at: "2026-09-13T09:34:00Z", latency_ms: 2980, error_message: null },
    { execution_id: "9e2bdf36-e04a-4484-a68f-51dc2f8c92a1", agent_name: "Document Agent", task_name: "Summarize a technical document", status: "running", started_at: "2026-09-13T09:31:00Z", latency_ms: null, error_message: null },
    { execution_id: "e739210c-92bf-4564-9c7d-c72f42c6c7b4", agent_name: "Workflow Agent", task_name: "Validate a workflow request", status: "success", started_at: "2026-09-13T09:28:00Z", latency_ms: 3470, error_message: null },
  ],
};

