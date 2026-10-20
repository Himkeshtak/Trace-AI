CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE agents (
    agent_id UUID PRIMARY KEY,
    agent_name VARCHAR(120) NOT NULL,
    version VARCHAR(40) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE executions (
    execution_id UUID PRIMARY KEY,
    agent_id UUID NOT NULL REFERENCES agents(agent_id),
    task_name VARCHAR(240) NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    ended_at TIMESTAMPTZ,
    status VARCHAR(24) NOT NULL CHECK (status IN ('running', 'success', 'failed')),
    latency_ms INTEGER CHECK (latency_ms IS NULL OR latency_ms >= 0),
    error_message TEXT
);

CREATE TABLE tool_calls (
    tool_call_id UUID PRIMARY KEY,
    execution_id UUID NOT NULL REFERENCES executions(execution_id) ON DELETE CASCADE,
    tool_name VARCHAR(120) NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    ended_at TIMESTAMPTZ NOT NULL,
    response_time_ms INTEGER NOT NULL CHECK (response_time_ms >= 0),
    status VARCHAR(24) NOT NULL CHECK (status IN ('success', 'failed'))
);

CREATE TABLE llm_usage (
    usage_id UUID PRIMARY KEY,
    execution_id UUID NOT NULL REFERENCES executions(execution_id) ON DELETE CASCADE,
    model_name VARCHAR(120) NOT NULL,
    input_tokens INTEGER NOT NULL CHECK (input_tokens >= 0),
    output_tokens INTEGER NOT NULL CHECK (output_tokens >= 0),
    estimated_cost_usd NUMERIC(12, 6) NOT NULL CHECK (estimated_cost_usd >= 0),
    latency_ms INTEGER NOT NULL CHECK (latency_ms >= 0),
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE memory_accesses (
    access_id UUID PRIMARY KEY,
    execution_id UUID NOT NULL REFERENCES executions(execution_id) ON DELETE CASCADE,
    memory_type VARCHAR(80) NOT NULL,
    access_type VARCHAR(24) NOT NULL CHECK (access_type IN ('read', 'write')),
    retrieval_time_ms INTEGER NOT NULL CHECK (retrieval_time_ms >= 0),
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE processed_events (
    event_id UUID PRIMARY KEY,
    event_type VARCHAR(40) NOT NULL,
    execution_id UUID NOT NULL,
    payload JSONB NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source_topic VARCHAR(120),
    source_partition INTEGER CHECK (source_partition IS NULL OR source_partition >= 0),
    source_offset BIGINT CHECK (source_offset IS NULL OR source_offset >= 0)
);

CREATE TABLE daily_metrics (
    metric_date DATE PRIMARY KEY,
    total_executions INTEGER NOT NULL DEFAULT 0,
    successful_executions INTEGER NOT NULL DEFAULT 0,
    failed_executions INTEGER NOT NULL DEFAULT 0,
    average_latency_ms NUMERIC(12, 2) NOT NULL DEFAULT 0,
    input_tokens BIGINT NOT NULL DEFAULT 0,
    output_tokens BIGINT NOT NULL DEFAULT 0,
    estimated_cost_usd NUMERIC(14, 6) NOT NULL DEFAULT 0,
    calculated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_executions_agent_started ON executions(agent_id, started_at DESC);
CREATE INDEX idx_executions_status_started ON executions(status, started_at DESC);
CREATE INDEX idx_tool_calls_execution ON tool_calls(execution_id);
CREATE INDEX idx_tool_calls_name ON tool_calls(tool_name);
CREATE INDEX idx_llm_usage_execution ON llm_usage(execution_id);
CREATE INDEX idx_llm_usage_created ON llm_usage(created_at DESC);
CREATE INDEX idx_memory_accesses_execution ON memory_accesses(execution_id);
CREATE INDEX idx_processed_events_execution ON processed_events(execution_id);

