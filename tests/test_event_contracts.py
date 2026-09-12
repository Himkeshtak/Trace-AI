from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from pydantic import ValidationError

from shared.traceai_contracts import trace_event_adapter


def test_execution_started_event_is_discriminated() -> None:
    event = trace_event_adapter.validate_python(
        {
            "event_id": str(uuid4()),
            "event_type": "execution_started",
            "execution_id": str(uuid4()),
            "occurred_at": datetime.now(UTC).isoformat(),
            "agent_id": str(uuid4()),
            "agent_name": "Research agent",
            "agent_version": "1.0.0",
            "task_name": "Summarize sources",
        }
    )
    assert event.event_type == "execution_started"
    assert event.agent_name == "Research agent"


def test_negative_token_counts_are_rejected() -> None:
    with pytest.raises(ValidationError):
        trace_event_adapter.validate_python(
            {
                "event_id": str(uuid4()),
                "event_type": "llm_usage",
                "execution_id": str(uuid4()),
                "occurred_at": datetime.now(UTC).isoformat(),
                "usage_id": str(uuid4()),
                "model_name": "example-model",
                "input_tokens": -1,
                "output_tokens": 100,
                "estimated_cost_usd": "0.001",
                "latency_ms": 500,
            }
        )


def test_tool_call_requires_ordered_timestamps() -> None:
    now = datetime.now(UTC)
    with pytest.raises(ValidationError, match="ended_at must not be before started_at"):
        trace_event_adapter.validate_python(
            {
                "event_id": str(uuid4()),
                "event_type": "tool_call",
                "execution_id": str(uuid4()),
                "occurred_at": now.isoformat(),
                "tool_call_id": str(uuid4()),
                "tool_name": "web_search",
                "started_at": now.isoformat(),
                "ended_at": (now - timedelta(seconds=1)).isoformat(),
                "response_time_ms": 500,
                "status": "success",
            }
        )

