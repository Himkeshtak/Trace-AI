from datetime import datetime
from decimal import Decimal
from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, model_validator


class BaseTraceEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: UUID
    execution_id: UUID
    occurred_at: datetime


class ExecutionStartedEvent(BaseTraceEvent):
    event_type: Literal["execution_started"]
    agent_id: UUID
    agent_name: str = Field(min_length=1, max_length=120)
    agent_version: str = Field(min_length=1, max_length=40)
    task_name: str = Field(min_length=1, max_length=240)


class ToolCallEvent(BaseTraceEvent):
    event_type: Literal["tool_call"]
    tool_call_id: UUID
    tool_name: str = Field(min_length=1, max_length=120)
    started_at: datetime
    ended_at: datetime
    response_time_ms: int = Field(ge=0)
    status: Literal["success", "failed"]

    @model_validator(mode="after")
    def validate_time_order(self) -> "ToolCallEvent":
        if self.ended_at < self.started_at:
            raise ValueError("ended_at must not be before started_at")
        return self


class LlmUsageEvent(BaseTraceEvent):
    event_type: Literal["llm_usage"]
    usage_id: UUID
    model_name: str = Field(min_length=1, max_length=120)
    input_tokens: int = Field(ge=0)
    output_tokens: int = Field(ge=0)
    estimated_cost_usd: Decimal = Field(ge=0, decimal_places=6)
    latency_ms: int = Field(ge=0)


class MemoryAccessEvent(BaseTraceEvent):
    event_type: Literal["memory_access"]
    access_id: UUID
    memory_type: str = Field(min_length=1, max_length=80)
    access_type: Literal["read", "write"]
    retrieval_time_ms: int = Field(ge=0)


class ExecutionCompletedEvent(BaseTraceEvent):
    event_type: Literal["execution_completed"]
    status: Literal["success", "failed"]
    ended_at: datetime
    latency_ms: int = Field(ge=0)
    error_message: str | None = Field(default=None, max_length=2_000)


TraceEvent = Annotated[
    ExecutionStartedEvent
    | ToolCallEvent
    | LlmUsageEvent
    | MemoryAccessEvent
    | ExecutionCompletedEvent,
    Field(discriminator="event_type"),
]

trace_event_adapter: TypeAdapter[TraceEvent] = TypeAdapter(TraceEvent)

