import argparse
import asyncio
import random
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from uuid import NAMESPACE_DNS, UUID, uuid4, uuid5

import httpx

from shared.traceai_contracts import trace_event_adapter

AGENTS = (
    ("Research Agent", "1.4.0"),
    ("Support Agent", "2.1.1"),
    ("Data Analyst", "1.2.3"),
    ("Workflow Agent", "0.9.5"),
    ("Document Agent", "1.1.0"),
)
TOOLS = ("web_search", "database_query", "calculator", "file_reader", "api_request")
MODELS = ("small-reasoning-model", "general-purpose-model", "fast-chat-model")
TASKS = (
    "Research a customer question",
    "Summarize a technical document",
    "Analyze operational metrics",
    "Prepare a support response",
    "Validate a workflow request",
)
MEMORY_TYPES = ("working", "episodic", "profile")


def stable_agent_id(agent_name: str) -> UUID:
    return uuid5(NAMESPACE_DNS, f"traceai.local/{agent_name}")


def generate_execution_events(rng: random.Random, days: int) -> list[dict[str, Any]]:
    execution_id = uuid4()
    agent_name, agent_version = rng.choice(AGENTS)
    started_at = datetime.now(UTC) - timedelta(
        days=rng.randrange(max(days, 1)),
        seconds=rng.randrange(0, 86_400),
    )
    events: list[dict[str, Any]] = [
        {
            "event_id": uuid4(),
            "event_type": "execution_started",
            "execution_id": execution_id,
            "occurred_at": started_at,
            "agent_id": stable_agent_id(agent_name),
            "agent_name": agent_name,
            "agent_version": agent_version,
            "task_name": rng.choice(TASKS),
        }
    ]

    cursor = started_at
    if rng.random() < 0.55:
        retrieval_time = rng.randint(8, 180)
        cursor += timedelta(milliseconds=retrieval_time)
        events.append(
            {
                "event_id": uuid4(),
                "event_type": "memory_access",
                "execution_id": execution_id,
                "occurred_at": cursor,
                "access_id": uuid4(),
                "memory_type": rng.choice(MEMORY_TYPES),
                "access_type": "read" if rng.random() < 0.8 else "write",
                "retrieval_time_ms": retrieval_time,
            }
        )

    llm_calls = rng.randint(1, 3)
    for _ in range(llm_calls):
        input_tokens = rng.randint(120, 2_400)
        output_tokens = rng.randint(60, 1_100)
        llm_latency = rng.randint(250, 2_600)
        cursor += timedelta(milliseconds=llm_latency)
        estimated_cost = Decimal(input_tokens) * Decimal("0.00000015") + Decimal(
            output_tokens
        ) * Decimal("0.00000060")
        events.append(
            {
                "event_id": uuid4(),
                "event_type": "llm_usage",
                "execution_id": execution_id,
                "occurred_at": cursor,
                "usage_id": uuid4(),
                "model_name": rng.choice(MODELS),
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
                "estimated_cost_usd": estimated_cost.quantize(Decimal("0.000001")),
                "latency_ms": llm_latency,
            }
        )

    tool_failed = False
    for _ in range(rng.randint(1, 4)):
        tool_started = cursor
        tool_latency = rng.randint(45, 3_200)
        cursor += timedelta(milliseconds=tool_latency)
        status = "failed" if rng.random() < 0.08 else "success"
        tool_failed = tool_failed or status == "failed"
        events.append(
            {
                "event_id": uuid4(),
                "event_type": "tool_call",
                "execution_id": execution_id,
                "occurred_at": cursor,
                "tool_call_id": uuid4(),
                "tool_name": rng.choice(TOOLS),
                "started_at": tool_started,
                "ended_at": cursor,
                "response_time_ms": tool_latency,
                "status": status,
            }
        )

    failed = tool_failed or rng.random() < 0.06
    latency_ms = int((cursor - started_at).total_seconds() * 1_000)
    events.append(
        {
            "event_id": uuid4(),
            "event_type": "execution_completed",
            "execution_id": execution_id,
            "occurred_at": cursor,
            "status": "failed" if failed else "success",
            "ended_at": cursor,
            "latency_ms": latency_ms,
            "error_message": "A simulated dependency returned an error." if failed else None,
        }
    )
    return [trace_event_adapter.validate_python(event).model_dump(mode="json") for event in events]


async def send_execution(
    client: httpx.AsyncClient,
    semaphore: asyncio.Semaphore,
    events: list[dict[str, Any]],
) -> int:
    async with semaphore:
        for event in events:
            response = await client.post("/events", json=event)
            response.raise_for_status()
        return len(events)


async def run_workload(
    base_url: str,
    execution_count: int,
    concurrency: int,
    seed: int,
    days: int,
) -> None:
    rng = random.Random(seed)
    workloads = [generate_execution_events(rng, days) for _ in range(execution_count)]
    semaphore = asyncio.Semaphore(concurrency)
    timeout = httpx.Timeout(30.0)
    async with httpx.AsyncClient(base_url=base_url.rstrip("/"), timeout=timeout) as client:
        totals = await asyncio.gather(
            *(send_execution(client, semaphore, events) for events in workloads)
        )
    print(
        f"Published {sum(totals)} events across {execution_count} executions "
        f"with seed {seed}."
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate deterministic TraceAI telemetry.")
    parser.add_argument("--base-url", default="http://localhost:8000/api/v1")
    parser.add_argument("--executions", type=int, default=250)
    parser.add_argument("--concurrency", type=int, default=12)
    parser.add_argument("--seed", type=int, default=22)
    parser.add_argument("--days", type=int, default=14)
    args = parser.parse_args()
    if args.executions < 1 or args.concurrency < 1 or args.days < 1:
        parser.error("executions, concurrency, and days must all be positive")
    return args


if __name__ == "__main__":
    arguments = parse_args()
    asyncio.run(
        run_workload(
            arguments.base_url,
            arguments.executions,
            arguments.concurrency,
            arguments.seed,
            arguments.days,
        )
    )

