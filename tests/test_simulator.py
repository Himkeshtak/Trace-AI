import random

from shared.traceai_contracts import trace_event_adapter
from simulator.generate_events import generate_execution_events


def test_generated_execution_has_ordered_lifecycle() -> None:
    events = generate_execution_events(random.Random(22), days=14)

    assert events[0]["event_type"] == "execution_started"
    assert events[-1]["event_type"] == "execution_completed"
    assert len({event["execution_id"] for event in events}) == 1
    assert all(trace_event_adapter.validate_python(event) for event in events)


def test_seed_reproduces_event_shapes() -> None:
    first = generate_execution_events(random.Random(22), days=14)
    second = generate_execution_events(random.Random(22), days=14)

    first_shape = [event["event_type"] for event in first]
    second_shape = [event["event_type"] for event in second]
    assert first_shape == second_shape

