
from uuid import UUID

from queuelens.events import JobEvent, JobEventType
from queuelens.transport.buffer import EventBuffer


def create_event() -> JobEvent:
    return JobEvent(
        task_id="task-123",
        task_name="orders.process",
        event_type=JobEventType.STARTED,
    )


def test_add_event_to_buffer():
    buffer = EventBuffer(capacity=10)
    event = create_event()

    result = buffer.add(event)

    assert result is True
    assert len(buffer) == 1


def test_get_batch_returns_events():
    buffer = EventBuffer(capacity=10)
    first = create_event()
    second = create_event()

    buffer.add(first)
    buffer.add(second)

    batch = buffer.get_batch(limit=10)

    assert len(batch) == 2
    assert batch[0].event_id == first.event_id
    assert batch[1].event_id == second.event_id


def test_get_batch_does_not_remove_events():
    buffer = EventBuffer(capacity=10)
    buffer.add(create_event())

    buffer.get_batch(limit=10)

    assert len(buffer) == 1


def test_get_batch_respects_limit():
    buffer = EventBuffer(capacity=10)

    for _ in range(5):
        buffer.add(create_event())

    batch = buffer.get_batch(limit=2)

    assert len(batch) == 2
    assert len(buffer) == 5


def test_buffer_rejects_events_when_full():
    buffer = EventBuffer(capacity=2)

    assert buffer.add(create_event()) is True
    assert buffer.add(create_event()) is True
    assert buffer.add(create_event()) is False

    assert len(buffer) == 2
    assert buffer.dropped_count == 1


def test_dropped_count_increments_for_every_rejected_event():
    buffer = EventBuffer(capacity=1)

    buffer.add(create_event())

    assert buffer.add(create_event()) is False
    assert buffer.add(create_event()) is False

    assert buffer.dropped_count == 2


def test_acknowledge_removes_delivered_events():
    buffer = EventBuffer(capacity=10)

    first = create_event()
    second = create_event()
    third = create_event()

    buffer.add(first)
    buffer.add(second)
    buffer.add(third)

    removed = buffer.acknowledge({first.event_id, second.event_id})

    assert removed == 2
    assert len(buffer) == 1

    remaining = buffer.get_batch(limit=10)
    assert remaining[0].event_id == third.event_id


def test_acknowledge_unknown_event_id():
    buffer = EventBuffer(capacity=10)
    event = create_event()

    buffer.add(event)

    removed = buffer.acknowledge({UUID("99999999-9999-4999-8999-999999999999")})

    assert removed == 0
    assert len(buffer) == 1


def test_acknowledge_empty_set():
    buffer = EventBuffer(capacity=10)
    buffer.add(create_event())

    removed = buffer.acknowledge(set())

    assert removed == 0
    assert len(buffer) == 1


def test_invalid_buffer_capacity():
    import pytest

    with pytest.raises(ValueError):
        EventBuffer(capacity=0)


def test_invalid_batch_limit():
    import pytest

    buffer = EventBuffer(capacity=10)

    with pytest.raises(ValueError):
        buffer.get_batch(limit=0)
