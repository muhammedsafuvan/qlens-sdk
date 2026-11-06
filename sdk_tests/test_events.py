
from datetime import UTC, datetime
from uuid import UUID

import pytest
from pydantic import ValidationError
from queuelens.events import JobEvent, JobEventType


def create_event(**overrides) -> JobEvent:
    data = {
        "task_id": "task-123",
        "task_name": "orders.process",
        "event_type": JobEventType.STARTED,
        "worker_name": "worker-1",
        "queue_name": "default",
        "attempt": 1,
    }
    data.update(overrides)
    return JobEvent(**data)


def test_event_is_created_with_default_values():
    event = create_event()

    assert isinstance(event.event_id, UUID)
    assert isinstance(event.occurred_at, datetime)
    assert event.occurred_at.tzinfo is not None
    assert event.attempt == 1


def test_event_generates_unique_ids():
    first = create_event()
    second = create_event()

    assert first.event_id != second.event_id


@pytest.mark.parametrize(
    "event_type",
    [
        JobEventType.RECEIVED,
        JobEventType.STARTED,
        JobEventType.SUCCEEDED,
        JobEventType.FAILED,
        JobEventType.RETRIED,
    ],
)
def test_supported_event_types(event_type):
    event = create_event(event_type=event_type)

    assert event.event_type == event_type


def test_invalid_event_type_is_rejected():
    with pytest.raises(ValidationError):
        create_event(event_type="unknown")


def test_empty_task_id_is_rejected():
    with pytest.raises(ValidationError):
        create_event(task_id="")


def test_empty_task_name_is_rejected():
    with pytest.raises(ValidationError):
        create_event(task_name="")


def test_attempt_must_be_at_least_one():
    with pytest.raises(ValidationError):
        create_event(attempt=0)


def test_naive_datetime_is_rejected():
    with pytest.raises(ValidationError):
        create_event(occurred_at=datetime(2026, 11, 6, 10, 0, 0))


def test_timezone_aware_datetime_is_accepted():
    timestamp = datetime(2026, 11, 6, 10, 0, 0, tzinfo=UTC)

    event = create_event(occurred_at=timestamp)

    assert event.occurred_at == timestamp


def test_event_can_be_serialized_to_json():
    event = create_event()

    serialized = event.model_dump(mode="json")

    assert serialized["task_id"] == "task-123"
    assert serialized["task_name"] == "orders.process"
    assert serialized["event_type"] == "started"
    assert isinstance(serialized["event_id"], str)
