
from unittest.mock import MagicMock

import httpx
import pytest
from queuelens.events import JobEvent, JobEventType
from queuelens.transport.buffer import EventBuffer
from queuelens.transport.sender import EventSender

API_KEY = "ql_test_sender_key"
ENDPOINT = "http://localhost:8001/internal/v1/events/batch"


def create_event() -> JobEvent:
    return JobEvent(
        task_id="task-123",
        task_name="orders.process",
        event_type=JobEventType.STARTED,
        worker_name="worker-1",
        queue_name="default",
    )


def create_sender(
    buffer: EventBuffer,
    client: MagicMock | None = None,
    batch_size: int = 100,
) -> tuple[EventSender, MagicMock]:
    mock_client = client or MagicMock()

    sender = EventSender(
        buffer=buffer,
        endpoint=ENDPOINT,
        api_key=API_KEY,
        batch_size=batch_size,
        client=mock_client,
    )

    return sender, mock_client


def mock_success_response(
    client: MagicMock,
    accepted_count: int,
    duplicate_count: int = 0,
) -> None:
    response = MagicMock()
    response.json.return_value = {
        "accepted_count": accepted_count,
        "duplicate_count": duplicate_count,
    }
    client.post.return_value = response


def test_flush_once_returns_zero_when_buffer_is_empty():
    buffer = EventBuffer(capacity=10)
    sender, client = create_sender(buffer)

    result = sender.flush_once()

    assert result == 0
    client.post.assert_not_called()


def test_flush_once_sends_events_successfully():
    buffer = EventBuffer(capacity=10)
    event = create_event()
    buffer.add(event)

    sender, client = create_sender(buffer)
    mock_success_response(client, accepted_count=1)

    result = sender.flush_once()

    assert result == 1
    assert len(buffer) == 0

    client.post.assert_called_once()

    call_kwargs = client.post.call_args.kwargs

    assert client.post.call_args.args[0] == ENDPOINT
    assert call_kwargs["headers"]["Authorization"] == f"Bearer {API_KEY}"
    assert call_kwargs["json"] == {
        "events": [event.model_dump(mode="json")]
    }


def test_flush_once_acknowledges_duplicate_events():
    buffer = EventBuffer(capacity=10)
    event = create_event()
    buffer.add(event)

    sender, client = create_sender(buffer)
    mock_success_response(
        client,
        accepted_count=0,
        duplicate_count=1,
    )

    result = sender.flush_once()

    assert result == 1
    assert len(buffer) == 0


def test_flush_once_sends_only_configured_batch_size():
    buffer = EventBuffer(capacity=10)
    events = [create_event() for _ in range(3)]

    for event in events:
        buffer.add(event)

    sender, client = create_sender(buffer, batch_size=2)
    mock_success_response(client, accepted_count=2)

    result = sender.flush_once()

    assert result == 2
    assert len(buffer) == 1

    remaining = buffer.get_batch(limit=10)
    assert remaining[0].event_id == events[2].event_id


def test_flush_once_retains_events_when_http_request_fails():
    buffer = EventBuffer(capacity=10)
    event = create_event()
    buffer.add(event)

    sender, client = create_sender(buffer)
    client.post.side_effect = httpx.ConnectError("Connection failed")

    result = sender.flush_once()

    assert result is None
    assert len(buffer) == 1
    assert buffer.get_batch(limit=10)[0].event_id == event.event_id


def test_flush_once_retains_events_when_api_response_is_invalid():
    buffer = EventBuffer(capacity=10)
    event = create_event()
    buffer.add(event)

    sender, client = create_sender(buffer)

    response = MagicMock()
    response.json.return_value = {
        "accepted_count": 1,
        # duplicate_count is missing
    }
    client.post.return_value = response

    result = sender.flush_once()

    assert result is None
    assert len(buffer) == 1


def test_flush_once_retains_events_when_batch_counts_are_inconsistent():
    buffer = EventBuffer(capacity=10)
    event = create_event()
    buffer.add(event)

    sender, client = create_sender(buffer)

    # One event was sent, but the API claims it processed zero.
    mock_success_response(
        client,
        accepted_count=0,
        duplicate_count=0,
    )

    result = sender.flush_once()

    assert result is None
    assert len(buffer) == 1


def test_flush_once_retains_events_when_http_status_is_unsuccessful():
    buffer = EventBuffer(capacity=10)
    event = create_event()
    buffer.add(event)

    sender, client = create_sender(buffer)

    request = httpx.Request("POST", ENDPOINT)
    response = httpx.Response(
        status_code=500,
        request=request,
    )
    client.post.return_value = response

    result = sender.flush_once()

    assert result is None
    assert len(buffer) == 1


def test_sender_does_not_close_injected_http_client():
    buffer = EventBuffer(capacity=10)
    client = MagicMock()

    sender, _ = create_sender(buffer, client=client)

    sender.close()

    client.close.assert_not_called()


def test_sender_closes_owned_http_client(monkeypatch):
    buffer = EventBuffer(capacity=10)
    owned_client = MagicMock()

    monkeypatch.setattr(
        httpx,
        "Client",
        lambda timeout: owned_client,
    )

    sender = EventSender(
        buffer=buffer,
        endpoint=ENDPOINT,
        api_key=API_KEY,
        timeout=3.0,
    )

    sender.close()

    owned_client.close.assert_called_once()


def test_sender_rejects_invalid_batch_size():
    buffer = EventBuffer(capacity=10)

    with pytest.raises(ValueError, match="batch_size must be at least 1"):
        EventSender(
            buffer=buffer,
            endpoint=ENDPOINT,
            api_key=API_KEY,
            batch_size=0,
        )

def test_unauthorized_response_retains_events():
    buffer = EventBuffer(capacity=10)
    event = create_event()
    buffer.add(event)

    mock_client = MagicMock()
    mock_client.post.return_value = httpx.Response(
        401,
        json={"detail": "Invalid or revoked API key"},
        request=httpx.Request(
            "POST",
            "http://localhost/events",
        ),
    )

    sender = EventSender(
        buffer=buffer,
        endpoint="http://localhost/events",
        api_key="invalid-key",
        client=mock_client,
    )

    result = sender.flush_once()

    assert result is None
    assert len(buffer) == 1
    assert buffer.get_batch(limit=1)[0].event_id == event.event_id

    mock_client.post.assert_called_once()