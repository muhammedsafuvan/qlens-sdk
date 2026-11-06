
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from celery import signals
from queuelens.config import QueueLensConfig
from queuelens.events import JobEventType
from queuelens.instrumentation.celery import CeleryInstrumentation


@pytest.fixture
def config():
    return QueueLensConfig(
        api_key="ql_test_key",
        endpoint="http://localhost:8001/internal/v1/events/batch",
        batch_size=100,
        flush_interval=5,
        timeout=5,
        buffer_capacity=2,
    )


@pytest.fixture
def instrumentation(config):
    return CeleryInstrumentation(config)


@pytest.fixture
def celery_app():
    return MagicMock(name="celery_app")


@pytest.fixture
def task_request():
    return SimpleNamespace(
        id="task-123",
        name="orders.process",
        hostname="worker-1",
        delivery_info={"queue": "default"},
        retries=0,
    )


def test_instrumentation_registers_all_signals(instrumentation, celery_app):
    instrumentation.instrument(celery_app)

    assert instrumentation._instrumented is True
    assert instrumentation._celery_app is celery_app


def test_instrumentation_is_idempotent(instrumentation, celery_app):
    with patch.object(signals.task_received, "connect") as connect:
        instrumentation.instrument(celery_app)
        first_call_count = connect.call_count

        instrumentation.instrument(celery_app)

        assert connect.call_count == first_call_count
        assert first_call_count == 1


def test_instrumentation_connects_all_required_signals(
    instrumentation,
    celery_app,
):
    instrumentation.instrument(celery_app)

    signal_receivers = [
        (signals.task_received, "task_received"),
        (signals.task_prerun, "task_started"),
        (signals.task_success, "task_succeeded"),
        (signals.task_failure, "task_failed"),
        (signals.task_retry, "task_retried"),
        (signals.worker_ready, "worker_ready"),
        (signals.worker_process_init, "worker_process_init"),
        (signals.worker_shutdown, "worker_shutdown"),
        (signals.worker_process_shutdown, "worker_process_shutdown"),
    ]

    for signal, name in signal_receivers:
        receiver_uid = f"{instrumentation._receiver_prefix}_{name}"

        assert any(
            receiver_uid in str(receiver)
            for receiver in signal.receivers
        )


def test_task_received_captures_event(instrumentation, task_request):
    instrumentation._on_task_received(request=task_request)

    events = instrumentation._buffer.get_batch(limit=10)

    assert len(events) == 1
    assert events[0].event_type == JobEventType.RECEIVED
    assert events[0].task_id == "task-123"
    assert events[0].task_name == "orders.process"


def test_task_started_captures_event(instrumentation, task_request):
    task = SimpleNamespace(
        name="orders.process",
        request=task_request,
    )

    instrumentation._on_task_started(
        task_id="task-123",
        task=task,
    )

    events = instrumentation._buffer.get_batch(limit=10)

    assert len(events) == 1
    assert events[0].event_type == JobEventType.STARTED


def test_task_succeeded_captures_event(instrumentation, task_request):
    sender = SimpleNamespace(
        name="orders.process",
        request=task_request,
    )

    instrumentation._on_task_succeeded(sender=sender)

    events = instrumentation._buffer.get_batch(limit=10)

    assert len(events) == 1
    assert events[0].event_type == JobEventType.SUCCEEDED


def test_task_failed_captures_event(instrumentation, task_request):
    sender = SimpleNamespace(
        name="orders.process",
        request=task_request,
    )

    instrumentation._on_task_failed(sender=sender)

    events = instrumentation._buffer.get_batch(limit=10)

    assert len(events) == 1
    assert events[0].event_type == JobEventType.FAILED


def test_task_failed_uses_explicit_task_id(instrumentation, task_request):
    sender = SimpleNamespace(
        name="orders.process",
        request=task_request,
    )

    instrumentation._on_task_failed(
        sender=sender,
        task_id="explicit-task-id",
    )

    events = instrumentation._buffer.get_batch(limit=10)

    assert events[0].task_id == "explicit-task-id"


def test_task_retried_captures_event(instrumentation, task_request):
    sender = SimpleNamespace(name="orders.process")

    instrumentation._on_task_retried(
        sender=sender,
        request=task_request,
    )

    events = instrumentation._buffer.get_batch(limit=10)

    assert len(events) == 1
    assert events[0].event_type == JobEventType.RETRIED


def test_event_contains_worker_metadata(instrumentation, task_request):
    instrumentation._on_task_received(request=task_request)

    event = instrumentation._buffer.get_batch(limit=10)[0]

    assert event.worker_name == "worker-1"
    assert event.queue_name == "default"
    assert event.attempt == 1


def test_event_attempt_uses_retry_count(instrumentation, task_request):
    task_request.retries = 2

    instrumentation._on_task_received(request=task_request)

    event = instrumentation._buffer.get_batch(limit=10)[0]

    assert event.attempt == 3


@pytest.mark.parametrize(
    "task_request",
    [
        None,
        SimpleNamespace(id=None, name="orders.process"),
        SimpleNamespace(id="task-123", name=None),
    ],
)
def test_task_received_ignores_missing_metadata(
    instrumentation,
    task_request,
):
    instrumentation._on_task_received(request=task_request)

    assert len(instrumentation._buffer) == 0


def test_task_started_ignores_missing_task(instrumentation):
    instrumentation._on_task_started(task_id="task-123", task=None)

    assert len(instrumentation._buffer) == 0


def test_task_succeeded_ignores_missing_sender(instrumentation):
    instrumentation._on_task_succeeded(sender=None)

    assert len(instrumentation._buffer) == 0


def test_task_failed_ignores_missing_sender(instrumentation):
    instrumentation._on_task_failed(sender=None)

    assert len(instrumentation._buffer) == 0


def test_task_retried_ignores_missing_sender_or_request(instrumentation):
    instrumentation._on_task_retried(sender=None, request=None)

    sender = SimpleNamespace(name="orders.process")
    instrumentation._on_task_retried(sender=sender, request=None)

    assert len(instrumentation._buffer) == 0


def test_buffer_overflow_does_not_raise(instrumentation, task_request):
    instrumentation._on_task_received(request=task_request)
    instrumentation._on_task_received(request=task_request)
    instrumentation._on_task_received(request=task_request)

    assert len(instrumentation._buffer) == 2
    assert instrumentation.dropped_count == 1


def test_record_event_handles_internal_errors(instrumentation):
    with patch.object(
        instrumentation._buffer,
        "add",
        side_effect=RuntimeError("Buffer failure"),
    ):
        instrumentation._record_event(
            task_id="task-123",
            task_name="orders.process",
            event_type=JobEventType.STARTED,
        )

    assert len(instrumentation._buffer) == 0


def test_worker_ready_starts_flusher(instrumentation):
    with (
        patch(
            "queuelens.instrumentation.celery.EventSender"
        ) as sender_class,
        patch(
            "queuelens.instrumentation.celery.EventFlusher"
        ) as flusher_class,
    ):
        flusher = flusher_class.return_value

        instrumentation._on_worker_ready()

        sender_class.assert_called_once()
        flusher_class.assert_called_once()
        flusher.start.assert_called_once()

        assert instrumentation._flusher is flusher


def test_worker_process_init_starts_flusher(instrumentation):
    with (
        patch(
            "queuelens.instrumentation.celery.EventSender"
        ),
        patch(
            "queuelens.instrumentation.celery.EventFlusher"
        ) as flusher_class,
    ):
        instrumentation._on_worker_process_init()

        flusher_class.return_value.start.assert_called_once()


def test_flusher_is_not_started_twice_in_same_process(instrumentation):
    with (
        patch(
            "queuelens.instrumentation.celery.EventSender"
        ),
        patch(
            "queuelens.instrumentation.celery.EventFlusher"
        ) as flusher_class,
    ):
        instrumentation._start_flusher()
        instrumentation._start_flusher()

        assert flusher_class.call_count == 1


def test_pid_change_reinitializes_flusher(instrumentation):
    first_instance = MagicMock()
    second_instance = MagicMock()

    with (
        patch(
            "queuelens.instrumentation.celery.os.getpid",
            side_effect=[100, 200],
        ),
        patch("queuelens.instrumentation.celery.EventSender"),
        patch(
            "queuelens.instrumentation.celery.EventFlusher",
            side_effect=[first_instance, second_instance],
        ) as flusher_class,
    ):
        instrumentation._start_flusher()
        first_flusher = instrumentation._flusher

        instrumentation._start_flusher()
        second_flusher = instrumentation._flusher

    assert flusher_class.call_count == 2
    assert first_flusher is first_instance
    assert second_flusher is second_instance
    assert second_flusher is not first_flusher


def test_worker_shutdown_stops_flusher(instrumentation):
    flusher = MagicMock()
    instrumentation._flusher = flusher

    import os

    instrumentation._flusher_pid = os.getpid()

    instrumentation._on_worker_shutdown()

    flusher.stop.assert_called_once()
    assert instrumentation._flusher is None
    assert instrumentation._flusher_pid is None


def test_worker_process_shutdown_stops_flusher(instrumentation):
    flusher = MagicMock()
    instrumentation._flusher = flusher

    import os

    instrumentation._flusher_pid = os.getpid()

    instrumentation._on_worker_process_shutdown()

    flusher.stop.assert_called_once()


def test_stop_flusher_ignores_different_process(instrumentation):
    flusher = MagicMock()
    instrumentation._flusher = flusher
    instrumentation._flusher_pid = -1

    instrumentation._stop_flusher()

    flusher.stop.assert_not_called()
    assert instrumentation._flusher is flusher


def test_stop_flusher_ignores_missing_flusher(instrumentation):
    instrumentation._flusher_pid = None

    instrumentation._stop_flusher()

    assert instrumentation._flusher is None


def test_close_disconnects_signals(instrumentation, celery_app):
    instrumentation.instrument(celery_app)

    instrumentation.close()

    assert instrumentation._instrumented is False


def test_close_is_idempotent(instrumentation, celery_app):
    instrumentation.instrument(celery_app)

    instrumentation.close()
    instrumentation.close()

    assert instrumentation._instrumented is False


def test_close_stops_active_flusher(instrumentation, celery_app):
    instrumentation.instrument(celery_app)

    flusher = MagicMock()
    instrumentation._flusher = flusher

    import os

    instrumentation._flusher_pid = os.getpid()

    instrumentation.close()

    flusher.stop.assert_called_once()
    assert instrumentation._flusher is None
