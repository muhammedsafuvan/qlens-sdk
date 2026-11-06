
from threading import Event
from unittest.mock import MagicMock

import pytest
import queuelens.transport.flusher as flusher_module
from queuelens.transport.flusher import EventFlusher


class FakeSender:
    def __init__(self, results=None):
        self.results = iter(results or [])
        self.flush_calls = 0
        self.closed = False

    def flush_once(self):
        self.flush_calls += 1

        result = next(self.results, 0)

        if isinstance(result, Exception):
            raise result

        return result

    def close(self):
        self.closed = True


class ControlledStopEvent:
    """Controls when the flusher's retry/wait loop should stop."""

    def __init__(self, stop_after_waits=1):
        self.stop_after_waits = stop_after_waits
        self.wait_calls = []
        self.stopped = False

    def is_set(self):
        return self.stopped

    def set(self):
        self.stopped = True

    def clear(self):
        self.stopped = False
        self.wait_calls.clear()

    def wait(self, timeout=None):
        self.wait_calls.append(timeout)

        if len(self.wait_calls) >= self.stop_after_waits:
            self.stopped = True

        return self.stopped


def create_flusher(sender, **kwargs):
    return EventFlusher(sender=sender, **kwargs)


def test_flusher_rejects_invalid_interval():
    with pytest.raises(ValueError, match="interval must be greater than 0"):
        create_flusher(FakeSender(), interval=0)


def test_flusher_rejects_invalid_batch_limit():
    with pytest.raises(ValueError, match="max_batches_per_cycle"):
        create_flusher(FakeSender(), max_batches_per_cycle=0)


def test_flusher_rejects_invalid_shutdown_timeout():
    with pytest.raises(ValueError, match="shutdown_timeout"):
        create_flusher(FakeSender(), shutdown_timeout=0)


def test_flusher_rejects_invalid_base_backoff():
    with pytest.raises(ValueError, match="base_backoff"):
        create_flusher(FakeSender(), base_backoff=0)


def test_flusher_rejects_max_backoff_less_than_base_backoff():
    with pytest.raises(ValueError, match="max_backoff"):
        create_flusher(
            FakeSender(),
            base_backoff=5,
            max_backoff=2,
        )


def test_calculate_retry_delay_uses_equal_jitter(monkeypatch):
    sender = FakeSender()
    flusher = create_flusher(sender)

    uniform = MagicMock(return_value=1.5)
    monkeypatch.setattr(flusher_module.random, "uniform", uniform)

    delay = flusher._calculate_retry_delay(2.0)

    assert delay == 1.5
    uniform.assert_called_once_with(1.0, 2.0)


def test_increase_backoff_doubles_current_backoff():
    flusher = create_flusher(FakeSender(), max_backoff=30)

    assert flusher._increase_backoff(1) == 2
    assert flusher._increase_backoff(2) == 4
    assert flusher._increase_backoff(4) == 8


def test_increase_backoff_respects_maximum():
    flusher = create_flusher(
        FakeSender(),
        base_backoff=2,
        max_backoff=5,
    )

    assert flusher._increase_backoff(2) == 4
    assert flusher._increase_backoff(4) == 5
    assert flusher._increase_backoff(5) == 5


def test_run_respects_max_batches_per_cycle():
    sender = FakeSender(results=[1, 1, 0])

    flusher = create_flusher(
        sender,
        max_batches_per_cycle=2,
        interval=5,
    )

    stop_event = ControlledStopEvent(stop_after_waits=1)
    flusher._stop_event = stop_event

    flusher._run()

    # Two regular batches and one final shutdown flush.
    assert sender.flush_calls == 3
    assert sender.closed is True
    assert stop_event.wait_calls == [5]


def test_run_stops_cycle_when_buffer_is_empty():
    sender = FakeSender(results=[0, 0])

    flusher = create_flusher(sender)
    flusher._stop_event = ControlledStopEvent()

    flusher._run()

    # One regular empty-buffer check and one shutdown flush.
    assert sender.flush_calls == 2
    assert sender.closed is True


def test_run_retries_after_failed_delivery(monkeypatch):
    sender = FakeSender(results=[None, 0])

    flusher = create_flusher(
        sender,
        base_backoff=2,
        max_backoff=10,
    )

    stop_event = ControlledStopEvent(stop_after_waits=1)
    flusher._stop_event = stop_event

    monkeypatch.setattr(
        flusher,
        "_calculate_retry_delay",
        lambda backoff: backoff * 0.75,
    )

    flusher._run()

    assert stop_event.wait_calls == [1.5]
    assert sender.flush_calls == 2
    assert sender.closed is True


def test_run_resets_backoff_after_success(monkeypatch):
    # Cycle 1: failure
    # Cycle 2: success (one batch, then empty)
    # Cycle 3: failure
    # Final call: shutdown flush
    sender = FakeSender(results=[None, 1, 0, None, 0])

    flusher = create_flusher(
        sender,
        interval=5,
        base_backoff=2,
        max_backoff=20,
    )

    stop_event = ControlledStopEvent(stop_after_waits=3)
    flusher._stop_event = stop_event

    monkeypatch.setattr(
        flusher,
        "_calculate_retry_delay",
        lambda backoff: backoff * 0.75,
    )

    flusher._run()

    assert stop_event.wait_calls == [
        1.5,  # First failure: 75% of 2
        5,    # Successful cycle: normal interval
        1.5,  # Backoff reset to 2 after success
    ]

    assert sender.closed is True


def test_run_handles_unexpected_sender_exception():
    sender = FakeSender(
        results=[
            RuntimeError("Unexpected sender failure"),
            0,
        ]
    )

    flusher = create_flusher(sender)
    flusher._stop_event = ControlledStopEvent()

    flusher._run()

    assert sender.flush_calls == 2
    assert sender.closed is True


def test_shutdown_flush_handles_sender_exception():
    sender = FakeSender(
        results=[RuntimeError("Shutdown delivery failed")]
    )

    flusher = create_flusher(sender)

    # Should catch the exception rather than propagate it.
    flusher._flush_on_shutdown()

    assert sender.flush_calls == 1


def test_start_is_idempotent():
    sender = FakeSender()
    flusher = create_flusher(sender, interval=60)

    try:
        flusher.start()

        assert flusher._thread is not None
        original_thread = flusher._thread

        flusher.start()

        assert flusher._thread is original_thread

    finally:
        flusher.stop()

    assert sender.closed is True


def test_stop_flushes_and_closes_sender():
    sender = FakeSender()
    initial_flush = Event()

    original_flush = sender.flush_once

    def tracked_flush():
        result = original_flush()
        initial_flush.set()
        return result

    sender.flush_once = tracked_flush

    flusher = create_flusher(sender, interval=60)

    flusher.start()

    try:
        assert initial_flush.wait(timeout=2), (
            "Flusher did not start its background loop"
        )
    finally:
        flusher.stop()

    # Initial loop flush + final shutdown flush.
    assert sender.flush_calls >= 2
    assert sender.closed is True
    assert not flusher._thread.is_alive()
