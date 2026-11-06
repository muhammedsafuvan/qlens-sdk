import logging
import os
from typing import Any

from celery import signals

from queuelens.config import QueueLensConfig
from queuelens.events import JobEvent, JobEventType
from queuelens.transport.buffer import EventBuffer
from queuelens.transport.flusher import EventFlusher
from queuelens.transport.sender import EventSender

logger = logging.getLogger(__name__)


class CeleryInstrumentation:
    def __init__(self, config: QueueLensConfig) -> None:
        self._config = config
        self._buffer = EventBuffer(config.buffer_capacity)

        self._flusher: EventFlusher | None = None
        self._flusher_pid: int | None = None
        self._instrumented = False
        self._receiver_prefix = f"queuelens_{id(self)}"

    def instrument(self, celery_app) -> None:
        if self._instrumented:
            return

        self._celery_app = celery_app

        receivers = [
            (signals.task_received, self._on_task_received, "task_received"),
            (signals.task_prerun, self._on_task_started, "task_started"),
            (signals.task_success, self._on_task_succeeded, "task_succeeded"),
            (signals.task_failure, self._on_task_failed, "task_failed"),
            (signals.task_retry, self._on_task_retried, "task_retried"),
            (signals.worker_ready, self._on_worker_ready, "worker_ready"),
            (
                signals.worker_process_init,
                self._on_worker_process_init,
                "worker_process_init",
            ),
            (signals.worker_shutdown, self._on_worker_shutdown, "worker_shutdown"),
            (
                signals.worker_process_shutdown,
                self._on_worker_process_shutdown,
                "worker_process_shutdown",
            ),
        ]

        for signal, receiver, name in receivers:
            signal.connect(
                receiver,
                weak=False,
                dispatch_uid=f"{self._receiver_prefix}_{name}",
            )

        self._instrumented = True
        logger.info("QueueLens Celery instrumentation enabled")

    def _record_event(
        self,
        *,
        task_id: str | None,
        task_name: str | None,
        event_type: JobEventType,
        request: Any = None,
    ) -> None:
        if not task_id or not task_name:
            return

        try:
            delivery_info = getattr(request, "delivery_info", None) or {}

            event = JobEvent(
                task_id=task_id,
                task_name=task_name,
                event_type=event_type,
                worker_name=getattr(request, "hostname", None),
                queue_name=delivery_info.get("queue"),
                attempt=getattr(request, "retries", 0) + 1,
            )

            if not self._buffer.add(event):
                logger.warning(
                    "QueueLens buffer full. Dropped events: %s",
                    self._buffer.dropped_count,
                )
                return

            logger.debug(
                "QueueLens captured event: %s",
                event.model_dump_json(),
            )

        except Exception:
            logger.exception("QueueLens failed to capture a job event")

    def _on_task_received(self, request=None, **kwargs: Any) -> None:
        if request is None:
            return

        self._record_event(
            task_id=getattr(request, "id", None),
            task_name=getattr(request, "name", None),
            event_type=JobEventType.RECEIVED,
            request=request,
        )

    def _on_task_started(
        self,
        task_id=None,
        task=None,
        **kwargs: Any,
    ) -> None:
        if task is None:
            return

        self._record_event(
            task_id=task_id,
            task_name=task.name,
            event_type=JobEventType.STARTED,
            request=task.request,
        )

    def _on_task_succeeded(self, sender=None, **kwargs: Any) -> None:
        if sender is None:
            return

        request = sender.request

        self._record_event(
            task_id=request.id,
            task_name=sender.name,
            event_type=JobEventType.SUCCEEDED,
            request=request,
        )

    def _on_task_failed(
        self,
        sender=None,
        task_id=None,
        **kwargs: Any,
    ) -> None:
        if sender is None:
            return

        request = sender.request

        self._record_event(
            task_id=task_id or request.id,
            task_name=sender.name,
            event_type=JobEventType.FAILED,
            request=request,
        )

    def _on_task_retried(
        self,
        sender=None,
        request=None,
        **kwargs: Any,
    ) -> None:
        if sender is None or request is None:
            return

        self._record_event(
            task_id=request.id,
            task_name=sender.name,
            event_type=JobEventType.RETRIED,
            request=request,
        )

    def _start_flusher(self) -> None:
        current_pid = os.getpid()

        if self._flusher_pid != current_pid:
            self._flusher = None
            self._flusher_pid = current_pid

        if self._flusher is not None:
            return

        sender = EventSender(
            buffer=self._buffer,
            endpoint=self._config.endpoint,
            api_key=self._config.api_key,
            batch_size=self._config.batch_size,
            timeout=self._config.timeout,
        )

        self._flusher = EventFlusher(
            sender=sender,
            interval=self._config.flush_interval,
            max_batches_per_cycle=self._config.max_batches_per_cycle,
            shutdown_timeout=self._config.shutdown_timeout,
        )

        self._flusher.start()

        logger.info(
            "QueueLens flusher initialized in process %s",
            current_pid,
        )

    def _stop_flusher(self) -> None:
        if self._flusher_pid != os.getpid():
            return

        if self._flusher is None:
            return

        self._flusher.stop()
        self._flusher = None
        self._flusher_pid = None

    def _on_worker_ready(self, **kwargs: Any) -> None:
        self._start_flusher()

    def _on_worker_process_init(self, **kwargs: Any) -> None:
        self._start_flusher()

    def _on_worker_shutdown(self, **kwargs: Any) -> None:
        self._stop_flusher()

    def _on_worker_process_shutdown(self, **kwargs: Any) -> None:
        self._stop_flusher()

    def close(self) -> None:
        if not self._instrumented:
            return

        receivers = [
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

        for signal, name in receivers:
            signal.disconnect(
                dispatch_uid=f"{self._receiver_prefix}_{name}",
            )

        self._stop_flusher()
        self._instrumented = False
        logger.info("QueueLens Celery instrumentation disabled")

    @property
    def dropped_count(self) -> int:
        return self._buffer.dropped_count