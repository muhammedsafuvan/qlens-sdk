import logging
import os
import traceback as traceback_module
from collections.abc import Mapping
from typing import Any

from celery import signals

from queuelens.config import QueueLensConfig
from queuelens.events import JobEvent, JobEventType
from queuelens.transport.buffer import EventBuffer
from queuelens.transport.flusher import EventFlusher
from queuelens.transport.sender import EventSender

logger = logging.getLogger(__name__)

MAX_EXCEPTION_MESSAGE_LENGTH = 4096
MAX_TRACEBACK_LENGTH = 16000


class CeleryInstrumentation:
    def __init__(self, config: QueueLensConfig) -> None:
        self._config = config
        self._buffer = EventBuffer(config.buffer_capacity)
        self._celery_app = None
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

    @staticmethod
    def _get_request_mappings(request: Any) -> list[Mapping[str, Any]]:
        """Return available Celery request dictionaries."""
        mappings: list[Mapping[str, Any]] = []

        if request is None:
            return mappings

        for attribute in ("request_dict", "_request_dict"):
            try:
                value = getattr(request, attribute, None)
            except AttributeError:
                continue

            if isinstance(value, Mapping):
                mappings.append(value)

        return mappings

    @classmethod
    def _get_attempt(cls, request: Any) -> int:
        """
        Extract the Celery retry count from different request representations.

        Celery's worker task_received signal and task execution signals can
        expose different request objects. Check both direct attributes and
        the underlying request dictionaries.
        """
        retry_counts: list[int] = []

        if request is not None:
            try:
                direct_retries = getattr(request, "retries", None)
                if direct_retries is not None:
                    retry_counts.append(int(direct_retries))
            except (TypeError, ValueError, AttributeError):
                pass

        for request_dict in cls._get_request_mappings(request):
            retries = request_dict.get("retries")

            if retries is None:
                continue

            try:
                retry_counts.append(int(retries))
            except (TypeError, ValueError):
                continue

        retries = max(retry_counts, default=0)
        return max(retries + 1, 1)

    @staticmethod
    def _get_exchange_name(exchange: Any) -> str | None:
        if exchange is None:
            return None

        name = getattr(exchange, "name", exchange)
        return str(name) if name else None

    def _get_delivery_metadata(self, request: Any) -> dict[str, str | None]:
        """
        Extract queue, exchange and routing key without guessing.

        Queue names are resolved only when Celery explicitly provides the
        queue or when a routing key matches a configured queue exactly.
        """
        delivery_info = getattr(request, "delivery_info", None) or {}

        if not isinstance(delivery_info, Mapping):
            delivery_info = {}

        queue_name = delivery_info.get("queue")
        exchange = self._get_exchange_name(delivery_info.get("exchange"))

        routing_key = delivery_info.get("routing_key")
        routing_key = str(routing_key) if routing_key else None

        if not queue_name and routing_key and self._celery_app is not None:
            try:
                configured_queues = self._celery_app.conf.task_queues or ()

                if isinstance(configured_queues, Mapping):
                    configured_queues = configured_queues.values()

                for queue in configured_queues:
                    configured_routing_key = getattr(queue, "routing_key", None)

                    if configured_routing_key != routing_key:
                        continue

                    configured_name = getattr(queue, "name", None)

                    if configured_name:
                        queue_name = configured_name
                        break

                # Resolve the default queue only when its routing key
                # explicitly matches the delivery routing key.
                if not queue_name:
                    default_routing_key = (
                        self._celery_app.conf.task_default_routing_key
                    )

                    if (
                        default_routing_key
                        and default_routing_key == routing_key
                    ):
                        default_queue = self._celery_app.conf.task_default_queue
                        queue_name = getattr(
                            default_queue,
                            "name",
                            default_queue,
                        )

            except Exception:
                logger.debug(
                    "QueueLens could not resolve configured queue metadata",
                    exc_info=True,
                )

        return {
            "queue_name": str(queue_name) if queue_name else None,
            "exchange": exchange,
            "routing_key": routing_key,
        }

    @staticmethod
    def _truncate(value: str | None, limit: int) -> str | None:
        if value is None:
            return None

        if len(value) <= limit:
            return value

        suffix = "... [truncated]"
        return value[: limit - len(suffix)] + suffix

    @classmethod
    def _format_traceback(
        cls,
        exception: BaseException | None,
        traceback_obj: Any = None,
        einfo: Any = None,
    ) -> str | None:
        """
        Prefer Celery's formatted traceback, then format the traceback object.
        """
        if einfo is not None:
            formatted = getattr(einfo, "traceback", None)

            if isinstance(formatted, str) and formatted:
                return cls._truncate(formatted, MAX_TRACEBACK_LENGTH)

        if isinstance(traceback_obj, str) and traceback_obj:
            return cls._truncate(traceback_obj, MAX_TRACEBACK_LENGTH)

        if exception is not None and traceback_obj is not None:
            try:
                formatted_lines = traceback_module.format_exception(
                    type(exception),
                    exception,
                    traceback_obj,
                )
                formatted = "".join(formatted_lines)

                return cls._truncate(formatted, MAX_TRACEBACK_LENGTH)
            except Exception:
                logger.debug(
                    "QueueLens could not format exception traceback",
                    exc_info=True,
                )

        return None

    def _record_event(
        self,
        *,
        task_id: str | None,
        task_name: str | None,
        event_type: JobEventType,
        request: Any = None,
        exception: BaseException | None = None,
        traceback_obj: Any = None,
        einfo: Any = None,
    ) -> None:
        if not task_id or not task_name:
            return

        try:
            delivery_metadata = self._get_delivery_metadata(request)

            exception_type = None
            exception_message = None
            formatted_traceback = None

            if event_type == JobEventType.FAILED and exception is not None:
                exception_type = type(exception).__name__
                exception_message = self._truncate(
                    str(exception),
                    MAX_EXCEPTION_MESSAGE_LENGTH,
                )
                formatted_traceback = self._format_traceback(
                    exception,
                    traceback_obj,
                    einfo,
                )

            event = JobEvent(
                task_id=task_id,
                task_name=task_name,
                event_type=event_type,
                worker_name=getattr(request, "hostname", None),
                attempt=self._get_attempt(request),
                **delivery_metadata,
                exception_type=exception_type,
                exception_message=exception_message,
                traceback=formatted_traceback,
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

    def _on_task_received(
        self,
        sender=None,
        request=None,
        **kwargs: Any,
    ) -> None:
        try:
            task = getattr(request, "task", None)

            if task is not None and not self._belongs_to_app(task):
                return

            if request is None:
                return

            self._record_event(
                task_id=getattr(request, "id", None),
                task_name=getattr(request, "name", None),
                event_type=JobEventType.RECEIVED,
                request=request,
            )
        except Exception:
            logger.exception("QueueLens failed to handle received task signal")

    def _on_task_started(
        self,
        sender=None,
        task_id=None,
        task=None,
        **kwargs: Any,
    ) -> None:
        try:
            if not self._belongs_to_app(task):
                return

            if task is None:
                return

            self._record_event(
                task_id=task_id,
                task_name=task.name,
                event_type=JobEventType.STARTED,
                request=task.request,
            )
        except Exception:
            logger.exception("QueueLens failed to handle started task signal")

    def _on_task_succeeded(self, sender=None, **kwargs: Any) -> None:
        try:
            if not self._belongs_to_app(sender):
                return

            if sender is None:
                return

            request = sender.request

            self._record_event(
                task_id=request.id,
                task_name=sender.name,
                event_type=JobEventType.SUCCEEDED,
                request=request,
            )
        except Exception:
            logger.exception("QueueLens failed to handle succeeded task signal")

    def _on_task_failed(
        self,
        sender=None,
        task_id=None,
        exception=None,
        traceback=None,
        einfo=None,
        **kwargs: Any,
    ) -> None:
        try:
            if not self._belongs_to_app(sender):
                return

            if sender is None:
                return

            request = sender.request

            self._record_event(
                task_id=task_id or request.id,
                task_name=sender.name,
                event_type=JobEventType.FAILED,
                request=request,
                exception=exception,
                traceback_obj=traceback,
                einfo=einfo,
            )
        except Exception:
            logger.exception("QueueLens failed to handle failed task signal")

    def _on_task_retried(
        self,
        sender=None,
        request=None,
        **kwargs: Any,
    ) -> None:
        try:
            if not self._belongs_to_app(sender):
                return

            if sender is None or request is None:
                return

            self._record_event(
                task_id=request.id,
                task_name=sender.name,
                event_type=JobEventType.RETRIED,
                request=request,
            )
        except Exception:
            logger.exception("QueueLens failed to handle retried task signal")

    def _start_flusher(self) -> None:
        current_pid = os.getpid()

        if self._flusher_pid != current_pid:
            self._flusher = None
            self._flusher_pid = current_pid

        if self._flusher is not None:
            if self._flusher.is_alive:
                return

            # The previous flusher has exited. Its sender is already closed.
            self._flusher = None
            self._flusher_pid = None

        sender = None

        try:
            sender = EventSender(
                buffer=self._buffer,
                endpoint=self._config.endpoint,
                api_key=self._config.api_key,
                batch_size=self._config.batch_size,
                timeout=self._config.timeout,
            )

            flusher = EventFlusher(
                sender=sender,
                interval=self._config.flush_interval,
                max_batches_per_cycle=self._config.max_batches_per_cycle,
                shutdown_timeout=self._config.shutdown_timeout,
            )

            flusher.start()
            self._flusher = flusher

            logger.info(
                "QueueLens flusher initialized in process %s",
                current_pid,
            )

        except Exception:
            logger.exception(
                "QueueLens failed to initialize event flusher in process %s",
                current_pid,
            )

            if sender is not None:
                try:
                    sender.close()
                except Exception:
                    logger.exception(
                        "QueueLens failed to close sender after initialization failure"
                    )

    def _stop_flusher(self) -> None:
        if self._flusher_pid != os.getpid():
            return

        if self._flusher is None:
            return

        try:
            stopped = self._flusher.stop()
        except Exception:
            logger.exception("QueueLens failed to stop event flusher")
            return

        if not stopped:
            return

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

    def _belongs_to_app(self, sender: Any) -> bool:
        if self._celery_app is None:
            return True

        sender_app = getattr(sender, "app", None)

        # Some Celery signals do not provide an app-bearing sender.
        # In those cases, ownership cannot be determined from the sender.
        if sender_app is None:
            return True

        return sender_app is self._celery_app

    @property
    def dropped_count(self) -> int:
        return self._buffer.dropped_count