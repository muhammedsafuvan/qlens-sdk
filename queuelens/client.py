
import logging
from typing import Any, Self

from queuelens.config import QueueLensConfig
from queuelens.exceptions import QueueLensConfigurationError

logger = logging.getLogger(__name__)


class QueueLens:
    """Public client for integrating QueueLens with Celery applications."""

    def __init__(
        self,
        api_key: str | None = None,
        endpoint: str | None = None,
        batch_size: int | None = None,
        flush_interval: float | None = None,
        timeout: float | None = None,
        buffer_capacity: int | None = None,
        max_batches_per_cycle: int | None = None,
        shutdown_timeout: float | None = None,
    ) -> None:
        try:
            self._config = QueueLensConfig.from_env(
                api_key=api_key,
                endpoint=endpoint,
                batch_size=batch_size,
                flush_interval=flush_interval,
                timeout=timeout,
                buffer_capacity=buffer_capacity,
                max_batches_per_cycle=max_batches_per_cycle,
                shutdown_timeout=shutdown_timeout,
            )
        except (TypeError, ValueError) as exc:
            raise QueueLensConfigurationError(str(exc)) from exc

        self._integrations: dict[int, Any] = {}
        self._closed = False

    def instrument(self, celery_app: Any) -> Any:
        """Instrument a Celery application with QueueLens."""

        if self._closed:
            raise RuntimeError(
                "QueueLens client is closed and cannot instrument applications"
            )

        from queuelens.instrumentation.celery import CeleryInstrumentation

        app_id = id(celery_app)

        if app_id in self._integrations:
            return self._integrations[app_id]

        integration = CeleryInstrumentation(self._config)
        integration.instrument(celery_app)

        self._integrations[app_id] = integration

        logger.info("QueueLens instrumented Celery application")

        return integration

    def close(self) -> None:
        """Close all registered integrations."""

        self._closed = True

        for app_id, integration in list(self._integrations.items()):
            try:
                integration.close()
            except Exception:
                logger.exception(
                    "QueueLens failed to close an integration for app %s",
                    app_id,
                )
            else:
                self._integrations.pop(app_id, None)

        logger.info("QueueLens client shutdown completed")

    def __enter__(self) -> Self:
        if self._closed:
            raise RuntimeError("QueueLens client is already closed")

        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()
