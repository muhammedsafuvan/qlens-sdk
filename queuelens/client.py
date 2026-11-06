import logging
import os

from queuelens.config import QueueLensConfig
from queuelens.exceptions import QueueLensConfigurationError

logger = logging.getLogger(__name__)


class QueueLens:
    def __init__(
        self,
        api_key: str | None = None,
        endpoint: str | None = None,
        batch_size: int = 100,
        flush_interval: float = 5.0,
        timeout: float = 5.0,
        buffer_capacity: int = 1000,
    ) -> None:
        resolved_api_key = api_key or os.getenv("QUEUELENS_API_KEY")
        resolved_endpoint = endpoint or os.getenv("QUEUELENS_ENDPOINT")

        if not resolved_api_key:
            raise QueueLensConfigurationError(
                "Provide api_key or set QUEUELENS_API_KEY"
            )

        if not resolved_endpoint:
            raise QueueLensConfigurationError(
                "Provide endpoint or set QUEUELENS_ENDPOINT"
            )

        try:
            self._config = QueueLensConfig(
                api_key=resolved_api_key,
                endpoint=resolved_endpoint,
                batch_size=batch_size,
                flush_interval=flush_interval,
                timeout=timeout,
                buffer_capacity=buffer_capacity,
            )
        except ValueError as exc:
            raise QueueLensConfigurationError(str(exc)) from exc

        self._integrations = {}

    def instrument(self, celery_app):
        from queuelens.instrumentation.celery import CeleryInstrumentation

        app_id = id(celery_app)

        if app_id in self._integrations:
            return self._integrations[app_id]

        integration = CeleryInstrumentation(self._config)
        integration.instrument(celery_app)

        self._integrations[app_id] = integration

        return integration

    def close(self) -> None:
        for integration in self._integrations.values():
            integration.close()

        self._integrations.clear()