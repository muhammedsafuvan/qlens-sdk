import logging

import httpx
from pydantic import ValidationError

from queuelens.events import IngestionResponse
from queuelens.transport.buffer import EventBuffer

logger = logging.getLogger(__name__)


class EventSender:
    def __init__(
        self,
        buffer: EventBuffer,
        endpoint: str,
        api_key: str,
        batch_size: int = 100,
        timeout: float = 5.0,
        client: httpx.Client | None = None,
    ) -> None:
        if batch_size < 1:
            raise ValueError("batch_size must be at least 1")

        self._buffer = buffer
        self._endpoint = endpoint
        self._api_key = api_key
        self._batch_size = batch_size
        self._client = client or httpx.Client(timeout=timeout)
        self._owns_client = client is None

    def flush_once(self) -> int | None:
        events = self._buffer.get_batch(limit=self._batch_size)

        if not events:
            return 0

        payload = {
            "events": [
                event.model_dump(mode="json")
                for event in events
            ]
        }

        try:
            response = self._client.post(
                self._endpoint,
                json=payload,
                headers={
                    "Authorization": f"Bearer {self._api_key}",
                },
            )
            response.raise_for_status()

            result = IngestionResponse.model_validate(response.json())

            processed_count = (
                result.accepted_count + result.duplicate_count
            )

            if processed_count != len(events):
                raise ValueError(
                    "Ingestion API returned inconsistent batch counts"
                )

        except (
            httpx.HTTPError,
            ValidationError,
            ValueError,
        ):
            logger.exception(
                "QueueLens failed to deliver %s events. Retaining for retry.",
                len(events),
            )
            return None

        return self._buffer.acknowledge(
            {event.event_id for event in events}
        )

    def close(self) -> None:
        if self._owns_client:
            self._client.close()