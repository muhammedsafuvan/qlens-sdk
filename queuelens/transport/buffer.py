from collections import deque
from threading import Lock
from uuid import UUID

from queuelens.events import JobEvent


class EventBuffer:
    def __init__(self, capacity: int = 1000) -> None:
        if capacity < 1:
            raise ValueError("capacity must be at least 1")

        self._events: deque[JobEvent] = deque()
        self._capacity = capacity
        self._lock = Lock()
        self._dropped_count = 0

    def add(self, event: JobEvent) -> bool:
        with self._lock:
            if len(self._events) >= self._capacity:
                self._dropped_count += 1
                return False

            self._events.append(event)
            return True

    def get_batch(self, limit: int = 100) -> list[JobEvent]:
        if limit < 1:
            raise ValueError("limit must be at least 1")

        with self._lock:
            return list(self._events)[:limit]

    def acknowledge(self, event_ids: set[UUID]) -> int:
        if not event_ids:
            return 0

        with self._lock:
            original_count = len(self._events)

            self._events = deque(
                event
                for event in self._events
                if event.event_id not in event_ids
            )

            return original_count - len(self._events)

    @property
    def dropped_count(self) -> int:
        with self._lock:
            return self._dropped_count

    def __len__(self) -> int:
        with self._lock:
            return len(self._events)