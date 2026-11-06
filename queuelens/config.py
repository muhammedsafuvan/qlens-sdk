import os
from dataclasses import dataclass


@dataclass(frozen=True)
class QueueLensConfig:
    api_key: str
    endpoint: str
    batch_size: int = 100
    flush_interval: float = 5.0
    timeout: float = 5.0
    buffer_capacity: int = 1000
    max_batches_per_cycle: int = 10
    shutdown_timeout: float = 10.0

    def __post_init__(self) -> None:
        if not self.api_key.strip():
            raise ValueError("API key cannot be empty")

        if not self.endpoint.strip():
            raise ValueError("Endpoint cannot be empty")

        if self.batch_size < 1:
            raise ValueError("batch_size must be at least 1")

        if self.flush_interval <= 0:
            raise ValueError("flush_interval must be greater than 0")

        if self.timeout <= 0:
            raise ValueError("timeout must be greater than 0")

        if self.buffer_capacity < 1:
            raise ValueError("buffer_capacity must be at least 1")

        if self.max_batches_per_cycle < 1:
            raise ValueError("max_batches_per_cycle must be at least 1")

        if self.shutdown_timeout <= 0:
            raise ValueError("shutdown_timeout must be greater than 0")

    @classmethod
    def from_env(
        cls,
        *,
        api_key: str | None = None,
        endpoint: str | None = None,
        batch_size: int | None = None,
        flush_interval: float | None = None,
        timeout: float | None = None,
        buffer_capacity: int | None = None,
        max_batches_per_cycle: int | None = None,
        shutdown_timeout: float | None = None,
    ) -> "QueueLensConfig":
        resolved_api_key = (
            api_key if api_key is not None else os.getenv("QUEUELENS_API_KEY")
        )
        resolved_endpoint = (
            endpoint if endpoint is not None else os.getenv("QUEUELENS_ENDPOINT")
        )

        if resolved_api_key is None:
            raise ValueError("QUEUELENS_API_KEY is required")

        if resolved_endpoint is None:
            raise ValueError("QUEUELENS_ENDPOINT is required")

        return cls(
            api_key=resolved_api_key,
            endpoint=resolved_endpoint,
            batch_size=(
                batch_size
                if batch_size is not None
                else int(os.getenv("QUEUELENS_BATCH_SIZE", "100"))
            ),
            flush_interval=(
                flush_interval
                if flush_interval is not None
                else float(os.getenv("QUEUELENS_FLUSH_INTERVAL", "5"))
            ),
            timeout=(
                timeout
                if timeout is not None
                else float(os.getenv("QUEUELENS_TIMEOUT", "5"))
            ),
            buffer_capacity=(
                buffer_capacity
                if buffer_capacity is not None
                else int(os.getenv("QUEUELENS_BUFFER_CAPACITY", "1000"))
            ),
            max_batches_per_cycle=(
                max_batches_per_cycle
                if max_batches_per_cycle is not None
                else int(os.getenv("QUEUELENS_MAX_BATCHES_PER_CYCLE", "10"))
            ),
            shutdown_timeout=(
                shutdown_timeout
                if shutdown_timeout is not None
                else float(os.getenv("QUEUELENS_SHUTDOWN_TIMEOUT", "10"))
            ),
        )