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

    @classmethod
    def from_env(cls) -> "QueueLensConfig":
        api_key = os.getenv("QUEUELENS_API_KEY")
        endpoint = os.getenv("QUEUELENS_ENDPOINT")

        if not api_key:
            raise ValueError("QUEUELENS_API_KEY is required")

        if not endpoint:
            raise ValueError("QUEUELENS_ENDPOINT is required")

        return cls(
            api_key=api_key,
            endpoint=endpoint,
            batch_size=int(os.getenv("QUEUELENS_BATCH_SIZE", "100")),
            flush_interval=float(
                os.getenv("QUEUELENS_FLUSH_INTERVAL", "5")
            ),
            timeout=float(os.getenv("QUEUELENS_TIMEOUT", "5")),
            buffer_capacity=int(
                os.getenv("QUEUELENS_BUFFER_CAPACITY", "1000")
            ),
        )