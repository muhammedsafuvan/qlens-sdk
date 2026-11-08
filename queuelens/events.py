import uuid
from datetime import UTC, datetime
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, field_validator


class JobEventType(StrEnum):
    RECEIVED = "received"
    STARTED = "started"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    RETRIED = "retried"


class JobEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: uuid.UUID = Field(default_factory=uuid.uuid4)
    task_id: str = Field(min_length=1)
    task_name: str = Field(min_length=1)
    event_type: JobEventType
    occurred_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    worker_name: str | None = None
    queue_name: str | None = None
    attempt: int = Field(default=1, ge=1)

    exchange: str | None = Field(default=None, max_length=255)
    routing_key: str | None = Field(default=None, max_length=255)

    exception_type: str | None = Field(default=None, max_length=255)
    exception_message: str | None = Field(default=None, max_length=4096)
    traceback: str | None = Field(default=None, max_length=16000)

    @field_validator("occurred_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("occurred_at must include timezone information")
        return value


class IngestionResponse(BaseModel):
    accepted_count: int = Field(ge=0)
    duplicate_count: int = Field(ge=0)