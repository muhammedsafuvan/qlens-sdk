
# QueueLens Python SDK

Background job observability for Celery applications.

QueueLens captures Celery task lifecycle events and sends them to the
QueueLens observability platform.

## Features

- Celery task lifecycle instrumentation
- Task execution event tracking
- Asynchronous event delivery
- Batched HTTP ingestion
- Bounded in-memory event buffering
- Automatic retry with exponential backoff
- Duplicate-safe event ingestion
- Minimal integration with existing Celery applications

## Requirements

- Python 3.12+
- Celery 5.6+
- A QueueLens API key
- A running QueueLens ingestion API

## Installation

Install QueueLens with Celery support:

```bash
pip install "queuelens[celery]"
```

Using uv:

```bash
uv add "queuelens[celery]"
```

## Quick start

### 1. Configure environment variables

```bash
export QUEUELENS_API_KEY="ql_test_your_api_key"
export QUEUELENS_ENDPOINT="http://localhost:8001/internal/v1/events/batch"
```

### 2. Initialize QueueLens

```python
from celery import Celery
from queuelens import QueueLens

app = Celery(
    "my_application",
    broker="redis://localhost:6379/0",
    backend="redis://localhost:6379/1",
)

queuelens = QueueLens()

queuelens.instrument(app)
```

That's it. QueueLens automatically registers Celery lifecycle signal handlers.

### 3. Run your Celery worker

```bash
celery -A app worker --loglevel=info
```

QueueLens will capture task lifecycle events and asynchronously send
them to the configured ingestion endpoint.

## Configuration

QueueLens supports environment variables and explicit constructor arguments.

| Environment variable | Constructor argument | Default |
|---|---|---:|
| `QUEUELENS_API_KEY` | `api_key` | Required |
| `QUEUELENS_ENDPOINT` | `endpoint` | Required |
| `QUEUELENS_BATCH_SIZE` | `batch_size` | 100 |
| `QUEUELENS_FLUSH_INTERVAL` | `flush_interval` | 5 seconds |
| `QUEUELENS_TIMEOUT` | `timeout` | 5 seconds |
| `QUEUELENS_BUFFER_CAPACITY` | `buffer_capacity` | 1,000 |
| `QUEUELENS_MAX_BATCHES_PER_CYCLE` | `max_batches_per_cycle` | 10 |
| `QUEUELENS_SHUTDOWN_TIMEOUT` | `shutdown_timeout` | 10 seconds |

Explicit constructor arguments take precedence over environment variables.

### Constructor configuration

```python
queuelens = QueueLens(
    api_key="ql_test_your_api_key",
    endpoint="http://localhost:8001/internal/v1/events/batch",
    batch_size=100,
    flush_interval=5,
    timeout=5,
    buffer_capacity=1000,
    max_batches_per_cycle=10,
    shutdown_timeout=10,
)

queuelens.instrument(app)
```

## Lifecycle management

QueueLens manages its background flusher using Celery worker lifecycle
signals.

You can also explicitly close the client:

```python
queuelens.close()
```

The client supports context-manager usage:

```python
with QueueLens() as queuelens:
    queuelens.instrument(app)
```

Exiting the context manager closes the registered integrations.

Do not use the context manager around a short-lived setup function if the
Celery worker is expected to continue running after that function returns.

## Multiple Celery applications

One QueueLens client can instrument multiple Celery applications:

```python
queuelens = QueueLens()

queuelens.instrument(app_one)
queuelens.instrument(app_two)
```

Each application receives its own instrumentation instance.

## Event lifecycle

QueueLens captures the following events:

- `received`
- `started`
- `succeeded`
- `failed`
- `retried`

Events are buffered and delivered asynchronously in batches.

## Delivery guarantees

The current SDK uses an in-memory bounded buffer.

Events are retried when HTTP delivery fails, and duplicate event IDs
are handled by the ingestion backend.

Pending events may be lost if a worker process crashes or is forcefully
terminated before delivery.

QueueLens does not currently provide durable disk-backed buffering or
exactly-once delivery.

See [DELIVERY.md](DELIVERY.md) for detailed delivery guarantees.

## Error handling

QueueLens instrumentation and transport failures are isolated from
customer task execution.

QueueLens failures should not interrupt your Celery tasks.

Exceptions raised by your actual task functions are not suppressed.

## License

See the repository license.
