# QueueLens Python SDK

QueueLens provides background job observability for Python applications using Celery.

It instruments Celery task lifecycle events and asynchronously delivers them to your QueueLens instance, allowing you to monitor job execution, failures, retries, and worker activity.

## Requirements

- Python 3.12+
- Celery 5.6+
- A running QueueLens instance
- A QueueLens workspace API key

## Installation

Install the SDK with Celery support:

```bash
pip install "queuelens[celery]"
```

This installs QueueLens and its optional Celery integration dependencies.

## Quick start

The recommended integration is **Standard Integration**.

### 1. Configure environment variables

```bash
export QUEUELENS_API_KEY="ql_test_your_api_key"
export QUEUELENS_ENDPOINT="http://localhost:8001/internal/v1/events/batch"
```

Replace the example API key and endpoint with the credentials and ingestion endpoint provided by your QueueLens deployment.

### 2. Instrument your Celery application

```python
from celery import Celery
from queuelens import QueueLens

app = Celery("orders")

queuelens = QueueLens()
queuelens.instrument(app)
```

That's it. QueueLens automatically registers its Celery signal handlers and manages event delivery through a background flusher.

You do not need to modify your existing task definitions.

### 3. Run your worker

Start your Celery worker as usual:

```bash
celery -A your_module.app worker --loglevel=info
```

Your task lifecycle events will be captured and delivered to QueueLens.

## Configuration

QueueLens supports configuration through constructor arguments or environment variables.

Constructor arguments take precedence over environment variables, which take precedence over defaults.

| Parameter | Environment variable | Default |
|---|---|---:|
| `api_key` | `QUEUELENS_API_KEY` | Required |
| `endpoint` | `QUEUELENS_ENDPOINT` | Required |
| `batch_size` | `QUEUELENS_BATCH_SIZE` | 100 |
| `flush_interval` | `QUEUELENS_FLUSH_INTERVAL` | 5 seconds |
| `timeout` | `QUEUELENS_TIMEOUT` | 5 seconds |
| `buffer_capacity` | `QUEUELENS_BUFFER_CAPACITY` | 1,000 |
| `max_batches_per_cycle` | `QUEUELENS_MAX_BATCHES_PER_CYCLE` | 10 |
| `shutdown_timeout` | `QUEUELENS_SHUTDOWN_TIMEOUT` | 10 seconds |

### Configure using Python

```python
from celery import Celery
from queuelens import QueueLens

app = Celery("orders")

queuelens = QueueLens(
    api_key="ql_test_your_api_key",
    endpoint="http://localhost:8001/internal/v1/events/batch",
    batch_size=100,
    flush_interval=5,
    timeout=5,
)

queuelens.instrument(app)
```

### Configure using environment variables

```bash
export QUEUELENS_API_KEY="ql_test_your_api_key"
export QUEUELENS_ENDPOINT="http://localhost:8001/internal/v1/events/batch"

export QUEUELENS_BATCH_SIZE=100
export QUEUELENS_FLUSH_INTERVAL=5
export QUEUELENS_TIMEOUT=5
export QUEUELENS_BUFFER_CAPACITY=1000
export QUEUELENS_MAX_BATCHES_PER_CYCLE=10
export QUEUELENS_SHUTDOWN_TIMEOUT=10
```

Keep API keys in environment variables or a secrets manager. Do not commit credentials to source control.

## Captured events

QueueLens captures the following Celery task lifecycle events:

| Event | Description |
|---|---|
| `received` | Task received by a worker |
| `started` | Task execution started |
| `succeeded` | Task completed successfully |
| `failed` | Task execution failed |
| `retried` | Task was scheduled for retry |

Each event includes available metadata such as:

- Task ID
- Task name
- Event type
- Event timestamp
- Worker name
- Queue name
- Attempt number

The attempt number is derived from Celery's retry count.

## Event delivery

QueueLens delivers events asynchronously so that instrumentation does not need to wait for HTTP requests during task execution.

The SDK uses:

- A bounded in-memory event buffer.
- Configurable batch sizes.
- Periodic background flushing.
- HTTP timeouts.
- Automatic retries with exponential backoff and jitter.
- Event acknowledgement after successful ingestion.
- A final delivery attempt during graceful shutdown.

### Retry behavior

When delivery fails, events remain in the buffer and are retried.

The retry delay uses exponential backoff, starting at approximately one second and increasing up to 30 seconds, with jitter.

Events are acknowledged only after the ingestion response is successfully received and validated.

QueueLens uses event IDs to support duplicate-safe ingestion. This provides retry-safe delivery behavior, not exactly-once delivery.

## Graceful shutdown

QueueLens automatically integrates with Celery worker lifecycle signals.

When a worker shuts down, the SDK:

1. Stops the background event flusher.
2. Attempts to deliver remaining buffered events.
3. Closes its HTTP client.

You generally do not need to manage the SDK lifecycle manually.

For explicit cleanup, you can call:

```python
queuelens.close()
```

Do not call this while the Celery worker is still expected to capture events.

## Reliability and limitations

QueueLens is designed to keep observability failures isolated from application task execution.

However, there are important limitations:

- **In-memory buffering:** Events are held in process memory and are not persisted to disk.
- **Process termination:** A crash, forceful termination, or machine failure can result in loss of pending events.
- **Buffer capacity:** When the buffer is full, new events are dropped and the SDK logs a warning.
- **Network failures:** Failed deliveries are retried while the process remains alive and events remain buffered.
- **Graceful shutdown:** The SDK makes a final delivery attempt, but delivery is not guaranteed if the endpoint remains unavailable or the shutdown timeout is exceeded.

The SDK should be treated as an observability integration, not as a replacement for Celery's task reliability mechanisms.

## Logging

QueueLens uses Python's standard logging module.

To enable debug-level SDK logs:

```python
import logging

logging.basicConfig(level=logging.INFO)
logging.getLogger("queuelens").setLevel(logging.DEBUG)
```

This can help diagnose event capture, delivery failures, retries, and buffer overflow.

## Example application

```python
from celery import Celery
from queuelens import QueueLens

app = Celery(
    "orders",
    broker="redis://localhost:6379/0",
    backend="redis://localhost:6379/1",
)

queuelens = QueueLens()
queuelens.instrument(app)


@app.task
def process_order(order_id: str):
    print(f"Processing order: {order_id}")
    return {"order_id": order_id, "status": "completed"}
```

Run the worker:

```bash
celery -A app worker --loglevel=info
```

## License

See the repository's license information.