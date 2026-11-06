
# QueueLens SDK — Event Delivery Guarantees

## Overview

The QueueLens SDK captures Celery task lifecycle events and asynchronously
transmits them to the QueueLens ingestion API.

The SDK is designed to observe customer workloads without becoming a
dependency of task execution.

Event delivery uses an in-memory bounded buffer, background HTTP delivery,
and retry handling.

## 1. Event buffering

The SDK maintains an in-memory event buffer.

Default configuration:

| Setting | Default |
|---|---:|
| Buffer capacity | 1,000 events |
| Batch size | 100 events |
| Flush interval | 5 seconds |
| Maximum batches per cycle | 10 |

Events are added to the buffer when Celery lifecycle signals are received.

The buffer is bounded to prevent unbounded memory consumption.

### Buffer overflow

When the buffer reaches capacity:

- New events are rejected.
- Existing buffered events are retained.
- The dropped event counter is incremented.
- A warning is logged.

The SDK exposes the dropped event count through its instrumentation instance.

## 2. Event transmission

The SDK sends buffered events to the QueueLens ingestion API using HTTP.

Each request contains a batch of events.

Authentication uses the configured QueueLens API key.

The SDK acknowledges events only after:

1. The HTTP request succeeds.
2. The ingestion response can be validated.
3. The accepted and duplicate counts match the submitted batch size.

Events that fail delivery remain in the buffer.

## 3. Retry behavior

When event delivery fails, the SDK retains the events for another attempt.

The background flusher uses exponential backoff with equal jitter.

Default retry settings:

| Setting | Default |
|---|---:|
| Initial backoff | 1 second |
| Maximum backoff | 30 seconds |
| Retry strategy | Exponential backoff with equal jitter |

A successful delivery cycle resets the retry backoff.

Retry attempts continue while the worker process remains alive.

## 4. Duplicate event handling

Each event has a unique event ID.

The ingestion backend uses this ID to identify duplicate events.

If an event was committed by the backend but the SDK did not receive the response, the SDK may transmit that event again.

The backend deduplicates the repeated event ID.

This provides duplicate-safe ingestion, but does not guarantee exactly-once delivery.

## 5. Shutdown behavior

During graceful shutdown:

1. The SDK signals the background flusher to stop.
2. The flusher waits for an in-progress delivery operation to finish.
3. The flusher makes one final delivery attempt.
4. The HTTP client is closed after the flusher exits.

The shutdown operation has a configurable timeout.

If the flusher does not stop within that timeout, it may continue running in the background until its current operation completes.

A shutdown timeout does not forcibly terminate the background thread.

## 6. Process termination and data loss

The current SDK uses in-memory buffering.

Pending events may be lost when:

- A worker process crashes.
- A container is forcefully terminated.
- The operating system kills the process.
- A machine loses power.
- The process exits before buffered events are delivered.

The SDK does not currently persist pending events to disk.

Graceful shutdown attempts to deliver pending events, but a final failed attempt can still leave events undelivered.

## 7. Failure isolation

QueueLens instrumentation failures must not interrupt customer task execution.

The SDK handles its own event capture and delivery failures independently.

Examples include:

- Event serialization failures.
- Buffer insertion failures.
- HTTP transport failures.
- Invalid ingestion responses.
- Background flusher initialization failures.
- Shutdown failures.

QueueLens does not suppress exceptions raised by the customer's actual Celery task.

## 8. Delivery guarantee

The current SDK provides:

- In-memory event buffering.
- Bounded memory usage.
- Background HTTP transmission.
- Retention and retries during process lifetime.
- Duplicate-safe ingestion.
- Best-effort graceful shutdown delivery.

The current SDK does not provide:

- Durable event storage.
- Guaranteed delivery after process crashes.
- Exactly-once delivery.
- Guaranteed delivery when the backend remains unavailable.
- Persistent recovery of buffered events after a restart.

## 9. Configuration example

The SDK can be configured through environment variables.

```bash
QUEUELENS_API_KEY=ql_test_your_api_key
QUEUELENS_ENDPOINT=https://api.example.com/internal/v1/events/batch

QUEUELENS_BATCH_SIZE=100
QUEUELENS_FLUSH_INTERVAL=5
QUEUELENS_TIMEOUT=5
QUEUELENS_BUFFER_CAPACITY=1000
QUEUELENS_MAX_BATCHES_PER_CYCLE=10
QUEUELENS_SHUTDOWN_TIMEOUT=10
```

Explicit constructor arguments take precedence over environment variables.

See the SDK configuration reference for all supported settings.

## 10. Future improvements

Potential future reliability improvements include:

- Optional disk-backed event persistence.
- Recovery of pending events after process restart.
- Configurable overflow strategies.
- Delivery health metrics.
- Persistent delivery acknowledgments.
- Improved visibility into dropped and undelivered events.

These features are not part of the current SDK implementation.
