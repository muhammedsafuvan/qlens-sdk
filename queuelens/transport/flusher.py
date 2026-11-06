import logging
import random
from threading import Event, Thread

from queuelens.transport.sender import EventSender
logger = logging.getLogger(__name__)


class EventFlusher:
    def __init__(
        self,
        sender: EventSender,
        interval: float = 5.0,
        max_batches_per_cycle: int = 10,
        shutdown_timeout: float = 10.0,
        base_backoff: float = 1.0,
        max_backoff: float = 30.0,
    ) -> None:
        if interval <= 0:
            raise ValueError("interval must be greater than 0")

        if max_batches_per_cycle < 1:
            raise ValueError("max_batches_per_cycle must be at least 1")

        if shutdown_timeout <= 0:
            raise ValueError("shutdown_timeout must be greater than 0")

        if base_backoff <= 0:
            raise ValueError("base_backoff must be greater than 0")

        if max_backoff < base_backoff:
            raise ValueError("max_backoff must be greater than or equal to base_backoff")
        

        self._sender = sender
        self._interval = interval
        self._max_batches_per_cycle = max_batches_per_cycle
        self._shutdown_timeout = shutdown_timeout
        self._base_backoff = base_backoff
        self._max_backoff = max_backoff

        self._stop_event = Event()
        self._thread: Thread | None = None

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return

        self._stop_event.clear()

        self._thread = Thread(
            target=self._run,
            name="queuelens-event-flusher",
            daemon=True,
        )
        self._thread.start()

        logger.info("QueueLens event flusher started")

    def stop(self) -> None:
        self._stop_event.set()

        if self._thread is not None and self._thread.is_alive():
            self._thread.join(timeout=self._shutdown_timeout)

            if self._thread.is_alive():
                logger.warning(
                    "QueueLens event flusher did not stop within %.1f seconds. "
                    "It will finish in the background.",
                    self._shutdown_timeout,
                )
                return

        logger.info("QueueLens event flusher stopped")

    
    def _run(self) -> None:
        backoff = self._base_backoff

        try:
            while not self._stop_event.is_set():
                failed = False

                try:
                    batches_sent = 0

                    while (
                        not self._stop_event.is_set()
                        and batches_sent < self._max_batches_per_cycle
                    ):
                        acknowledged = self._sender.flush_once()

                        if acknowledged is None:
                            failed = True
                            break

                        if acknowledged == 0:
                            break

                        batches_sent += 1

                except Exception:
                    logger.exception(
                        "Unexpected error in QueueLens event flusher"
                    )
                    failed = True

                if failed:
                    # Equal jitter: wait between 50% and 100%
                    # of the current backoff duration.
                    delay = self._calculate_retry_delay(backoff)

                    logger.warning(
                        "QueueLens delivery failed. "
                        "Retrying in %.2f seconds.",
                        delay,
                    )

                    self._stop_event.wait(delay)

                    backoff = self._increase_backoff(backoff)
                else:
                    # Reset backoff after a successful cycle or
                    # when there are no pending events.
                    backoff = self._base_backoff
                    self._stop_event.wait(self._interval)

        finally:
            self._flush_on_shutdown()
            self._sender.close()


    def _flush_on_shutdown(self) -> None:
        """Make one final delivery attempt before closing the sender."""
        try:
            acknowledged = self._sender.flush_once()

            logger.info(
                "QueueLens shutdown flush completed: acknowledged=%s",
                acknowledged,
            )

        except Exception:
            logger.exception(
                "QueueLens failed to flush pending events during shutdown"
            )


    def _calculate_retry_delay(self, backoff: float) -> float:
        """Apply equal jitter to the current backoff duration."""
        return random.uniform(backoff / 2, backoff)


    def _increase_backoff(self, backoff: float) -> float:
        """Double the backoff, respecting the configured maximum."""
        return min(backoff * 2, self._max_backoff)
