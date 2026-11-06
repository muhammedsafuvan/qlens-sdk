class QueueLensError(Exception):
    """Base exception for the QueueLens SDK."""


class QueueLensConfigurationError(QueueLensError):
    """Raised when SDK configuration is invalid."""