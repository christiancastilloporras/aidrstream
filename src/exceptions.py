"""Shared exceptions for the poller."""

class TransientError(RuntimeError):
    """Raised when a transient error occurs that should be retried with backoff."""


class PermanentError(RuntimeError):
    """Raised when a permanent error occurs that should not be retried."""