"""
pyeda.exceptions — typed error hierarchy
"""


class PyEDAError(Exception):
    """Base exception for all pyeda errors."""


class EventNotRegisteredError(PyEDAError):
    """Raised when emitting an event type that was never defined."""


class HandlerError(PyEDAError):
    """Raised when a handler fails after exhausting retries."""

    def __init__(self, handler_name: str, event_name: str, cause: Exception) -> None:
        self.handler_name = handler_name
        self.event_name = event_name
        self.cause = cause
        super().__init__(
            f"Handler '{handler_name}' failed for event '{event_name}': {cause}"
        )


class ConfigError(PyEDAError):
    """Raised when configuration is invalid or missing."""


class BusNotRunningError(PyEDAError):
    """Raised when emitting to a bus that has not been started."""


class DeadLetterError(PyEDAError):
    """Raised when an event is sent to the dead-letter queue."""

    def __init__(self, event_name: str, reason: str) -> None:
        self.event_name = event_name
        self.reason = reason
        super().__init__(f"Event '{event_name}' dead-lettered: {reason}")
