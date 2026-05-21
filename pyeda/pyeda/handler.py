"""
pyeda.handler — Handler registry and @on decorator.

Handlers are async functions decorated with @on("event.name").
Multiple handlers can subscribe to the same event.
"""

from __future__ import annotations

import asyncio
import inspect
from dataclasses import dataclass, field
from typing import Any, Callable, Coroutine, Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from pyeda.bus import EventBus
    from pyeda.event import BaseEvent

HandlerFn = Callable[["BaseEvent"], Coroutine[Any, Any, None]]


# ---------------------------------------------------------------------------
# Handler descriptor
# ---------------------------------------------------------------------------

@dataclass
class HandlerRegistration:
    name:       str          # user-visible handler name (function name by default)
    event_name: str          # the event this handler subscribes to
    fn:         HandlerFn    # the coroutine function
    semaphore:  asyncio.Semaphore = field(init=False)
    concurrency: int = 3

    def __post_init__(self) -> None:
        self.semaphore = asyncio.Semaphore(self.concurrency)

    def set_concurrency(self, n: int) -> None:
        """Replace semaphore — call before the bus starts."""
        self.concurrency = n
        self.semaphore = asyncio.Semaphore(n)


# ---------------------------------------------------------------------------
# Registry (singleton per bus instance)
# ---------------------------------------------------------------------------

class HandlerRegistry:
    """Stores and looks up handler registrations."""

    def __init__(self) -> None:
        # event_name → list of registrations
        self._map: Dict[str, List[HandlerRegistration]] = {}

    def register(self, reg: HandlerRegistration) -> None:
        self._map.setdefault(reg.event_name, []).append(reg)

    def get(self, event_name: str) -> List[HandlerRegistration]:
        return self._map.get(event_name, [])

    def all_registrations(self) -> List[HandlerRegistration]:
        out: List[HandlerRegistration] = []
        for regs in self._map.values():
            out.extend(regs)
        return out

    def all_event_names(self) -> List[str]:
        return list(self._map.keys())

    def __len__(self) -> int:
        return sum(len(v) for v in self._map.values())


# ---------------------------------------------------------------------------
# @on decorator
# ---------------------------------------------------------------------------

# Global pending registrations — collected before a bus exists
_pending: List[tuple[str, str, HandlerFn]] = []


def on(
    event_name: str,
    *,
    bus: Optional["EventBus"] = None,
    name: Optional[str] = None,
) -> Callable[[HandlerFn], HandlerFn]:
    """
    Decorator that subscribes an async function to an event.

    Args:
        event_name: The event to subscribe to (e.g. ``"video.downloaded"``).
        bus:        Optional :class:`EventBus` to register with immediately.
                    If omitted the registration is queued and applied when
                    :meth:`EventBus.include_pending` is called.
        name:       Override the handler name (defaults to the function name).

    Example::

        @on("video.downloaded", bus=bus)
        async def extract_audio(event: VideoDownloaded) -> None:
            ...
    """
    def decorator(fn: HandlerFn) -> HandlerFn:
        if not inspect.iscoroutinefunction(fn):
            raise TypeError(
                f"@on handler '{fn.__name__}' must be an async function."
            )

        handler_name = name or fn.__name__

        if bus is not None:
            bus.registry.register(
                HandlerRegistration(
                    name=handler_name,
                    event_name=event_name,
                    fn=fn,
                )
            )
        else:
            _pending.append((event_name, handler_name, fn))

        return fn

    return decorator


def collect_pending() -> List[tuple[str, str, HandlerFn]]:
    """Return and clear the global pending registration list."""
    items = list(_pending)
    _pending.clear()
    return items
