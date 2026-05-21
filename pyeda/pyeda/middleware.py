"""
pyeda.middleware — Composable middleware pipeline.

Each middleware is an async callable with signature::

    async def mw(event, handler_name, call_next) -> None

Middleware is applied in list order; the innermost call is the actual handler.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Awaitable, Callable, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from pyeda.event import BaseEvent
    from pyeda.config import HandlerConfig

logger = logging.getLogger("pyeda")

# Type alias
Next = Callable[[], Awaitable[None]]
Middleware = Callable[["BaseEvent", str, Next], Awaitable[None]]


# ---------------------------------------------------------------------------
# Built-in middleware
# ---------------------------------------------------------------------------

async def logging_middleware(
    event: "BaseEvent",
    handler_name: str,
    call_next: Next,
) -> None:
    """Log event dispatch and completion with timing."""
    start = time.perf_counter()
    logger.debug(
        "dispatch event='%s' id=%s handler='%s' attempt=%d",
        event.name, event.event_id[:8], handler_name, event.attempt,
    )
    try:
        await call_next()
        elapsed = (time.perf_counter() - start) * 1000
        logger.debug(
            "done     event='%s' id=%s handler='%s' elapsed=%.1fms",
            event.name, event.event_id[:8], handler_name, elapsed,
        )
    except Exception as exc:
        elapsed = (time.perf_counter() - start) * 1000
        logger.warning(
            "failed   event='%s' id=%s handler='%s' elapsed=%.1fms error=%s",
            event.name, event.event_id[:8], handler_name, elapsed, exc,
        )
        raise


def make_retry_middleware(handler_cfg: "HandlerConfig") -> Middleware:
    """
    Build a retry middleware closure bound to the given handler config.
    Supports fixed and exponential back-off.
    """
    async def retry_middleware(
        event: "BaseEvent",
        handler_name: str,
        call_next: Next,
    ) -> None:
        max_attempts = handler_cfg.retry_attempts
        delay = handler_cfg.retry_delay_s
        backoff = handler_cfg.retry_backoff

        for attempt in range(max_attempts):
            try:
                await call_next()
                return
            except Exception as exc:
                if attempt >= max_attempts - 1:
                    raise
                wait = delay * (2 ** attempt) if backoff == "exponential" else delay
                logger.warning(
                    "retry    event='%s' handler='%s' attempt=%d/%d wait=%.1fs error=%s",
                    event.name, handler_name, attempt + 1, max_attempts, wait, exc,
                )
                await asyncio.sleep(wait)

    return retry_middleware


def make_timeout_middleware(timeout_s: Optional[float]) -> Optional[Middleware]:
    """Build a timeout middleware, or return None if no timeout configured."""
    if timeout_s is None:
        return None

    async def timeout_middleware(
        event: "BaseEvent",
        handler_name: str,
        call_next: Next,
    ) -> None:
        try:
            await asyncio.wait_for(call_next(), timeout=timeout_s)
        except asyncio.TimeoutError:
            raise asyncio.TimeoutError(
                f"Handler '{handler_name}' timed out after {timeout_s}s "
                f"for event '{event.name}'"
            )

    return timeout_middleware


# ---------------------------------------------------------------------------
# Pipeline builder
# ---------------------------------------------------------------------------

def build_pipeline(
    middlewares: List[Middleware],
    endpoint: Callable[[], Awaitable[None]],
) -> Callable[[], Awaitable[None]]:
    """
    Wrap `endpoint` with `middlewares` in order (first = outermost).

    Each middleware receives a ``call_next`` that invokes the next layer.
    """
    # Build from inside out
    call: Callable[[], Awaitable[None]] = endpoint
    for mw in reversed(middlewares):
        # Capture current call in closure
        _call = call
        _mw = mw

        # We need event + handler_name in scope; they are injected at call time
        # so we defer binding via a partial-like async closure factory.
        call = _make_layer(_mw, _call)
    return call


def _make_layer(
    mw: Middleware,
    inner: Callable[[], Awaitable[None]],
) -> Callable[[], Awaitable[None]]:
    """Return a zero-arg coroutine factory that calls mw with inner as next."""
    # This is intentionally a factory — callers replace event/handler context
    # by calling build_pipeline_with_context instead.
    raise NotImplementedError("Use build_pipeline_with_context.")


def build_pipeline_with_context(
    middlewares: List[Middleware],
    endpoint: Callable[[], Awaitable[None]],
    event: "BaseEvent",
    handler_name: str,
) -> Callable[[], Awaitable[None]]:
    """
    Bind event and handler_name into the middleware chain and return a
    zero-arg awaitable that executes the full pipeline.
    """
    call: Callable[[], Awaitable[None]] = endpoint
    for mw in reversed(middlewares):
        _call = call
        _mw = mw
        _event = event
        _handler_name = handler_name

        async def layer(
            _m=_mw, _e=_event, _h=_handler_name, _c=_call
        ) -> None:
            await _m(_e, _h, _c)

        call = layer

    return call
