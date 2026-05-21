"""
pyeda.bus — Core EventBus.

Responsibilities:
  - Accept event emissions from user code.
  - Route events to registered handlers via asyncio.Queue.
  - Apply middleware pipeline per dispatch.
  - Manage worker concurrency via per-handler semaphores.
  - Publish dead-lettered events to "dead.{event_name}" topics.
  - Optionally journal events to SQLite.
  - Expose metrics for the UI.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from pyeda.config import PyEDAConfig, load_config
from pyeda.exceptions import BusNotRunningError
from pyeda.handler import HandlerRegistry, HandlerRegistration, collect_pending
from pyeda.metrics import MetricsCollector
from pyeda.middleware import (
    logging_middleware,
    make_retry_middleware,
    make_timeout_middleware,
    build_pipeline_with_context,
)

if TYPE_CHECKING:
    from pyeda.event import BaseEvent

logger = logging.getLogger("pyeda")

_SENTINEL = object()   # signals worker shutdown


class EventBus:
    """
    Central event bus.

    Typical usage::

        bus = EventBus.from_config("pyeda.yml")

        @on("video.downloaded", bus=bus)
        async def handle(event):
            ...

        asyncio.run(bus.run())
    """

    def __init__(self, config: Optional[PyEDAConfig] = None) -> None:
        self.config   = config or load_config()
        self.registry = HandlerRegistry()
        self.metrics  = MetricsCollector()

        # event_name → list of per-handler queues (fan-out)
        self._queues:  Dict[str, List[asyncio.Queue]] = {} # type: ignore
        # handler_key → queue (for shutdown)
        self._handler_queues: Dict[str, asyncio.Queue] = {} # type: ignore
        self._workers: List[asyncio.Task]        = []
        self._running: bool                      = False
        self._journal = None   # set in _setup_journal

    # ------------------------------------------------------------------
    # Constructors
    # ------------------------------------------------------------------

    @classmethod
    def from_config(cls, source: Any = None) -> "EventBus":
        """Create a bus from a YAML path, dict, or auto-discovered config."""
        return cls(config=load_config(source))

    # ------------------------------------------------------------------
    # Registration helpers
    # ------------------------------------------------------------------

    def include_pending(self) -> None:
        """
        Import any @on() decorators used without an explicit bus= argument.
        Call this after importing all your handler modules.
        """
        for event_name, handler_name, fn in collect_pending():
            self.registry.register(
                HandlerRegistration(
                    name=handler_name,
                    event_name=event_name,
                    fn=fn,
                )
            )

    # ------------------------------------------------------------------
    # Emit
    # ------------------------------------------------------------------

    async def emit(self, event: BaseEvent) -> None:
        """
        Publish an event onto the bus.

        The call returns immediately after enqueuing; handlers run
        concurrently in the background.

        Raises:
            BusNotRunningError: If the bus has not been started yet.
        """
        if not self._running:
            raise BusNotRunningError(
                "Call await bus.run() (or start it as a background task) "
                "before emitting events."
            )

        queues = self._queues.get(event.name)
        if not queues:
            # No handlers registered — still log; useful for debugging
            logger.debug("emit: no handlers for event '%s'", event.name)
            return

        if self._journal:
            await self._journal.append(event)

        for q in queues:
            await q.put(event)
        logger.debug("emit: queued event '%s' id=%s to %d handler(s)",
                     event.name, event.event_id[:8], len(queues))

    async def emit_nowait(self, event: BaseEvent) -> None:
        """Non-blocking emit; raises asyncio.QueueFull if any queue is full."""
        if not self._running:
            raise BusNotRunningError("Bus not running.")
        for q in self._queues.get(event.name, []):
            q.put_nowait(event)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """
        Start the bus without blocking.  Use when embedding inside
        another async application (e.g. FastAPI lifespan).
        """
        await self._boot()

    async def run(self) -> None:
        """
        Start the bus and block until cancelled.

        Use as the top-level entry point when running standalone::

            asyncio.run(bus.run())
        """
        await self._boot()
        try:
            # Block until cancelled
            await asyncio.gather(*self._workers, return_exceptions=True)
        except asyncio.CancelledError:
            pass
        finally:
            await self.stop()

    async def stop(self) -> None:
        """Gracefully drain queues and stop all workers."""
        logger.info("pyeda: shutting down bus …")
        # Send one sentinel per per-handler queue
        for q in self._handler_queues.values():
            await q.put(_SENTINEL)

        if self._workers:
            await asyncio.gather(*self._workers, return_exceptions=True)

        if self._journal:
            await self._journal.close()

        self._running = False
        logger.info("pyeda: bus stopped.")

    # ------------------------------------------------------------------
    # Internal boot
    # ------------------------------------------------------------------

    async def _boot(self) -> None:
        if self._running:
            return

        # Apply handler config (concurrency limits)
        self._apply_config()

        # Collect any pending @on() decorators
        self.include_pending()

        # Set up journal if SQLite backend requested
        await self._setup_journal()

        # Create one queue PER HANDLER REGISTRATION (not per event name).
        # This ensures every handler gets every event independently.
        qs = self.config.bus.queue_size
        for reg in self.registry.all_registrations():
            q: asyncio.Queue = asyncio.Queue(maxsize=qs)
            self._handler_queues[reg.name + "@" + reg.event_name] = q
            # Map event_name → list of queues (fan-out on emit)
            self._queues.setdefault(reg.event_name, []).append(q)
            task = asyncio.create_task(
                self._worker(reg, q),
                name=f"pyeda.worker.{reg.name}",
            )
            self._workers.append(task)

        self._running = True

        # Replay unacknowledged events from the journal
        if self._journal:
            await self._replay_pending()

        n = len(self._workers)
        logger.info("pyeda: bus started — %d worker(s) across %d event type(s).",
                    n, len(self._queues))

    def _apply_config(self) -> None:
        """Push per-handler concurrency limits from config into registrations."""
        for reg in self.registry.all_registrations():
            hcfg = self.config.for_handler(reg.name)
            reg.set_concurrency(hcfg.concurrency)

    async def _setup_journal(self) -> None:
        if self.config.bus.backend == "sqlite":
            from pyeda.journal import Journal
            self._journal = Journal(self.config.bus.journal_path)
            await self._journal.open()
            logger.info("pyeda: SQLite journal opened at '%s'.",
                        self.config.bus.journal_path)

    async def _replay_pending(self) -> None:
        from pyeda.event import Event
        pending = await self._journal.pending_events()
        if not pending:
            return
        logger.info("pyeda: replaying %d pending event(s) from journal.", len(pending))
        for raw in pending:
            try:
                event = Event.from_dict(raw)
                await self.emit(event)
            except Exception as exc:
                logger.warning("pyeda: could not replay event %s: %s", raw, exc)

    # ------------------------------------------------------------------
    # Worker loop
    # ------------------------------------------------------------------

    async def _worker(self, reg: HandlerRegistration, q: asyncio.Queue) -> None:
        """
        Long-running coroutine that pulls events off the queue and dispatches
        them through the middleware pipeline.
        """
        hcfg = self.config.for_handler(reg.name)
        middlewares = self._build_middlewares(hcfg)

        while True:
            item = await q.get()
            if item is _SENTINEL:
                q.task_done()
                break

            event: BaseEvent = item
            asyncio.create_task(
                self._dispatch(event, reg, middlewares),
                name=f"pyeda.dispatch.{reg.name}.{event.event_id[:8]}",
            )
            q.task_done()

    def _build_middlewares(self, hcfg) -> list:
        """Assemble the middleware list for a given handler config."""
        mw = []
        names = self.config.middleware

        if "logging" in names:
            mw.append(logging_middleware)

        timeout_mw = make_timeout_middleware(hcfg.timeout_s)
        if timeout_mw:
            mw.append(timeout_mw)

        if "retry" in names:
            mw.append(make_retry_middleware(hcfg))

        return mw

    async def _dispatch(
        self,
        event: BaseEvent,
        reg: HandlerRegistration,
        middlewares: list,
    ) -> None:
        """Run one event through a handler's middleware pipeline."""
        self.metrics.mark_active(reg.name, event.name, +1)
        start = time.perf_counter()

        async def endpoint() -> None:
            await reg.fn(event)

        pipeline = build_pipeline_with_context(middlewares, endpoint, event, reg.name)

        try:
            async with reg.semaphore:
                await pipeline()

            duration_ms = (time.perf_counter() - start) * 1000
            self.metrics.record(
                event.name, reg.name, "success", duration_ms, event.attempt
            )
            if self._journal:
                await self._journal.mark_delivered(event.event_id)

        except Exception as exc:
            duration_ms = (time.perf_counter() - start) * 1000
            error_str = str(exc)

            # Dead-letter
            dead_event_name = f"dead.{event.name}"
            self.metrics.record(
                event.name, reg.name, "dead_lettered", duration_ms,
                event.attempt, error_str
            )
            if self._journal:
                await self._journal.mark_dead(event.event_id, error_str)

            logger.error(
                "dead-letter event='%s' id=%s handler='%s' error=%s",
                event.name, event.event_id[:8], reg.name, exc,
            )

            # Emit to dead.* topic if anyone subscribed
            dead_queues = self._queues.get(dead_event_name, [])
            for dq in dead_queues:
                await dq.put(event)

        finally:
            self.metrics.mark_active(reg.name, event.name, -1)

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    def stats(self, since_seq: int = 0) -> Dict[str, Any]:
        """Return a snapshot of current metrics (safe to call any time)."""
        return self.metrics.snapshot(since_seq=since_seq)

    def is_running(self) -> bool:
        return self._running