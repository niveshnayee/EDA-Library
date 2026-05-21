"""
pyeda.metrics — In-process metrics for the event bus.

Lightweight counters and histograms stored in memory.
Exposed via EventBus.stats() and the UI dashboard.
"""

from __future__ import annotations

import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from threading import Lock
from typing import Any, Deque, Dict, List, Optional


@dataclass
class EventMetric:
    event_name:   str
    handler_name: str
    status:       str      # "success" | "failed" | "dead_lettered"
    duration_ms:  float
    timestamp:    float
    attempt:      int
    seq:          int = 0  # monotonic — used as reliable dedup key in UI
    error:        Optional[str] = None


@dataclass
class HandlerStats:
    name:          str
    event_name:    str
    total:         int   = 0
    success:       int   = 0
    failed:        int   = 0
    dead_lettered: int   = 0
    total_ms:      float = 0.0
    active:        int   = 0   # currently executing

    @property
    def avg_ms(self) -> float:
        return self.total_ms / self.success if self.success else 0.0

    @property
    def error_rate(self) -> float:
        return self.failed / self.total if self.total else 0.0


class MetricsCollector:
    """Thread- and asyncio-safe metrics store."""

    _MAX_HISTORY = 1000  # ring buffer size

    def __init__(self) -> None:
        self._lock = Lock()
        self._handler_stats: Dict[str, HandlerStats] = {}
        self._event_counts:  Dict[str, int]           = defaultdict(int)
        self._history:       Deque[EventMetric]       = deque(maxlen=self._MAX_HISTORY)
        self._start_time:    float                    = time.time()
        self._seq:           int                      = 0   # monotonic counter

    # ------------------------------------------------------------------
    # Recording
    # ------------------------------------------------------------------

    def record(
        self,
        event_name: str,
        handler_name: str,
        status: str,
        duration_ms: float,
        attempt: int,
        error: Optional[str] = None,
    ) -> None:
        key = f"{handler_name}@{event_name}"
        with self._lock:
            self._seq += 1
            metric = EventMetric(
                event_name=event_name,
                handler_name=handler_name,
                status=status,
                duration_ms=duration_ms,
                timestamp=time.time(),
                attempt=attempt,
                seq=self._seq,
                error=error,
            )
            if key not in self._handler_stats:
                self._handler_stats[key] = HandlerStats(
                    name=handler_name, event_name=event_name
                )
            s = self._handler_stats[key]
            s.total += 1
            self._event_counts[event_name] += 1

            if status == "success":
                s.success += 1
                s.total_ms += duration_ms
            elif status == "failed":
                s.failed += 1
            elif status == "dead_lettered":
                s.dead_lettered += 1

            self._history.append(metric)

    def mark_active(self, handler_name: str, event_name: str, delta: int) -> None:
        key = f"{handler_name}@{event_name}"
        with self._lock:
            if key not in self._handler_stats:
                self._handler_stats[key] = HandlerStats(
                    name=handler_name, event_name=event_name
                )
            self._handler_stats[key].active += delta

    # ------------------------------------------------------------------
    # Querying
    # ------------------------------------------------------------------

    def snapshot(self, since_seq: int = 0) -> Dict[str, Any]:
        """
        Return a full metrics snapshot.

        Args:
            since_seq: If > 0, the ``recent`` list only contains entries
                       with seq > since_seq (incremental diff for the UI).
        """
        with self._lock:
            if since_seq > 0:
                recent_items = [m for m in self._history if m.seq > since_seq]
            else:
                recent_items = list(reversed(self._history))[:100]

            return {
                "uptime_s":     round(time.time() - self._start_time, 1),
                "event_counts": dict(self._event_counts),
                "max_seq":      self._seq,
                "handlers": {
                    k: {
                        "name":          v.name,
                        "event_name":    v.event_name,
                        "total":         v.total,
                        "success":       v.success,
                        "failed":        v.failed,
                        "dead_lettered": v.dead_lettered,
                        "avg_ms":        round(v.avg_ms, 2),
                        "error_rate":    round(v.error_rate, 4),
                        "active":        v.active,
                    }
                    for k, v in self._handler_stats.items()
                },
                "recent": [
                    {
                        "seq":          m.seq,
                        "event_name":   m.event_name,
                        "handler_name": m.handler_name,
                        "status":       m.status,
                        "duration_ms":  round(m.duration_ms, 2),
                        "timestamp":    m.timestamp,
                        "attempt":      m.attempt,
                        "error":        m.error,
                    }
                    for m in recent_items
                ],
            }