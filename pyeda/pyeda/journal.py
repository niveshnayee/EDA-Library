"""
pyeda.journal — Optional SQLite-backed event journal.

Provides durability: events survive process restart if they haven't been
acknowledged.  Uses WAL mode for concurrent reads during writes.
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
import time
from pathlib import Path
from typing import AsyncIterator, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from pyeda.event import BaseEvent

# Status constants
STATUS_PENDING   = "pending"
STATUS_DELIVERED = "delivered"
STATUS_DEAD      = "dead"


class Journal:
    """
    Async-safe SQLite journal.

    All SQLite calls are dispatched to a dedicated thread via
    ``asyncio.to_thread`` so the event loop is never blocked.
    """

    def __init__(self, path: str = ".pyeda/journal.db") -> None:
        self._path = Path(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._conn: Optional[sqlite3.Connection] = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def open(self) -> None:
        await asyncio.to_thread(self._open_sync)

    def _open_sync(self) -> None:
        self._conn = sqlite3.connect(str(self._path), check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS events (
                event_id    TEXT PRIMARY KEY,
                event_name  TEXT NOT NULL,
                payload     TEXT NOT NULL,
                status      TEXT NOT NULL DEFAULT 'pending',
                created_at  REAL NOT NULL,
                updated_at  REAL NOT NULL,
                attempts    INTEGER NOT NULL DEFAULT 0,
                error       TEXT
            )
        """)
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_status ON events (status)"
        )
        self._conn.commit()

    async def close(self) -> None:
        if self._conn:
            await asyncio.to_thread(self._conn.close)

    # ------------------------------------------------------------------
    # Write operations
    # ------------------------------------------------------------------

    async def append(self, event: "BaseEvent") -> None:
        """Persist a new event with status=pending."""
        now = time.time()
        payload = json.dumps(event.to_dict())
        await asyncio.to_thread(
            self._exec,
            """
            INSERT OR IGNORE INTO events
              (event_id, event_name, payload, status, created_at, updated_at, attempts)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (event.event_id, event.name, payload, STATUS_PENDING, now, now, 0),
        )

    async def mark_delivered(self, event_id: str) -> None:
        await asyncio.to_thread(
            self._exec,
            "UPDATE events SET status=?, updated_at=? WHERE event_id=?",
            (STATUS_DELIVERED, time.time(), event_id),
        )

    async def mark_dead(self, event_id: str, error: str) -> None:
        await asyncio.to_thread(
            self._exec,
            "UPDATE events SET status=?, updated_at=?, error=? WHERE event_id=?",
            (STATUS_DEAD, time.time(), error, event_id),
        )

    async def increment_attempts(self, event_id: str) -> None:
        await asyncio.to_thread(
            self._exec,
            "UPDATE events SET attempts=attempts+1, updated_at=? WHERE event_id=?",
            (time.time(), event_id),
        )

    # ------------------------------------------------------------------
    # Read operations
    # ------------------------------------------------------------------

    async def pending_events(self) -> List[dict]:
        """Return all events that were pending when the process last exited."""
        rows = await asyncio.to_thread(self._fetchall,
            "SELECT payload FROM events WHERE status=? ORDER BY created_at",
            (STATUS_PENDING,),
        )
        return [json.loads(r[0]) for r in rows]

    async def recent(self, limit: int = 100) -> List[dict]:
        rows = await asyncio.to_thread(self._fetchall,
            """
            SELECT event_id, event_name, status, created_at, updated_at, attempts, error
            FROM events ORDER BY created_at DESC LIMIT ?
            """,
            (limit,),
        )
        cols = ["event_id", "event_name", "status", "created_at", "updated_at",
                "attempts", "error"]
        return [dict(zip(cols, r)) for r in rows]

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _exec(self, sql: str, params: tuple = ()) -> None:
        assert self._conn
        self._conn.execute(sql, params)
        self._conn.commit()

    def _fetchall(self, sql: str, params: tuple = ()) -> list:
        assert self._conn
        cur = self._conn.execute(sql, params)
        return cur.fetchall()
