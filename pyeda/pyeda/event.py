"""
pyeda.event — Event dataclass factory and registry.

Events are immutable, typed value objects. Users create event *types* via
Event.define() and instances by calling those types.
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field, fields, make_dataclass
from typing import Any, ClassVar, Dict, List, Optional, Type


# ---------------------------------------------------------------------------
# Registry — maps event name → event class
# ---------------------------------------------------------------------------

_REGISTRY: Dict[str, Type["BaseEvent"]] = {}


def get_event_class(name: str) -> Optional[Type["BaseEvent"]]:
    return _REGISTRY.get(name)


def registered_events() -> List[str]:
    return list(_REGISTRY.keys())


# ---------------------------------------------------------------------------
# BaseEvent — all events inherit from this
# ---------------------------------------------------------------------------

@dataclass
class BaseEvent:
    """
    All events carry a standard envelope regardless of payload fields.

    Attributes:
        event_id:   Unique identifier for this event instance.
        event_name: Registered name of the event type  (set by factory).
        emitted_at: Unix timestamp (float) when the event was created.
        attempt:    How many delivery attempts have been made (0 = first).
        metadata:   Arbitrary key-value pairs; attach trace IDs, etc.
    """

    # Envelope fields — always present
    event_id:   str   = field(default_factory=lambda: str(uuid.uuid4()))
    emitted_at: float = field(default_factory=time.time)
    attempt:    int   = field(default=0)
    metadata:   Dict[str, Any] = field(default_factory=dict)

    # Subclass fills this in via the class variable
    _event_name: ClassVar[str] = "__base__"

    @property
    def name(self) -> str:
        return self.__class__._event_name

    def with_attempt(self, n: int) -> "BaseEvent":
        """Return a shallow copy with an incremented attempt counter."""
        import copy
        clone = copy.copy(self)
        object.__setattr__(clone, "attempt", n)
        return clone

    def to_dict(self) -> Dict[str, Any]:
        return {f.name: getattr(self, f.name) for f in fields(self)}

    def __repr__(self) -> str:
        payload = {
            f.name: getattr(self, f.name)
            for f in fields(self)
            if f.name not in ("event_id", "emitted_at", "attempt", "metadata")
        }
        return (
            f"<Event '{self.name}' id={self.event_id[:8]} "
            f"attempt={self.attempt} payload={payload}>"
        )


# ---------------------------------------------------------------------------
# Event factory
# ---------------------------------------------------------------------------

class Event:
    """
    Namespace for event creation helpers.

    Usage::

        VideoDownloaded = Event.define("video.downloaded", ["video_id", "local_path"])
        ev = VideoDownloaded(video_id="abc", local_path="/tmp/vid.mp4")
    """

    @staticmethod
    def define(
        name: str,
        fields: Optional[List[str | tuple]] = None,
        *,
        doc: str = "",
    ) -> Type[BaseEvent]:
        """
        Dynamically create and register an event dataclass.

        Args:
            name:   Dot-separated event name e.g. ``"video.downloaded"``.
            fields: List of field names (strings) or (name, type, default)
                    tuples.  All string-only fields default to ``Any``.
            doc:    Optional docstring for the generated class.

        Returns:
            A new dataclass that is a subclass of :class:`BaseEvent`.

        Raises:
            ValueError: If ``name`` is already registered with a *different*
                        definition (re-registration of the same class is a
                        no-op).
        """
        if name in _REGISTRY:
            return _REGISTRY[name]

        raw_fields: List[Any] = []
        for f in (fields or []):
            if isinstance(f, str):
                raw_fields.append((f, Any, field(default=None)))
            elif isinstance(f, tuple) and len(f) == 2:
                raw_fields.append((f[0], f[1], field(default=None)))
            elif isinstance(f, tuple) and len(f) == 3:
                raw_fields.append(f)
            else:
                raise ValueError(f"Invalid field spec: {f!r}")

        cls = make_dataclass(
            cls_name=_name_to_classname(name),
            fields=raw_fields,
            bases=(BaseEvent,),
            frozen=False,
        )
        cls._event_name = name  # type: ignore[attr-defined]
        cls.__doc__ = doc or f"Auto-generated event class for '{name}'."

        _REGISTRY[name] = cls
        return cls

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> BaseEvent:
        """Re-hydrate a BaseEvent from its ``to_dict()`` representation."""
        name = data.get("event_name") or data.get("_event_name")
        if not name:
            raise ValueError("Cannot deserialise event: missing 'event_name' key.")
        cls = _REGISTRY.get(name)
        if cls is None:
            raise ValueError(f"Unknown event type '{name}'.")
        known = {f.name for f in fields(cls)}
        kwargs = {k: v for k, v in data.items() if k in known}
        return cls(**kwargs)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _name_to_classname(name: str) -> str:
    """``"video.downloaded"`` → ``"VideoDownloaded"``"""
    return "".join(part.capitalize() for part in name.replace("-", ".").split("."))
