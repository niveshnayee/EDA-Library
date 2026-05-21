"""
pyeda — Pure-Python in-process Event-Driven Architecture library.

Public API::

    from pyeda import EventBus, Event, on

Everything else (bus internals, middleware, journal, UI) is available
under ``pyeda.*`` sub-modules for advanced use.

Quickstart::

    import asyncio
    from pyeda import EventBus, Event, on

    bus = EventBus.from_config("pyeda.yml")   # or EventBus() for defaults

    # 1. Define typed events
    VideoDownloaded = Event.define("video.downloaded", ["video_id", "local_path"])
    AudioExtracted  = Event.define("audio.extracted",  ["video_id", "audio_path"])
    Transcribed     = Event.define("video.transcribed", ["video_id", "transcript"])

    # 2. Wire handlers as a pipeline
    @on("video.downloaded", bus=bus)
    async def extract_audio(event):
        audio_path = await run_ffmpeg(event.local_path)
        await bus.emit(AudioExtracted(video_id=event.video_id, audio_path=audio_path))

    @on("audio.extracted", bus=bus)
    async def transcribe(event):
        text = await run_whisper(event.audio_path)
        await bus.emit(Transcribed(video_id=event.video_id, transcript=text))

    @on("video.transcribed", bus=bus)
    async def save_to_mongo(event):
        await db.transcripts.insert_one({...})

    # 3. Start everything
    asyncio.run(bus.run())
"""

from pyeda.bus     import EventBus
from pyeda.event   import Event, BaseEvent, registered_events
from pyeda.handler import on
from pyeda.config  import load_config, PyEDAConfig
from pyeda.exceptions import (
    PyEDAError,
    BusNotRunningError,
    HandlerError,
    ConfigError,
    DeadLetterError,
    EventNotRegisteredError,
)

__all__ = [
    # Core
    "EventBus",
    "Event",
    "BaseEvent",
    "on",
    # Config
    "load_config",
    "PyEDAConfig",
    # Errors
    "PyEDAError",
    "BusNotRunningError",
    "HandlerError",
    "ConfigError",
    "DeadLetterError",
    "EventNotRegisteredError",
    # Introspection
    "registered_events",
]

__version__ = "0.1.0"
__author__  = "pyeda contributors"
