# pyeda

**Pure-Python in-process Event-Driven Architecture library.**

Zero required dependencies. Drop it in, decorate your handlers, emit events.

```
pip install pyeda[all]          # with YAML config + dashboard UI
pip install pyeda               # core only (no extra deps)
``` 

---

## Quickstart

```python
import asyncio
from pyeda import EventBus, Event, on

bus = EventBus()   # or EventBus.from_config("pyeda.yml")

# 1. Define typed events
VideoDownloaded = Event.define("video.downloaded", ["video_id", "local_path"])
AudioExtracted  = Event.define("audio.extracted",  ["video_id", "audio_path"])
Transcribed     = Event.define("video.transcribed", ["video_id", "transcript"])

# 2. Wire handlers — each handler emits the next event
@on("video.downloaded", bus=bus)
async def extract_audio(event):
    path = await run_ffmpeg(event.local_path)
    await bus.emit(AudioExtracted(video_id=event.video_id, audio_path=path))

@on("audio.extracted", bus=bus)
async def transcribe(event):
    text = await run_whisper(event.audio_path)
    await bus.emit(Transcribed(video_id=event.video_id, transcript=text))

@on("video.transcribed", bus=bus)
async def save_to_mongo(event):
    await db.insert_one({"video_id": event.video_id, "transcript": event.transcript})

# 3. Start
asyncio.run(bus.run())
```

---

## Architecture

```
emit(event)
    │
    ▼
asyncio.Queue  ──►  Worker (asyncio task, one per handler)
                         │
                         ▼
               Middleware Pipeline
               ┌─────────────────────┐
               │  logging_middleware │  ← structured log lines
               │  retry_middleware   │  ← exponential back-off
               │  timeout_middleware │  ← deadline for each attempt
               │  ─────────────────  │
               │  handler_fn(event)  │  ← your async function
               └─────────────────────┘
                         │
              ┌──────────┴──────────┐
           success               failure (exhausted retries)
              │                       │
         metrics.record          dead.{event_name} topic
                                 + metrics.record(dead_lettered)
```

---

## Event Definition

```python
# Simple — all fields are Any with default=None
VideoDownloaded = Event.define("video.downloaded", ["video_id", "local_path"])

# Typed fields
AudioExtracted = Event.define("audio.extracted", [
    ("video_id",   str),
    ("audio_path", str),
])

# With defaults — use (name, type, default_value) tuples
JobEvent = Event.define("job.created", [
    ("job_id",   str,  None),
    ("priority", int,  1),
    ("retried",  bool, False),
])

# Instantiate
ev = VideoDownloaded(video_id="abc", local_path="/tmp/vid.mp4")
print(ev.name)        # "video.downloaded"
print(ev.event_id)    # UUID
print(ev.emitted_at)  # float timestamp
```

---

## Handler Registration

### With bus= (explicit, recommended)

```python
@on("video.downloaded", bus=bus)
async def extract_audio(event):
    ...
```

### Without bus= (deferred, for modular apps)

```python
# handlers/audio.py
from pyeda import on

@on("video.downloaded")
async def extract_audio(event):
    ...
```

```python
# main.py
import handlers.audio   # just importing registers the handler
bus.include_pending()   # flush pending @on() into this bus
```

### Multiple handlers per event

```python
@on("video.downloaded", bus=bus)
async def extract_audio(event): ...

@on("video.downloaded", bus=bus)
async def log_download_to_analytics(event): ...
```

### Dead-letter subscription

```python
@on("dead.video.downloaded", bus=bus)
async def on_download_failed(event):
    failure = event.metadata["dead_letter"]
    # {"handler": "extract_audio", "error": "...", "error_type": "RuntimeError", "attempts": 3}
    await alert_team(event, failure["error"])
```

A dead-letter handler receives the original event. If it fails itself, the
failure is logged and counted but not dead-lettered again.

---

## Configuration

### pyeda.yml

```yaml
bus:
  backend: memory        # "memory" | "sqlite"
  queue_size: 1000
  journal_path: .pyeda/journal.db
  shutdown_timeout_s: 30.0   # how long stop() waits for running handlers; null = forever

ui:
  enabled: true
  host: 127.0.0.1
  port: 7373

defaults:
  concurrency: 3
  retry_attempts: 3
  retry_delay_s: 1.0
  retry_backoff: exponential   # "fixed" | "exponential"
  timeout_s: null              # deadline for each attempt

middleware:
  - logging
  - retry

handlers:
  transcribe:
    concurrency: 1
    timeout_s: 300.0
  save_metadata:
    concurrency: 5
    retry_attempts: 5
```

### Programmatic

```python
bus = EventBus.from_config({
    "bus": {"backend": "sqlite"},
    "ui":  {"port": 8080},
    "handlers": {
        "transcribe": {"concurrency": 1}
    },
})
```

---

## FastAPI Integration

```python
from contextlib import asynccontextmanager
from fastapi import FastAPI
from pyeda import EventBus, Event, on
from pyeda.ui import start_ui

bus = EventBus.from_config("pyeda.yml")
JobCreated = Event.define("job.created", ["job_id", "payload"])

@on("job.created", bus=bus)
async def process_job(event):
    await do_work(event.payload)

@asynccontextmanager
async def lifespan(app: FastAPI):
    await bus.start()
    await start_ui(bus)
    yield
    await bus.stop()

app = FastAPI(lifespan=lifespan)

@app.post("/jobs")
async def create_job(payload: dict):
    job_id = str(uuid4())
    await bus.emit(JobCreated(job_id=job_id, payload=payload))
    return {"job_id": job_id}
```

---

## Metrics / Stats

```python
snap = bus.stats()
# {
#   "uptime_s": 42.0,
#   "event_counts": {"video.downloaded": 10, "audio.extracted": 10},
#   "handlers": {
#     "extract_audio@video.downloaded": {
#       "total": 10, "success": 9, "failed": 1,
#       "avg_ms": 342.1, "active": 1, ...
#     }
#   },
#   "recent": [...]
# }
```

---

## Live Dashboard

Start the UI with:

```python
from pyeda.ui import start_ui
await start_ui(bus)
```

Then open **http://127.0.0.1:7373** in your browser.

Features:
- KPI cards: total / success / failed / dead-lettered / active / handler count
- Per-handler cards with success bar, avg latency, active badge
- Event throughput bars
- Live event log with status, handler, duration, timestamp (WebSocket push)

---

## SQLite Durability

```yaml
bus:
  backend: sqlite
  journal_path: .pyeda/journal.db
```

- Events are written to the journal **before** being queued.
- On startup, any `pending` events (not yet delivered when the process died) are **replayed automatically**.
- Delivered events are marked `delivered`; failed-final events are marked `dead`.
- Replayed handlers run again from the start, so they should be safe to repeat.

## Shutdown

`await bus.stop()` waits for queued and running handlers, and for any events
they emit, for up to `bus.shutdown_timeout_s` seconds (or `stop(timeout=...)`).
Handlers still running after that are cancelled; with the SQLite backend their
events stay `pending` and are replayed on the next start.

---

## Project Structure

```
pyeda/
├── __init__.py      # public API
├── bus.py           # EventBus core
├── event.py         # Event factory + registry
├── handler.py       # @on decorator + HandlerRegistry
├── middleware.py    # logging / retry / timeout pipeline
├── config.py        # YAML + dict config loader
├── journal.py       # SQLite durability backend
├── metrics.py       # in-memory stats collector
├── ui.py            # aiohttp dashboard + WebSocket push
└── exceptions.py    # typed exception hierarchy
```

---

## License

MIT
