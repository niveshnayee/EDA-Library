"""
examples/video_pipeline.py
──────────────────────────
Simulates the Smart Video Search ingestion pipeline:

  video.enqueued
      → download_video       (download from cloud/yt-dlp)
  video.downloaded
      → extract_audio        (ffmpeg audio strip)
  audio.extracted
      → transcribe_audio     (faster-whisper)
  audio.transcribed
      → save_metadata        (MongoDB insert)
  video.processed            ← terminal event

Dead-letter handler is also wired so failed events are visible.

Run:
    pip install pyeda aiohttp
    python examples/video_pipeline.py
Then open http://127.0.0.1:7373 in your browser.
"""

import asyncio
import logging
import random
import sys
import os

# Make the parent directory importable when running from examples/
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from pyeda import EventBus, Event, on

# ── Logging ─────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)-7s] %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("pipeline")

# ── Bus ──────────────────────────────────────────────────────────────────────
bus = EventBus.from_config({
    "bus": {"backend": "memory", "queue_size": 500},
    "ui":  {"enabled": True, "host": "127.0.0.1", "port": 7373},
    "middleware": ["logging", "retry"],
    "defaults": {
        "concurrency": 3,
        "retry_attempts": 3,
        "retry_delay_s": 0.5,
        "retry_backoff": "exponential",
    },
    "handlers": {
        "transcribe_audio": {"concurrency": 1, "timeout_s": 60.0},
        "save_metadata":    {"concurrency": 5, "retry_attempts": 5},
    },
})

# ── Event types ──────────────────────────────────────────────────────────────
VideoEnqueued  = Event.define("video.enqueued",    ["video_id", "url"])
VideoDownloaded = Event.define("video.downloaded", ["video_id", "local_path"])
AudioExtracted  = Event.define("audio.extracted",  ["video_id", "audio_path"])
AudioTranscribed = Event.define("audio.transcribed", ["video_id", "transcript", "word_count"])
VideoProcessed  = Event.define("video.processed",  ["video_id", "status"])

# Dead-letter events (auto-emitted by bus on permanent failure)
DeadVideoDownloaded = Event.define("dead.video.downloaded", ["video_id", "local_path"])

# ── Handlers ─────────────────────────────────────────────────────────────────

@on("video.enqueued", bus=bus)
async def download_video(event: VideoEnqueued) -> None:
    """Simulate downloading a video from cloud / yt-dlp."""
    log.info("⬇  Downloading video %s from %s", event.video_id, event.url)
    await asyncio.sleep(random.uniform(0.4, 1.2))   # simulate network I/O

    # Simulate occasional download failures (10% chance)
    if random.random() < 0.10:
        raise RuntimeError(f"Download failed for {event.video_id}: network timeout")

    local_path = f"/tmp/videos/{event.video_id}.mp4"
    log.info("✓  Downloaded → %s", local_path)
    await bus.emit(VideoDownloaded(video_id=event.video_id, local_path=local_path))


@on("video.downloaded", bus=bus)
async def extract_audio(event: VideoDownloaded) -> None:
    """Simulate ffmpeg audio extraction."""
    log.info("🎵 Extracting audio from %s", event.local_path)
    await asyncio.sleep(random.uniform(0.2, 0.6))

    audio_path = event.local_path.replace(".mp4", ".wav")
    log.info("✓  Audio extracted → %s", audio_path)
    await bus.emit(AudioExtracted(video_id=event.video_id, audio_path=audio_path))


@on("audio.extracted", bus=bus)
async def transcribe_audio(event: AudioExtracted) -> None:
    """Simulate faster-whisper transcription."""
    log.info("📝 Transcribing %s", event.audio_path)
    await asyncio.sleep(random.uniform(0.8, 2.0))   # whisper is slow

    words = random.randint(200, 2000)
    transcript = f"[Transcript of {event.video_id}: {words} words of technical content]"
    log.info("✓  Transcribed %d words for %s", words, event.video_id)
    await bus.emit(AudioTranscribed(
        video_id=event.video_id,
        transcript=transcript,
        word_count=words,
    ))


@on("audio.transcribed", bus=bus)
async def save_metadata(event: AudioTranscribed) -> None:
    """Simulate MongoDB insert of transcript + metadata."""
    log.info("💾 Saving metadata for %s (%d words)", event.video_id, event.word_count)
    await asyncio.sleep(random.uniform(0.05, 0.15))  # fast DB write

    # Simulate occasional DB write failure (5% chance)
    if random.random() < 0.05:
        raise RuntimeError(f"MongoDB write failed for {event.video_id}")

    log.info("✓  Saved metadata for %s", event.video_id)
    await bus.emit(VideoProcessed(video_id=event.video_id, status="complete"))


@on("video.processed", bus=bus)
async def on_complete(event: VideoProcessed) -> None:
    """Terminal handler — log completion."""
    log.info("🎉 Pipeline complete: video_id=%s status=%s", event.video_id, event.status)


# Dead-letter handler — catches events that failed after all retries
@on("dead.video.enqueued", bus=bus)
@on("dead.video.downloaded", bus=bus)
@on("dead.audio.extracted", bus=bus)
@on("dead.audio.transcribed", bus=bus)
async def on_dead_letter(event) -> None:
    log.error("☠  Dead-lettered: %r", event)


# ── Seed data producer ───────────────────────────────────────────────────────

async def produce_videos() -> None:
    """Emit a stream of video.enqueued events to drive the pipeline."""
    await asyncio.sleep(0.5)   # give the bus a moment to fully start

    video_urls = [
        "https://youtube.com/watch?v=abc123",
        "https://youtube.com/watch?v=def456",
        "https://youtube.com/watch?v=ghi789",
        "https://r2.example.com/lectures/ml-intro.mp4",
        "https://r2.example.com/lectures/fastapi-deep-dive.mp4",
        "https://r2.example.com/lectures/neo4j-graphrag.mp4",
        "https://youtube.com/watch?v=xyz999",
        "https://r2.example.com/lectures/redis-streams.mp4",
    ]

    for i, url in enumerate(video_urls):
        video_id = f"vid_{i+1:04d}"
        log.info("📥 Enqueuing %s from %s", video_id, url)
        await bus.emit(VideoEnqueued(video_id=video_id, url=url))
        await asyncio.sleep(random.uniform(0.3, 0.8))

    log.info("All videos enqueued — pipeline running…")

    # Keep producing more videos indefinitely to show the dashboard live
    counter = len(video_urls) + 1
    while True:
        await asyncio.sleep(random.uniform(2.0, 5.0))
        video_id = f"vid_{counter:04d}"
        url = f"https://r2.example.com/lectures/video_{counter}.mp4"
        log.info("📥 Enqueuing %s", video_id)
        await bus.emit(VideoEnqueued(video_id=video_id, url=url))
        counter += 1


# ── Entry point ──────────────────────────────────────────────────────────────

async def main() -> None:
    from pyeda.ui import start_ui

    # Start the bus first (non-blocking)
    await bus.start()

    # Start the dashboard
    await start_ui(bus)

    # Start producing test events
    asyncio.create_task(produce_videos(), name="producer")

    print("\n" + "="*60)
    print("  pyeda Video Pipeline — running")
    print("  Dashboard: http://127.0.0.1:7373")
    print("  Press Ctrl-C to stop")
    print("="*60 + "\n")

    # Keep running
    try:
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        pass
    finally:
        await bus.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nStopped.")
