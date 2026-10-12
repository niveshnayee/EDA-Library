"""
tests/test_pyeda.py — Core test suite for pyeda.

Run with:
    pip install pytest pytest-asyncio
    pytest tests/ -v
"""
import asyncio
import pytest
from pyeda import EventBus, Event, on
from pyeda.exceptions import BusNotRunningError


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def bus():
    return EventBus(config=None)   # all defaults


# ── Event factory ─────────────────────────────────────────────────────────────

def test_event_define_and_instantiate():
    Ev = Event.define("test.basic", ["foo", "bar"])
    ev = Ev(foo="hello", bar=42)
    assert ev.name == "test.basic"
    assert ev.foo == "hello"
    assert ev.bar == 42
    assert ev.event_id is not None
    assert ev.emitted_at > 0


def test_event_define_idempotent():
    A = Event.define("test.idempotent", ["x"])
    B = Event.define("test.idempotent", ["x"])
    assert A is B


def test_event_to_dict():
    Ev = Event.define("test.dict", ["val"])
    ev = Ev(val=99)
    d = ev.to_dict()
    assert d["val"] == 99
    assert "event_id" in d
    assert "emitted_at" in d


# ── Bus lifecycle ─────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_bus_emit_before_start_raises():
    b = EventBus()
    Ev = Event.define("test.early", ["x"])
    with pytest.raises(BusNotRunningError):
        await b.emit(Ev(x=1))


@pytest.mark.asyncio
async def test_handler_receives_event():
    b = EventBus()
    Ev = Event.define("test.receive", ["val"])
    received = []

    @on("test.receive", bus=b)
    async def capture(event):
        received.append(event.val)

    await b.start()
    await b.emit(Ev(val="hello"))
    await asyncio.sleep(0.2)
    await b.stop()

    assert "hello" in received


@pytest.mark.asyncio
async def test_pipeline_chain():
    b = EventBus()
    A = Event.define("test.chain.a", ["n"])
    B = Event.define("test.chain.b", ["n"])
    C = Event.define("test.chain.c", ["n"])
    results = []

    @on("test.chain.a", bus=b)
    async def step1(ev):
        await b.emit(B(n=ev.n + 1))

    @on("test.chain.b", bus=b)
    async def step2(ev):
        await b.emit(C(n=ev.n + 1))

    @on("test.chain.c", bus=b)
    async def step3(ev):
        results.append(ev.n)

    await b.start()
    await b.emit(A(n=0))
    await asyncio.sleep(0.5)
    await b.stop()

    assert results == [2]


@pytest.mark.asyncio
async def test_dead_letter_on_failure():
    b = EventBus(config=__import__("pyeda.config", fromlist=["PyEDAConfig"]).PyEDAConfig(
        **{
            "defaults": __import__("pyeda.config", fromlist=["HandlerConfig"]).HandlerConfig(
                retry_attempts=1   # fail immediately, no retry
            )
        }
    ))
    # Simpler: just use from_config dict
    b = EventBus.from_config({
        "middleware": ["retry"],
        "defaults": {"retry_attempts": 1, "retry_delay_s": 0.0},
    })

    Fail = Event.define("test.fail.ev", ["x"])
    dead_received = []

    @on("test.fail.ev", bus=b)
    async def will_fail(ev):
        raise ValueError("intentional")

    @on("dead.test.fail.ev", bus=b)
    async def catch_dead(ev):
        dead_received.append(ev)

    await b.start()
    await b.emit(Fail(x=1))
    await asyncio.sleep(0.5)
    await b.stop()

    assert len(dead_received) == 1


@pytest.mark.asyncio
async def test_multiple_handlers_same_event():
    b = EventBus()
    Ev = Event.define("test.multi", ["val"])
    calls = []

    @on("test.multi", bus=b)
    async def h1(ev): calls.append("h1")

    @on("test.multi", bus=b)
    async def h2(ev): calls.append("h2")

    await b.start()
    await b.emit(Ev(val=1))
    await asyncio.sleep(0.2)
    await b.stop()

    assert "h1" in calls
    assert "h2" in calls


@pytest.mark.asyncio
async def test_metrics_recorded():
    b = EventBus()
    Ev = Event.define("test.metrics", ["val"])

    @on("test.metrics", bus=b)
    async def noop(ev): pass

    await b.start()
    await b.emit(Ev(val=1))
    await asyncio.sleep(0.2)
    await b.stop()

    snap = b.stats()
    assert snap["event_counts"].get("test.metrics", 0) >= 1
    handler_stats = list(snap["handlers"].values())
    assert any(h["success"] >= 1 for h in handler_stats)


@pytest.mark.asyncio
async def test_dead_letter_carries_error_details():
    b = EventBus.from_config({"defaults": {"retry_attempts": 2, "retry_delay_s": 0.0}})
    Fail = Event.define("test.dead.details", ["x"])
    dead_received = []

    @on("test.dead.details", bus=b)
    async def will_fail(ev):
        raise ValueError("intentional")

    @on("dead.test.dead.details", bus=b)
    async def catch_dead(ev):
        dead_received.append(ev)

    await b.start()
    await b.emit(Fail(x=1))
    await b.stop()

    assert len(dead_received) == 1
    info = dead_received[0].metadata["dead_letter"]
    assert info == {
        "handler": "will_fail",
        "error": "intentional",
        "error_type": "ValueError",
        "attempts": 2,
    }


@pytest.mark.asyncio
async def test_failing_dead_letter_handler_does_not_loop():
    b = EventBus.from_config({"defaults": {"retry_attempts": 1}})
    Fail = Event.define("test.dead.loop", ["x"])
    calls = []

    @on("test.dead.loop", bus=b)
    async def will_fail(ev):
        raise ValueError("intentional")

    @on("dead.test.dead.loop", bus=b)
    async def dead_also_fails(ev):
        calls.append(1)
        raise RuntimeError("dead handler broke")

    await b.start()
    await b.emit(Fail(x=1))
    await b.stop()

    assert len(calls) == 1


@pytest.mark.asyncio
async def test_attempt_counter_increments_on_retry():
    b = EventBus.from_config({"defaults": {"retry_attempts": 3, "retry_delay_s": 0.0}})
    Ev = Event.define("test.attempts", ["x"])
    attempts = []

    @on("test.attempts", bus=b)
    async def flaky(ev):
        attempts.append(ev.attempt)
        if ev.attempt < 2:
            raise ValueError("not yet")

    await b.start()
    await b.emit(Ev(x=1))
    await b.stop()

    assert attempts == [0, 1, 2]


@pytest.mark.asyncio
async def test_timeout_applies_to_each_attempt():
    b = EventBus.from_config({
        "defaults": {"retry_attempts": 3, "retry_delay_s": 0.0, "timeout_s": 0.1},
    })
    Ev = Event.define("test.timeout.attempt", ["x"])
    attempts = []

    @on("test.timeout.attempt", bus=b)
    async def slow_then_fast(ev):
        attempts.append(ev.attempt)
        if ev.attempt < 2:
            await asyncio.sleep(0.08)   # under the deadline on its own,
            raise ValueError("retry")   # but three of these exceed it in total
        await asyncio.sleep(0.08)

    await b.start()
    await b.emit(Ev(x=1))
    await b.stop()

    assert attempts == [0, 1, 2]
    assert any(h["success"] == 1 for h in b.stats()["handlers"].values())


# ── Shutdown ──────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_stop_waits_for_running_handlers_and_their_events():
    b = EventBus()
    A = Event.define("test.stop.a", ["n"])
    B = Event.define("test.stop.b", ["n"])
    done = []

    @on("test.stop.a", bus=b)
    async def first(ev):
        await asyncio.sleep(0.2)
        await b.emit(B(n=ev.n))

    @on("test.stop.b", bus=b)
    async def second(ev):
        done.append(ev.n)

    await b.start()
    await b.emit(A(n=7))
    await b.stop()

    assert done == [7]
    assert not b.is_running()


@pytest.mark.asyncio
async def test_stop_cancels_handlers_after_timeout():
    b = EventBus()
    Ev = Event.define("test.stop.timeout", ["x"])
    finished = []

    @on("test.stop.timeout", bus=b)
    async def never_ends(ev):
        await asyncio.sleep(30)
        finished.append(1)

    await b.start()
    await b.emit(Ev(x=1))
    await asyncio.sleep(0.05)
    await asyncio.wait_for(b.stop(timeout=0.1), timeout=2)

    assert finished == []
    assert not b.is_running()


# ── SQLite journal ────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_event_dict_round_trip():
    Ev = Event.define("test.roundtrip", ["val"])
    ev = Ev(val=5)
    clone = Event.from_dict(ev.to_dict())
    assert clone.name == "test.roundtrip"
    assert clone.val == 5
    assert clone.event_id == ev.event_id


@pytest.mark.asyncio
async def test_pending_events_are_replayed_after_restart(tmp_path):
    cfg = {
        "bus": {"backend": "sqlite", "journal_path": str(tmp_path / "journal.db")},
        "defaults": {"retry_attempts": 1},
    }
    Ev = Event.define("test.replay", ["video_id"])

    # First run: the process "dies" while the handler is still working.
    first = EventBus.from_config(cfg)

    @on("test.replay", bus=first)
    async def interrupted(ev):
        await asyncio.sleep(30)

    await first.start()
    await first.emit(Ev(video_id="v1"))
    await asyncio.sleep(0.1)
    await first.stop(timeout=0)

    # Second run: the unfinished event is delivered again, exactly once.
    replayed = []
    second = EventBus.from_config(cfg)

    @on("test.replay", bus=second)
    async def resumed(ev):
        replayed.append(ev.video_id)

    await second.start()
    await second.stop()
    assert replayed == ["v1"]

    third = EventBus.from_config(cfg)

    @on("test.replay", bus=third)
    async def again(ev):
        replayed.append(ev.video_id)

    await third.start()
    await third.stop()
    assert replayed == ["v1"]


# ── Dashboard ─────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_dashboard_pushes_updates_to_connected_clients():
    aiohttp = pytest.importorskip("aiohttp")
    import socket
    from pyeda.ui import start_ui

    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]

    b = EventBus.from_config({"ui": {"port": port}})
    Ev = Event.define("test.dashboard", ["x"])

    @on("test.dashboard", bus=b)
    async def noop(ev): pass

    await b.start()
    await start_ui(b)
    try:
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(f"http://127.0.0.1:{port}/ws") as ws:
                first = await ws.receive_json(timeout=3)     # snapshot on connect
                assert first["handlers"] == {}
                await b.emit(Ev(x=1))
                pushed = await ws.receive_json(timeout=3)    # sent by the push loop
                while not pushed["handlers"]:
                    pushed = await ws.receive_json(timeout=3)
                assert pushed["handlers"]["noop@test.dashboard"]["success"] == 1
    finally:
        for task in asyncio.all_tasks():
            if task.get_name() == "pyeda.ui.push":
                task.cancel()
        await b.stop()


# ── Config ────────────────────────────────────────────────────────────────────

def test_load_config_from_dict():
    from pyeda.config import load_config
    cfg = load_config({"bus": {"queue_size": 42}, "defaults": {"concurrency": 7}})
    assert cfg.bus.queue_size == 42
    assert cfg.defaults.concurrency == 7


def test_handler_config_override():
    from pyeda.config import load_config
    cfg = load_config({
        "defaults": {"concurrency": 3},
        "handlers": {"my_handler": {"concurrency": 10}},
    })
    assert cfg.for_handler("my_handler").concurrency == 10
    assert cfg.for_handler("other").concurrency == 3


def test_handler_override_equal_to_builtin_default_is_kept():
    from pyeda.config import load_config
    cfg = load_config({
        "defaults": {"concurrency": 5, "retry_attempts": 5},
        "handlers": {"my_handler": {"concurrency": 3}},
    })
    hcfg = cfg.for_handler("my_handler")
    assert hcfg.concurrency == 3      # explicit override wins
    assert hcfg.retry_attempts == 5   # everything else comes from defaults


def test_unknown_handler_option_is_rejected():
    from pyeda.config import load_config
    from pyeda.exceptions import ConfigError
    with pytest.raises(ConfigError):
        load_config({"handlers": {"my_handler": {"retries": 3}}})
