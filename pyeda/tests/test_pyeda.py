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
