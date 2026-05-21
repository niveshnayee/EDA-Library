"""
pyeda.ui — Live dashboard server.

Serves a self-contained HTML/JS dashboard over HTTP + WebSocket.
The dashboard receives incremental metric diffs every second via WS push.

Dependencies (optional — UI silently disabled if missing):
    aiohttp>=3.9
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Set, TYPE_CHECKING

if TYPE_CHECKING:
    from pyeda.bus import EventBus

logger = logging.getLogger("pyeda.ui")

# ---------------------------------------------------------------------------
# HTML dashboard
# ---------------------------------------------------------------------------

DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8" />
<meta name="viewport" content="width=device-width, initial-scale=1.0" />
<title>pyeda — Event Bus Dashboard</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600;700&family=Syne:wght@400;600;800&display=swap" rel="stylesheet">
<style>
  :root {
    --bg:        #0a0c10;
    --surface:   #111318;
    --surface2:  #1a1d24;
    --border:    #252830;
    --accent:    #00e5a0;
    --accent2:   #7b6cff;
    --warn:      #ff6b6b;
    --muted:     #4a5060;
    --text:      #e2e6f0;
    --text-dim:  #8892a4;
    --mono:      'JetBrains Mono', monospace;
    --sans:      'Syne', sans-serif;
    --radius:    8px;
  }
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    background: var(--bg);
    color: var(--text);
    font-family: var(--sans);
    min-height: 100vh;
    overflow-x: hidden;
  }
  body::before {
    content: '';
    position: fixed; inset: 0;
    background-image:
      linear-gradient(rgba(0,229,160,0.02) 1px, transparent 1px),
      linear-gradient(90deg, rgba(0,229,160,0.02) 1px, transparent 1px);
    background-size: 40px 40px;
    pointer-events: none; z-index: 0;
  }

  header {
    position: sticky; top: 0; z-index: 100;
    display: flex; align-items: center; justify-content: space-between;
    padding: 14px 28px;
    background: rgba(10,12,16,0.85);
    backdrop-filter: blur(12px);
    border-bottom: 1px solid var(--border);
  }
  .logo { font-family: var(--mono); font-size: 1.1rem; font-weight: 700; letter-spacing: -0.02em; }
  .logo span { color: var(--accent); }
  .status-pill {
    display: flex; align-items: center; gap: 8px;
    font-family: var(--mono); font-size: 0.72rem; color: var(--text-dim);
    background: var(--surface); border: 1px solid var(--border);
    border-radius: 20px; padding: 6px 14px;
  }
  .dot { width: 7px; height: 7px; border-radius: 50%; background: var(--muted); transition: background 0.3s; }
  .dot.live { background: var(--accent); box-shadow: 0 0 8px var(--accent); animation: pulse 2s infinite; }
  .dot.dead { background: var(--warn); }
  @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:0.4} }

  main {
    position: relative; z-index: 1;
    max-width: 1400px; margin: 0 auto;
    padding: 28px 28px 60px;
    display: grid; gap: 20px;
  }

  .kpi-row { display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 14px; }
  .kpi {
    background: var(--surface); border: 1px solid var(--border);
    border-radius: var(--radius); padding: 20px;
    position: relative; overflow: hidden; transition: border-color 0.2s;
  }
  .kpi::after {
    content: ''; position: absolute; bottom: 0; left: 0; right: 0;
    height: 2px; background: var(--accent); opacity: 0.3;
  }
  .kpi:hover { border-color: var(--accent); }
  .kpi-label { font-size: 0.68rem; color: var(--text-dim); letter-spacing: 0.1em; text-transform: uppercase; margin-bottom: 10px; }
  .kpi-value { font-family: var(--mono); font-size: 2rem; font-weight: 700; color: var(--accent); line-height: 1; }

  .section-title {
    font-size: 0.72rem; letter-spacing: 0.12em; text-transform: uppercase;
    color: var(--text-dim); display: flex; align-items: center; gap: 10px; margin-bottom: 12px;
  }
  .section-title::after { content: ''; flex: 1; height: 1px; background: var(--border); }

  .handlers-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(280px, 1fr)); gap: 14px; }
  .handler-card {
    background: var(--surface); border: 1px solid var(--border);
    border-radius: var(--radius); padding: 18px;
    transition: border-color 0.2s, transform 0.15s; position: relative;
  }
  .handler-card:hover { border-color: var(--accent2); transform: translateY(-2px); }
  .handler-card.active-now { border-color: var(--accent); }
  .handler-name { font-family: var(--mono); font-size: 0.88rem; font-weight: 600; color: var(--text); margin-bottom: 4px; }
  .handler-event { font-family: var(--mono); font-size: 0.7rem; color: var(--accent2); margin-bottom: 14px; }
  .handler-stats { display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 8px; }
  .stat-cell { text-align: center; }
  .stat-val { font-family: var(--mono); font-size: 1.1rem; font-weight: 600; }
  .stat-lbl { font-size: 0.6rem; color: var(--text-dim); text-transform: uppercase; letter-spacing: 0.08em; margin-top: 2px; }
  .stat-val.ok   { color: var(--accent); }
  .stat-val.fail { color: var(--warn); }
  .stat-val.dead { color: #ff9944; }
  .handler-bar { margin-top: 12px; height: 3px; background: var(--border); border-radius: 2px; overflow: hidden; }
  .handler-bar-fill { height: 100%; background: var(--accent); border-radius: 2px; transition: width 0.4s ease; }
  .handler-bar-fill.has-errors { background: var(--warn); }
  .active-badge {
    position: absolute; top: 14px; right: 14px;
    background: var(--accent); color: #000;
    font-family: var(--mono); font-size: 0.62rem; font-weight: 700;
    padding: 2px 8px; border-radius: 10px;
  }
  .avg-ms { font-family: var(--mono); font-size: 0.7rem; color: var(--text-dim); margin-top: 8px; text-align: right; }

  .log-container { background: var(--surface); border: 1px solid var(--border); border-radius: var(--radius); overflow: hidden; }
  .log-header {
    display: flex; align-items: center; justify-content: space-between;
    padding: 12px 18px; border-bottom: 1px solid var(--border); background: var(--surface2);
  }
  .log-title { font-family: var(--mono); font-size: 0.78rem; font-weight: 600; }
  .log-count { font-family: var(--mono); font-size: 0.68rem; color: var(--text-dim); }
  #log-body { height: 400px; overflow-y: auto; font-family: var(--mono); font-size: 0.74rem; }
  #log-body::-webkit-scrollbar { width: 4px; }
  #log-body::-webkit-scrollbar-thumb { background: var(--border); border-radius: 2px; }
  .log-col-header {
    display: grid; grid-template-columns: 80px 160px 160px 90px 70px;
    gap: 12px; padding: 8px 18px;
    font-size: 0.62rem; color: var(--muted); text-transform: uppercase; letter-spacing: 0.08em;
    border-bottom: 1px solid var(--border); background: var(--surface2);
  }
  .log-row {
    display: grid; grid-template-columns: 80px 160px 160px 90px 70px;
    gap: 12px; padding: 8px 18px;
    border-bottom: 1px solid rgba(255,255,255,0.03);
    align-items: center;
    animation: fadeIn 0.25s ease;
  }
  @keyframes fadeIn { from { opacity:0; background: rgba(0,229,160,0.04); } to { opacity:1; background: transparent; } }
  .log-row:hover { background: var(--surface2); }
  .log-status { font-weight: 700; font-size: 0.72rem; }
  .log-status.success       { color: var(--accent); }
  .log-status.failed        { color: var(--warn); }
  .log-status.dead_lettered { color: #ff9944; }
  .log-event   { color: var(--accent2); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .log-handler { color: var(--text-dim); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .log-ms      { color: var(--text-dim); text-align: right; }
  .log-time    { color: var(--muted); font-size: 0.68rem; }

  .two-col { display: grid; grid-template-columns: 1fr 1.6fr; gap: 20px; }
  @media (max-width: 1000px) { .two-col { grid-template-columns: 1fr; } }

  .panel { background: var(--surface); border: 1px solid var(--border); border-radius: var(--radius); padding: 20px; }
  .event-bar-list { display: flex; flex-direction: column; gap: 10px; }
  .event-bar-row { display: grid; grid-template-columns: 180px 1fr 55px; align-items: center; gap: 14px; font-family: var(--mono); font-size: 0.75rem; }
  .event-bar-name { color: var(--accent2); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .event-bar-track { height: 6px; background: var(--border); border-radius: 3px; overflow: hidden; }
  .event-bar-fill { height: 100%; background: linear-gradient(90deg, var(--accent2), var(--accent)); border-radius: 3px; transition: width 0.5s ease; }
  .event-bar-count { color: var(--text-dim); text-align: right; }

  .empty-state { display: flex; flex-direction: column; align-items: center; justify-content: center; height: 80px; color: var(--muted); font-family: var(--mono); font-size: 0.78rem; gap: 8px; }

  .uptime-lbl { font-family: var(--mono); font-size: 0.7rem; color: var(--text-dim); }
</style>
</head>
<body>

<header>
  <div class="logo">py<span>eda</span></div>
  <div style="display:flex;align-items:center;gap:16px">
    <span class="uptime-lbl" id="uptime-label">uptime —</span>
    <div class="status-pill">
      <div class="dot" id="ws-dot"></div>
      <span id="ws-label">connecting…</span>
    </div>
  </div>
</header>

<main>
  <div class="kpi-row">
    <div class="kpi"><div class="kpi-label">Total Dispatched</div><div class="kpi-value" id="kpi-total">—</div></div>
    <div class="kpi"><div class="kpi-label">Success</div><div class="kpi-value" id="kpi-success" style="color:var(--accent)">—</div></div>
    <div class="kpi"><div class="kpi-label">Failed</div><div class="kpi-value" id="kpi-failed" style="color:var(--warn)">—</div></div>
    <div class="kpi"><div class="kpi-label">Dead-lettered</div><div class="kpi-value" id="kpi-dead" style="color:#ff9944">—</div></div>
    <div class="kpi"><div class="kpi-label">Active Now</div><div class="kpi-value" id="kpi-active" style="color:var(--accent2)">—</div></div>
    <div class="kpi"><div class="kpi-label">Handlers</div><div class="kpi-value" id="kpi-handlers" style="color:var(--text)">—</div></div>
  </div>

  <div>
    <div class="section-title">Handlers</div>
    <div class="handlers-grid" id="handlers-grid">
      <div class="empty-state"><span>⏳</span><span>Waiting for data…</span></div>
    </div>
  </div>

  <div class="two-col">
    <div class="panel">
      <div class="section-title">Event Throughput</div>
      <div class="event-bar-list" id="event-bars">
        <div class="empty-state"><span>No events yet</span></div>
      </div>
    </div>

    <div class="log-container">
      <div class="log-header">
        <span class="log-title">Live Event Log</span>
        <span class="log-count" id="log-count">0 events</span>
      </div>
      <div class="log-col-header">
        <span>Status</span><span>Event</span><span>Handler</span><span>Duration</span><span>Time</span>
      </div>
      <div id="log-body"></div>
    </div>
  </div>
</main>

<script>
// ── State ───────────────────────────────────────────────────────────────────
let ws;
let totalLogCount = 0;
let seenSeqs = new Set();     // dedup by server-assigned seq number
const MAX_DOM_ROWS = 200;

// ── WebSocket ────────────────────────────────────────────────────────────────
function connect() {
  const proto = location.protocol === 'https:' ? 'wss' : 'ws';
  ws = new WebSocket(`${proto}://${location.host}/ws`);

  ws.onopen = () => {
    document.getElementById('ws-dot').className = 'dot live';
    document.getElementById('ws-label').textContent = 'live';
  };
  ws.onclose = () => {
    document.getElementById('ws-dot').className = 'dot dead';
    document.getElementById('ws-label').textContent = 'reconnecting…';
    setTimeout(connect, 2000);
  };
  ws.onerror = () => ws.close();
  ws.onmessage = (evt) => {
    try {
      const data = JSON.parse(evt.data);
      render(data);
    } catch(e) {
      console.error('parse error', e);
    }
  };
}

// ── Helpers ───────────────────────────────────────────────────────────────────
function fmt(ts) {
  const d = new Date(ts * 1000);
  return d.toLocaleTimeString([], {hour:'2-digit',minute:'2-digit',second:'2-digit'});
}
function fmtUptime(s) {
  s = Math.round(s);
  if (s < 60) return `${s}s`;
  if (s < 3600) return `${Math.floor(s/60)}m ${s%60}s`;
  const h = Math.floor(s/3600), m = Math.floor((s%3600)/60);
  return `${h}h ${m}m`;
}

// ── Render ────────────────────────────────────────────────────────────────────
function render(snap) {
  // Uptime
  document.getElementById('uptime-label').textContent = `uptime ${fmtUptime(snap.uptime_s)}`;

  const handlers = Object.values(snap.handlers || {});
  const totalDispatched = handlers.reduce((a,h) => a + h.total, 0);
  const totalSuccess    = handlers.reduce((a,h) => a + h.success, 0);
  const totalFailed     = handlers.reduce((a,h) => a + h.failed, 0);
  const totalDead       = handlers.reduce((a,h) => a + h.dead_lettered, 0);
  const totalActive     = handlers.reduce((a,h) => a + h.active, 0);

  document.getElementById('kpi-total').textContent   = totalDispatched.toLocaleString();
  document.getElementById('kpi-success').textContent = totalSuccess.toLocaleString();
  document.getElementById('kpi-failed').textContent  = totalFailed.toLocaleString();
  document.getElementById('kpi-dead').textContent    = totalDead.toLocaleString();
  document.getElementById('kpi-active').textContent  = totalActive;
  document.getElementById('kpi-handlers').textContent = handlers.length;

  // Handler cards
  const grid = document.getElementById('handlers-grid');
  if (handlers.length === 0) {
    grid.innerHTML = '<div class="empty-state"><span>No handlers registered yet</span></div>';
  } else {
    grid.innerHTML = handlers.map(h => {
      const pct = h.total ? (h.success / h.total * 100).toFixed(1) : 0;
      const hasErr = h.failed > 0 || h.dead_lettered > 0;
      return `
        <div class="handler-card ${h.active > 0 ? 'active-now' : ''}">
          ${h.active > 0 ? `<div class="active-badge">${h.active} active</div>` : ''}
          <div class="handler-name">${h.name}</div>
          <div class="handler-event">↳ ${h.event_name}</div>
          <div class="handler-stats">
            <div class="stat-cell"><div class="stat-val ok">${h.success}</div><div class="stat-lbl">Success</div></div>
            <div class="stat-cell"><div class="stat-val fail">${h.failed}</div><div class="stat-lbl">Failed</div></div>
            <div class="stat-cell"><div class="stat-val dead">${h.dead_lettered}</div><div class="stat-lbl">Dead</div></div>
          </div>
          <div class="handler-bar">
            <div class="handler-bar-fill ${hasErr ? 'has-errors' : ''}" style="width:${pct}%"></div>
          </div>
          <div class="avg-ms">${h.avg_ms > 0 ? `avg ${h.avg_ms}ms` : 'no completions yet'}</div>
        </div>`;
    }).join('');
  }

  // Event throughput bars
  const counts = snap.event_counts || {};
  const evKeys = Object.keys(counts).filter(k => !k.startsWith('dead.'));
  const barsEl = document.getElementById('event-bars');
  if (evKeys.length === 0) {
    barsEl.innerHTML = '<div class="empty-state"><span>No events yet</span></div>';
  } else {
    const maxC = Math.max(...evKeys.map(k => counts[k]));
    barsEl.innerHTML = evKeys.sort((a,b) => counts[b]-counts[a]).map(k => `
      <div class="event-bar-row">
        <div class="event-bar-name">${k}</div>
        <div class="event-bar-track">
          <div class="event-bar-fill" style="width:${maxC ? (counts[k]/maxC*100).toFixed(1) : 0}%"></div>
        </div>
        <div class="event-bar-count">${counts[k]}</div>
      </div>`).join('');
  }

  // Live log — only render rows we haven't seen (dedup by seq)
  const recent = snap.recent || [];
  const logBody = document.getElementById('log-body');
  let added = 0;

  // recent comes newest-first when since_seq=0, oldest-first when incremental
  // We always prepend to DOM so newest is at top
  const toAdd = recent.filter(r => !seenSeqs.has(r.seq));

  // Sort ascending by seq so we prepend in correct order
  toAdd.sort((a, b) => a.seq - b.seq);

  for (const row of toAdd) {
    seenSeqs.add(row.seq);
    totalLogCount++;
    added++;

    const div = document.createElement('div');
    div.className = 'log-row';
    const statusLabel = row.status === 'success' ? '✓ ok'
                      : row.status === 'failed'  ? '✗ fail'
                      : '⚠ dead';
    div.innerHTML = `
      <span class="log-status ${row.status}">${statusLabel}</span>
      <span class="log-event">${row.event_name}</span>
      <span class="log-handler">${row.handler_name}</span>
      <span class="log-ms">${row.duration_ms.toFixed(1)}ms</span>
      <span class="log-time">${fmt(row.timestamp)}</span>`;
    logBody.insertBefore(div, logBody.firstChild);
  }

  // Trim excess DOM rows
  while (logBody.children.length > MAX_DOM_ROWS) {
    logBody.removeChild(logBody.lastChild);
  }

  document.getElementById('log-count').textContent =
    `${totalLogCount.toLocaleString()} event${totalLogCount !== 1 ? 's' : ''}`;
}

connect();
</script>
</body>
</html>
"""


# ---------------------------------------------------------------------------
# Server
# ---------------------------------------------------------------------------

async def start_ui(bus: "EventBus") -> None:
    """
    Launch the dashboard HTTP + WebSocket server.
    Silently skips if aiohttp is not installed or UI is disabled.
    """
    try:
        from aiohttp import web  # type: ignore
    except ImportError:
        logger.warning(
            "pyeda UI requires aiohttp: pip install aiohttp  — dashboard disabled."
        )
        return

    cfg = bus.config.ui
    if not cfg.enabled:
        return

    app = web.Application()
    _clients: Set = set()

    async def index(request: web.Request) -> web.Response:
        return web.Response(text=DASHBOARD_HTML, content_type="text/html")

    async def ws_handler(request: web.Request) -> web.WebSocketResponse:
        ws_resp = web.WebSocketResponse(heartbeat=10)
        await ws_resp.prepare(request)
        _clients.add(ws_resp)
        logger.debug("pyeda UI: client connected (%d total)", len(_clients))
        try:
            # Send a full snapshot immediately on connect so the page
            # isn't blank while waiting for the next push tick.
            initial = json.dumps(bus.stats(since_seq=0))
            await ws_resp.send_str(initial)
            async for _ in ws_resp:
                pass  # we don't read client messages
        except Exception:
            pass
        finally:
            _clients.discard(ws_resp)
            logger.debug("pyeda UI: client disconnected (%d total)", len(_clients))
        return ws_resp

    app.router.add_get("/", index)
    app.router.add_get("/ws", ws_handler)

    # ── Push loop: send incremental updates every second ──────────────
    async def push_loop() -> None:
        last_seq = 0
        while True:
            try:
                await asyncio.sleep(1)
                if not _clients:
                    continue

                snap = bus.stats(since_seq=last_seq)
                last_seq = snap.get("max_seq", last_seq)
                payload = json.dumps(snap)

                dead: Set = set()
                for client in list(_clients):
                    try:
                        await client.send_str(payload)
                    except Exception:
                        dead.add(client)
                _clients -= dead
            except asyncio.CancelledError:
                break
            except Exception as exc:
                # Log and keep going — never let the push loop die silently
                logger.exception("pyeda UI push loop error: %s", exc)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, cfg.host, cfg.port)
    await site.start()

    logger.info("pyeda UI: dashboard → http://%s:%d", cfg.host, cfg.port)

    # Keep a reference so the task isn't GC'd
    task = asyncio.create_task(push_loop(), name="pyeda.ui.push")
    # Attach to app so it lives as long as the server
    app["push_task"] = task