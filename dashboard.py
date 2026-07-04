"""dashboard - a live view of the whole architecture.

Subscribes to every event on the broker and streams them to the browser with
Server-Sent Events, so you can watch components light up as events flow, see a
running event feed, and read the current order-status view. A teaching aid, not
part of the core flow - it only observes.

Run: uvicorn dashboard:app --host 0.0.0.0 --port 8002
"""

import asyncio
import json
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, StreamingResponse

import bus

clients = set()  # one asyncio.Queue per connected browser


async def on_event(event):
    payload = json.dumps(
        {
            "type": event.type,
            "id": event.id,
            "time": event.time,
            "correlation_id": event.correlation_id,
            "data": event.data,
        }
    )
    for queue in list(clients):
        queue.put_nowait(payload)


@asynccontextmanager
async def lifespan(app):
    await bus.start("dashboard")
    # "#" matches every routing key on the topic exchange, so we see all events.
    await bus.consume("dashboard", ["#"], on_event)
    yield
    await bus.stop()


app = FastAPI(title="dashboard", lifespan=lifespan)


@app.get("/events")
async def events():
    queue = asyncio.Queue()
    clients.add(queue)

    async def stream():
        try:
            while True:
                payload = await queue.get()
                yield f"data: {payload}\n\n"
        finally:
            clients.discard(queue)

    return StreamingResponse(stream(), media_type="text/event-stream")


@app.get("/", response_class=HTMLResponse)
async def index():
    return PAGE


PAGE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Event-Driven Architecture - Live Dashboard</title>
<style>
  :root { --bg:#0f1720; --panel:#16212e; --line:#2b3a4a; --text:#e6edf3; --muted:#8aa0b2; --accent:#4fd1c5; }
  * { box-sizing:border-box; }
  body { margin:0; font:14px/1.5 -apple-system,Segoe UI,Roboto,sans-serif; background:var(--bg); color:var(--text); }
  header { padding:16px 20px; border-bottom:1px solid var(--line); }
  header h1 { margin:0; font-size:18px; }
  header p { margin:4px 0 0; color:var(--muted); }
  .wrap { display:grid; grid-template-columns:1.5fr 1fr; gap:16px; padding:16px 20px; }
  @media (max-width:820px){ .wrap{ grid-template-columns:1fr; } }
  .panel { background:var(--panel); border:1px solid var(--line); border-radius:10px; padding:14px; }
  .panel h2 { margin:0 0 12px; font-size:12px; text-transform:uppercase; letter-spacing:.06em; color:var(--muted); }
  .arch { display:flex; align-items:center; gap:8px; flex-wrap:wrap; }
  .tier { display:flex; flex-direction:column; gap:8px; }
  .arrow { color:var(--muted); font-size:18px; }
  .box { background:#1d2a38; border:1px solid var(--line); border-radius:8px; padding:9px 11px; min-width:118px; transition:border-color .12s, box-shadow .12s, background .12s; }
  .box .name { font-weight:600; }
  .box .role { color:var(--muted); font-size:11px; }
  .box.active { border-color:var(--accent); box-shadow:0 0 0 2px rgba(79,209,197,.35); background:#1f3540; }
  .controls { margin-top:14px; display:flex; gap:8px; }
  button { background:var(--accent); color:#04252b; border:0; padding:8px 12px; border-radius:6px; font-weight:600; cursor:pointer; }
  button.poison { background:#f0806c; color:#2b0b06; }
  #feed { list-style:none; margin:0; padding:0; max-height:340px; overflow:auto; }
  #feed li { padding:6px 8px; border-bottom:1px solid var(--line); font-family:ui-monospace,SFMono-Regular,monospace; font-size:12px; }
  .t-orders{color:#8ec7ff;} .t-payments{color:#7ee787;} .t-inventory{color:#ffd479;}
  table { width:100%; border-collapse:collapse; font-size:12px; }
  th,td { text-align:left; padding:5px 6px; border-bottom:1px solid var(--line); }
  th { color:var(--muted); font-weight:600; }
  .s-CONFIRMED{color:#7ee787;} .s-PLACED{color:#ffd479;}
  code { color:var(--muted); }
</style>
</head>
<body>
<header>
  <h1>Event-Driven Architecture - Live Dashboard</h1>
  <p>Place an order and watch the services react. Components light up as events flow through the broker.</p>
</header>
<div class="wrap">
  <div>
    <div class="panel">
      <h2>Architecture</h2>
      <div class="arch">
        <div class="tier"><div class="box" id="c-client"><div class="name">client</div><div class="role">places orders</div></div></div>
        <div class="arrow">&rarr;</div>
        <div class="tier"><div class="box" id="c-order"><div class="name">order-service</div><div class="role">producer</div></div></div>
        <div class="arrow">&rarr;</div>
        <div class="tier"><div class="box" id="c-mq"><div class="name">RabbitMQ</div><div class="role">event broker</div></div></div>
        <div class="arrow">&rarr;</div>
        <div class="tier">
          <div class="box" id="c-payment"><div class="name">payment-service</div><div class="role">reacts</div></div>
          <div class="box" id="c-inventory"><div class="name">inventory-service</div><div class="role">reacts</div></div>
          <div class="box" id="c-projection"><div class="name">projection-service</div><div class="role">read model</div></div>
        </div>
        <div class="arrow">&rarr;</div>
        <div class="tier">
          <div class="box" id="c-pg"><div class="name">PostgreSQL</div><div class="role">order status</div></div>
          <div class="box" id="c-dlq"><div class="name">dead-letter</div><div class="role">failed messages</div></div>
        </div>
      </div>
      <div class="controls">
        <button onclick="placeOrder('keyboard')">Place order</button>
        <button class="poison" onclick="placeOrder('POISON')">Place POISON order</button>
      </div>
    </div>
    <div class="panel" style="margin-top:16px;">
      <h2>Read model (order status)</h2>
      <table><thead><tr><th>order</th><th>status</th><th>payment</th><th>inventory</th></tr></thead><tbody id="orders"></tbody></table>
    </div>
  </div>
  <div class="panel">
    <h2>Live events</h2>
    <ul id="feed"></ul>
  </div>
</div>
<script>
const ORDER_API = 'http://localhost:8000';
const READ_API  = 'http://localhost:8001';
const MAP = {
  'orders.placed':      ['c-order','c-mq','c-payment','c-inventory','c-projection'],
  'payments.captured':  ['c-payment','c-mq','c-projection'],
  'inventory.reserved': ['c-inventory','c-mq','c-projection'],
};
function highlight(type){
  (MAP[type]||[]).forEach(id => {
    const el = document.getElementById(id);
    if(!el) return;
    el.classList.add('active');
    setTimeout(() => el.classList.remove('active'), 900);
  });
}
const feed = document.getElementById('feed');
function typeClass(type){
  const head = type.split('.')[0];
  return head === 'orders' ? 't-orders' : head === 'payments' ? 't-payments' : 't-inventory';
}
function addFeed(ev){
  const li = document.createElement('li');
  const label = document.createElement('span');
  label.className = typeClass(ev.type);
  label.textContent = ev.type;
  const code = document.createElement('code');
  code.textContent = 'corr=' + (ev.correlation_id || '').slice(0, 8);
  li.append(label, document.createTextNode(' order=' + (ev.data.order_id || '').slice(0, 8) + ' '), code);
  feed.prepend(li);
  while(feed.children.length > 50) feed.removeChild(feed.lastChild);
}
const es = new EventSource('/events');
es.onmessage = e => { const ev = JSON.parse(e.data); highlight(ev.type); addFeed(ev); };
function placeOrder(item){
  fetch(ORDER_API + '/orders', {
    method: 'POST',
    headers: {'content-type': 'application/json'},
    body: JSON.stringify({customer: 'demo', item: item, amount_cents: 4999}),
  }).catch(() => {});
}
function cell(text, className){
  const td = document.createElement('td');
  td.textContent = text || '';
  if(className) td.className = className;
  return td;
}
async function pollOrders(){
  try {
    const rows = await (await fetch(READ_API + '/orders')).json();
    const tbody = document.getElementById('orders');
    tbody.replaceChildren();
    for(const row of rows){
      const tr = document.createElement('tr');
      const idCell = document.createElement('td');
      const code = document.createElement('code');
      code.textContent = (row.order_id || '').slice(0, 8);
      idCell.appendChild(code);
      tr.append(idCell, cell(row.status, 's-' + (row.status || '')), cell(row.payment), cell(row.inventory));
      tbody.appendChild(tr);
    }
  } catch(e) {}
}
setInterval(pollOrders, 2000);
pollOrders();
</script>
</body>
</html>"""
