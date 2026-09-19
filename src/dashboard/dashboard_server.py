"""
Dashboard backend v2: reads directly from the same files
order_book_logger.py and trade_logger.py are already writing, instead of
opening a third independent connection to Coinbase. This guarantees the
dashboard shows exactly what's being recorded — one source of truth.

The order book logger already writes a full top-N snapshot once per
second, so we just read the most recent line of today's file. The trade
logger appends one line per trade, so we tail new lines as they arrive.

Usage:
    uvicorn dashboard_server:app --host 0.0.0.0 --port 8000
"""

import asyncio
import json
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse

app = FastAPI()

PROJECT_ROOT = Path(__file__).resolve().parents[2]
ORDER_BOOK_DIR = PROJECT_ROOT / "data" / "raw" / "order_book" / "btcusd"
TRADES_DIR = PROJECT_ROOT / "data" / "raw" / "trades_live" / "btcusd"
INDEX_HTML_PATH = Path(__file__).parent / "static" / "index.html"

BROADCAST_INTERVAL_SECONDS = 0.5
MAX_RECENT_TRADES = 30


def todays_path(base_dir: Path) -> Path:
    day_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return base_dir / f"{day_str}.jsonl"


class SharedState:
    def __init__(self):
        self.bids = []
        self.asks = []
        self.ready = False
        self.recent_trades = deque(maxlen=MAX_RECENT_TRADES)
        self.logger_active = False  # true if the book file has been updated recently


state = SharedState()
active_clients: set[WebSocket] = set()


def read_last_line(path: Path) -> str | None:
    """Efficiently read just the last line of a large, growing file."""
    try:
        with open(path, "rb") as f:
            f.seek(0, 2)
            file_size = f.tell()
            chunk_size = min(4096, file_size)
            f.seek(-chunk_size, 2)
            data = f.read().decode(errors="ignore")
            lines = [l for l in data.splitlines() if l.strip()]
            return lines[-1] if lines else None
    except (FileNotFoundError, OSError, ValueError):
        return None


async def book_tail_loop():
    """Polls the order book logger's file for its latest snapshot line."""
    last_mtime = None
    last_change_time = time.time()
    STALE_THRESHOLD_SECONDS = 5  # logger writes ~once/sec; allow buffer for jitter

    while True:
        path = todays_path(ORDER_BOOK_DIR)
        try:
            mtime = path.stat().st_mtime
        except FileNotFoundError:
            mtime = None

        now = time.time()
        if mtime is not None and mtime != last_mtime:
            last_change_time = now
            last_mtime = mtime

        state.logger_active = mtime is not None and (now - last_change_time) < STALE_THRESHOLD_SECONDS

        line = read_last_line(path)
        if line:
            try:
                record = json.loads(line)
                if "bids" in record and "asks" in record:
                    state.bids = [(float(p), float(s)) for p, s in record["bids"]]
                    state.asks = [(float(p), float(s)) for p, s in record["asks"]]
                    state.ready = True
            except (json.JSONDecodeError, KeyError, ValueError):
                pass

        await asyncio.sleep(BROADCAST_INTERVAL_SECONDS)


async def trades_tail_loop():
    """Tails newly appended lines in the trade logger's file."""
    current_path = None
    position = 0

    while True:
        path = todays_path(TRADES_DIR)

        if path != current_path:
            current_path = path
            position = 0  # new day's file: start following from its beginning

        try:
            with open(path, "r") as f:
                f.seek(position)
                new_lines = f.readlines()
                position = f.tell()
        except FileNotFoundError:
            new_lines = []

        for line in new_lines:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
                if "price" in record and "size" in record:
                    state.recent_trades.appendleft(record)
            except json.JSONDecodeError:
                continue

        await asyncio.sleep(BROADCAST_INTERVAL_SECONDS)


async def broadcaster_loop():
    while True:
        await asyncio.sleep(BROADCAST_INTERVAL_SECONDS)
        if not active_clients:
            continue

        payload = json.dumps({
            "logger_active": state.logger_active,
            "ready": state.ready,
            "bids": state.bids,
            "asks": state.asks,
            "trades": list(state.recent_trades),
        })

        dead_clients = set()
        for client in active_clients:
            try:
                await client.send_text(payload)
            except Exception:
                dead_clients.add(client)
        active_clients.difference_update(dead_clients)


@app.on_event("startup")
async def startup():
    asyncio.create_task(book_tail_loop())
    asyncio.create_task(trades_tail_loop())
    asyncio.create_task(broadcaster_loop())


@app.get("/", response_class=HTMLResponse)
async def index():
    return INDEX_HTML_PATH.read_text()


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    active_clients.add(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        active_clients.discard(websocket)