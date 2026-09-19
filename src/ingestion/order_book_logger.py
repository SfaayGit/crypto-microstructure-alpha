"""
Live order book logger for Coinbase (Exchange / Advanced Trade WebSocket feed).

Coinbase's book feed works differently from Binance's: instead of a
convenient "give me the top 20 levels every second" stream, Coinbase
sends one full snapshot on subscribe, then a continuous stream of
incremental updates (individual price levels changing). This script
maintains the full order book in memory by applying those updates, and
periodically writes out just the top N levels — so the output format on
disk ends up looking the same as the original Binance version.

Run this LOCALLY (not in a sandboxed environment) — it needs sustained
network access to Coinbase's WebSocket API.

Usage:
    python order_book_logger.py --product BTC-USD --depth 20 --interval 1.0

Stop it any time with Ctrl+C. Safe to stop and restart — each day's data
goes into its own file.
"""

import argparse
import json
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import websocket  # from websocket-client package

RAW_DIR = Path(__file__).resolve().parents[2] / "data" / "raw" / "order_book"

WS_URL = "wss://ws-feed.exchange.coinbase.com"


class OrderBookState:
    """Maintains a full in-memory order book for one product, updated
    from Coinbase's snapshot + l2update messages."""

    def __init__(self):
        self.bids = {}  # price (float) -> size (float)
        self.asks = {}
        self.lock = threading.Lock()
        self.ready = False

    def apply_snapshot(self, bids, asks):
        with self.lock:
            self.bids = {float(p): float(s) for p, s in bids}
            self.asks = {float(p): float(s) for p, s in asks}
            self.ready = True

    def apply_update(self, changes):
        # changes: list of [side, price_str, size_str]
        with self.lock:
            for side, price_str, size_str in changes:
                price, size = float(price_str), float(size_str)
                book_side = self.bids if side == "buy" else self.asks
                if size == 0.0:
                    book_side.pop(price, None)
                else:
                    book_side[price] = size

    def top_levels(self, depth: int):
        with self.lock:
            top_bids = sorted(self.bids.items(), key=lambda kv: -kv[0])[:depth]
            top_asks = sorted(self.asks.items(), key=lambda kv: kv[0])[:depth]
            return top_bids, top_asks


def current_log_path(product: str) -> Path:
    day_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    out_dir = RAW_DIR / product.lower().replace("-", "")
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir / f"{day_str}.jsonl"


def snapshot_writer(book: OrderBookState, product: str, depth: int, interval: float, stop_event):
    """Runs in a background thread, writing a snapshot every `interval` seconds."""
    while not stop_event.is_set():
        time.sleep(interval)
        if not book.ready:
            continue
        top_bids, top_asks = book.top_levels(depth)
        record = {
            "recv_time_ms": int(time.time() * 1000),
            "bids": [[str(p), str(s)] for p, s in top_bids],
            "asks": [[str(p), str(s)] for p, s in top_asks],
        }
        log_path = current_log_path(product)
        with open(log_path, "a") as f:
            f.write(json.dumps(record) + "\n")


def run_logger(product: str, depth: int, interval: float) -> None:
    print(f"Logging to {RAW_DIR / product.lower().replace('-', '')} (one file per UTC day)")
    print("Press Ctrl+C to stop.")

    while True:
        book = OrderBookState()
        stop_event = threading.Event()

        def on_open(ws, book=book, stop_event=stop_event):
            print(f"Connected. Subscribing to level2_batch for {product}...")
            ws.send(json.dumps({
                "type": "subscribe",
                "product_ids": [product],
                "channels": ["level2_batch"],
            }))
            writer_thread = threading.Thread(
                target=snapshot_writer, args=(book, product, depth, interval, stop_event), daemon=True
            )
            writer_thread.start()

        def on_message(ws, message, book=book):
            data = json.loads(message)
            msg_type = data.get("type")
            if msg_type == "snapshot":
                book.apply_snapshot(data.get("bids", []), data.get("asks", []))
            elif msg_type == "l2update":
                book.apply_update(data.get("changes", []))
            elif msg_type == "error":
                print(f"Coinbase error message: {data}")

        def on_error(ws, error):
            print(f"WebSocket error: {error}")

        def on_close(ws, close_status_code, close_msg, stop_event=stop_event):
            stop_event.set()
            print(f"WebSocket closed (code={close_status_code}, msg={close_msg}). "
                  f"Reconnecting in 5 seconds...")

        try:
            ws = websocket.WebSocketApp(
                WS_URL,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )
            ws.run_forever(ping_interval=30, ping_timeout=10)
            time.sleep(5)
        except KeyboardInterrupt:
            stop_event.set()
            print("\nStopped by user.")
            break
        except Exception as e:
            print(f"Unexpected error: {e}. Reconnecting in 5 seconds...")
            time.sleep(5)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Log live Coinbase order book snapshots.")
    parser.add_argument("--product", default="BTC-USD", help="Product ID, e.g. BTC-USD")
    parser.add_argument("--depth", type=int, default=20, help="Number of levels per side to log")
    parser.add_argument("--interval", type=float, default=1.0, help="Seconds between logged snapshots")
    args = parser.parse_args()

    run_logger(args.product, args.depth, args.interval)