"""
Live trade logger for Coinbase (Exchange WebSocket feed, "matches" channel).

Coinbase's public REST trade history endpoint only exposes a short recent
window (tens of thousands of trades, not deep history) — for BTC-USD that
turned out to be about 10 minutes' worth. So just like the order book,
real trade history has to be collected going forward. This script logs
every executed trade in real time, meant to run alongside
order_book_logger.py so trades and book snapshots build up together on
the same timeline.

Run this LOCALLY (not in a sandboxed environment) — it needs sustained
network access to Coinbase's WebSocket API.

Usage:
    python trade_logger.py --product BTC-USD

Stop it any time with Ctrl+C. Safe to stop and restart — each day's data
goes into its own file.
"""

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import websocket  # from websocket-client package

RAW_DIR = Path(__file__).resolve().parents[2] / "data" / "raw" / "trades_live"

WS_URL = "wss://ws-feed.exchange.coinbase.com"


def current_log_path(product: str) -> Path:
    day_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    out_dir = RAW_DIR / product.lower().replace("-", "")
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir / f"{day_str}.jsonl"


def run_logger(product: str) -> None:
    print(f"Logging to {RAW_DIR / product.lower().replace('-', '')} (one file per UTC day)")
    print("Press Ctrl+C to stop.")

    while True:
        def on_open(ws):
            print(f"Connected. Subscribing to matches for {product}...")
            ws.send(json.dumps({
                "type": "subscribe",
                "product_ids": [product],
                "channels": ["matches"],
            }))

        def on_message(ws, message):
            data = json.loads(message)
            if data.get("type") not in ("match", "last_match"):
                return
            record = {
                "recv_time_ms": int(time.time() * 1000),
                "trade_id": data.get("trade_id"),
                "time": data.get("time"),        # exchange-reported trade time
                "price": data.get("price"),
                "size": data.get("size"),
                "side": data.get("side"),        # side of the taker order
            }
            log_path = current_log_path(product)
            with open(log_path, "a") as f:
                f.write(json.dumps(record) + "\n")

        def on_error(ws, error):
            print(f"WebSocket error: {error}")

        def on_close(ws, close_status_code, close_msg):
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
            print("\nStopped by user.")
            break
        except Exception as e:
            print(f"Unexpected error: {e}. Reconnecting in 5 seconds...")
            time.sleep(5)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Log live Coinbase trades.")
    parser.add_argument("--product", default="BTC-USD", help="Product ID, e.g. BTC-USD")
    args = parser.parse_args()

    run_logger(args.product)