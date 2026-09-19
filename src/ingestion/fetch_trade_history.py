"""
Fetch historical trade data for a product from Coinbase's public REST API.

This pulls individual executed trades (price, size, side, time) going
back as far as Coinbase's API allows for the given product. Used for the
"Option B" fast-path model: approximating price impact patterns from
trade history alone, without needing full historical order book depth
(which Coinbase, like most exchanges, doesn't offer for free).

Run this LOCALLY (not in a sandboxed environment) — it needs network
access to Coinbase's REST API.

Usage:
    python fetch_trade_history.py --product BTC-USD --max-pages 500
"""

import argparse
import time
from pathlib import Path

import pandas as pd
import requests

RAW_DIR = Path(__file__).resolve().parents[2] / "data" / "raw" / "trades"

BASE_URL = "https://api.exchange.coinbase.com"
REQUEST_DELAY_SECONDS = 0.35  # stay comfortably under Coinbase's public rate limit


def fetch_trade_page(product: str, before_cursor: str | None):
    """Fetch one page (up to 100 trades) of historical trades.

    Coinbase paginates newest-first. Passing `before_cursor` (from the
    previous response's CB-BEFORE header) walks further back in time.
    """
    url = f"{BASE_URL}/products/{product}/trades"
    headers = {}
    if before_cursor:
        headers["CB-BEFORE"] = before_cursor

    resp = requests.get(url, headers=headers, timeout=15)
    resp.raise_for_status()
    trades = resp.json()
    next_before = resp.headers.get("CB-BEFORE")  # cursor for the next (older) page
    return trades, next_before


def fetch_trade_history(product: str, max_pages: int) -> pd.DataFrame:
    all_trades = []
    before_cursor = None

    for page in range(max_pages):
        try:
            trades, before_cursor = fetch_trade_page(product, before_cursor)
        except requests.HTTPError as e:
            print(f"Stopping early on page {page}: {e}")
            break

        if not trades:
            print("No more trades returned; reached the end of available history.")
            break

        all_trades.extend(trades)

        if (page + 1) % 20 == 0:
            print(f"  Fetched {page + 1} pages ({len(all_trades)} trades so far)")

        if not before_cursor:
            print("No pagination cursor returned; stopping.")
            break

        time.sleep(REQUEST_DELAY_SECONDS)

    df = pd.DataFrame(all_trades)
    return df


def main():
    parser = argparse.ArgumentParser(description="Fetch historical Coinbase trades.")
    parser.add_argument("--product", default="BTC-USD")
    parser.add_argument("--max-pages", type=int, default=500,
                         help="Each page is ~100 trades; Coinbase only exposes recent history "
                              "via this endpoint (typically the last several thousand trades, "
                              "not years back — check output volume before assuming full coverage).")
    args = parser.parse_args()

    RAW_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Fetching trade history for {args.product}...")
    df = fetch_trade_history(args.product, args.max_pages)

    if df.empty:
        print("No trades fetched.")
        return

    out_path = RAW_DIR / f"{args.product.lower().replace('-', '')}_trades.csv"
    df.to_csv(out_path, index=False)
    print(f"Saved {len(df)} trades to {out_path}")
    print(f"Time range: {df['time'].min()} to {df['time'].max()}")


if __name__ == "__main__":
    main()