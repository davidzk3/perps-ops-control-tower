import asyncio
import json
import os
import signal
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras
import websockets

PG_DSN = os.environ.get("PG_DSN", "dbname=perps user=perps password=perps host=localhost port=5432")

# Binance uses lowercase symbols like btcusdt on perps (USDT-M futures)
SYMBOLS = ["btcusdt", "ethusdt"]  # add more if you like
VENUE = "binance_perps"

TRADE_STREAMS = [f"{s}@trade" for s in SYMBOLS]
BOOK_STREAMS  = [f"{s}@bookTicker" for s in SYMBOLS]

# multiplex URL (one socket for all streams)
WS_URL = "wss://fstream.binance.com/stream?streams=" + "/".join(TRADE_STREAMS + BOOK_STREAMS)

INSERT_TRADE_SQL = """
INSERT INTO ops.raw_trades (ts, venue, symbol, price, size, side)
VALUES (%s, %s, %s, %s, %s, %s);
"""

INSERT_L1_SQL = """
INSERT INTO ops.raw_book_l1 (ts, venue, symbol, bid_price, bid_size, ask_price, ask_size)
VALUES (%s, %s, %s, %s, %s, %s, %s);
"""

def ms_to_utc(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)

async def writer_loop(queue: asyncio.Queue):
    """Single DB writer to avoid many concurrent connections."""
    conn = psycopg2.connect(PG_DSN)
    conn.autocommit = True
    cur = conn.cursor()
    while True:
        kind, payload = await queue.get()
        try:
            if kind == "trade":
                cur.execute(INSERT_TRADE_SQL, payload)
            elif kind == "l1":
                cur.execute(INSERT_L1_SQL, payload)
        except Exception as e:
            # Keep going even if one row fails
            print("DB write error:", e)
        finally:
            queue.task_done()

async def socket_loop():
    backoff = 1
    queue = asyncio.Queue()
    writer_task = asyncio.create_task(writer_loop(queue))
    try:
        while True:
            try:
                print("Connecting:", WS_URL)
                async with websockets.connect(WS_URL, ping_interval=15, ping_timeout=15) as ws:
                    print("Connected to Binance perps.")
                    backoff = 1
                    async for msg in ws:
                        m = json.loads(msg)
                        stream = m.get("stream", "")
                        data = m.get("data", {})

                        # trade
                        if stream.endswith("@trade"):
                            sym = data["s"].lower()  # e.g. BTCUSDT
                            price = data.get("p")
                            qty   = data.get("q")
                            # Side: 'm' True means buyer is market maker -> trade side was SELL
                            side = "sell" if data.get("m") else "buy"
                            ts   = ms_to_utc(data.get("T", data.get("E")))
                            row = (ts, VENUE, sym, price, qty, side)
                            await queue.put(("trade", row))

                        # bookTicker (best bid/ask)
                        elif stream.endswith("@bookTicker"):
                            sym = data["s"].lower()
                            bid_p = data.get("b"); bid_q = data.get("B")
                            ask_p = data.get("a"); ask_q = data.get("A")
                            ts    = ms_to_utc(data.get("E"))
                            row = (ts, VENUE, sym, bid_p, bid_q, ask_p, ask_q)
                            await queue.put(("l1", row))
            except Exception as e:
                print("WS error, reconnecting shortly:", e)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)  # cap backoff
    finally:
        writer_task.cancel()

if __name__ == "__main__":
    try:
        asyncio.run(socket_loop())
    except KeyboardInterrupt:
        print("Stopped by user")