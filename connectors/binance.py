import asyncio
import json
import os
import signal
import time
from datetime import datetime, timezone

import psycopg2
import websockets
from websockets.exceptions import ConnectionClosed, ConnectionClosedError

PG_DSN = os.environ.get("PG_DSN", "dbname=perps user=perps password=perps host=localhost port=5432")

SYMBOLS = ["btcusdt", "ethusdt"]  # start small; add later
VENUE = "binance_perps"

TRADE_STREAMS = [f"{s}@trade" for s in SYMBOLS]
BOOK_STREAMS = [f"{s}@bookTicker" for s in SYMBOLS]

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

def log(msg: str):
    now = datetime.now().strftime("%H:%M:%S")
    print(f"[{now}] {msg}", flush=True)

async def writer_loop(queue: asyncio.Queue, stop_event: asyncio.Event):
    conn = psycopg2.connect(PG_DSN)
    conn.autocommit = True
    cur = conn.cursor()

    try:
        while not stop_event.is_set():
            try:
                kind, payload = await asyncio.wait_for(queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue

            try:
                if kind == "trade":
                    cur.execute(INSERT_TRADE_SQL, payload)
                elif kind == "l1":
                    cur.execute(INSERT_L1_SQL, payload)
            except Exception as e:
                log(f"DB write error: {e}")
            finally:
                queue.task_done()
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass

async def socket_loop():
    stop_event = asyncio.Event()

    def _stop(*_):
        stop_event.set()

    try:
        signal.signal(signal.SIGINT, _stop)
        signal.signal(signal.SIGTERM, _stop)
    except Exception:
        pass

    queue: asyncio.Queue = asyncio.Queue(maxsize=50_000)
    writer_task = asyncio.create_task(writer_loop(queue, stop_event))

    backoff = 1
    last_error_log = 0.0

    try:
        while not stop_event.is_set():
            try:
                log(f"Connecting: {WS_URL}")

                # IMPORTANT: read_limit/write_limit removed (older websockets on Windows breaks with them)
                async with websockets.connect(
                    WS_URL,
                    ping_interval=15,
                    ping_timeout=20,
                    close_timeout=5,
                    max_queue=1000,
                ) as ws:
                    log("Connected to Binance perps.")
                    backoff = 1

                    while not stop_event.is_set():
                        try:
                            # Give it a bit more time before considering it "quiet"
                            msg = await asyncio.wait_for(ws.recv(), timeout=60.0)
                        except asyncio.TimeoutError:
                            continue

                        m = json.loads(msg)
                        stream = m.get("stream", "")
                        data = m.get("data", {})

                        if stream.endswith("@trade"):
                            sym = data["s"].lower()
                            price = data.get("p")
                            qty = data.get("q")
                            side = "sell" if data.get("m") else "buy"
                            ts = ms_to_utc(data.get("T", data.get("E")))
                            row = (ts, VENUE, sym, price, qty, side)

                            if not queue.full():
                                await queue.put(("trade", row))

                        elif stream.endswith("@bookTicker"):
                            sym = data["s"].lower()
                            bid_p = data.get("b"); bid_q = data.get("B")
                            ask_p = data.get("a"); ask_q = data.get("A")
                            ts = ms_to_utc(data.get("E"))
                            row = (ts, VENUE, sym, bid_p, bid_q, ask_p, ask_q)

                            if not queue.full():
                                await queue.put(("l1", row))

            except (ConnectionClosed, ConnectionClosedError) as e:
                # common: "no close frame received or sent"
                now = time.monotonic()
                if now - last_error_log > 2.0:
                    log(f"WS closed, reconnecting: {e}")
                    last_error_log = now

            except Exception as e:
                log(f"WS error, reconnecting: {e}")

            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)

    finally:
        stop_event.set()
        try:
            await asyncio.wait_for(queue.join(), timeout=5.0)
        except Exception:
            pass
        writer_task.cancel()

if __name__ == "__main__":
    try:
        asyncio.run(socket_loop())
    except KeyboardInterrupt:
        print("Stopped by user")
