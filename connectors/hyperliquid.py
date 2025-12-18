import asyncio
import json
import os
import signal
from datetime import datetime, timezone

import psycopg2
import websockets
from websockets.exceptions import ConnectionClosed, ConnectionClosedError

PG_DSN = os.environ.get("PG_DSN", "dbname=perps user=perps password=perps host=localhost port=5432")

# Hyperliquid uses coin symbols like "BTC", "ETH"
COINS = os.environ.get("HL_COINS", "BTC,ETH").split(",")
COINS = [c.strip().upper() for c in COINS if c.strip()]
VENUE = "hyperliquid_perps"

WS_URL = os.environ.get("HL_WS_URL", "wss://api.hyperliquid.xyz/ws")

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

def best_bid_ask(levels):
    """
    levels: [bids[], asks[]]
    each level: { px: str, sz: str, n: number }
    We compute best bid as max(px), best ask as min(px).
    """
    if not levels or len(levels) != 2:
        return None

    bids = levels[0] or []
    asks = levels[1] or []

    best_bid = None
    for lv in bids:
        try:
            px = float(lv.get("px"))
            sz = float(lv.get("sz"))
        except Exception:
            continue
        if best_bid is None or px > best_bid[0]:
            best_bid = (px, sz)

    best_ask = None
    for lv in asks:
        try:
            px = float(lv.get("px"))
            sz = float(lv.get("sz"))
        except Exception:
            continue
        if best_ask is None or px < best_ask[0]:
            best_ask = (px, sz)

    if best_bid is None or best_ask is None:
        return None

    return best_bid[0], best_bid[1], best_ask[0], best_ask[1]

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

async def subscribe(ws):
    # Hyperliquid subscription messages:
    # { "type": "trades", "coin": "<coin_symbol>" }
    # { "type": "l2Book", "coin": "<coin_symbol>" }
    # :contentReference[oaicite:2]{index=2}
    for coin in COINS:
        await ws.send(json.dumps({"type": "trades", "coin": coin}))
        await ws.send(json.dumps({"type": "l2Book", "coin": coin}))
    log(f"Subscribed to Hyperliquid: {', '.join(COINS)}")

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

    try:
        while not stop_event.is_set():
            try:
                log(f"Connecting: {WS_URL}")

                # IMPORTANT:
                # Do NOT pass read_limit/write_limit here (your earlier error),
                # keep connect args minimal so it works across websockets versions.
                async with websockets.connect(
                    WS_URL,
                    ping_interval=15,
                    ping_timeout=10,
                    close_timeout=5,
                    max_queue=1000,
                ) as ws:
                    log("Connected to Hyperliquid.")
                    backoff = 1

                    await subscribe(ws)

                    while not stop_event.is_set():
                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=30.0)
                        except asyncio.TimeoutError:
                            continue

                        m = json.loads(msg)
                        channel = m.get("channel")
                        data = m.get("data")

                        # trades: data is WsTrade[]
                        # WsTrade fields include: coin, side, px, sz, time (ms) :contentReference[oaicite:3]{index=3}
                        if channel == "trades" and isinstance(data, list):
                            for t in data:
                                coin = t.get("coin")
                                side = (t.get("side") or "").lower()
                                px = t.get("px")
                                sz = t.get("sz")
                                ts = ms_to_utc(t.get("time"))
                                symbol = (coin or "").lower()

                                if coin and px and sz and side:
                                    if not queue.full():
                                        await queue.put(("trade", (ts, VENUE, symbol, px, sz, side)))

                        # l2Book: data is WsBook { coin, levels, time } :contentReference[oaicite:4]{index=4}
                        elif channel == "l2Book" and isinstance(data, dict):
                            coin = data.get("coin")
                            levels = data.get("levels")
                            t_ms = data.get("time")
                            symbol = (coin or "").lower()

                            bba = best_bid_ask(levels)
                            if coin and t_ms and bba:
                                bid_p, bid_s, ask_p, ask_s = bba
                                ts = ms_to_utc(t_ms)
                                if not queue.full():
                                    await queue.put(("l1", (ts, VENUE, symbol, bid_p, bid_s, ask_p, ask_s)))

            except (ConnectionClosed, ConnectionClosedError) as e:
                log(f"WS closed, reconnecting: {e}")
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
