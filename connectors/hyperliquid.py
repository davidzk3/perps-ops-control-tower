# connectors/hyperliquid.py
import os
import json
import asyncio
from datetime import datetime, timezone

import psycopg2
import websockets

PG_DSN = os.environ.get(
    "PG_DSN",
    "dbname=perps user=perps password=perps host=localhost port=5432",
)
WS_URL = "wss://api.hyperliquid.xyz/ws"
COINS = ["BTC", "ETH"]
VENUE = "hyperliquid_perps"

DEBUG_LIMIT = 5
debug_insert_count = {"trade": 0, "l1": 0}
book_msg_count = 0
sampled_unparsed = 0

INSERT_TRADE_SQL = """
INSERT INTO ops.raw_trades (ts, venue, symbol, price, size, side)
VALUES (%s, %s, %s, %s, %s, %s);
"""
INSERT_L1_SQL = """
INSERT INTO ops.raw_book_l1 (ts, venue, symbol, bid_price, bid_size, ask_price, ask_size)
VALUES (%s, %s, %s, %s, %s, %s, %s);
"""

def now_utc():
    return datetime.now(timezone.utc)

def symbol_of(coin: str) -> str:
    c = (coin or "").strip().lower()
    return f"{c}usdt" if c else ""

# ---------- robust numeric extractor ----------
def to_num(v):
    """Return float for numbers that may be int/float/str or nested dicts."""
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str):
        try:
            return float(v)
        except Exception:
            return None
    if isinstance(v, dict):
        # common shapes: {'n':'123.4'}, {'v':'0.1'}, {'px':'...'}, {'size':'...'}
        for k in ("n", "v", "value", "px", "price", "sz", "size", "amount"):
            if k in v:
                return to_num(v[k])
        # fall back: first numeric-looking value
        for val in v.values():
            n = to_num(val)
            if n is not None:
                return n
    return None

async def writer_loop(queue: asyncio.Queue):
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
            if debug_insert_count[kind] < DEBUG_LIMIT:
                debug_insert_count[kind] += 1
                print(f"[HL->DB] wrote {kind} #{debug_insert_count[kind]}: {payload}")
        except Exception as e:
            print("DB write error:", e)
        finally:
            queue.task_done()

def subscribe_msgs():
    msgs = []
    for c in COINS:
        msgs.append({"method": "subscribe", "subscription": {"type": "trades", "coin": c}})
        msgs.append({"method": "subscribe", "subscription": {"type": "l2Book", "coin": c}})
    return msgs

def parse_trade_msg(msg: dict):
    data = msg.get("data", {})
    coin = (data.get("coin") or msg.get("coin") or "").strip()
    sym = symbol_of(coin)
    if not sym:
        return []
    out = []
    trades = data.get("trades")
    if isinstance(trades, list):
        for t in trades:
            px = to_num(t.get("px"))
            sz = to_num(t.get("sz"))
            if px is None or sz is None:
                continue
            side = "buy" if t.get("side") in ("B", "Buy", "buy", True) else "sell"
            ts_ms = to_num(t.get("time"))
            ts = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc) if ts_ms else now_utc()
            out.append((ts, VENUE, sym, px, sz, side))
        return out
    if isinstance(data, dict) and "px" in data and "sz" in data:
        px = to_num(data.get("px"))
        sz = to_num(data.get("sz"))
        if px is not None and sz is not None:
            side = "buy" if data.get("side") in ("B", "Buy", "buy", True) else "sell"
            ts_ms = to_num(data.get("time"))
            ts = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc) if ts_ms else now_utc()
            out.append((ts, VENUE, sym, px, sz, side))
    return out

def normalize_side_list(arr):
    """Return two lists (bids, asks) from mixed structures."""
    bids, asks = [], []
    for x in arr:
        if isinstance(x, (list, tuple)):
            px = to_num(x[0]) if len(x) > 0 else None
            sz = to_num(x[1]) if len(x) > 1 else None
            side = x[2] if len(x) > 2 else None
        elif isinstance(x, dict):
            px = to_num(x.get("px") if "px" in x else x.get("price"))
            sz = to_num(x.get("sz") if "sz" in x else x.get("size") or x.get("amount"))
            side = x.get("side")
            if side is None and "isBid" in x:
                side = "bid" if x["isBid"] else "ask"
        else:
            continue
        if px is None or sz is None:
            continue
        s = str(side).lower() if side is not None else ""
        if s.startswith(("bid","b")) or side is True:
            bids.append((px, sz))
        elif s.startswith(("ask","a")) or side is False:
            asks.append((px, sz))
        else:
            # unknown side, put aside; we'll split by price later if needed
            bids.append((px, sz))  # temp; will sort/split below if asks empty
    return bids, asks

def extract_bids_asks_from_data(data: dict):
    # 1) levels as dict {bids:[...], asks:[...]}
    if isinstance(data.get("levels"), dict):
        b1, _ = normalize_side_list(data["levels"].get("bids", []))
        _, a1 = normalize_side_list(data["levels"].get("asks", []))
        return b1, a1
    # 2) top-level bids/asks
    if isinstance(data.get("bids"), list) and isinstance(data.get("asks"), list):
        b1, _ = normalize_side_list(data["bids"])
        _, a1 = normalize_side_list(data["asks"])
        return b1, a1
    # 3) levels as flat list with side flags or just [px,sz,...]
    if isinstance(data.get("levels"), list):
        return normalize_side_list(data["levels"])
    # 4) nested book object
    if isinstance(data.get("book"), dict):
        b1, _ = normalize_side_list(data["book"].get("bids", []))
        _, a1 = normalize_side_list(data["book"].get("asks", []))
        return b1, a1
    return [], []

def parse_l2_to_l1(msg: dict):
    data = msg.get("data", {})
    if not isinstance(data, dict):
        return None
    coin = (data.get("coin") or msg.get("coin") or "").strip()
    sym = symbol_of(coin)
    if not sym:
        return None
    bids, asks = extract_bids_asks_from_data(data)

    # If still missing one side, try naive split by price extremes
    if (not bids or not asks) and (bids or asks):
        flat = sorted((bids + asks), key=lambda t: t[0])
        if not bids and flat:
            bids = [flat[-1]]
        if not asks and flat:
            asks = [flat[0]]

    if not bids or not asks:
        return None

    bids = sorted(bids, key=lambda t: t[0], reverse=True)
    asks = sorted(asks, key=lambda t: t[0])
    bid_p, bid_q = bids[0]
    ask_p, ask_q = asks[0]
    if bid_p is None or ask_p is None or bid_p <= 0 or ask_p <= 0:
        return None
    return (now_utc(), VENUE, sym, float(bid_p), float(bid_q), float(ask_p), float(ask_q))

async def handle_envelope(envelope, queue: asyncio.Queue):
    global book_msg_count, sampled_unparsed
    if isinstance(envelope, list):
        for item in envelope:
            await handle_envelope(item, queue)
        return
    if not isinstance(envelope, dict):
        return

    channel = (
        envelope.get("channel")
        or envelope.get("type")
        or (isinstance(envelope.get("subscription"), dict) and envelope["subscription"].get("type"))
    )
    data = envelope.get("data")

    if isinstance(data, list):
        for item in data:
            await handle_envelope({"channel": channel, "data": item}, queue)
        return

    try:
        if channel and channel.lower().startswith("trade"):
            for r in parse_trade_msg(envelope):
                await queue.put(("trade", r))
        elif channel and ("l2" in channel.lower() or "book" in channel.lower()):
            book_msg_count += 1
            row = parse_l2_to_l1(envelope)
            if row:
                await queue.put(("l1", row))
            elif sampled_unparsed < 3:
                sampled_unparsed += 1
                print("\n[HL DEBUG] Unparsed book sample:")
                print(json.dumps(envelope)[:900], "\n")
    except Exception as e:
        print("Parse error:", e)

async def socket_loop():
    queue = asyncio.Queue()
    writer_task = asyncio.create_task(writer_loop(queue))
    backoff = 1
    try:
        while True:
            try:
                print("Connecting:", WS_URL)
                async with websockets.connect(WS_URL, ping_interval=15, ping_timeout=15) as ws:
                    for m in subscribe_msgs():
                        await ws.send(json.dumps(m))
                    print("Subscribed to Hyperliquid trades and l2Book.")
                    backoff = 1
                    async for raw in ws:
                        try:
                            msg = json.loads(raw)
                        except Exception:
                            continue
                        await handle_envelope(msg, queue)
            except Exception as e:
                print("WS error, reconnecting shortly:", e)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)
    finally:
        writer_task.cancel()

if __name__ == "__main__":
    try:
        asyncio.run(socket_loop())
    except KeyboardInterrupt:
        print("Stopped by user")
