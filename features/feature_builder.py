import os
import time
from datetime import datetime, timezone, timedelta

import psycopg2

PG_DSN = os.environ.get(
    "PG_DSN",
    "dbname=perps user=perps password=perps host=localhost port=5432",
)

FETCH_LATEST_BOOK = """
SELECT DISTINCT ON (venue, symbol)
    venue,
    symbol,
    ts,
    bid_price,
    bid_size,
    ask_price,
    ask_size
FROM raw_book_l1
ORDER BY venue, symbol, ts DESC;
"""

INSERT_FEATURE = """
INSERT INTO features_1m (
    ts,
    venue,
    symbol,
    spread_bps,
    depth_10bps_bid,
    depth_10bps_ask,
    imbalance,
    freshness_ms
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
"""

def compute_features(row):
    venue, symbol, ts, bid_p, bid_q, ask_p, ask_q = row

    # Safety checks
    if not bid_p or not ask_p or bid_p <= 0 or ask_p <= 0:
        return None

    mid = (bid_p + ask_p) / 2
    spread_bps = (ask_p - bid_p) / mid * 10_000

    # Imbalance between bid and ask size
    denom = (bid_q + ask_q)
    imbalance = (bid_q - ask_q) / denom if denom > 0 else 0

    freshness_ms = int((datetime.now(timezone.utc) - ts).total_seconds() * 1000)

    # Depth at 10bps not implemented yet → placeholder
    depth_10bps_bid = bid_q
    depth_10bps_ask = ask_q

    return (
        datetime.now(timezone.utc),
        venue,
        symbol,
        spread_bps,
        depth_10bps_bid,
        depth_10bps_ask,
        imbalance,
        freshness_ms,
    )

def run_once():
    conn = psycopg2.connect(PG_DSN)
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute(FETCH_LATEST_BOOK)
    rows = cur.fetchall()

    inserted = 0
    for r in rows:
        feat = compute_features(r)
        if feat:
            cur.execute(INSERT_FEATURE, feat)
            inserted += 1

    print(f"Inserted {inserted} feature rows")

def main():
    print("Starting feature builder (1m cadence)")
    while True:
        try:
            run_once()
        except Exception as e:
            print("Feature error:", e)
        time.sleep(60)

if __name__ == "__main__":
    main()
