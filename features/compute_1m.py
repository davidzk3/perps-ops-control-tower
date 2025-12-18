# features/compute_1m.py
# Minimal 1 minute feature builder from L1 book snapshots.
# Writes into ops.features_1m and also creates public.features_1m view for compatibility.
#
# Fixes:
# - Dedupes existing rows so unique index creation never fails
# - Creates unique index once (idempotent) after dedupe
# - Keeps UPSERT logic so re-computation updates instead of duplicating

import os
import time
from datetime import datetime, timezone, timedelta

import psycopg2

PG_DSN = os.environ.get(
    "PG_DSN",
    "dbname=perps user=perps password=perps host=localhost port=5432",
)

LOOP_SECONDS = int(os.environ.get("FEATURES_LOOP_SECONDS", "10"))
LOOKBACK_MINUTES = int(os.environ.get("FEATURES_LOOKBACK_MINUTES", "3"))

DDL_SCHEMA_TABLE_VIEW = """
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.features_1m (
  id BIGSERIAL PRIMARY KEY,
  ts TIMESTAMPTZ NOT NULL,
  venue TEXT NOT NULL,
  symbol TEXT NOT NULL,
  spread_bps NUMERIC,
  depth_10bps_bid NUMERIC,
  depth_10bps_ask NUMERIC,
  imbalance NUMERIC,
  freshness_ms INTEGER
);

-- Compatibility view so code that queries unqualified "features_1m" still works
CREATE OR REPLACE VIEW public.features_1m AS
SELECT * FROM ops.features_1m;
"""

# One time cleanup so the unique index can always be created safely
DEDUP_FEATURES_1M = """
DELETE FROM ops.features_1m a
USING ops.features_1m b
WHERE a.ts = b.ts
  AND a.venue = b.venue
  AND a.symbol = b.symbol
  AND a.id < b.id;
"""

CREATE_UNIQUE_INDEX = """
CREATE UNIQUE INDEX IF NOT EXISTS ux_features_1m_ts_venue_symbol
ON ops.features_1m (ts, venue, symbol);
"""

# We compute a single row per minute per (venue,symbol) using the last L1 snapshot in that minute
FEATURES_UPSERT = """
WITH last_l1 AS (
  SELECT DISTINCT ON (venue, symbol)
    venue,
    symbol,
    date_trunc('minute', ts) AS bucket_ts,
    ts AS last_ts,
    bid_price::numeric AS bid_price,
    bid_size::numeric AS bid_size,
    ask_price::numeric AS ask_price,
    ask_size::numeric AS ask_size
  FROM ops.raw_book_l1
  WHERE ts >= %s AND ts < %s
  ORDER BY venue, symbol, ts DESC
)
INSERT INTO ops.features_1m
  (ts, venue, symbol, spread_bps, depth_10bps_bid, depth_10bps_ask, imbalance, freshness_ms)
SELECT
  bucket_ts AS ts,
  venue,
  symbol,
  CASE
    WHEN bid_price > 0 AND ask_price > 0 AND (bid_price + ask_price) > 0
      THEN ((ask_price - bid_price) / ((ask_price + bid_price) / 2.0)) * 10000.0
    ELSE NULL
  END AS spread_bps,

  -- With only L1, treat L1 size as a proxy for "depth within 10 bps"
  bid_size AS depth_10bps_bid,
  ask_size AS depth_10bps_ask,

  CASE
    WHEN (bid_size + ask_size) > 0
      THEN (bid_size - ask_size) / (bid_size + ask_size)
    ELSE NULL
  END AS imbalance,

  EXTRACT(EPOCH FROM (%s::timestamptz - last_ts)) * 1000.0 AS freshness_ms
FROM last_l1
ON CONFLICT (ts, venue, symbol) DO UPDATE
SET
  spread_bps = EXCLUDED.spread_bps,
  depth_10bps_bid = EXCLUDED.depth_10bps_bid,
  depth_10bps_ask = EXCLUDED.depth_10bps_ask,
  imbalance = EXCLUDED.imbalance,
  freshness_ms = EXCLUDED.freshness_ms;
"""

def log(msg: str):
    now = datetime.now().strftime("%H:%M:%S")
    print(f"[{now}] {msg}", flush=True)

def bootstrap(cur):
    # Create schema/table/view first
    cur.execute(DDL_SCHEMA_TABLE_VIEW)

    # Deduplicate any existing data so the unique index can always be created
    cur.execute(DEDUP_FEATURES_1M)

    # Now create the unique index safely
    cur.execute(CREATE_UNIQUE_INDEX)

def main():
    log("Features builder starting")
    conn = psycopg2.connect(PG_DSN)
    conn.autocommit = True

    with conn.cursor() as cur:
        bootstrap(cur)

    while True:
        try:
            now = datetime.now(timezone.utc)

            # We rebuild a small lookback window each loop (safe due to UPSERT)
            end_minute = now.replace(second=0, microsecond=0)
            rebuild_from = end_minute - timedelta(minutes=LOOKBACK_MINUTES)
            rebuild_to = end_minute

            with conn.cursor() as cur:
                t = rebuild_from
                while t < rebuild_to:
                    s = t
                    e = t + timedelta(minutes=1)
                    cur.execute(FEATURES_UPSERT, (s, e, now))
                    t = e

            log(f"Updated features up to minute {(end_minute - timedelta(minutes=1)).isoformat()}")

        except Exception as e:
            log(f"Error: {e}")

        time.sleep(LOOP_SECONDS)

if __name__ == "__main__":
    main()
