# risk/engine.py

import os
import time
import json
from datetime import datetime, timezone, timedelta

import psycopg2
import yaml

PG_DSN = os.environ.get(
    "PG_DSN",
    "dbname=perps user=perps password=perps host=localhost port=5432",
)
RULES_PATH = os.environ.get("RISK_RULES_PATH", "risk/rules.yaml")
SLEEP_SECONDS = int(os.environ.get("RISK_LOOP_SECONDS", "30"))

OPS_SCHEMA = os.environ.get("OPS_SCHEMA", "ops")
INJECT_TEST = os.environ.get("RISK_INJECT_TEST", "0") == "1"

# --------- DDL (idempotent) ---------
DDL_SCHEMA = f"CREATE SCHEMA IF NOT EXISTS {OPS_SCHEMA};"

DDL_RISK_EVENTS = f"""
CREATE TABLE IF NOT EXISTS {OPS_SCHEMA}.risk_events (
  id          BIGSERIAL PRIMARY KEY,
  ts          TIMESTAMPTZ NOT NULL,
  venue       TEXT NOT NULL,
  symbol      TEXT NOT NULL,
  rule_id     TEXT NOT NULL,
  severity    TEXT NOT NULL,
  metric      TEXT NOT NULL,
  value       DOUBLE PRECISION,
  threshold   DOUBLE PRECISION,
  message     TEXT,
  actions     JSONB
);
"""

DDL_INDEXES = [
    f"CREATE INDEX IF NOT EXISTS idx_risk_events_ts ON {OPS_SCHEMA}.risk_events(ts);",
    f"CREATE INDEX IF NOT EXISTS idx_risk_events_vs ON {OPS_SCHEMA}.risk_events(venue, symbol);",
    f"CREATE INDEX IF NOT EXISTS idx_risk_events_rule ON {OPS_SCHEMA}.risk_events(rule_id);",
]

QUERY_FEATURES = f"""
WITH recent AS (
  SELECT
    venue, symbol, ts,
    spread_bps, depth_10bps_bid, depth_10bps_ask, imbalance, freshness_ms,
    ROW_NUMBER() OVER (PARTITION BY venue, symbol ORDER BY ts DESC) AS rn
  FROM {OPS_SCHEMA}.features_1m
  WHERE ts >= %s
)
SELECT venue, symbol, ts, spread_bps, depth_10bps_bid, depth_10bps_ask, imbalance, freshness_ms
FROM recent
WHERE rn <= %s
ORDER BY venue, symbol, ts DESC;
"""

_last_fired = {}  # (rule_id, venue, symbol) -> datetime

def log(msg: str):
    now = datetime.now().strftime("%H:%M:%S")
    print(f"[{now}] {msg}", flush=True)

def ensure_schema(cur):
    cur.execute(DDL_SCHEMA)
    cur.execute(DDL_RISK_EVENTS)
    for stmt in DDL_INDEXES:
        cur.execute(stmt)

def inject_manual_test_event(conn):
    cur = conn.cursor()
    ensure_schema(cur)

    cur.execute(
        f"""
        INSERT INTO {OPS_SCHEMA}.risk_events
          (ts, venue, symbol, rule_id, severity, metric, value, threshold, message, actions)
        VALUES
          (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
        """,
        (
            datetime.now(timezone.utc),
            "binance_perps",
            "btcusdt",
            "manual_test_event",
            "critical",
            "manual",
            999.0,
            100.0,
            "Manual test injection to validate pipeline",
            json.dumps([{"action": "none", "note": "test event"}]),
        ),
    )
    conn.commit()
    log("Inserted manual_test_event into ops.risk_events")

def load_rules(path: str):
    with open(path, "r", encoding="utf-8") as f:
        doc = yaml.safe_load(f) or {}
    defaults = doc.get("defaults", {}) or {}
    rules = doc.get("rules", []) or []
    return defaults, rules

def match_scope(rule: dict, venue: str, symbol: str) -> bool:
    scope = rule.get("scope") or {}
    if "venue" in scope and scope["venue"] != venue:
        return False
    if "symbol" in scope and scope["symbol"] != symbol:
        return False
    return True

def op_eval(op: str, value, threshold: float, prev_value=None) -> bool:
    if value is None:
        return False
    if op == ">":   return value >  threshold
    if op == ">=":  return value >= threshold
    if op == "<":   return value <  threshold
    if op == "<=":  return value <= threshold
    if op == "abs>": return abs(value) > threshold
    if op == "pct_change>":
        if prev_value in (None, 0):
            return False
        pct = abs((value - prev_value) / prev_value) * 100.0
        return pct > threshold
    return False

def cooloff_ok(rule_id: str, venue: str, symbol: str, minutes: int) -> bool:
    key = (rule_id, venue, symbol)
    last = _last_fired.get(key)
    if not last:
        return True
    return (datetime.now(timezone.utc) - last) >= timedelta(minutes=minutes)

def mark_fired(rule_id: str, venue: str, symbol: str):
    _last_fired[(rule_id, venue, symbol)] = datetime.now(timezone.utc)

def series_from_rows(rows):
    groups = {}
    for r in rows:
        venue, symbol, ts, spread_bps, d_bid, d_ask, imbalance, freshness_ms = r
        groups.setdefault((venue, symbol), []).append({
            "venue": venue,
            "symbol": symbol,
            "ts": ts,
            "spread_bps": spread_bps,
            "depth_10bps_bid": d_bid,
            "depth_10bps_ask": d_ask,
            "imbalance": imbalance,
            "freshness_ms": freshness_ms,
        })
    return groups

def evaluate_once(conn, defaults, rules):
    lookback_minutes = int(defaults.get("lookback_minutes", 5))
    min_rows = int(defaults.get("min_rows", 2))
    cooloff_minutes = int(defaults.get("cooloff_minutes", 3))

    since = datetime.now(timezone.utc) - timedelta(minutes=lookback_minutes)

    cur = conn.cursor()
    ensure_schema(cur)

    cur.execute(QUERY_FEATURES, (since, min_rows))
    rows = cur.fetchall()
    groups = series_from_rows(rows)

    fired_count = 0
    for (venue, symbol), arr in groups.items():
        head = arr[0]
        prev = arr[1] if len(arr) >= 2 else None

        for rule in rules:
            if not match_scope(rule, venue, symbol):
                continue

            rule_id = rule["id"]
            metric = rule["metric"]
            op = rule["op"]
            threshold = float(rule["threshold"])
            severity = rule.get("severity", "warn")
            desc = rule.get("desc", rule_id)
            actions = rule.get("actions", [])

            value = head.get(metric)
            prev_value = prev.get(metric) if prev else None

            if not cooloff_ok(rule_id, venue, symbol, cooloff_minutes):
                continue

            if op_eval(op, value, threshold, prev_value):
                msg = f"{desc} | {metric}={value} thr={threshold} | {venue}:{symbol}"
                cur.execute(
                    f"""
                    INSERT INTO {OPS_SCHEMA}.risk_events
                      (ts, venue, symbol, rule_id, severity, metric, value, threshold, message, actions)
                    VALUES
                      (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
                    """,
                    (
                        datetime.now(timezone.utc),
                        venue, symbol,
                        rule_id, severity, metric,
                        float(value) if value is not None else None,
                        threshold,
                        msg,
                        json.dumps(actions),
                    ),
                )
                conn.commit()
                mark_fired(rule_id, venue, symbol)
                fired_count += 1
                log(f"FIRED  {msg}")

    log(f"Checked {len(groups)} markets; fired {fired_count} events.")

def main():
    log("Risk engine starting…")
    try:
        conn = psycopg2.connect(PG_DSN)
        conn.autocommit = False
    except Exception as e:
        log(f"DB connect failed: {e}")
        return

    try:
        if INJECT_TEST:
            inject_manual_test_event(conn)

        while True:
            try:
                defaults, rules = load_rules(RULES_PATH)
                if not rules:
                    log("No rules found; sleeping…")
                else:
                    evaluate_once(conn, defaults, rules)
            except psycopg2.Error as db_err:
                log(f"DB error: {db_err}")
                try:
                    conn.rollback()
                    log("Transaction rolled back.")
                except Exception:
                    pass
            except FileNotFoundError:
                log(f"Rules file not found at {RULES_PATH}")
            except Exception as e:
                log(f"Engine error: {e}")
                try:
                    conn.rollback()
                except Exception:
                    pass

            time.sleep(SLEEP_SECONDS)
    finally:
        try:
            conn.close()
        except Exception:
            pass
        log("Risk engine stopped.")

if __name__ == "__main__":
    main()
