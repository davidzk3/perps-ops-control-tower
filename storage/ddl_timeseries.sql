CREATE TABLE IF NOT EXISTS raw_trades (
    ts TIMESTAMPTZ NOT NULL,
    venue TEXT NOT NULL,
    symbol TEXT NOT NULL,
    price NUMERIC,
    size NUMERIC,
    side TEXT
);

CREATE TABLE IF NOT EXISTS raw_book_l1 (
    ts TIMESTAMPTZ NOT NULL,
    venue TEXT NOT NULL,
    symbol TEXT NOT NULL,
    bid_price NUMERIC,
    bid_size NUMERIC,
    ask_price NUMERIC,
    ask_size NUMERIC
);

CREATE TABLE IF NOT EXISTS features_1m (
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

CREATE TABLE IF NOT EXISTS risk_events (
    id BIGSERIAL PRIMARY KEY,
    ts TIMESTAMPTZ NOT NULL,
    venue TEXT NOT NULL,
    symbol TEXT NOT NULL,
    rule TEXT NOT NULL,
    value NUMERIC,
    threshold NUMERIC,
    severity TEXT,
    context_json JSONB
);

CREATE TABLE IF NOT EXISTS risk_scores (
    id BIGSERIAL PRIMARY KEY,
    ts TIMESTAMPTZ NOT NULL,
    venue TEXT NOT NULL,
    symbol TEXT NOT NULL,
    liq_score INTEGER,
    vol_score INTEGER,
    infra_score INTEGER,
    composite INTEGER
);
