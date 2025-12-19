# Perps Ops Control Tower (MVP)

An operations control tower for perpetual futures markets.

This repository ingests live perps market data from multiple venues, stores it in Postgres, computes 1-minute market health features, and exposes everything through Metabase dashboards for operational monitoring.

Built as a practical MVP to demonstrate operational depth: data reliability, market health monitoring, and early risk monitoring foundations.

---

## What this MVP does

### 1) Live market data ingestion

- Connects to **Binance Perps** via WebSocket  
- Connects to **Hyperliquid** via WebSocket (Hyperliquid connector is an early integration and demonstrates multi-venue ingestion and schema alignment. Some feeds may disconnect periodically due to upstream idle policies.)

Ingests:
- Trades
- L1 best bid / ask snapshots

All raw market data is written into Postgres for durability, replay, and downstream analysis.

---

### 2) Feature computation (1-minute resolution)

- Aggregates raw L1 snapshots into **one row per minute per symbol**
- Computes core market health features:
  - Bid-ask spread (bps)
  - Order book depth proxy (L1 size)
  - Buy / sell imbalance proxy
  - Data freshness / latency proxy

These features form the base layer for monitoring market quality and operational health.

---

### 3) BI dashboards (Metabase)

Dashboards are designed for **ops workflows**, not trading UIs:

- **Ops Overview**
- **Market Health**
- **Risk Monitor (MVP)**

---

## Architecture

High-level data flow:

- `connectors/binance.py`  
  → `ops.raw_trades`  
  → `ops.raw_book_l1`

- `connectors/hyperliquid.py`  
  → `ops.raw_trades`  
  → `ops.raw_book_l1`

- `features/compute_1m.py`  
  → `ops.features_1m`

- Metabase  
  → dashboards over `ops` schema tables

All components are **loosely coupled** and can be run independently.

---

**Metabase dashboards included in this MVP:** **[Open dashboard](http://localhost:3000/public/dashboard/fab979f3-9f69-448a-94d8-76b6eea02fc0)**

### Ops Overview
High-level ingestion health, venue and symbol coverage, and data freshness.

### Market Health
Bid-ask spread behavior, imbalance, depth proxies, and volume trends.

### Risk Monitor (MVP)
Early risk indicators derived from market health signals.


---

## Data model

### Key tables

- `ops.raw_trades`  
  Raw per-trade events from venues

- `ops.raw_book_l1`  
  Best bid / ask snapshots

- `ops.features_1m`  
  One-minute aggregated market health features

- `ops.risk_events`  
  Risk event log (schema implemented, population in progress)

- `ops.risk_scores`  
  Rolling risk scores (schema implemented, population in progress)

---

## How to run locally

### Prerequisites

- Docker Desktop
- Python 3.10+
- Git

---

### 1) Start Postgres and Metabase

From the repo root:

```bash
docker compose up -d
```

This starts:

- Postgres
- Metabase (connected to Postgres)

---

### 2) Run data connectors

**Binance Perps**

```bash
python connectors/binance.py
```

**Hyperliquid (early integration)**

```bash
python connectors/hyperliquid.py
```

---

### 3) Run feature computation

```bash
python features/compute_1m.py
```

---

### 4) Open Metabase

- Open browser at: http://localhost:3000
- Connect Metabase to the Postgres database
- Open dashboards under the `ops` schema

---

## Current state and roadmap

This repository represents an MVP.

### Implemented

- Multi-venue perps ingestion
- Durable raw market data storage
- Market health feature computation
- Ops-focused BI dashboards

### In progress

- Extended venue coverage
- Alerting and escalation logic

---

## Why this project

This project was built to demonstrate:

- Operational thinking for perps markets
- Data reliability and observability
- Practical market health monitoring
- Foundations for real-time risk systems

The system is intentionally simple, explicit, and inspectable, with clear extension paths toward production-grade risk and alerting infrastructure.
