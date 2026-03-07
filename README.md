# Polymarket Orderbook Collector & Dashboard

Collects, historises, and visualises Polymarket CLOB orderbook data.
Emits **5-minute** and **daily** snapshots, persists to SQLite (hot) and Parquet (cold), and exposes a real-time Dash dashboard backed by a FastAPI REST API.

---

## Architecture

```
polymarket-collector/
├── collector/
│   ├── clob_client.py        # aiohttp wrapper for Polymarket CLOB REST
│   ├── orderbook_stream.py   # WebSocket listener — maintains in-memory orderbook
│   ├── snapshot_5m.py        # 5-minute window aggregation
│   ├── snapshot_daily.py     # daily aggregation from 5m snapshots
│   └── persistence.py        # batched writer (SQLite + Parquet)
├── storage/
│   ├── db.py                 # SQLAlchemy Core schema + query helpers
│   └── parquet_store.py      # PyArrow Parquet cold archive
├── dashboard/
│   ├── app.py                # FastAPI + Dash (Flask) mounted together
│   ├── layouts/              # per-page Dash layouts
│   └── callbacks.py          # all Dash callbacks
├── config.py                 # Pydantic Settings
├── state.py                  # in-memory shared state singleton
├── main.py                   # single asyncio entrypoint
└── tests/
    └── test_snapshot_5m.py
```

---

## Installation

```bash
python -m venv .venv
# Windows
.venv\Scripts\activate
# Linux / macOS
source .venv/bin/activate

pip install -r requirements.txt
```

---

## Configuration

Copy `.env.example` to `.env` and edit:

```ini
# .env
CLOB_API_URL=https://clob.polymarket.com
CLOB_WS_URL=wss://ws-subscriptions-clob.polymarket.com/ws/market

# Comma-separated token IDs to track from startup (optional)
MARKETS=["0xabc...","0xdef..."]

DB_PATH=storage/poly.db
PARQUET_DIR=storage/parquet
SNAPSHOT_5M_INTERVAL=300
TICK_BATCH_SIZE=100
TICK_FLUSH_INTERVAL=5.0
DASHBOARD_PORT=8050
LOG_LEVEL=INFO
```

`MARKETS` accepts a JSON array of Polymarket **token IDs** (the 66-char hex strings that identify YES/NO outcome tokens in the CLOB).
Additional markets can be added at runtime via the REST API without restarting.

---

## Running

```bash
python main.py
```

The dashboard is available at **http://localhost:8050/dash/**.

---

## Dashboard

| URL | Description |
|-----|-------------|
| `/dash/orderbook` | Live bid/ask ladder (500 ms refresh) |
| `/dash/timeseries` | 5m mid-price & spread over 1h / 6h / 24h / 7d |
| `/dash/markets` | Tracked markets table + add-market widget |

---

## REST API

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/health` | Liveness check |
| `GET` | `/api/markets` | List tracked markets |
| `POST` | `/api/markets/add` | `{"market_id": "<token_id>"}` |
| `GET` | `/api/snapshots/5m` | `?market_id=&from_ts=&to_ts=` |
| `GET` | `/api/snapshots/daily` | `?market_id=&from_ts=&to_ts=` |
| `GET` | `/api/orderbook/live` | `?market_id=` — current in-memory orderbook |

### Examples

```bash
# Health
curl http://localhost:8050/api/health

# Add a market
curl -X POST http://localhost:8050/api/markets/add \
     -H "Content-Type: application/json" \
     -d '{"market_id": "0xabc123..."}'

# Fetch last hour of 5m snapshots
NOW=$(date +%s)
curl "http://localhost:8050/api/snapshots/5m?market_id=0xabc123...&from_ts=$((NOW-3600))&to_ts=$NOW"
```

---

## Storage

**SQLite** (`storage/poly.db`) — hot data, last ~30 days:
- `ticks` — raw best-bid/ask per WebSocket update
- `snapshots_5m` — 5-minute OHLC + depth metrics
- `snapshots_daily` — daily OHLC + VWAP
- `tracked_markets` — persisted market registry

**Parquet** (`storage/parquet/<market_id>/<YYYY-MM>/<date>_5m.parquet`) — cold archive written daily at 00:05 UTC, compressed with Snappy.

---

## Tests

```bash
pytest tests/ -v
```

Three test cases in `tests/test_snapshot_5m.py`:
1. **Empty window** — `finalize()` returns `None`
2. **Normal window** — correct OHLC, spread, depth, imbalance
3. **Abnormal spread (>50%)** — depth within 1% of mid is correctly zero

---

## Data Flow

```
WebSocket  ──► OrderBookAggregator ──► tick_queue
                                           │
                                     process_ticks()
                                      ├── PersistenceWriter.write_tick()
                                      └── Snapshot5MAggregator
                                               │
                                         snapshot_5m_queue
                                               │
                                        process_5m()
                                         ├── PersistenceWriter.write_snapshot_5m()
                                         └── SnapshotDailyAggregator
                                                  │
                                            snapshot_daily_queue
                                                  │
                                           process_daily()
                                            └── PersistenceWriter.write_snapshot_daily()
```
