# duckport-rs

[中文](README.md) | English

Wraps DuckDB as a **gRPC database service**, providing multi-process read/write access via the Arrow Flight protocol.

- **Read plane**: Implements the Airport protocol via [`airport-rs`](./crates/airport/README.md). DuckDB clients can directly `ATTACH 'grpc://...' AS dp (TYPE AIRPORT)` and run arbitrary SELECT queries.
- **Write plane**: Custom `duckport.*`-prefixed Flight DoAction + DoPut RPCs, providing explicit transactions and bulk Arrow append on top of DuckDB's single-writer model.

The core motivation is to free DuckDB from the "same-process embedding" constraint, so it can serve as a lightweight data service shared by multiple upstream/downstream processes.

---

## Architecture Overview

```
┌─────────────────┐        ┌─────────────────┐        ┌────────────────┐
│ binance-ingestor│        │  consumer client│        │ DuckDB CLI /   │
│ (Python)        │        │  (Python)       │        │ DBeaver, etc.  │
│  write + read   │        │  read           │        │  read (Airport) │
└────────┬────────┘        └────────┬────────┘        └────────┬───────┘
         │DoPut/DoAction            │DoGet                     │Airport
         │ (duckport.*)             │(duckport.query)          │ (native)
         └──────────────┬───────────┴──────────────────────────┘
                        │ Arrow Flight (gRPC)
                        ▼
              ┌──────────────────────────────────────┐
              │        duckport-server (Rust)         │
              │                                       │
              │  ┌────────────────┐  ┌─────────────┐ │
              │  │ DuckportService│  │  Airport    │ │
              │  │  (write plane) │──▶  (read      │ │
              │  │ intercept      │  │  plane)     │ │
              │  │ duckport.*     │  │             │ │
              │  └───────┬────────┘  └──────┬──────┘ │
              │          │                  │        │
              │  ┌───────▼──────────────────▼──────┐ │
              │  │   Backend                       │ │
              │  │   ├─ writer (Mutex<Connection>) │ │
              │  │   └─ reader pool (r2d2)         │ │
              │  └───────┬─────────────────────────┘ │
              │          │ duckdb FFI                │
              │          ▼                           │
              │      duckport.db (DuckDB 1.5.6)      │
              └──────────────────────────────────────┘
```

Key design decisions:

- **Single DuckDB instance**: `Backend::open` opens the DB file once; both writer and readers share the same `Database` instance via `try_clone()`, so writer commits are immediately visible to readers.
- **Serialized single-writer**: The writer connection is wrapped in a `tokio::sync::Mutex`; all write RPCs execute FIFO, conforming to DuckDB's single-writer model.
- **Concurrent reads**: r2d2 connection pool; all pool connections are `try_clone` of the writer, sharing MVCC state.
- **Catalog epoch**: After any write RPC, `bump_catalog_epoch()` is called so Airport clients can refresh their schema cache.
- **System metadata tables**: `duckport-server` creates and migrates `data.config_dict` and `data.watermark` on startup; ingestors only upsert the watermark rows for their own business tables.

See `extra-enhancement.md` for the concurrency model decision record.

---

## Project Structure

```
duckport-rs/
├── Cargo.toml                    # cargo workspace root
├── crates/
│   ├── airport/                  # airport-rs (vendored fork, Arrow 58)
│   │   ├── src/                  # Generic server/catalog trait implementation for Airport protocol
│   │   └── README.md             # Original library docs
│   └── duckport-server/          # duckport server binary
│       └── src/
│           ├── main.rs           # Entry: load config → build Backend → start tonic
│           ├── config.rs         # All DUCKPORT_* env var parsing
│           ├── backend/mod.rs    # DuckDB connection pool + writer Mutex + catalog epoch
│           ├── airport_adapter/  # DuckDB implementation of airport traits
│           │   ├── catalog.rs    # duckdb_schemas() → airport::Catalog
│           │   ├── schema.rs     # duckdb_tables() → airport::Schema
│           │   └── table.rs      # Table scan + Arrow IPC encoding
│           └── write_plane/      # Custom duckport.* RPCs
│               ├── mod.rs        # FlightService intercept/forward logic
│               ├── actions.rs    # DoAction: ping / execute / execute_transaction
│               ├── put.rs        # DoPut:   duckport.append (bulk Arrow)
│               ├── query.rs      # DoGet:   duckport.query (arbitrary read-only SQL)
│               └── proto.rs      # JSON request/response types
│
├── ingestor/                     # Plugin registry (not Python source)
│   └── registry.json             # Installable plugin list (name → repo URL + exec)
│
├── duckport                      # Service management CLI (available after install)
│
├── install.sh                    # One-click bootstrap script (curl | bash)
├── deploy.sh                     # Server-side setup (binary + systemd + CLI registration)
│
├── client/                       # Python consumer package duckport-consumer
│   ├── pyproject.toml
│   └── duckport_consumer/
│       ├── client.py             # DuckportConsumer (query/get_market/get_symbol)
│       └── resample.py           # K-line resample SQL builder
│
├── tests/python/                 # End-to-end integration tests (Rust server + Python client)
│   ├── test_phase1_airport_read.py
│   ├── test_phase2a_write_plane.py
│   ├── test_phase2b_append.py
│   ├── test_phase4_ingestor.py
│   ├── test_loadhist.py
│   └── test_consumer_read.py
│
├── roadmap.md                    # Phase progress + TODOs
├── deploy.md                     # Production deployment + operations manual
├── migration-guide.md            # Guide for migrating data from the old Python duckport
├── extra-enhancement.md          # Concurrency model D1a/D1b decision
└── path-b-retention-plan.md      # Archived architecture refactor plan (Path B)
```

### Plugin Repositories (standalone git projects)

Data ingestion plugins are maintained in separate repositories and installed via `duckport install`:

| Plugin | Repository | Description |
|--------|-----------|-------------|
| `binance-ingestor` | [duckport-binance-ingestor](https://github.com/sjmc-quant/duckport-binance-ingestor) | Binance Spot + USDT-Perp K-line ingestion |

---

## Core Flight RPC Interface

All interfaces are exposed via standard Arrow Flight gRPC, default port `50051`.

### 1. DoAction — Control / Write Operations

| Action Type | Request Body (JSON) | Response Body (JSON) | Description |
|-------------|--------------------|--------------------|-------------|
| `duckport.ping` | `{}` | `{server, server_version, catalog, duckdb_version}` | Health + version probe |
| `duckport.execute` | `{"sql": "..."}` | `{"rows_affected": N}` | Single-statement DDL/DML |
| `duckport.execute_transaction` | `{"statements": ["...", "..."]}` | `{"rows_affected": [N1, N2, ...]}` | Multi-statement atomic execution (BEGIN/COMMIT; auto ROLLBACK on failure) |
| `duckport.watch_watermark` | `{}` | Endless stream of `{"watermarks": [{table_name, duck_time}, ...]}` | Subscribe to `data.watermark`, see below |

`duckport.watch_watermark` replaces polling the watermark. The stream starts with all current watermarks and sends them again whenever the `duck_time` of any table changes. After 15 seconds without a change the current state is repeated, so a client can tell the connection is alive. `duck_time` is UTC, formatted `2026-01-01T00:05:00.000000`, or `null` when unset.

- A message is sent after the write transaction commits, so the data is already readable.
- The stream carries state, not an event queue: a slow reader gets only the latest values and a reconnect starts with the current ones, so nothing is missed.
- It holds no read connection. At most 256 streams are open at once. Streams end when the server stops.
- Loopback and LAN only (public peers may only call `duckport.ping`).

SQL parameterization is not yet supported (planned for Phase 2c); clients must interpolate literals themselves.

### 2. DoPut — Bulk Arrow Append

- **Descriptor path**: `["duckport.append", <schema>, <table>]`
- **Body**: Standard Arrow IPC stream (first message is schema, subsequent messages are RecordBatches)
- **Semantics**: The entire stream is appended atomically within a single `BEGIN ... COMMIT`; any error triggers a full ROLLBACK
- **Response**: `PutResult.app_metadata` is JSON `{schema, table, batches, rows_appended}`

For large-scale data, clients should split into chunks (see `duckport_client.py:bulk_write_kline`, default 200K rows/chunk).

### 3. DoGet — Query

Two ticket formats:

**a. Airport native table scan** (handled by airport-rs)

```json
{"schema": "data", "table": "usdt_perp_5m"}
```

Used automatically by the DuckDB airport client or `airport-rs` client library.

**b. Custom `duckport.query`** (consumer use case)

```json
{"type": "duckport.query", "sql": "SELECT ... FROM ..."}
```

Only `SELECT` / `WITH` / `EXPLAIN` prefix statements are allowed; `query.rs` enforces this prefix check server-side. Results are streamed back as Arrow IPC.

---

## Quick Start

### One-Click Deploy (Linux Server)

```bash
curl -fsSL https://raw.githubusercontent.com/sjmc-quant/duckport-rs/main/install.sh | bash
```

`install.sh` clones the repo and runs `deploy.sh`, which: downloads/compiles the binary, writes `server.env`, registers the systemd unit, and installs the `duckport` CLI.

After installation, manage the service via CLI:

```bash
duckport list                       # List installable plugins
duckport install binance_ingestor   # Install Binance data plugin (interactive)
duckport start                      # Start duckport-server
duckport start all                  # Start duckport-server + all installed ingestors
duckport status                     # Show service status and data watermarks
duckport config binance-5m          # Edit instance config file
duckport logs binance-5m            # Tail logs in real time
duckport loadhist binance-5m        # Backfill historical data
duckport retention set usdt_perp_1m 120   # Keep only the last 120 days (needs DUCKPORT_RETENTION_ENABLED=true)
duckport retention status           # Retention policies and the latest run
sudo duckport doctor                # read-only: compare database, system tables and plugin versions with this release's desired state (--deep, --json)
sudo duckport upgrade               # upgrade server -> plugins -> data alignment (--plan to preview, --proxy for network); see upgrade.md
```

### Local Development

```bash
cargo build --release    # Produces target/release/duckport-server
```

First build takes ~5 minutes (DuckDB bundled feature compiles from source).

```bash
export DUCKPORT_DB_PATH=./duckport.db
export DUCKPORT_LISTEN_ADDR=0.0.0.0:50051
export RUST_LOG=duckport_server=info,airport=info

./target/release/duckport-server
```

### Health Check

```python
import pyarrow.flight as flight, json
client = flight.FlightClient("grpc://localhost:50051")
resp = list(client.do_action(flight.Action("duckport.ping", b"")))
print(json.loads(resp[0].body.to_pybytes()))
# {'server': 'duckport', 'server_version': '1.1.0', 'catalog': 'duckport', 'duckdb_version': 'v1.5.6'}
```

### End-to-End Tests

```bash
cd tests/python
pip install pyarrow pandas
pytest test_phase1_airport_read.py test_phase2a_write_plane.py test_phase2b_append.py -v
```

---

## Configuration

### Server (`duckport-server`)

All parameters are passed via environment variables, all prefixed with `DUCKPORT_`.

| Variable | Default | Description |
|----------|---------|-------------|
| `DUCKPORT_DB_PATH` | `./duckport.db` | DuckDB file path; `:memory:` for in-memory mode |
| `DUCKPORT_LISTEN_ADDR` | `0.0.0.0:50051` | Flight/gRPC listen address |
| `DUCKPORT_ADVERTISED_ADDR` | *(empty)* | FlightEndpoint advertised address; **must be explicitly set when `LISTEN_ADDR` uses `0.0.0.0`** — set to a client-reachable IP (e.g. `10.8.20.101:8815`), otherwise Airport clients will attempt to connect to the non-routable `0.0.0.0` |
| `DUCKPORT_CATALOG_NAME` | `duckport` | Catalog name seen by Airport clients when they `ATTACH` |
| `DUCKPORT_READ_POOL_SIZE` | `4` | Read connection pool size |
| `DUCKPORT_DUCKDB_THREADS` | `0` (DuckDB default) | DuckDB `threads` PRAGMA |
| `DUCKPORT_DUCKDB_MEMORY_LIMIT` | *(empty, DuckDB default)* | DuckDB `memory_limit` PRAGMA, e.g. `"2GB"` |
| `DUCKPORT_RETENTION_ENABLED` | `false` | Start the rolling-window retention; per-table rules live in `data.retention_policy` (`duckport retention set`) |
| `DUCKPORT_RETENTION_DRY_RUN` | `true` | Count rows only, never delete; switch to `false` after checking `duckport retention log`. Only true/false are accepted; an empty or misspelled value stops the server from starting |
| `DUCKPORT_RETENTION_INTERVAL_SECS` | `3600` | Pause between runs (minimum 60) |
| `DUCKPORT_RETENTION_MIN_KEEP_DAYS` | `7` | Policies keeping fewer days are refused |
| `DUCKPORT_RETENTION_BATCH_PAUSE_MS` | `200` | Pause between two DELETE batches so ingestor writes interleave |
| `DUCKPORT_RETENTION_TABLE` | — | Deprecated in 1.1.0; only logs a warning |
| `DUCKPORT_AUTH_TOKENS` | *(empty)* | Comma-separated `name:token` entries. Empty rejects every public peer. Loopback never requires a token; LAN stays open until tokens are set |
| `DUCKPORT_TRUSTED_CIDRS` | *(empty)* | Extra CIDRs treated as LAN for writes. They do not become auth-exempt loopback |
| `DUCKPORT_DOGET_MAX_BYTES` | `16777216` | Public DoGet Arrow byte cap only (16MiB). `0` disables it |
| `DUCKPORT_DOGET_TIMEOUT_SECS` | `10` | Public DoGet timeout in seconds. `0` disables it. The query is interrupted at the deadline |
| `DUCKPORT_TLS_CERT` | *(empty)* | PEM certificate path. Must be set together with `DUCKPORT_TLS_KEY` |
| `DUCKPORT_TLS_KEY` | *(empty)* | PEM private key path |
| `DUCKPORT_SEED_DEMO` | *(unset)* | Set to `1` to seed a demo schema on startup for smoke testing |
| `RUST_LOG` | `info` | Standard tracing filter |

### Ingestor Instance Configuration

Each instance's config file is at `/opt/duckport/ingestors/<instance-name>/config.env`, editable via `duckport config <instance-name>`.

| Variable | Default | Description |
|----------|---------|-------------|
| `KLINE_INTERVAL` | `5m` | Base K-line interval |
| `DATA_SOURCES` | `usdt_perp,usdt_spot` | Enabled markets (comma-separated) |
| `CONCURRENCY` | `2` | Ingestion concurrency (REST/WS) |
| `START_DATE` | `2021-01-01` | Historical data start date |
| `PROXY_URL` | *(empty)* | HTTP proxy (empty = direct connection) |
| `PARQUET_DIR` | `/data/duckport/pqt` | loadhist Parquet archive directory (optional override) |
| `RESOURCE_PATH` | `/data/duckport/hist` | Historical data download temp directory (optional override) |

> `DUCKPORT_ADDR` is derived automatically by the ingestor from `DUCKPORT_LISTEN_ADDR` in `/opt/duckport/server.env`; no manual configuration needed. `ENABLE_WS` defaults to `true` (WebSocket real-time push).

### Consumer (Python `duckport-consumer`)

Constructor parameters (see `client/duckport_consumer/client.py`):

```python
from duckport_consumer import DuckportConsumer
c = DuckportConsumer(
    addr="duckport.prod:50051",
    schema="data",
    kline_interval_minutes=5,
    suffix="_5m",
    # Plan A: pqt_path / redundancy_hours deprecated (queries data.kline_* main table only)
)

# interval is the resample size in minutes. It must divide 60 and be a
# multiple of kline_interval_minutes. Omitting asset_class returns both
# Crypto and TradFi for usdt_perp.
bars = c.get_market("usdt_perp", interval=60)
crypto = c.get_market("usdt_perp", interval=60, asset_class="crypto")
tradfi = c.get_market("usdt_perp", interval=60, asset_class="tradifi")
# EQUITY includes EQUITY, HK_EQUITY, KR_EQUITY, and CN_EQUITY.
equities = c.get_market("usdt_perp", interval=60, asset_class="EQUITY")
# Any other value matches underlying_type exactly, e.g. COMMODITY, FX, PREMARKET, COIN.
gold = c.get_symbol("usdt_perp", "XAUUSDT", interval=60, asset_class="COMMODITY")
info = c.get_exginfo("usdt_perp", asset_class="tradifi")

# Wait until the usdt_perp_5m watermark passes `last`. Pushed by the server, no polling.
# Returns None on timeout.
duck_time = c.wait_duck_time("usdt_perp", after=last, timeout=120)
```

`wait_duck_time(market, after=None, timeout=None)` waits on the `{market}{suffix}` row of `data.watermark` and returns its `duck_time` as an aware UTC datetime. A naive `after` is taken as UTC; without `after` it returns as soon as the table has a watermark. Against a server without `duckport.watch_watermark` (older version) it polls `data.watermark` once a second instead. To follow several tables use `watch_watermark()`, which yields `{table_name: duck_time}`.

`asset_class` applies only to the perp markets (`usdt_perp`, `others_perp`), on `get_market`, `get_symbol`, and `get_exginfo`. `crypto` selects `contract_type='PERPETUAL'`; `tradifi` selects `TRADIFI_PERPETUAL`. Do not pass it for `usdt_spot`. A raw `query()` against `usdt_perp_5m` or `usdt_perp_funding` is not filtered.

Since binance-ingestor 0.7.0 every row of the perp kline tables (`usdt_perp_*`, `others_perp_*`) carries `contract_type` and `underlying_type`, and `asset_class` on `get_market` / `get_symbol` filters on those columns: delisted contracts are included, a symbol name that was reused (ANTUSDT: crypto until 2024-04, TradFi since 2026-11) is split by period, and pre-market rows are `PREMARKET`. Tables without the columns (older plugin) fall back to the symbols `exginfo` lists as trading now. `get_exginfo` always reflects current listings only. When filtering by `underlying_type` (`PREMARKET`, `COIN`, `EQUITY`, ...), the one resample bar in which a contract changes type is incomplete on both sides and is not returned.

---

## Hardware Requirements by Data Volume

DuckDB query performance depends heavily on **RAM** (used as buffer pool + intermediate query state). When the dataset exceeds available RAM, DuckDB spills to disk and latency degrades significantly. The table below gives empirical guidelines; always benchmark for your specific query patterns (single symbol vs. full market, time window length).

| Data Volume | Typical Scenario | Recommended CPU/RAM | Server Config |
|-------------|-----------------|-------------------|---------------|
| **< 10 GB** | 5m K-lines × 500 symbols × 3 years; POC stage | 2C/4G | `READ_POOL_SIZE=4`, `DUCKDB_THREADS=2`, `DUCKDB_MEMORY_LIMIT=2GB` |
| **10–50 GB** | 5m K-lines × 500 symbols × 10 years; 1m × 500 × 3 years | 4C/8G | `READ_POOL_SIZE=4`, `DUCKDB_THREADS=4`, `DUCKDB_MEMORY_LIMIT=5GB` |
| **50–200 GB** | 1m K-lines × 1000 symbols × 5 years | 8C/16G | `READ_POOL_SIZE=6`, `DUCKDB_THREADS=6`, `DUCKDB_MEMORY_LIMIT=10GB` |
| **> 200 GB** | Tick data / multi-asset merge | 16C/32G+ | Consider Parquet overlay (see `path-b-retention-plan.md`): hot data in DuckDB, cold data via `read_parquet` |

### Low-Spec Environment: 2C/2G (Minimum Requirements)

The minimum hardware target in `deploy.md` is 2C2G. This configuration is viable under the following constraints:

- DuckDB data **< 10 GB**
- Queries are primarily **single-symbol** (high row group pruning hit rate)
- **No** full-market multi-year aggregation queries
- `DUCKPORT_DUCKDB_MEMORY_LIMIT=800MB`, `READ_POOL_SIZE=2`, `DUCKDB_THREADS=2`

Exceeding these constraints triggers frequent disk spills; P95 latency degrades from seconds to minutes. Two remedies: upgrade hardware to 4C/8G, or enable Parquet overlay to keep hot data under a few hundred MB.

### Write Performance Tuning

- A single DoPut append should **not exceed ~500 MB** Arrow IPC stream (the server buffers the full stream before writing); split into chunks if larger
- Typical ingestor chunk size: **200,000 rows** (empirical value, see `bulk_write_kline`)
- After large writes, proactively send `duckport.execute` SQL `CHECKPOINT` to merge the WAL into the main file

---

## Adding a New Ingestor

### Quick Method: Install via Plugin Registry

`ingestor/registry.json` maintains repo addresses for all known plugins. To add a new plugin, append one entry to this file; other users can then install it via `duckport install`:

```json
{
  "my-ingestor": {
    "repo": "https://github.com/your-org/duckport-my-ingestor",
    "description": "My exchange K-line ingestor",
    "exec": "my-ingestor",
    "default_instance": "my-5m",
    "config": [
      {
        "name": "KLINE_INTERVAL",
        "label": "K-line interval",
        "default": "5m"
      },
      {
        "name": "CONCURRENCY",
        "label": "Concurrency",
        "default": "2"
      },
      {
        "name": "START_DATE",
        "label": "START_DATE",
        "default": "2021-01-01"
      },
      {
        "name": "PROXY_URL",
        "label": "PROXY_URL",
        "default": "",
        "empty_hint": "leave empty to skip"
      }
    ]
  }
}
```

```bash
duckport install my_ingestor    # Install from registry, interactive config setup
```

The `config` array defines variables prompted during installation and written to `config.env`; `INGESTOR_EXEC` is generated from `exec` automatically and should not be listed in `config`.

### Building an Ingestor Plugin from Scratch

The basic approach: write a Flight client that calls RPCs in the following sequence. Python / Rust / Go `pyarrow`/`arrow-flight` libraries all support the required operations.

### Step 1: Initialize Schema

Use `duckport.execute_transaction` to create tables idempotently (DDL):

```python
import json, pyarrow.flight as flight

client = flight.FlightClient("grpc://localhost:50051")

def execute_transaction(stmts):
    body = json.dumps({"statements": stmts}).encode("utf-8")
    action = flight.Action("duckport.execute_transaction", body)
    return list(client.do_action(action))

execute_transaction([
    "CREATE SCHEMA IF NOT EXISTS metrics",
    """CREATE TABLE IF NOT EXISTS metrics.events (
        ts TIMESTAMP,
        device_id VARCHAR,
        value DOUBLE
    )""",  # no PRIMARY KEY: indexed tables never reclaim deleted rows; the write SQL deduplicates
    "CREATE TABLE IF NOT EXISTS _staging_events (ts TIMESTAMP, device_id VARCHAR, value DOUBLE)",
])
```

**Conventions**:

- Business tables go in any schema other than `main` (`main` is hidden by `airport_adapter`)
- **Staging tables go in the `main` schema** (invisible to Airport clients, but accessible via SQL) — staging tables have no PK and are intended for bulk DoPut
- **Business tables carry no `PRIMARY KEY` / `UNIQUE` / index**: DuckDB 1.5 does not vacuum indexed tables, so deleted rows are never reclaimed, and a composite key is never used by queries. Reasons and measurements: [`ingestor-dev-guide.md` 5.1](./ingestor-dev-guide.md#51-为什么业务表不建主键)
- `CREATE SCHEMA IF NOT EXISTS` must be in the **same transaction** as `CREATE TABLE`; otherwise the next statement won't see the schema

### Step 2: Bulk Write Data (Staging Pattern)

Solution: **DoPut → staging → INSERT with an anti-join**. `lo` and `hi` are the earliest and latest timestamps of the batch, inlined as constants so the subquery only scans that time range:

```python
def merge_sql(lo, hi):
    return (
        "INSERT INTO metrics.events "
        "SELECT s.* FROM (SELECT DISTINCT ON (ts, device_id) * FROM _staging_events) s "
        "ANTI JOIN (SELECT ts, device_id FROM metrics.events "
        f"          WHERE ts BETWEEN TIMESTAMP '{lo}' AND TIMESTAMP '{hi}') t "
        "ON s.ts = t.ts AND s.device_id = t.device_id"
    )
```

```python
import pyarrow as pa

def append_to_staging(schema, table, arrow_table):
    descriptor = flight.FlightDescriptor.for_path("duckport.append", schema, table)
    writer, reader = client.do_put(descriptor, arrow_table.schema)
    writer.write_table(arrow_table)
    writer.done_writing()
    resp = json.loads(bytes(reader.read()))
    writer.close()
    return resp

arrow = pa.Table.from_pylist([
    {"ts": "2026-04-22 10:00:00", "device_id": "dev-1", "value": 1.23},
    {"ts": "2026-04-22 10:00:00", "device_id": "dev-2", "value": 4.56},
])

append_to_staging("main", "_staging_events", arrow)

execute_transaction([
    merge_sql(lo, hi),   # lo, hi = pc.min(arrow["ts"]).as_py(), pc.max(arrow["ts"]).as_py()
    "TRUNCATE _staging_events",
])
```

**Chunked processing for large batches**:

```python
CHUNK_SIZE = 200_000
for offset in range(0, len(arrow), CHUNK_SIZE):
    chunk = arrow.slice(offset, CHUNK_SIZE)
    execute_transaction(["TRUNCATE _staging_events"])
    append_to_staging("main", "_staging_events", chunk)
    execute_transaction([
        merge_sql(pc.min(chunk["ts"]).as_py(), pc.max(chunk["ts"]).as_py()),
        "TRUNCATE _staging_events",
    ])
```

### Step 3: Update Watermark (if needed)

Recommended: update the watermark in the same transaction to guarantee atomic consistency between data and cursor:

```python
execute_transaction([
    merge_sql(lo, hi),
    f"INSERT INTO data.watermark (table_name, ingestor, max_lag_seconds, time_column, start_time, duck_time, updated_at) "
    f"VALUES ('metrics.events', '{ingestor_name}', 900, 'event_time', NULL, '{latest_ts}', CURRENT_TIMESTAMP) "
    f"ON CONFLICT (table_name) DO UPDATE SET "
    f"ingestor = excluded.ingestor, max_lag_seconds = excluded.max_lag_seconds, "
    f"duck_time = excluded.duck_time, updated_at = excluded.updated_at",
    "TRUNCATE _staging_events",
])
```

### Step 4: Error Handling and Idempotency

- If any statement in `duckport.execute_transaction` fails, the entire transaction is ROLLBACKed; clients should catch `FlightError` and retry
- DoPut failures (schema mismatch, type error) do not pollute the table
- The anti-join makes repeated DoPuts of the same batch **idempotent** — consumers resuming from a watermark checkpoint produce no side effects. duckport has a single writer connection, so concurrent writes cannot create duplicates

### Step 5 (Optional): Read-Only Queries

The consumer read path goes through `duckport.query` DoGet:

```python
def query(sql):
    ticket_data = json.dumps({"type": "duckport.query", "sql": sql})
    ticket = flight.Ticket(ticket_data.encode("utf-8"))
    reader = client.do_get(ticket)
    return reader.read_all()

tbl = query("SELECT * FROM metrics.events WHERE ts >= '2026-04-22' ORDER BY ts LIMIT 100")
print(tbl.to_pandas())
```

Only `SELECT` / `WITH` / `EXPLAIN` are allowed; DDL/DML must go through `duckport.execute`.

### Reference Implementations

- [`duckport-binance-ingestor`](https://github.com/sjmc-quant/duckport-binance-ingestor): Complete Python ingestor reference (`duckport_client.py` / `data_jobs.py` / `loadhist.py`)
- `client/duckport_consumer/client.py`: Read-only consumer
- `tests/python/test_phase4_ingestor.py`: End-to-end write test template

### New Ingestor Project Structure

```
duckport-my-ingestor/           # Repo name should use duckport- prefix
├── pyproject.toml              # deps: pyarrow>=15, pandas>=2
│                               # [project.scripts] my-ingestor = "my_ingestor.main:main"
│                               #                   loadhist    = "my_ingestor.loadhist:main"
├── my_ingestor/
│   ├── duckport_client.py      # Flight client wrapper (reference: binance-ingestor)
│   ├── config.py               # Pure env-driven config (reads INGESTOR_ENV_FILE)
│   ├── data_jobs.py            # Data source fetch + write loop
│   ├── loadhist.py             # Historical backfill entry point
│   └── main.py                 # Entry: init_schema → data_jobs
└── config.env.example          # Only 5 variables needed
```

Different ingestor instances share a single `duckport-server`, writing to different market tables without conflict.

After completing, add the repo URL to `ingestor/registry.json` so `duckport install` can find it.

---

## Related Documentation

| Document | Purpose |
|----------|---------|
| [`roadmap.md`](./roadmap.md) | Phase progress + pending TODOs |
| [`deploy.md`](./deploy.md) | Production deployment, systemd, backup/restore, troubleshooting |
| [`ingestor-dev-guide.md`](./ingestor-dev-guide.md) | Ingestor plugin development guidelines (reference: binance-ingestor) |
| [`upgrade-1.1.md`](./upgrade-1.1.md) | Upgrading to 1.1.0: DuckDB 1.5.6, new retention, dropping primary keys from business tables (Chinese) |
| [`migration-guide.md`](./migration-guide.md) | Migrating data from the old Python duckport |
| [`extra-enhancement.md`](./extra-enhancement.md) | Concurrency model decision (D1a per-table actor vs D1b single-writer) |
| [`path-b-retention-plan.md`](./path-b-retention-plan.md) | Parquet overlay transparent archival plan (large data volume path) |
| [`crates/airport/README.md`](./crates/airport/README.md) | airport-rs library usage documentation |

---

## Versions and Dependencies

- **Rust**: 1.85.1+ (edition 2021)
- **DuckDB**: 1.5.6 (crate `duckdb 1.10506.1`, feature `bundled` — compiles inline)
- **Arrow**: 58.x (aligned with DuckDB crate)
- **tonic**: 0.14 / **prost**: 0.14
- **Python (ingestor + consumer)**: 3.10+, `pyarrow >= 15`

Upgrading the DuckDB version has a cascading effect on the Arrow major version (DuckDB 1.5.x → Arrow 58). All `arrow-*` crates in the workspace must stay on a unified version.

---

## License

MIT (consistent with the upstream airport-rs).
