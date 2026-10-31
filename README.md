# KOI-to-XSIAM Session Streamer

Streams KOI (Agentic Endpoint Security) AI agent sessions into Cortex XSIAM HTTP Log Collector in near-realtime.

## Overview

KOI's tenant UI does not support streaming sessions to external systems as a built-in feature. This poller fills that gap by:

1. Polling the KOI `/agent-activity/sessions` API every 5 minutes
2. Starting each UTC day at 00:00:00 and only stepping `created_at_lte` forward per poll
3. Deduplicating sessions already forwarded in the last 24 hours (by composite key `session_id + host + agent`)
4. Forwarding raw session JSON to the XSIAM HTTP Log Collector endpoint

## Architecture

```
┌─────────────────────────┐
│   KOI API               │
│   api.prod.koi.security │
│   /agent-activity/      │
│   sessions              │
└────────────┬────────────┘
             │ poll every 5 min
             │ created_at_gte (daily 00:00 UTC start)
             │ created_at_lte (advances per poll)
             │ Bearer token auth
             ▼
┌─────────────────────────┐
│   Poller Container      │
│  (Python 3.12)          │
│  - koi_api.py           │
│  - state.py (SQLite)    │
│  - xsiam_client.py      │
│  - poller.py            │
│  - exceptions.py        │
└────────────┬────────────┘
             │ NDJSON POST (gzip compressed)
             │ Authorization: API key token (raw, no Bearer prefix)
             ▼
┌─────────────────────────┐
│   XSIAM HTTP Collector  │
│   https://api-{tenant}  │
│   /logs/v1/event        │
└─────────────────────────┘
```

## Knowledge Graph

An automated knowledge graph of the codebase is available in the `graphify-out/` directory (generated with [`graphify`](https://github.com/safishamsi/graphify)). It includes:

- Interactive HTML visualization: `graphify-out/graph.html`
- Plain-language report: `graphify-out/GRAPH_REPORT.md`
- Raw graph data: `graphify-out/graph.json`

The graph reveals core abstractions, surprising connections, and suggested questions for deeper understanding. See the report for details.

### Key Design Decisions

| Concern | Solution |
|---------|----------|
| Near-realtime | 5-minute poll interval (configurable via `POLL_INTERVAL_SECONDS`) |
| No session loss | Daily start at 00:00:00 UTC; `created_at_lte` only advances per poll (capped at now) |
| Sliding safety buffer | `created_at_gte` advances by 1 hour for every 2 hours elapsed since midnight (e.g., at 04:00 UTC, `from` = 02:00), providing a ~1h overlap to catch late-arriving sessions while bounding re-fetch volume |
| No duplicates | Sessions forwarded in the last 24 hours are skipped, keyed by `(session_id, host, agent)` composite to handle the same session appearing from different hosts/agents |
| Empty poll | `created_at_lte` advances by `POLL_INTERVAL_SECONDS`, capped at now |
| 24h window limit | Automatic chunking if the day exceeds 24 hours |
| State persistence | SQLite in Docker volume mounted at `/data` |
| Container reproducibility | Docker Compose, single service, `.env` for secrets |
| Timezone awareness | `TZ` env var (default `America/Mexico_City`) affects local timestamp interpretation; all KOI/XSIAM interactions use UTC |
| State isolation | `STATE_DB_PATH` env var (default `/data/state.db`) controls SQLite DB location |

## Prerequisites

- Docker and Docker Compose
- KOI API key (create in KOI UI: Settings > API Access > Create New API Key)
- XSIAM HTTP collector configured with:
  - Collector URL (e.g., `https://api-{tenant}/logs/v1/event`)
  - API key token
  - GZIP compression + JSON format

## Setup

Choose one of two options to run the poller:

### Option 1: Build from source (local build)

Use this for development, testing, or custom modifications:

1. Clone this repository
2. Copy `example.env` to `.env`:
   ```bash
   cp example.env .env
   ```
3. Edit `.env` and fill in your KOI API key, XSIAM URL, and XSIAM API key token
4. Start the poller:
   ```bash
   docker compose up -d
   ```

### Option 2: Use pre-built image (recommended for production)

A pre-built image is published on Docker Hub as `ccastilloporras/aidrstream:latest`. Use this no-setup-required production compose file instead:

```bash
cp example.env .env
# edit .env with your credentials
docker compose -f docker-compose.prod.yml up -d
```

| Option | `docker-compose.yml` | `docker-compose.prod.yml` |
|---|---|---|
| Source | Builds from local Dockerfile | Pulls `ccastilloporras/aidrstream:latest` |
| Best for | Development, forking, custom builds | Production, standard deployment |
| Updates | Rebuild image manually | `docker compose -f docker-compose.prod.yml pull && docker compose -f docker-compose.prod.yml up -d --force-recreate` |

The pre-built image is kept up to date with the `main` branch. To receive updates, pull the latest image before recreating:
```bash
docker compose -f docker-compose.prod.yml pull && \
docker compose -f docker-compose.prod.yml up -d --force-recreate
```

## Configuration

All configuration is done via environment variables in `.env`:

| Variable | Description | Default |
|----------|-------------|---------|
| `KOI_API_URL` | KOI API base URL | `https://api.prod.koi.security/api/external/v2` |
| `KOI_API_KEY` | KOI API bearer token | _required_ |
| `XSIAM_URL` | XSIAM HTTP collector endpoint | _required_ |
| `XSIAM_API_KEY` | XSIAM HTTP collector API key token | _required_ |
| `POLL_INTERVAL_SECONDS` | Poll interval in seconds | `300` (5 min) |
| `KOI_PAGE_SIZE` | KOI API page size | `100` (max 500) |
| `TZ` | Timezone for the poller | `America/Mexico_City` |
| `STATE_DB_PATH` | SQLite state database path | `/data/state.db` |

## KOI API Constraints (Enforced)

- Time window (`created_at_gte` to `created_at_lte`) is required
- Each UTC day resets `created_at_gte` to 00:00:00Z; only `created_at_lte` advances during the day
- Empty polls keep `created_at_gte` and only move `created_at_lte` forward by one poll interval (never into the future)
- Maximum window: 24 hours (auto-chunked if exceeded)
- End time cannot be in the future
- Page size: 1–100 default, max 500

## State Model

The poller persists state in an SQLite database with three keys:

| Key | Purpose |
|-----|---------|
| `window_from` | Start of the current poll window (UTC midnight of the current day) |
| `window_to` | End of the current poll window (advances by one interval per poll, capped at now) |
| `sent_sessions` | Table of `(session_id, host, agent, sent_at)` for 24h dedup |

On a new UTC day, the cursor resets to midnight. On the same day, only `window_to` advances. Failed fetches or sends leave the cursor unchanged (at-least-once delivery; dedup prevents duplicates).

## Testing

Tests are local-only (excluded from the Docker image and CI via `.dockerignore`).

```bash
# E2E test with mock KOI API and XSIAM servers
python3 tests/test_poller_e2e.py

# Window cursor + dedup logic (pure unit tests)
python3 tests/test_window_cursor.py
```

## Logs

Container logs are written to stdout:
```bash
docker compose logs -f
```

## License

See LICENSE file.