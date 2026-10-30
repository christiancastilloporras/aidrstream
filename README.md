# KOI-to-XSIAM Session Streamer

Streams KOI (Agentic Endpoint Security) AI agent sessions into Cortex XSIAM HTTP Log Collector in near-realtime.

## Overview

KOI's tenant UI does not support streaming sessions to external systems as a built-in feature. This poller fills that gap by:

1. Polling the KOI `/agent-activity/sessions` API every 5 minutes
2. Tracking a high-water mark (HWM) in SQLite to avoid losing sessions between polls
3. Deduplicating sessions created within the same timestamp
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
             │ created_at_gte filter
             ▼
┌─────────────────────────┐
│   Poller Container      │
│  (Python 3.12)          │
│  - koi_api.py           │
│  - state.py (SQLite)    │
│  - xsiam_client.py      │
│  - poller.py            │
└────────────┬────────────┘
             │ NDJSON POST (gzip compressed)
             │ Authorization: API ***
             ▼
┌─────────────────────────┐
│   XSIAM HTTP Collector  │
│   https://api-{tenant}  │
│   /logs/v1/event        │
└─────────────────────────┘

## Knowledge Graph

An automated knowledge graph of the codebase is available in the `graphify-out/` directory (generated with [`graphify`](https://github.com/safishamsi/graphify)). It includes:

- Interactive HTML visualization: `graphify-out/graph.html`
- Plain-language report: `graphify-out/GRAPH_REPORT.md`
- Raw graph data: `graphify-out/graph.json`

The graph reveals core abstractions, surprising connections, and suggested questions for deeper understanding. See the report for details.
```

### Key Design Decisions

| Concern | Solution |
|---------|----------|
| Near-realtime | 5-minute poll interval (configurable via `POLL_INTERVAL_SECONDS`) |
| No session loss | High-water mark (HWM) stored in SQLite volume |
| No duplicates | HWM timestamp + boundary ID set deduplication |
| 24h window limit | Automatic window chunking if HWM is stale |
| State persistence | SQLite in Docker volume mounted at `/data` |
| Container reproducibility | Docker Compose, single service, `.env` for secrets |

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

## KOI API Constraints (Enforced)

- Time window (`created_at_gte` to `created_at_lte`) is required
- Maximum window: 24 hours (auto-chunked if exceeded)
- End time cannot be in the future
- Page size: 1–100 default, max 500

## Logs

Container logs are written to stdout:
```bash
docker compose logs -f
```

## License

See LICENSE file.
