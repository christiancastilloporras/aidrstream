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
             │ NDJSON POST
             │ Authorization: API key
             ▼
┌─────────────────────────┐
│   XSIAM HTTP Collector  │
│   https://api-{tenant}  │
│   /logs/v1/event        │
└─────────────────────────┘
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
  - API key

## Setup

1. Clone this repository
2. Copy `example.env` to `.env`:
   ```bash
   cp example.env .env
   ```
3. Edit `.env` and fill in your KOI API key, XSIAM URL, and XSIAM API key
4. Start the poller:
   ```bash
   docker compose up -d
   ```

## Configuration

All configuration is done via environment variables in `.env`:

| Variable | Description | Default |
|----------|-------------|---------|
| `KOI_API_URL` | KOI API base URL | `https://api.prod.koi.security/api/external/v2` |
| `KOI_API_KEY` | KOI API bearer token | _required_ |
| `XSIAM_URL` | XSIAM HTTP collector endpoint | _required_ |
| `XSIAM_API_KEY_ID` | XSIAM API key ID (numeric) | _required_ |
| `XSIAM_API_KEY` | XSIAM API key secret | _required_ |
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
