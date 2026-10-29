"""
End-to-end test of the KOI-to-XSIAM poller using mock servers.

Tests:
1. First poll cycle: fetches sessions from mock KOI API, sends to mock XSIAM
2. State persistence: SQLite state is saved and read correctly
3. Second poll cycle: HWM prevents re-fetching old sessions
4. Deduplication: sessions at the HWM boundary are not duplicated
5. 24-hour window: tested separately if needed
"""

import json
import os
import sys
import threading
import time
from datetime import datetime, timedelta, timezone
from http.server import HTTPServer, BaseHTTPRequestHandler

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from koi_api import KoiApiClient
from state import StateManager
from xsiam_client import XsiamHttpCollectorClient

# ---------------------------------------------------------------------------
# Mock servers
# ---------------------------------------------------------------------------

KOI_API_BASE = "http://localhost:18081/api/external/v2"
XSIAM_URL = "http://localhost:18082/logs/v1/event"

# Test state DB path (temporary)
TEST_DB_PATH = "/tmp/test_koi_state.db"

# Global counters for mock servers
koi_calls = {"sessions": 0}
xsiam_received = []


def iso_ts(dt):
    """Format datetime as ISO 8601 UTC string."""
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

# Test sessions with timestamps
NOW = datetime.now(timezone.utc)
SESSION_1 = {
    "session_id": "sess-001",
    "agent": "claude_code",
    "host": "dev-laptop",
    "user_emails": ["user@example.com"],
    "start_time": iso_ts(NOW - timedelta(minutes=10)),
    "end_time": iso_ts(NOW - timedelta(minutes=9)),
    "event_count": 5,
    "models": ["claude-sonnet-4"],
    "decisions": {"allow": 5, "ask": 0, "block": 0},
    "governed_by": [],
}

SESSION_2 = {
    "session_id": "sess-002",
    "agent": "codex",
    "host": "ci-server",
    "user_emails": ["dev@acme.com", "sec@acme.com"],
    "start_time": iso_ts(NOW - timedelta(minutes=3)),
    "end_time": iso_ts(NOW - timedelta(minutes=2)),
    "event_count": 12,
    "models": ["o1-preview"],
    "decisions": {"allow": 10, "ask": 2, "block": 0},
    "governed_by": ["pol_abc"],
}

SESSION_3 = {
    "session_id": "sess-003",
    "agent": "claude_code",
    "host": "prod-vm",
    "user_emails": ["admin@acme.com"],
    "start_time": iso_ts(NOW - timedelta(minutes=1)),
    "end_time": iso_ts(NOW - timedelta(minutes=0, seconds=30)),
    "event_count": 3,
    "models": ["claude-sonnet-4"],
    "decisions": {"allow": 1, "ask": 2, "block": 0},
    "governed_by": [],
}

# Sessions to return from mock KOI API based on the created_at_gte filter
# We parse the 'created_at_gte' query param and return sessions after that time
ALL_SESSIONS = [SESSION_1, SESSION_2, SESSION_3]


class MockKoiHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path.startswith("/api/external/v2/agent-activity/sessions"):
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()

            # Parse query params
            from urllib.parse import urlparse, parse_qs
            parsed = urlparse(self.path)
            params = parse_qs(parsed.query)

            created_at_gte = params.get("created_at_gte", [None])[0]
            page_size = int(params.get("page_size", ["100"])[0])
            page = int(params.get("page", ["1"])[0])

            koi_calls["sessions"] += 1

            # Filter sessions by created_at_gte
            sessions = ALL_SESSIONS
            if created_at_gte:
                sessions = [
                    s for s in sessions
                    if s["start_time"] >= created_at_gte
                ]

            # Paginate
            start = (page - 1) * page_size
            end = start + page_size
            page_sessions = sessions[start:end]

            response = {
                "data": page_sessions,
                "total_count": len(sessions),
            }
            self.wfile.write(json.dumps(response).encode())
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        pass  # Suppress logs


class MockXsiamHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        if self.path == "/logs/v1/event":
            content_length = int(self.headers["Content-Length"])
            body = self.rfile.read(content_length).decode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"status": "ok"}')

            # Parse NDJSON and store events
            for line in body.strip().split("\n"):
                if line:
                    xsiam_received.append(json.loads(line))
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        pass  # Suppress logs


def start_mock_koi():
    server = HTTPServer(("localhost", 18081), MockKoiHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server


def start_mock_xsiam():
    server = HTTPServer(("localhost", 18082), MockXsiamHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server


def run_test():
    # Clean up any previous test state
    if os.path.exists(TEST_DB_PATH):
        os.remove(TEST_DB_PATH)

    # Start mock servers
    koi_server = start_mock_koi()
    xsiam_server = start_mock_xsiam()
    time.sleep(0.5)

    print("=== Test: KOI-to-XSIAM Poller End-to-End ===\n")

    # Initialize components
    state_manager = StateManager(db_path=TEST_DB_PATH)
    state_manager.init()

    koi_client = KoiApiClient(
        base_url=KOI_API_BASE,
        api_key="test_key",
        page_size=100,
    )

    xsiam_client = XsiamHttpCollectorClient(
        url=XSIAM_URL,
        api_key="test_xsiam_key",
    )

    # --- Cycle 1: First poll ---
    print("--- Cycle 1: First poll (fresh state) ---")
    import poller

    # Temporarily override get_utc_now and the module-level config
    original_get_utc_now = poller.get_utc_now
    poller.get_utc_now = lambda: NOW

    poller.poll_cycle(koi_client, xsiam_client, state_manager)

    print(f"\nKOI API calls: {koi_calls['sessions']}")
    print(f"XSIAM events received: {len(xsiam_received)}")
    assert len(xsiam_received) == 2, f"Expected 2 events (sessions within last 5 min), got {len(xsiam_received)}"
    print("✓ Sessions within lookback window forwarded to XSIAM (session 1 at -10min excluded)")

    # Verify state was saved
    hwm = state_manager.get_last_hwm()
    ids = state_manager.get_last_seen_ids()
    print(f"HWM saved: {hwm}")
    print(f"Known IDs at HWM: {ids}")
    assert hwm is not None, "HWM should be saved"
    assert SESSION_3["start_time"] in hwm, f"HWM should be session 3's start_time"

    # --- Cycle 2: Second poll (no new sessions) ---
    print("\n--- Cycle 2: Second poll (no new sessions) ---")
    koi_calls["sessions"] = 0
    xsiam_received.clear()

    poller.poll_cycle(koi_client, xsiam_client, state_manager)

    print(f"KOI API calls: {koi_calls['sessions']}")
    print(f"XSIAM events received: {len(xsiam_received)}")
    assert len(xsiam_received) == 0, f"Expected 0 new events, got {len(xsiam_received)}"
    print("✓ No duplicate sessions forwarded")

    # --- Cycle 3: Third poll (new session arrives) ---
    print("\n--- Cycle 3: New session arrives ---")
    SESSION_4 = {
        "session_id": "sess-004",
        "agent": "codex",
        "host": "new-server",
        "user_emails": ["new@user.com"],
        "start_time": iso_ts(NOW + timedelta(minutes=1)),
        "end_time": iso_ts(NOW + timedelta(minutes=1, seconds=30)),
        "event_count": 1,
        "models": ["gpt-4.5"],
        "decisions": {"allow": 1, "ask": 0, "block": 0},
        "governed_by": [],
    }
    ALL_SESSIONS.append(SESSION_4)

    # Update NOW to include the new session's time
    poller.get_utc_now = lambda: NOW + timedelta(minutes=2)

    koi_calls["sessions"] = 0
    xsiam_received.clear()

    poller.poll_cycle(koi_client, xsiam_client, state_manager)

    print(f"KOI API calls: {koi_calls['sessions']}")
    print(f"XSIAM events received: {len(xsiam_received)}")
    assert len(xsiam_received) == 1, f"Expected 1 new event, got {len(xsiam_received)}"
    assert xsiam_received[0]["session_id"] == "sess-004"
    print("✓ Only the new session was forwarded to XSIAM")

    # --- Cleanup ---
    koi_server.shutdown()
    xsiam_server.shutdown()

    # Restore original function
    poller.get_utc_now = original_get_utc_now

    # Clean up test DB
    if os.path.exists(TEST_DB_PATH):
        os.remove(TEST_DB_PATH)

    print("\n=== ALL TESTS PASSED ===")


if __name__ == "__main__":
    run_test()
