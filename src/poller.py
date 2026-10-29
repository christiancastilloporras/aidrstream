"""
KOI-to-XSIAM Poller

Streams KOI agent-activity sessions into Cortex XSIAM HTTP Log Collector
in near-realtime (default: poll every 5 minutes).

Key design decisions:
- Uses KOI /agent-activity/sessions endpoint
- High-water mark (HWM) tracking via SQLite to prevent duplicates
- Deduplication at the boundary timestamp (handles multiple sessions created in the same second)
- Respects KOI's 24-hour time window constraint
- Polls every 5 minutes (configurable via POLL_INTERVAL_SECONDS)
- Forwards raw session JSON to XSIAM (parsing rules can be added in XSIAM if needed)

API constraints from KOI:
- Time window required (created_at_gte and created_at_lte)
- Window cannot exceed 24 hours
- End time cannot be in the future
- Page size: default 100, max 500
"""

import os
import sys
import time
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from koi_api import KoiApiClient
from state import StateManager
from xsiam_client import XsiamHttpCollectorClient

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

# Read from environment
KOI_API_URL = os.environ.get("KOI_API_URL", "https://api.prod.koi.security/api/external/v2")
KOI_API_KEY = os.environ.get("KOI_API_KEY", "")
XSIAM_URL = os.environ.get("XSIAM_URL", "")
XSIAM_API_KEY_ID = os.environ.get("XSIAM_API_KEY_ID", "")
XSIAM_API_KEY = os.environ.get("XSIAM_API_KEY", "")
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL_SECONDS", "300"))
KOI_PAGE_SIZE = int(os.environ.get("KOI_PAGE_SIZE", "100"))

# KOI time window constraint
MAX_WINDOW_HOURS = 24

# SQLite state DB path
STATE_DB_PATH = os.environ.get("STATE_DB_PATH", "/data/state.db")


def get_utc_now() -> datetime:
    """Get current UTC time."""
    return datetime.now(timezone.utc)


def iso_format(dt: datetime) -> str:
    """Format datetime as ISO 8601 string (XSIAM-compatible)."""
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_iso_timestamp(ts: str) -> datetime:
    """Parse an ISO 8601 timestamp string into a datetime object.

    Handles both Z-suffixed and explicit timezone formats.
    """
    # Replace Z with +00:00 for fromisoformat compatibility
    ts = ts.replace("Z", "+00:00")
    dt = datetime.fromisoformat(ts)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def deduplicate_sessions(sessions: list[dict], known_ids: set[str]) -> list[dict]:
    """Filter out sessions whose session_id is in the known_ids set.

    Args:
        sessions: List of session dicts from KOI API
        known_ids: Set of session IDs that have already been processed

    Returns:
        List of new (non-duplicate) session dicts
    """
    if not known_ids:
        return sessions

    new_sessions = []
    for s in sessions:
        sid = s.get("session_id")
        if sid and sid not in known_ids:
            new_sessions.append(s)

    skipped = len(sessions) - len(new_sessions)
    if skipped > 0:
        logger.debug(f"Deduplication: skipped {skipped} already-seen sessions")

    return new_sessions


def fetch_sessions_for_window(
    koi_client: KoiApiClient,
    start_time: datetime,
    end_time: datetime,
) -> list[dict]:
    """Fetch sessions within a specific time window, handling 24h constraint.

    If the window exceeds 24 hours, it is split into 24-hour chunks
    and each chunk is fetched sequentially.

    Args:
        koi_client: KOI API client
        start_time: Start of the time window
        end_time: End of the time window

    Returns:
        List of session dicts
    """
    all_sessions: list[dict] = []
    current_start = start_time

    while current_start < end_time:
        # Calculate window end (max 24 hours from current_start)
        window_end = min(current_start + timedelta(hours=MAX_WINDOW_HOURS), end_time)

        # Ensure end time is not in the future
        now = get_utc_now()
        if window_end > now:
            window_end = now

        start_str = iso_format(current_start)
        end_str = iso_format(window_end)

        logger.info(f"Fetching sessions window: {start_str} -> {end_str}")

        sessions = koi_client.get_sessions_since(
            created_after=start_str,
            created_before=end_str,
            max_pages=100,
        )

        all_sessions.extend(sessions)

        # Move to next window
        current_start = window_end

    return all_sessions


def poll_cycle(
    koi_client: KoiApiClient,
    xsiam_client: XsiamHttpCollectorClient,
    state_manager: StateManager,
) -> None:
    """Execute one complete poll cycle.

    1. Read HWM from state
    2. Fetch sessions since HWM (respecting 24h window)
    3. Deduplicate against known IDs at boundary
    4. Forward to XSIAM
    5. Update state with new HWM
    """
    # --- Step 1: Read state ---
    last_hwm = state_manager.get_last_hwm()
    known_ids = state_manager.get_last_seen_ids()

    if last_hwm:
        logger.info(f"Resuming from HWM: {last_hwm}")
        start_dt = parse_iso_timestamp(last_hwm)
    else:
        # First run: use a default lookback of 5 minutes
        logger.info("First run — starting from 5 minutes ago")
        start_dt = get_utc_now() - timedelta(minutes=5)

    end_dt = get_utc_now()

    # --- Step 2: Fetch sessions ---
    sessions = fetch_sessions_for_window(koi_client, start_dt, end_dt)

    if not sessions:
        logger.info("No new sessions found in this poll cycle")
        return

    # --- Step 3: Deduplicate ---
    new_sessions = deduplicate_sessions(sessions, known_ids)

    if not new_sessions:
        logger.info("All sessions were duplicates — state will still be updated")
        # Even if all are duplicates, update the HWM to avoid re-reading
        # the same window on the next poll
    else:
        logger.info(f"Forwarding {len(new_sessions)} new sessions to XSIAM (of {len(sessions)} fetched)")

    # --- Step 4: Forward to XSIAM ---
    if new_sessions:
        # Enrich with _time field for XSIAM
        for s in new_sessions:
            if "start_time" in s:
                s["_time"] = s["start_time"]
            s["source_log_type"] = "koi_agent_activity_session"

        success = xsiam_client.send_events(new_sessions)
        if not success:
            logger.error("Failed to send events to XSIAM — state will NOT be updated, events will be retried next cycle")
            return

    # --- Step 5: Update state (HWM + dedup IDs) ---
    # The HWM is the start_time of the last session in the fetched window.
    # We also collect all session IDs that share this timestamp to prevent
    # duplicates on the next poll (in case sessions are created within the same second).

    # Sort sessions by start_time to find the true high-water mark
    sorted_sessions = sorted(sessions, key=lambda s: s.get("start_time", ""))

    new_hwm = sorted_sessions[-1].get("start_time", iso_format(end_dt))
    new_hwm_dt = parse_iso_timestamp(new_hwm)

    # Collect IDs at the HWM timestamp
    hwm_ids: set[str] = set()
    for s in sorted_sessions:
        ts = s.get("start_time", "")
        if ts == new_hwm:
            sid = s.get("session_id")
            if sid:
                hwm_ids.add(sid)

    # If the HWM hasn't changed, merge with previous known IDs
    if last_hwm and last_hwm == new_hwm:
        hwm_ids = hwm_ids | known_ids

    state_manager.save_state(hwm=new_hwm, seen_ids=hwm_ids)
    logger.info(f"Poll cycle complete. New HWM: {new_hwm}")


def main() -> None:
    """Main entry point — runs the polling loop indefinitely."""
    logger.info("=" * 60)
    logger.info("KOI-to-XSIAM Poller starting")
    logger.info(f"  KOI API:     {KOI_API_URL}")
    logger.info(f"  Poll interval: {POLL_INTERVAL}s")
    logger.info(f"  State DB:    {STATE_DB_PATH}")
    logger.info("=" * 60)

    # Validate environment
    if not KOI_API_KEY:
        logger.error("KOI_API_KEY environment variable is required")
        sys.exit(1)
    if not XSIAM_URL:
        logger.error("XSIAM_URL environment variable is required")
        sys.exit(1)
    if not XSIAM_API_KEY_ID:
        logger.error("XSIAM_API_KEY_ID environment variable is required")
        sys.exit(1)
    if not XSIAM_API_KEY:
        logger.error("XSIAM_API_KEY environment variable is required")
        sys.exit(1)

    # Initialize components
    state_manager = StateManager(db_path=STATE_DB_PATH)
    state_manager.init()

    koi_client = KoiApiClient(
        base_url=KOI_API_URL,
        api_key=KOI_API_KEY,
        page_size=KOI_PAGE_SIZE,
    )

    xsiam_client = XsiamHttpCollectorClient(
        url=XSIAM_URL,
        api_key_id=XSIAM_API_KEY_ID,
        api_key=XSIAM_API_KEY,
    )

    # Polling loop
    while True:
        try:
            poll_cycle(koi_client, xsiam_client, state_manager)
        except Exception as e:
            logger.error(f"Poll cycle failed with error: {e}", exc_info=True)
        else:
            logger.info(f"Poll cycle completed, sleeping for {POLL_INTERVAL}s")

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
