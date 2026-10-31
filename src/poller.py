"""
KOI-to-XSIAM Poller

Streams KOI agent-activity sessions into Cortex XSIAM HTTP Log Collector
in near-realtime (default: poll every 5 minutes).

Key design decisions:
- Uses KOI /agent-activity/sessions endpoint
- Sticky query window: empty polls keep created_at_gte and step created_at_lte by one interval
- Day-scoped session_id dedup so a re-read window only forwards new sessions
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


def advance_window(
    had_new_sessions: bool,
    window_from: datetime,
    window_to: datetime,
    now: datetime,
    poll_interval_seconds: int,
    newest_start: Optional[datetime],
) -> tuple[datetime, datetime]:
    """Return the next (from, to) query cursor.

    Empty or duplicate-only polls keep `from` and step `to` by one interval.
    A poll that forwarded sessions moves `from` to the newest forwarded start_time
    and sets `to` to now.
    """
    if had_new_sessions and newest_start is not None:
        return newest_start, now

    stepped = window_to + timedelta(seconds=poll_interval_seconds)
    if stepped > now:
        stepped = now
    if stepped < window_from:
        stepped = window_from
    return window_from, stepped


def select_unsent(sessions: list[dict], already_sent: set[str]) -> list[dict]:
    """Keep sessions whose session_id was not forwarded in the retention window.

    Sessions with no session_id are dropped: they cannot be deduped safely.
    """
    chosen = []
    for session in sessions:
        sid = session.get("session_id")
        if not sid or sid in already_sent:
            continue
        chosen.append(session)
    return chosen


def newest_start_time(sessions: list[dict]) -> Optional[datetime]:
    stamps = [parse_iso_timestamp(s["start_time"]) for s in sessions if s.get("start_time")]
    return max(stamps) if stamps else None


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
    """Execute one poll.

    Empty polls keep window_from and step window_to by one interval.
    Forwarded sessions move window_from to the newest start_time.
    A failed send leaves both the cursor and the sent-id set unchanged.
    """
    now = get_utc_now()
    state_manager.prune_sent_ids(older_than_hours=MAX_WINDOW_HOURS)

    window_from_s, window_to_s = state_manager.get_window()
    if window_from_s:
        start_dt = parse_iso_timestamp(window_from_s)
        logger.info(f"Resuming from window_from: {window_from_s}")
    else:
        logger.info(f"First run — starting from {POLL_INTERVAL // 60} minutes ago")
        start_dt = now - timedelta(seconds=POLL_INTERVAL)

    if window_to_s:
        end_dt = parse_iso_timestamp(window_to_s) + timedelta(seconds=POLL_INTERVAL)
    else:
        end_dt = now
    if end_dt > now:
        end_dt = now

    sessions = fetch_sessions_for_window(koi_client, start_dt, end_dt)
    already_sent = state_manager.get_sent_ids_within(hours=MAX_WINDOW_HOURS)
    new_sessions = select_unsent(sessions, already_sent)

    if not new_sessions:
        logger.info(
            "No new sessions in %s -> %s; keeping from, advancing to",
            iso_format(start_dt),
            iso_format(end_dt),
        )
        new_from, new_to = advance_window(
            had_new_sessions=False,
            window_from=start_dt,
            window_to=end_dt,
            now=now,
            poll_interval_seconds=POLL_INTERVAL,
            newest_start=None,
        )
        state_manager.save_window(iso_format(new_from), iso_format(new_to))
        return

    for session in new_sessions:
        if "start_time" in session:
            session["_time"] = session["start_time"]
        session["source_log_type"] = "koi_agent_activity_session"

    logger.info(f"Forwarding {len(new_sessions)} new sessions to XSIAM (of {len(sessions)} fetched)")
    if not xsiam_client.send_events(new_sessions):
        logger.error("Failed to send events to XSIAM — window and sent-ids will NOT be updated")
        return

    newest = newest_start_time(new_sessions)
    if newest is None:
        logger.warning("Forwarded sessions have no start_time; window_from will not move")
    new_from, new_to = advance_window(
        had_new_sessions=True,
        window_from=start_dt,
        window_to=end_dt,
        now=now,
        poll_interval_seconds=POLL_INTERVAL,
        newest_start=newest,
    )
    sent_at = iso_format(now)
    ids = [s["session_id"] for s in new_sessions if s.get("session_id")]
    state_manager.record_sent_ids(ids, sent_at=sent_at)
    state_manager.save_window(iso_format(new_from), iso_format(new_to))
    state_manager.save_state(hwm=iso_format(new_from), seen_ids=set(ids))
    logger.info(f"Poll cycle complete. Window {iso_format(new_from)} -> {iso_format(new_to)}")


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
