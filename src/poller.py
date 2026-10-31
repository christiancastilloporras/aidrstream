"""
KOI-to-XSIAM Poller

Streams KOI agent-activity sessions into Cortex XSIAM HTTP Log Collector
in near-realtime (default: poll every 5 minutes).

Key design decisions:
- Uses KOI /agent-activity/sessions endpoint
- Daily window: each UTC day starts at 00:00:00; created_at_lte steps forward per poll
  while created_at_gte advances by 1h every 2h (sliding safety buffer)
- 24h sent_session_id dedup so re-read/overlapping windows never resend
- Respects KOI's 24-hour time window constraint (auto-chunked)
- State update is the last step: failed KOI fetch or XSIAM send leaves cursor unchanged
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

from .koi_api import KoiApiClient
from .state import StateManager
from .xsiam_client import XsiamHttpCollectorClient

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


def day_boundary(now: datetime) -> datetime:
    """UTC midnight of the given datetime's day — the daily query start."""
    return now.replace(hour=0, minute=0, second=0, microsecond=0)


def compute_window_from(now: datetime) -> datetime:
    """Compute the query window start based on elapsed time since midnight.

    The window start advances by 1 hour for every 2 hours elapsed since
    UTC midnight. This keeps a sliding ~1h safety buffer so late-arriving
    sessions from the previous window are still caught on the next poll,
    while progressively reducing the overlap to bound the re-fetch volume.

    On day change (handled by the caller), from resets to midnight.

    Examples (UTC):
        00:00 - 01:59  -> from = 00:00
        02:00 - 03:59  -> from = 01:00
        04:00 - 05:59  -> from = 02:00
        ...
    """
    midnight = day_boundary(now)
    elapsed_hours = (now - midnight).total_seconds() / 3600
    from_hour = int(elapsed_hours // 2)  # advance 1h per 2h elapsed
    window_from = midnight + timedelta(hours=from_hour)
    # Never exceed now
    if window_from > now:
        window_from = midnight
    return window_from


def advance_window(
    day_changed: bool,
    prev_window_from: datetime,
    prev_window_to: datetime,
    now: datetime,
    poll_interval_seconds: int,
    had_new_sessions: bool,
    newest_start: Optional[datetime],
) -> tuple[datetime, datetime]:
    """Return the next (from, to) query cursor.

    On a new UTC day, from resets to midnight and to is now.
    Same day: from advances by 1 hour for every 2 hours elapsed since
    midnight (computed from the current time, not poll count, so it stays
    correct even if polls are missed). to steps by one poll interval
    (capped at now). Forwarded sessions do not move from — the sent-id
    set absorbs the overlap.
    """
    if day_changed:
        return day_boundary(now), now

    stepped = prev_window_to + timedelta(seconds=poll_interval_seconds)
    if stepped > now:
        stepped = now
    # Compute the sliding window start; never move it backward
    window_from = compute_window_from(now)
    if window_from < prev_window_from:
        window_from = prev_window_from
    if stepped < window_from:
        stepped = window_from
    return window_from, stepped


def select_unsent(sessions: list[dict], already_sent: set[tuple[str, str, str]]) -> list[dict]:
    """Keep sessions whose session_id was not forwarded in the retention window.

    Sessions with no session_id are dropped: they cannot be deduped safely.
    """
    chosen = []
    for session in sessions:
        sid = session.get("session_id")
        host = session.get("host", "")
        agent = session.get("agent", "")
        if not sid or (sid, host, agent) in already_sent:
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

    Each UTC day starts the query at 00:00:00; the from cursor advances by 1h
    every 2h (sliding safety buffer) and to steps forward by one poll interval.
    Forwarded session ids are remembered for 24h so an overlapping end never
    resends. A failed KOI fetch or XSIAM send leaves the cursor and the sent-id
    set unchanged.
    """
    now = get_utc_now()
    state_manager.prune_sent_ids(older_than_hours=MAX_WINDOW_HOURS)

    prev_from_s, prev_to_s = state_manager.get_window()
    prev_from = parse_iso_timestamp(prev_from_s) if prev_from_s else None
    prev_to = parse_iso_timestamp(prev_to_s) if prev_to_s else None
    day_changed = prev_from is None or prev_from.date() != now.date()

    start_dt = day_boundary(now)
    if prev_to is not None:
        end_dt = prev_to + timedelta(seconds=POLL_INTERVAL)
    else:
        end_dt = now
    if end_dt > now:
        end_dt = now

    sessions = fetch_sessions_for_window(koi_client, start_dt, end_dt)
    already_sent = state_manager.get_sent_ids_within(hours=MAX_WINDOW_HOURS)
    new_sessions = select_unsent(sessions, already_sent)

    if new_sessions:
        for session in new_sessions:
            if "start_time" in session:
                session["_time"] = session["start_time"]
            session["source_log_type"] = "koi_agent_activity_session"

        logger.info(
            f"Forwarding {len(new_sessions)} new sessions to XSIAM "
            f"(of {len(sessions)} fetched in {iso_format(start_dt)} -> {iso_format(end_dt)})"
        )
        if not xsiam_client.send_events(new_sessions):
            logger.error("Failed to send events to XSIAM — window and sent-ids will NOT be updated")
            return

        sent_at = iso_format(now)
        state_manager.record_sent_ids(new_sessions, sent_at=sent_at)
    else:
        logger.info(
            f"No new sessions in {iso_format(start_dt)} -> {iso_format(end_dt)}; "
            f"keeping from, advancing to"
        )

    new_from, new_to = advance_window(
        day_changed=day_changed,
        prev_window_from=prev_from or now,
        prev_window_to=prev_to or (now - timedelta(seconds=POLL_INTERVAL)),
        now=now,
        poll_interval_seconds=POLL_INTERVAL,
        had_new_sessions=bool(new_sessions),
        newest_start=None,
    )
    state_manager.save_window(iso_format(new_from), iso_format(new_to))
    # Keep the legacy key for rollback compatibility; it is not used for cursor logic.
    state_manager.save_state(hwm=iso_format(new_from), seen_ids=set())
    logger.info(f"Window: {iso_format(new_from)} -> {iso_format(new_to)}")


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
