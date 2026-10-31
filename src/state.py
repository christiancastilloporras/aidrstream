"""
SQLite-based state persistence for the KOI-to-XSIAM poller.

Stores the high-water mark (HWM) timestamp and deduplication cache
so sessions are never lost or duplicated between polls.
"""

import sqlite3
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

logger = logging.getLogger(__name__)


class StateManager:
    """Manages poller state using an SQLite database.

    State consists of:
    - last_hwm: The last-seen session start_time (ISO 8601 string)
    - last_seen_ids: JSON-encoded set of session_ids seen at the HWM timestamp
                     (prevents duplicates when multiple sessions share the same timestamp)
    """

    def __init__(self, db_path: str = "/data/state.db"):
        self.db_path = db_path

    def _get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def init(self) -> None:
        """Create the state table if it doesn't exist."""
        with self._get_connection() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS poller_state (
                    key   TEXT PRIMARY KEY,
                    value TEXT
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS sent_sessions (
                    session_id TEXT PRIMARY KEY,
                    sent_at    TEXT NOT NULL
                )
            """)
            conn.commit()
        logger.info(f"State database initialized at {self.db_path}")

    def get_last_hwm(self) -> Optional[str]:
        """Retrieve the high-water mark timestamp from the last successful poll.

        Returns:
            ISO 8601 string of the last processed session start_time,
            or None if no state has been recorded yet.
        """
        with self._get_connection() as conn:
            row = conn.execute(
                "SELECT value FROM poller_state WHERE key = ?", ("last_hwm",)
            ).fetchone()
            if row:
                logger.debug(f"Last HWM: {row['value']}")
                return row["value"]
        return None

    def get_last_seen_ids(self) -> set[str]:
        """Retrieve the set of session IDs seen at the HWM timestamp.

        Returns:
            Set of session_id strings. Empty set if none.
        """
        with self._get_connection() as conn:
            row = conn.execute(
                "SELECT value FROM poller_state WHERE key = ?", ("last_seen_ids",)
            ).fetchone()
            if row and row["value"]:
                return set(row["value"].split(","))
        return set()

    def save_state(self, hwm: str, seen_ids: set[str]) -> None:
        """Persist the high-water mark and seen session IDs.

        Args:
            hwm: The new high-water mark timestamp (ISO 8601)
            seen_ids: Set of session IDs observed at this timestamp
        """
        ids_str = ",".join(sorted(seen_ids)) if seen_ids else ""

        with self._get_connection() as conn:
            conn.execute("""
                INSERT INTO poller_state (key, value)
                VALUES (?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """, ("last_hwm", hwm))
            conn.execute("""
                INSERT INTO poller_state (key, value)
                VALUES (?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """, ("last_seen_ids", ids_str))
            conn.commit()
        logger.info(f"State saved: HWM={hwm}, seen_ids_count={len(seen_ids)}")

    def get_window(self) -> tuple[Optional[str], Optional[str]]:
        """Return (window_from, window_to). Falls back to last_hwm for from."""
        with self._get_connection() as conn:
            rows = conn.execute(
                "SELECT key, value FROM poller_state WHERE key IN ('window_from', 'window_to', 'last_hwm')"
            ).fetchall()
        found = {row["key"]: row["value"] for row in rows}
        return found.get("window_from") or found.get("last_hwm"), found.get("window_to")

    def save_window(self, window_from: str, window_to: str) -> None:
        """Persist the query cursor. Does not imply sessions were forwarded."""
        with self._get_connection() as conn:
            for key, val in (("window_from", window_from), ("window_to", window_to)):
                conn.execute(
                    """
                    INSERT INTO poller_state (key, value)
                    VALUES (?, ?)
                    ON CONFLICT(key) DO UPDATE SET value = excluded.value
                    """,
                    (key, val),
                )
            conn.commit()
        logger.info(f"Window saved: from={window_from} to={window_to}")

    def record_sent_ids(self, session_ids: list[str], sent_at: str) -> None:
        """Remember ids forwarded at sent_at. Repeat inserts keep the newest sent_at."""
        rows = [(sid, sent_at) for sid in session_ids if sid]
        if not rows:
            return
        with self._get_connection() as conn:
            conn.executemany(
                """
                INSERT INTO sent_sessions (session_id, sent_at)
                VALUES (?, ?)
                ON CONFLICT(session_id) DO UPDATE SET sent_at = excluded.sent_at
                """,
                rows,
            )
            conn.commit()

    def get_sent_ids_within(self, hours: int = 24) -> set[str]:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
        cutoff_s = cutoff.strftime("%Y-%m-%dT%H:%M:%SZ")
        with self._get_connection() as conn:
            rows = conn.execute(
                "SELECT session_id FROM sent_sessions WHERE sent_at >= ?",
                (cutoff_s,),
            ).fetchall()
        return {row["session_id"] for row in rows}

    def prune_sent_ids(self, older_than_hours: int = 24) -> int:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=older_than_hours)
        cutoff_s = cutoff.strftime("%Y-%m-%dT%H:%M:%SZ")
        with self._get_connection() as conn:
            cur = conn.execute(
                "DELETE FROM sent_sessions WHERE sent_at < ?",
                (cutoff_s,),
            )
            conn.commit()
            return cur.rowcount
