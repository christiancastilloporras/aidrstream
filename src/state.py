"""SQLite-based state persistence for the KOI-to-XSIAM poller.

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
            conn.execute("DROP TABLE IF EXISTS sent_sessions")
            conn.execute("""
                CREATE TABLE sent_sessions (
                    session_id TEXT NOT NULL,
                    host TEXT NOT NULL DEFAULT '',
                    agent TEXT NOT NULL DEFAULT '',
                    sent_at TEXT NOT NULL,
                    PRIMARY KEY (session_id, host, agent)
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
                "SELECT value FROM poller_state WHERE key = 'last_hwm'"
            ).fetchone()
        return row["value"] if row else None

    def get_last_seen_ids(self) -> set[str]:
        """Retrieve the set of session_ids seen at the HWM timestamp.

        Returns:
            A set of session_id strings.
        """
        with self._get_connection() as conn:
            row = conn.execute(
                "SELECT value FROM poller_state WHERE key = 'last_seen_ids'"
            ).fetchone()
        if row and row["value"]:
            import json
            return set(json.loads(row["value"]))
        return set()

    def save_state(self, hwm: str, seen_ids: set[str]) -> None:
        """Save the high-water mark and the set of session_ids seen at that HWM."""
        import json
        with self._get_connection() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO poller_state (key, value) VALUES (?, ?)",
                ("last_hwm", hwm),
            )
            conn.execute(
                "INSERT OR REPLACE INTO poller_state (key, value) VALUES (?, ?)",
                ("last_seen_ids", json.dumps(list(seen_ids))),
            )
            conn.commit()

    def get_window(self) -> tuple[Optional[str], Optional[str]]:
        """Retrieve the polling window (window_from, window_to) as ISO strings.

        Returns:
            Tuple of (window_from, window_to). Either can be None if not set.
        """
        with self._get_connection() as conn:
            row = conn.execute(
                "SELECT value FROM poller_state WHERE key = 'window_from'"
            ).fetchone()
            window_from = row["value"] if row else None
            row = conn.execute(
                "SELECT value FROM poller_state WHERE key = 'window_to'"
            ).fetchone()
            window_to = row["value"] if row else None
        return window_from, window_to

    def save_window(self, window_from: str, window_to: str) -> None:
        """Save the polling window (window_from, window_to) as ISO strings."""
        with self._get_connection() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO poller_state (key, value) VALUES (?, ?)",
                ("window_from", window_from),
            )
            conn.execute(
                "INSERT OR REPLACE INTO poller_state (key, value) VALUES (?, ?)",
                ("window_to", window_to),
            )
            conn.commit()

    def record_sent_ids(self, sessions: list[dict], sent_at: str) -> None:
        """Remember ids forwarded at sent_at. Repeat inserts keep the newest sent_at."""
        rows = []
        for session in sessions:
            sid = session.get("session_id")
            host = session.get("host", "")
            agent = session.get("agent", "")
            if sid:  # only record if we have a session_id
                rows.append((sid, host, agent, sent_at))
        if not rows:
            return
        with self._get_connection() as conn:
            conn.executemany(
                """INSERT INTO sent_sessions (session_id, host, agent, sent_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(session_id, host, agent) DO UPDATE SET sent_at = excluded.sent_at""",
                rows,
            )
            conn.commit()

    def get_sent_ids_within(self, hours: int = 24) -> set[tuple[str, str, str]]:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
        cutoff_s = cutoff.strftime("%Y-%m-%dT%H:%M:%SZ")
        with self._get_connection() as conn:
            rows = conn.execute(
                """SELECT session_id, host, agent FROM sent_sessions WHERE sent_at >= ?""",
                (cutoff_s,),
            ).fetchall()
        return {(row["session_id"], row["host"], row["agent"]) for row in rows}

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
