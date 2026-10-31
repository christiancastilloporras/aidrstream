"""KOI API client for fetching agent activity sessions.

API: https://api.prod.koi.security/api/external/v2/agent-activity/sessions
Auth: Bearer token (API key)
"""

import requests
import logging
from typing import Optional

from .exceptions import TransientError

logger = logging.getLogger(__name__)


class KoiApiClient:
    """Client for the KOI agent-activity/sessions API endpoint."""

    def __init__(
        self,
        base_url: str,
        api_key: str,
        page_size: int = 100,
        timeout: int = 30,
    ):
        """Initialize the KOI API client.

        Args:
            base_url: KOI API base URL (e.g., https://api.prod.koi.security/api/external/v2)
            api_key: Bearer token for authentication
            page_size: Number of results per page (default 100, max 500)
            timeout: HTTP request timeout in seconds
        """
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.page_size = min(page_size, 500)
        self.timeout = timeout

    def get_sessions_page(
        self,
        created_at_gte: str,
        created_at_lte: Optional[str] = None,
        page: int = 1,
        **filters: str,
    ) -> dict:
        """Fetch a single page of sessions from the KOI API.

        Args:
            created_at_gte: ISO 8601 timestamp — sessions created at or after this time
            created_at_lte: ISO 8601 timestamp — sessions created at or before this time
            page: Page number (1-based)
            **filters: Optional filter parameters (agent, host, user_email, etc.)

        Returns:
            Raw JSON response dict: {"data": [...], "total_count": N}
        """
        url = f"{self.base_url}/agent-activity/sessions"
        params: dict = {
            "created_at_gte": created_at_gte,
            "page": page,
            "page_size": self.page_size,
        }
        if created_at_lte:
            params["created_at_lte"] = created_at_lte
        # Add any additional filters
        params.update(filters)

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Accept": "application/json",
        }

        logger.debug(f"KOI API GET {url} params={params}")
        try:
            response = requests.get(url, headers=headers, params=params, timeout=self.timeout)
        except requests.exceptions.RequestException as e:
            logger.warning(f"KOI API request exception: {e}")
            raise TransientError(str(e)) from e

        if response.status_code == 429:
            # Rate limited — let the caller handle retry
            logger.warning(f"KOI API rate limited (429) on page {page}")
            raise TransientError(f"KOI API rate limited (429) on page {page}")

        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            # 5xx or other 4xx besides 429
            logger.warning(f"KOI API HTTP error: {e.response.status_code} {e.response.text}")
            raise TransientError(str(e)) from e

        result = response.json()
        logger.debug(f"KOI API response: {len(result.get('data', []))} sessions on page {page}")
        return result

    def get_sessions_since(
        self,
        created_after: str,
        created_before: Optional[str] = None,
        max_pages: int = 100,
    ) -> list[dict]:
        """Fetch all sessions since a given timestamp, auto-paginating.

        Args:
            created_after: ISO 8601 timestamp for created_at_gte filter
            created_before: ISO 8601 timestamp for created_at_lte filter
            max_pages: Safety limit to prevent infinite loops

        Returns:
            List of session dicts
        """
        all_sessions: list[dict] = []
        page = 1

        while page <= max_pages:
            result = self.get_sessions_page(
                created_at_gte=created_after,
                created_at_lte=created_before,
                page=page,
            )

            sessions = result.get("data", [])
            if not sessions:
                break

            all_sessions.extend(sessions)
            logger.info(f"Fetched page {page}: {len(sessions)} sessions (total: {len(all_sessions)})")

            # If we got fewer than page_size, we've exhausted all results
            if len(sessions) < self.page_size:
                break

            page += 1

        return all_sessions