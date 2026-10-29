"""
Cortex XSIAM HTTP Log Collector client.

API: https://api-{tenant_external_url}/logs/v1/event
Auth: Two headers required:
  - x-xdr-auth-id: API key ID (numeric)
  - Authorization: API secret value
Content-Type: application/json (or text/plain for Raw format)
Body: Newline-delimited JSON (one event per line)
"""

import requests
import logging
import json
from typing import Optional

logger = logging.getLogger(__name__)


class XsiamHttpCollectorClient:
    """Client for sending events to the Cortex XSIAM HTTP Log Collector.

    Authentication:
    XSIAM HTTP collector requires two headers:
    - x-xdr-auth-id: The API key ID (numeric identifier)
    - Authorization: The API key secret value

    These are obtained from the XSIAM UI when you generate an API key
    (Settings → Integrations → API Keys).
    """

    def __init__(
        self,
        url: str,
        api_key_id: str,
        api_key: str,
        timeout: int = 30,
    ):
        """
        Initialize the XSIAM HTTP collector client.

        Args:
            url: XSIAM HTTP collector endpoint URL (e.g., https://api-{tenant}/logs/v1/event)
            api_key_id: API key ID (sent as x-xdr-auth-id header)
            api_key: API key secret (sent as Authorization header)
            timeout: HTTP request timeout in seconds
        """
        self.url = url.rstrip("/")
        self.api_key_id = api_key_id
        self.api_key = api_key
        self.timeout = timeout

    def send_events(self, events: list[dict]) -> bool:
        """Send a batch of events to the XSIAM HTTP collector.

        Events are sent as newline-delimited JSON (NDJSON format).
        The request body can contain up to 10MB (1MB recommended per XSIAM docs).

        Args:
            events: List of event dictionaries to send

        Returns:
            True if successful, False otherwise
        """
        if not events:
            return True

        # Build NDJSON body: one JSON object per line
        body_lines = []
        for event in events:
            # Ensure the event has a _time field for XSIAM timestamping
            if "_time" not in event and "start_time" in event:
                event["_time"] = event["start_time"]
            # Add source identifier
            if "source_log_type" not in event:
                event["source_log_type"] = "koi_agent_activity_session"
            body_lines.append(json.dumps(event))

        body = "\n".join(body_lines)

        headers = {
            "x-xdr-auth-id": str(self.api_key_id),
            "Authorization": self.api_key,
            "Content-Type": "application/json",
        }

        logger.debug(f"Sending {len(events)} events to XSIAM ({len(body)} bytes)")
        try:
            response = requests.post(
                self.url,
                headers=headers,
                data=body.encode("utf-8"),
                timeout=self.timeout,
            )
            response.raise_for_status()
            logger.info(f"Successfully sent {len(events)} events to XSIAM")
            return True
        except requests.exceptions.HTTPError as e:
            logger.error(f"XSIAM HTTP error sending events: {e.response.status_code} {e.response.text}")
            return False
        except requests.exceptions.RequestException as e:
            logger.error(f"XSIAM request error: {e}")
            return False
