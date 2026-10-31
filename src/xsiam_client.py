"""
Cortex XSIAM HTTP Log Collector client.

API: https://api-{tenant_external_url}/logs/v1/event
Auth: Single header:
  - Authorization: API key token
Content-Type: application/json (uncompressed) or application/gzip (compressed)
Body: Newline-delimited JSON (one event per line), optionally GZIP compressed

The XSIAM HTTP collector is configured with GZIP compression and JSON format,
so we compress the NDJSON body and set Content-Encoding: gzip.
"""

import requests
import logging
import json
import gzip

from .exceptions import TransientError

logger = logging.getLogger(__name__)


class XsiamHttpCollectorClient:
    """Client for sending events to the Cortex XSIAM HTTP Log Collector.

    Authentication:
    XSIAM HTTP collector uses a single Authorization header with the API key token.
    The token is obtained from the XSIAM UI (Data Sources & Integrations > HTTP Collector).

    Compression:
    The collector configuration determines whether gzip is used.
    Currently the collector is configured with GZIP compression, so
    the body is gzip-compressed and Content-Type is application/gzip.
    """

    def __init__(
        self,
        url: str,
        api_key: str,
        timeout: int = 30,
        compression: str = "gzip",
    ):
        """
        Initialize the XSIAM HTTP collector client.

        Args:
            url: XSIAM HTTP collector endpoint URL (e.g., https://api-{tenant}/logs/v1/event)
            api_key: API key token (sent as Authorization header)
            timeout: HTTP request timeout in seconds
            compression: Compression method ("gzip" or None for uncompressed)
        """
        self.url = url.rstrip("/")
        self.api_key = api_key
        self.timeout = timeout
        self.compression = compression

    def send_events(self, events: list[dict]) -> bool:
        """Send a batch of events to the XSIAM HTTP collector.

        Events are sent as newline-delimited JSON (NDJSON format).
        When compression is enabled, the body is gzip-compressed.

        The request body can contain up to 10MB uncompressed (1MB recommended
        per XSIAM docs).

        Args:
            events: List of event dictionaries to send

        Returns:
            True if successful, False if the collector returned a 4xx error
            (permanent failure). Raises TransientError on 5xx or network errors.
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
            "Authorization": self.api_key,
        }

        # Apply compression based on collector configuration
        if self.compression == "gzip":
            compressed_body = gzip.compress(body.encode("utf-8"))
            headers["Content-Type"] = "application/gzip"
            headers["Content-Encoding"] = "gzip"
            payload = compressed_body
            logger.debug(
                f"Sending {len(events)} events to XSIAM "
                f"(compressed: {len(body)} -> {len(payload)} bytes)"
            )
        else:
            headers["Content-Type"] = "application/json"
            payload = body.encode("utf-8")
            logger.debug(f"Sending {len(events)} events to XSIAM ({len(payload)} bytes)")

        try:
            response = requests.post(
                self.url,
                headers=headers,
                data=payload,
                timeout=self.timeout,
            )
            response.raise_for_status()
            logger.info(f"Successfully sent {len(events)} events to XSIAM")
            return True
        except requests.exceptions.HTTPError as e:
            status = e.response.status_code
            if status == 429 or 500 <= status < 600:
                logger.error(
                    f"XSIAM transient error sending events: {status} {e.response.text}"
                )
                raise TransientError(f"XSIAM transient error: {status}")
            else:  # 4xx other than 429
                logger.error(
                    f"XSIAM permanent error sending events: {status} {e.response.text}"
                )
                return False
        except requests.exceptions.RequestException as e:
            logger.error(f"XSIAM request error: {e}")
            raise TransientError(str(e))
