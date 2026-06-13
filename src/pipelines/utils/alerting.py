"""
src/pipelines/utils/alerting.py

Discord webhook alerting for pipeline events.
Non-fatal — if webhook is not configured or the request fails, the pipeline continues.
"""

import logging
import os
from datetime import datetime

import requests

logger = logging.getLogger(__name__)

_COLORS = {
    "success":  0x2ECC71,
    "info":     0x3498DB,
    "warning":  0xF39C12,
    "critical": 0xE74C3C,
}


def send_discord_alert(
    title: str,
    message: str,
    level: str = "warning",
    webhook_url: str | None = None,
) -> None:
    """Send a Discord embedded alert. Silently skips if no webhook is configured."""
    url = webhook_url or os.getenv("DISCORD_WEBHOOK_URL", "")
    if not url:
        logger.debug("[alerting] No Discord webhook configured — skipping.")
        return

    payload = {
        "embeds": [{
            "title":       title,
            "description": message,
            "color":       _COLORS.get(level, _COLORS["warning"]),
            "footer":      {
                "text": f"fraud-detection · {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}"
            },
        }]
    }
    try:
        resp = requests.post(url, json=payload, timeout=5)
        resp.raise_for_status()
        logger.info(f"[alerting] Discord alert sent: {title!r}")
    except Exception as exc:
        logger.warning(f"[alerting] Discord alert failed (non-fatal): {exc}")
