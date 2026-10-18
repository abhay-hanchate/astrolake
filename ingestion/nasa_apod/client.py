"""NASA Astronomy Picture of the Day ingestion client.

NASA migrated APOD from api.nasa.gov to the NASA Science WordPress API in
September 2026. The collection is paginated; date filtering is done locally
because the new endpoint currently ignores the legacy range parameters.
"""
import html
import logging
import re
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import List, Optional

import requests

logger = logging.getLogger(__name__)

APOD_URL = "https://science.nasa.gov/wp-json/wp/v2/apod-basic/"
USER_AGENT = "AstroLake/1.0 (educational space analytics dashboard)"


@dataclass
class APODRecord:
    apod_date: str
    title: Optional[str] = None
    media_type: Optional[str] = None
    url: Optional[str] = None
    hdurl: Optional[str] = None
    explanation: Optional[str] = None
    copyright: Optional[str] = None
    _ingested_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> dict:
        return {
            "apod_date": self.apod_date,
            "title": self.title,
            "media_type": self.media_type,
            "url": self.url,
            "hdurl": self.hdurl,
            "explanation": self.explanation,
            "copyright": self.copyright,
            "_ingested_at": self._ingested_at,
        }


def _plain_text(value: Optional[str]) -> Optional[str]:
    """Convert the migrated feed's HTML-rich fields to safe readable text."""
    if not value:
        return None
    value = re.sub(r"<[^>]+>", " ", value)
    return " ".join(html.unescape(value).split())


def fetch_apod_range(start_date: str, end_date: str, max_retries: int = 3) -> List[APODRecord]:
    """Fetch APOD metadata for an inclusive ISO-date range."""
    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    if start > end:
        raise ValueError("start_date must be on or before end_date")

    logger.info("Fetching APOD metadata from %s to %s", start_date, end_date)
    records: List[APODRecord] = []
    page = 1

    while True:
        rows = None
        for attempt in range(1, max_retries + 1):
            try:
                response = requests.get(
                    APOD_URL,
                    params={"page": page, "per_page": 25},
                    headers={"User-Agent": USER_AGENT},
                    timeout=90,
                )
                response.raise_for_status()
                rows = response.json()
                break
            except requests.RequestException as exc:
                logger.warning("APOD page %s attempt %s/%s failed: %s", page, attempt, max_retries, exc)
                if attempt == max_retries:
                    raise
                time.sleep(2 ** attempt)

        if not isinstance(rows, list) or not rows:
            break

        valid_dates = []
        for row in rows:
            try:
                row_date = date.fromisoformat(row.get("date", ""))
            except (TypeError, ValueError):
                continue
            valid_dates.append(row_date)
            if not start <= row_date <= end:
                continue

            media_type = row.get("media_type", "other")
            hdurl = row.get("hdurl")
            # `url` is now an article permalink, not an image asset. Preserve
            # the dashboard's media-url contract with the full-size asset.
            media_url = hdurl if media_type == "image" and hdurl else row.get("url")
            records.append(APODRecord(
                apod_date=row_date.isoformat(),
                title=_plain_text(row.get("title")),
                media_type=media_type,
                url=media_url,
                hdurl=hdurl,
                explanation=(_plain_text(row.get("explanation")) or "")[:2000],
                copyright=_plain_text(row.get("copyright") or row.get("credit")),
            ))

        if valid_dates and min(valid_dates) <= start:
            break
        page += 1

    records.sort(key=lambda record: record.apod_date)
    logger.info("Fetched %s APOD records", len(records))
    return records


def default_date_range(days_back: int = 90):
    """Return an inclusive date range ending today."""
    end = date.today()
    start = end - timedelta(days=days_back)
    return start.isoformat(), end.isoformat()
