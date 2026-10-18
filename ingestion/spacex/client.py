"""SpaceX launch ingestion client backed by Launch Library 2.

The former r/SpaceX API was archived in June 2026 and its hosted endpoint is
unavailable. Launch records now come from the maintained open database operated
by The Space Devs, filtered to SpaceX's launch-service-provider ID (121).
"""
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Optional

import requests

logger = logging.getLogger(__name__)

BASE_URL = "https://ll.thespacedevs.com/2.3.0/launches/"
DEV_BASE_URL = "https://lldev.thespacedevs.com/2.3.0/launches/"
ARCHIVE_BASE_URL = (
    "https://raw.githubusercontent.com/nisrulz/android-spacex-app/"
    "main/api-server/server/data/"
)
SPACEX_AGENCY_ID = 121
USER_AGENT = "AstroLake/1.0 (educational space analytics dashboard)"


@dataclass
class LaunchRecord:
    launch_id: str
    mission_name: Optional[str] = None
    launch_date: Optional[str] = None
    rocket_id: Optional[str] = None
    rocket_name: Optional[str] = None
    launchpad_id: Optional[str] = None
    launchpad_name: Optional[str] = None
    launchpad_locality: Optional[str] = None
    outcome: Optional[str] = None
    flight_number: Optional[int] = None
    details: Optional[str] = None
    _ingested_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> dict:
        return {
            "launch_id": self.launch_id,
            "mission_name": self.mission_name,
            "launch_date": self.launch_date,
            "rocket_id": self.rocket_id,
            "rocket_name": self.rocket_name,
            "launchpad_id": self.launchpad_id,
            "launchpad_name": self.launchpad_name,
            "launchpad_locality": self.launchpad_locality,
            "outcome": self.outcome,
            "flight_number": self.flight_number,
            "details": (self.details or "")[:1000],
            "_ingested_at": self._ingested_at,
        }


def _outcome(status: dict) -> str:
    label = f"{status.get('abbrev', '')} {status.get('name', '')}".lower()
    if "success" in label and "failure" not in label:
        return "SUCCESS"
    if "failure" in label or "failed" in label:
        return "FAILURE"
    return "UNKNOWN"


class LaunchLibraryRateLimitError(RuntimeError):
    pass


def _get_page(params: dict, base_url: str = BASE_URL, retries: int = 3) -> dict:
    headers = {"User-Agent": USER_AGENT}
    api_token = os.getenv("LAUNCH_LIBRARY_TOKEN")
    if api_token:
        headers["Authorization"] = f"Token {api_token}"

    for attempt in range(1, retries + 1):
        try:
            response = requests.get(base_url, params=params, headers=headers, timeout=90)
            if response.status_code == 429:
                raise LaunchLibraryRateLimitError("Launch Library anonymous quota exhausted")
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict) or "results" not in payload:
                raise ValueError("Launch Library returned an unexpected response")
            return payload
        except LaunchLibraryRateLimitError:
            raise
        except (requests.RequestException, ValueError) as exc:
            logger.warning("Launch Library attempt %s/%s failed: %s", attempt, retries, exc)
            if attempt == retries:
                raise
            time.sleep(2 ** attempt)
    return {"results": []}


def _fetch_archived_launches() -> List[LaunchRecord]:
    """Load the archived r/SpaceX dataset when the LL2 dev mirror is limited.

    The mirror contains the original 205-record v5 response plus the matching
    rocket and launchpad resources. `launch_library_id` is used when available
    so a later production LL2 refresh replaces, rather than duplicates, rows.
    """
    headers = {"User-Agent": USER_AGENT}

    def fetch_json(filename: str):
        response = requests.get(ARCHIVE_BASE_URL + filename, headers=headers, timeout=90)
        response.raise_for_status()
        return response.json()

    try:
        launches = fetch_json("launches_v5.json")
        rockets = {item["id"]: item.get("name") for item in fetch_json("rockets.json")}
        pads = {item["id"]: item for item in fetch_json("launchpads.json")}
    except (requests.RequestException, ValueError, KeyError) as exc:
        logger.warning("Could not load the archived r/SpaceX snapshot: %s", exc)
        return []

    records = []
    for row in launches:
        launch_id = row.get("launch_library_id") or row.get("id")
        if not launch_id:
            continue
        success = row.get("success")
        outcome = "SUCCESS" if success is True else "FAILURE" if success is False else "UNKNOWN"
        pad = pads.get(row.get("launchpad"), {})
        records.append(LaunchRecord(
            launch_id=str(launch_id),
            mission_name=row.get("name"),
            launch_date=row.get("date_utc"),
            rocket_id=row.get("rocket"),
            rocket_name=rockets.get(row.get("rocket")),
            launchpad_id=row.get("launchpad"),
            launchpad_name=pad.get("name") or pad.get("full_name"),
            launchpad_locality=pad.get("locality"),
            outcome=outcome,
            flight_number=row.get("flight_number"),
            details=row.get("details"),
        ))
    return records


def fetch_launches() -> List[LaunchRecord]:
    """Fetch all SpaceX launches through today from Launch Library 2."""
    records: List[LaunchRecord] = []
    offset = 0
    limit = 100
    base_url = BASE_URL
    today_utc = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    while True:
        params = {
            "lsp__id": SPACEX_AGENCY_ID,
            "net__lte": today_utc,
            "ordering": "net",
            "mode": "normal",
            "limit": limit,
            "offset": offset,
        }
        try:
            payload = _get_page(params, base_url=base_url)
        except LaunchLibraryRateLimitError:
            if base_url == DEV_BASE_URL:
                raise
            logger.warning("Production quota reached; continuing with the official LL2 development mirror")
            base_url = DEV_BASE_URL
            payload = _get_page(params, base_url=base_url)
        rows = payload.get("results", [])
        for row in rows:
            launch_id = row.get("id")
            if not launch_id:
                continue
            rocket = (row.get("rocket") or {}).get("configuration") or {}
            pad = row.get("pad") or {}
            location = pad.get("location") or {}
            mission = row.get("mission") or {}
            details = mission.get("description") or row.get("failreason") or ""
            records.append(LaunchRecord(
                launch_id=str(launch_id),
                mission_name=row.get("name") or mission.get("name"),
                launch_date=row.get("net"),
                rocket_id=str(rocket.get("id")) if rocket.get("id") is not None else None,
                rocket_name=rocket.get("full_name") or rocket.get("name"),
                launchpad_id=str(pad.get("id")) if pad.get("id") is not None else None,
                launchpad_name=pad.get("name"),
                launchpad_locality=location.get("name"),
                outcome=_outcome(row.get("status") or {}),
                flight_number=row.get("agency_launch_attempt_count"),
                details=details,
            ))

        offset += len(rows)
        if not rows or offset >= int(payload.get("count", offset)):
            break

    if base_url == DEV_BASE_URL:
        archived = _fetch_archived_launches()
        merged = {record.launch_id: record for record in archived}
        merged.update({record.launch_id: record for record in records})
        records = sorted(merged.values(), key=lambda record: record.launch_date or "")

    logger.info("Fetched %s SpaceX launch records", len(records))
    return records
