"""
Exoplanet Ingestion Client
Source: NASA Exoplanet Archive TAP interface
Purpose: Fetch confirmed exoplanet records with key scientific attributes.
Supports incremental fetch via rowupdate column to avoid full re-downloads.
"""
import os, logging, time, requests
from datetime import datetime, timezone
from dataclasses import dataclass, field
from typing import List, Optional

logger = logging.getLogger(__name__)

TAP_URL = "https://exoplanetarchive.ipac.caltech.edu/TAP/sync"

COLUMNS = (
    "pl_name,hostname,disc_year,discoverymethod,"
    "pl_rade,pl_bmasse,pl_orbper,sy_dist,rowupdate"
)

@dataclass
class ExoplanetRecord:
    pl_name: str
    hostname: Optional[str] = None
    disc_year: Optional[int] = None
    discoverymethod: Optional[str] = None
    pl_rade: Optional[float] = None       # Radius in Earth radii
    pl_bmasse: Optional[float] = None     # Mass in Earth masses
    pl_orbper: Optional[float] = None     # Orbital period in days
    sy_dist: Optional[float] = None       # Distance in parsecs
    rowupdate: Optional[str] = None
    _ingested_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> dict:
        return {
            "pl_name": self.pl_name,
            "hostname": self.hostname,
            "disc_year": self.disc_year,
            "discoverymethod": self.discoverymethod,
            "pl_rade": self.pl_rade,
            "pl_bmasse": self.pl_bmasse,
            "pl_orbper": self.pl_orbper,
            "sy_dist": self.sy_dist,
            "rowupdate": self.rowupdate,
            "_ingested_at": self._ingested_at,
        }


def _safe_float(val) -> Optional[float]:
    try:
        return float(val) if val not in (None, "", "nan") else None
    except (ValueError, TypeError):
        return None


def _safe_int(val) -> Optional[int]:
    try:
        return int(val) if val not in (None, "", "nan") else None
    except (ValueError, TypeError):
        return None


def fetch_exoplanets(since_date: Optional[str] = None, max_retries: int = 3) -> List[ExoplanetRecord]:
    """
    Fetch exoplanet records from NASA Exoplanet Archive.
    Args:
        since_date: ISO date string (YYYY-MM-DD). If provided, only fetch records
                    updated since this date (incremental mode).
        max_retries: Number of retry attempts on failure.
    Returns:
        List of ExoplanetRecord dataclass instances.
    """
    where = "default_flag=1"
    if since_date:
        where += f" AND rowupdate>='{since_date}'"
        logger.info(f"Incremental fetch: records updated since {since_date}")
    else:
        logger.info("Full fetch: all confirmed exoplanets")

    adql = f"SELECT {COLUMNS} FROM ps WHERE {where}"
    params = {"query": adql, "format": "json"}

    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(TAP_URL, params=params, timeout=120)
            resp.raise_for_status()
            rows = resp.json()
            records = []
            for r in rows:
                if not r.get("pl_name"):
                    continue
                records.append(ExoplanetRecord(
                    pl_name=r["pl_name"],
                    hostname=r.get("hostname"),
                    disc_year=_safe_int(r.get("disc_year")),
                    discoverymethod=r.get("discoverymethod", "").lower().strip() or None,
                    pl_rade=_safe_float(r.get("pl_rade")),
                    pl_bmasse=_safe_float(r.get("pl_bmasse")),
                    pl_orbper=_safe_float(r.get("pl_orbper")),
                    sy_dist=_safe_float(r.get("sy_dist")),
                    rowupdate=r.get("rowupdate"),
                ))
            logger.info(f"Fetched {len(records)} exoplanet records")
            return records
        except requests.RequestException as e:
            logger.warning(f"Attempt {attempt}/{max_retries} failed: {e}")
            if attempt < max_retries:
                time.sleep(2 ** attempt)
            else:
                raise

    return []
