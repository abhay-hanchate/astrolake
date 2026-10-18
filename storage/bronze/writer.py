"""
Bronze Layer Storage Utilities
Writes raw ingested records to the local data lake as Parquet files.
Layer purpose: Preserve original source data exactly as received.

The data lake lives on local disk (data_lake/{bronze,silver,gold}/) so the
whole pipeline runs without Docker, MinIO, or any object-store daemon.
"""
import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)

DATA_LAKE_ROOT = Path(__file__).resolve().parents[2] / "data_lake"
BRONZE_ROOT = DATA_LAKE_ROOT / "bronze"


def write_bronze(records: list[dict], source: str, batch_id: str) -> str:
    """
    Write raw records to the local bronze layer.
    Path: data_lake/bronze/{source}/batch_{batch_id}/data.parquet
    Returns the absolute path written, or "" if there was nothing to write.
    """
    if not records:
        logger.warning(f"No records to write for {source}")
        return ""

    df = pd.DataFrame(records)
    df["_source"] = source
    df["_batch_id"] = batch_id
    df["_ingestion_ts"] = datetime.now(timezone.utc).isoformat()

    out_dir = BRONZE_ROOT / source / f"batch_{batch_id}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "data.parquet"

    table = pa.Table.from_pandas(df)
    pq.write_table(table, out_path)

    logger.info(f"Bronze: wrote {len(records)} records -> {out_path}")
    return str(out_path)
