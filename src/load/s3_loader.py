"""
S3 data-lake loader (raw + processed layers).

Design goal: the project must run even with NO AWS credentials. So every
function degrades gracefully — if credentials/bucket are missing or boto3
isn't installed, we log a warning and return without raising. The local
filesystem (mounted ./data) remains the source of truth for v1.

Layers used:
    s3://<bucket>/raw/<dataset>/<date>/<file>          (raw extracted JSON)
    s3://<bucket>/processed/air_quality_metrics/<date> (curated parquet/csv)
"""
from __future__ import annotations

import logging
import os
from typing import Optional

from src.utils.config import settings

logger = logging.getLogger(__name__)


def _client():
    """Build a boto3 S3 client, or return None if not possible."""
    if not settings.has_aws_credentials:
        logger.warning("AWS credentials / bucket not configured — skipping S3 upload.")
        return None
    try:
        import boto3  # imported lazily so the project works without boto3 installed

        return boto3.client(
            "s3",
            aws_access_key_id=settings.aws_access_key_id,
            aws_secret_access_key=settings.aws_secret_access_key,
            region_name=settings.aws_region,
        )
    except Exception as exc:  # noqa: BLE001 - never let S3 break the pipeline
        logger.warning("Could not create S3 client: %s", exc)
        return None


def upload_file(local_path: str, s3_key: str) -> Optional[str]:
    """Upload a single file. Returns the s3:// URI on success, else None."""
    client = _client()
    if client is None:
        return None
    if not os.path.exists(local_path):
        logger.warning("Local file not found, cannot upload: %s", local_path)
        return None
    try:
        client.upload_file(local_path, settings.s3_bucket_name, s3_key)
        uri = f"s3://{settings.s3_bucket_name}/{s3_key}"
        logger.info("Uploaded %s -> %s", local_path, uri)
        return uri
    except Exception as exc:  # noqa: BLE001
        logger.warning("S3 upload failed for %s: %s", local_path, exc)
        return None


def upload_directory(local_dir: str, s3_prefix: str) -> int:
    """Recursively upload every file under ``local_dir``. Returns files uploaded."""
    client = _client()
    if client is None:
        return 0
    uploaded = 0
    for root, _dirs, files in os.walk(local_dir):
        for name in files:
            full = os.path.join(root, name)
            rel = os.path.relpath(full, local_dir).replace(os.sep, "/")
            key = f"{s3_prefix.rstrip('/')}/{rel}"
            if upload_file(full, key):
                uploaded += 1
    logger.info("Uploaded %d files from %s to s3://%s/%s", uploaded, local_dir, settings.s3_bucket_name, s3_prefix)
    return uploaded
