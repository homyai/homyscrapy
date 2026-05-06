"""One-time migration: convert JSON array files in GCS to NDJSON.

Each spider's raw output is a JSON array ([{...}, {...}, ...]).
BigQuery external tables (CSV trick) need NDJSON (one JSON object per line).

Usage:
    python scripts/migrate_json_to_ndjson.py [--dry-run] [--spiders encuent24,mls]

Backs up originals to gs://web-scraper-data/raw-archive-json-arrays/ before overwriting.
Skips files that are already NDJSON (first non-whitespace char is '{').
"""

import argparse
import json
import logging
import sys

from google.cloud import storage

BUCKET_NAME = "web-scraper-data"
ALL_SPIDERS = ["encuentra24", "inhaus", "mls", "recr", "cccbr", "fazwaz"]
BACKUP_PREFIX = "raw-archive-json-arrays"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def migrate_blob(blob: storage.Blob, bucket: storage.Bucket, dry_run: bool) -> bool:
    """Convert a single JSON array blob to NDJSON. Returns True if converted."""
    content = blob.download_as_text(encoding="utf-8")
    stripped = content.lstrip()

    if not stripped:
        log.warning("  SKIP (empty): %s", blob.name)
        return False

    if stripped[0] == "{":
        log.info("  SKIP (already NDJSON): %s", blob.name)
        return False

    if stripped[0] != "[":
        log.warning("  SKIP (unexpected format): %s", blob.name)
        return False

    try:
        records = json.loads(content)
    except json.JSONDecodeError as e:
        log.error("  ERROR (bad JSON): %s — %s", blob.name, e)
        return False

    if not isinstance(records, list):
        log.warning("  SKIP (not a list): %s", blob.name)
        return False

    if not records:
        log.info("  SKIP (empty array): %s", blob.name)
        return False

    ndjson = "\n".join(json.dumps(r, ensure_ascii=False) for r in records)

    if dry_run:
        log.info("  DRY-RUN would convert: %s (%d records)", blob.name, len(records))
        return True

    # Backup original
    backup_path = blob.name.replace("raw/", f"{BACKUP_PREFIX}/", 1)
    bucket.copy_blob(blob, bucket, backup_path)
    log.info("  Backed up → %s", backup_path)

    # Overwrite with NDJSON
    blob.upload_from_string(ndjson, content_type="application/json")
    log.info("  Converted: %s (%d records)", blob.name, len(records))
    return True


def main():
    parser = argparse.ArgumentParser(description="Migrate GCS JSON arrays to NDJSON")
    parser.add_argument("--dry-run", action="store_true", help="Preview without modifying")
    parser.add_argument(
        "--spiders",
        default=",".join(ALL_SPIDERS),
        help=f"Comma-separated spider names (default: {','.join(ALL_SPIDERS)})",
    )
    args = parser.parse_args()

    spiders = [s.strip() for s in args.spiders.split(",")]
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    total_converted = 0
    total_skipped = 0
    total_errors = 0

    for spider in spiders:
        prefix = f"raw/{spider}/"
        blobs = list(bucket.list_blobs(prefix=prefix))
        log.info("Spider '%s': %d files found", spider, len(blobs))

        for blob in blobs:
            if not blob.name.endswith(".json"):
                continue
            try:
                if migrate_blob(blob, bucket, args.dry_run):
                    total_converted += 1
                else:
                    total_skipped += 1
            except Exception as e:
                log.error("  FAILED: %s — %s", blob.name, e)
                total_errors += 1

    log.info("Done. Converted: %d, Skipped: %d, Errors: %d", total_converted, total_skipped, total_errors)
    if total_errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
