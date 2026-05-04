#!/bin/bash
# Cloud Run Jobs entrypoint.
# CLOUD_RUN_TASK_INDEX (0-based) is injected automatically by Cloud Run.
# Maps task index → spider name. All spiders run to completion (no page limit).

set -euo pipefail

SPIDERS=(encuentra24 mls recr inhaus)
INDEX=${CLOUD_RUN_TASK_INDEX:-0}
SPIDER=${SPIDERS[$INDEX]}

echo "[run_job] Task $INDEX → spider: $SPIDER"
exec scrapy crawl "$SPIDER"
