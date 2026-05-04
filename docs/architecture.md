# Homyscrapy — Architecture

## Overview

Homyscrapy is the web scraping layer for the Homy platform. It collects Costa Rican real estate listings from multiple marketplaces and lands them in Google Cloud Storage as structured JSON files, which are then consumed by the dbt data warehouse.

```
Cloud Scheduler (monthly, 4th at 06:49 UTC)
       │
       ▼
Cloud Run Job "homyscrapy" (4 parallel tasks)
  ├── Task 0 → scrapy crawl encuentra24   ~50 min, ~8,300 items
  ├── Task 1 → scrapy crawl mls           ~15 min, ~2,500 items
  ├── Task 2 → scrapy crawl recr          ~20 min, ~2,000 items
  └── Task 3 → scrapy crawl inhaus        ~10 min, ~1,500 items
       │
       ▼
GCS: gs://web-scraper-data/raw/<spider>/YYYY-MM-DD.json
       │
       ▼
dbt-dwh (BigQuery staging models)
```

All tasks run in parallel — total wall time is bounded by the slowest spider (~50 min for encuentra24).

---

## GCP Resources

| Resource | Name | Notes |
|---|---|---|
| Artifact Registry | `us-central1-docker.pkg.dev/datalake-homyai/homyscrapy/homyscrapy` | Docker image |
| Cloud Run Job | `homyscrapy` (us-central1) | 4 tasks, 2 vCPU / 2 GB each |
| Cloud Scheduler | `homyscrapy-monthly` (us-central1) | `49 6 4 * *` UTC |
| GCS bucket | `gs://web-scraper-data/raw/` | Output landing zone |
| Service account | `homy-webscrapy@datalake-homyai.iam.gserviceaccount.com` | Runs the job |
| Secret Manager | `homyscrapy-proxy-server/user/password` | Decodo residential proxy |
| Secret Manager | `homyscrapy-service-account` | GCS upload credentials |

---

## Output Format

Files land at:
```
gs://web-scraper-data/raw/<spider>/YYYY-MM-DD.json
```

Where `YYYY-MM-DD` is the **Costa Rica local date** (UTC-6) at spider start time — not the UTC date — so files are always dated correctly regardless of when the job runs.

Each file is a JSON array of `PropertyItem` records:

```json
[
  {
    "source": "Encuentra24",
    "country": "Costa Rica",
    "extraction_date": "2026-05-04",
    "url": "https://...",
    "title": "Casa en venta...",
    "price": "$ 250,000",
    "location_pcd": "Escazú, San José",
    "bedrooms": "3",
    "bathrooms": "2",
    "area": "180 m²",
    "images": ["https://..."],
    "description": "...",
    "features": [],
    "metadata": {},
    "external_id": "12345678"
  }
]
```

---

## Local Development

```bash
# Build the Docker image
make build

# Run a single spider locally (local JSON output)
make crawl SPIDER=mls

# Run with item limit for testing
make crawl SPIDER=encuentra24 LIMIT=50

# Run without proxy (for spiders that don't need it)
make crawl SPIDER=mls NO_PROXY=1

# Upload to GCS
make crawl SPIDER=encuentra24 STORAGE=gcs

# Run tests
make test
```

---

## Deployment

```bash
# Build + push image + update Cloud Run Job
make deploy

# Trigger the job immediately (full extraction)
make run-job
```

The `deploy` target:
1. Builds a new Docker image
2. Pushes it to Artifact Registry
3. Updates the Cloud Run Job to use the new image

The job starts automatically on the next scheduled run. To trigger immediately: `make run-job`.

---

## Adding a New Spider

1. Create `homyscrapy/spiders/costa_rica/<name>.py` — extend `BasePropertySpider`
2. Add the spider name to `SPIDERS` array in `run_job.sh`
3. Increase `--tasks` in the Cloud Run Job: `make deploy`
4. Add fixture + tests in `tests/`

---

## Secrets Management

Proxy credentials are stored in Secret Manager and injected as environment variables at runtime — never in the image or source code. The GCS service account key is mounted as a file at `/secrets/sa/key.json`.

To rotate proxy credentials:
```bash
echo -n "new-value" | gcloud secrets versions add homyscrapy-proxy-password --data-file=-
```

The next job execution will automatically use the new version.
