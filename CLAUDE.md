@/Users/medranonico/Documents/context/CLAUDE.md
@/Users/medranonico/Documents/context/personal/homy/CLAUDE.md

# homy — homyscrapy

## Purpose

This is the primary web scraping framework for homy. It crawls Costa Rican real estate marketplaces to collect property listings and uploads them to Google Cloud Storage (GCS) as structured JSON/Parquet files. The scraped data is then consumed by `dbt-dwh` for analytics. This is the current, actively maintained scraper (as opposed to the older `scrapy` repo).

## Tech Stack

- **Python 3.10+**
- **Scrapy** `>=2.11.0` — core scraping framework
- **scrapy-playwright** `>=0.0.33` — Playwright integration for JavaScript-rendered sites
- **playwright-stealth** — browser fingerprinting evasion (Cloudflare bypass)
- **scrapy-fake-useragent** — random user agent rotation
- **Google Cloud Storage** — output destination for scraped data
- **Google Cloud BigQuery** — for querying already-crawled URLs
- **pandas** — data manipulation in pipeline
- **Devcontainer** (VS Code) — containerized dev environment

## Project Structure

```
homyscrapy/
├── homyscrapy/                    # Main Scrapy project
│   ├── spiders/
│   │   └── costa_rica/            # All Costa Rica marketplace spiders
│   │       ├── century21.py       # Century21 CR — Playwright + JSON-LD, Cloudflare bypass
│   │       ├── inhaus.py          # InHaus — Sitemap + Playwright, SSR hydration
│   │       ├── encuentra24.py     # Encuentra24 — Playwright, infinite scroll
│   │       ├── mls.py             # MLS Costa Rica — static (Requests), fast
│   │       ├── inmotico.py        # Inmotico — static (Requests)
│   │       └── recr.py            # RE.CR — static (Requests)
│   ├── items.py                   # PropertyItem schema (unified across all spiders)
│   ├── pipelines.py               # GCS upload pipeline
│   ├── middlewares.py             # RandomProxyMiddleware + user agent middleware
│   ├── settings.py                # Scrapy settings (Playwright config, rate limiting)
│   └── common/
│       └── google_cloud_tools/    # GCS upload utilities
├── data/
│   ├── keys.json                  # Spider/marketplace configuration
│   └── (local test outputs)
├── docs/
│   ├── proxy_setup_guide.md
│   └── project_memory_bank.md     # Deep technical context and architecture decisions
├── Dockerfile
├── requirements.txt
└── scrapy.cfg
```

## Supported Marketplaces

| Spider | Strategy | Key Challenge |
|---|---|---|
| `century21` | Playwright + JSON-LD | Cloudflare protection — requires stealth mode |
| `inhaus` | Sitemap + Playwright | SSR hydration — content rendered after load |
| `encuentra24` | Playwright | Dynamic rendering + infinite scroll pagination |
| `mls` | Static Requests | Table parsing — fast and simple |
| `inmotico` | Static Requests | Legacy system |
| `recr` | Static Requests | Specialized metadata extraction |

## Unified Data Schema (`PropertyItem`)

All spiders output a standard `PropertyItem`:

```python
url, extraction_date, price, title, description
location_pcd      # Province, Canton, District
lat, lon          # Coordinates
bedrooms, bathrooms, area, lot_area
features, garage, remarks, images
property_category  # house, apartment, land, etc.
source, country, status, city, state
external_id, metadata
```

## Pipeline / Output

The `HomyscrapyPipeline` collects all items and uploads them to GCS at the end of each spider run:

- **Bucket**: `web-scraper-data`
- **Path pattern**: `{KEY}/{sales/houses}/raw-data/{YYYY-MM-DD}.json`
- **Key mapping**:
  - `inmotico` → `INT/`
  - `encuentra24` → `C24/`
  - Others → spider name uppercased

These GCS files become the source for `dbt-dwh`'s staging layer (`stg_gcs_sale_houses_*`).

## Scraping Settings

Key settings in `settings.py`:
- `CONCURRENT_REQUESTS = 1` — single request at a time (polite scraping)
- `DOWNLOAD_DELAY = 10` — 10 second delay between requests
- `RANDOMIZE_DOWNLOAD_DELAY = True`
- `RETRY_TIMES = 3`
- Playwright timeout: 90 seconds navigation, 60 seconds launch

## Dev Setup

### Prerequisites

- Docker + VS Code with Devcontainers extension
- GCP service account with GCS and BigQuery access

### Environment Setup

Create `.devcontainer/.env`:

```ini
GOOGLE_APPLICATION_CREDENTIALS=./.devcontainer/datalake-homyai-990ddbaa84ae.json
```

### Running Spiders

```bash
# Run a spider and save locally
scrapy crawl century21 -O data/century21.json

# Run with item limit (for testing)
scrapy crawl encuentra24 -O data/test.json -s CLOSESPIDER_ITEMCOUNT=10

# Run all production (uploads to GCS automatically)
scrapy crawl inmotico
scrapy crawl encuentra24
# etc.
```

### Proxy Setup

See `docs/proxy_setup_guide.md` for residential proxy configuration. Proxies are applied via `RandomProxyMiddleware` from `proxies.txt` in the parent directory.

## Connection to Other Repos

- **`dbt-dwh`**: Consumes the JSON files uploaded to GCS by this scraper. The staging models (`stg_gcs_sale_houses_c24`, `stg_gcs_sale_houses_inm`, etc.) read from the same GCS paths written here.
- **`scrapy`**: The older scraper repo — originally only had Inmotico. `homyscrapy` is the expanded replacement with more spiders and Playwright support.

## Key Notes

- **Do not enable `ROBOTSTXT_OBEY`** — it is explicitly set to `False` in settings.
- Cloudflare-protected sites (Century21, InHaus) require `playwright-stealth` — adding new spiders for these sites needs stealth context.
- The `common/google_cloud_tools/` module is shared with the `scrapy` repo — keep them in sync if making changes to GCS upload logic.
- `data/keys.json` controls marketplace targets/configuration — modify here to add new targets.
- See `docs/project_memory_bank.md` for detailed architecture decisions and implementation notes.
