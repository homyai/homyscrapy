# Project Memory Bank: HomyScrapy

> **Context for AI Agents & Developers**: This document outlines the core architecture, decisions, and patterns used in `homyscrapy`. Read this before modifying the codebase.

## 1. Core Architecture

### Technology Stack
- **Framework**: Scrapy 2.11+
- **Browser Engine**: Scrapy-Playwright (Chromium)
- **Runtime**: Python 3.10 (Dockerized)
- **Storage**: Google Cloud Storage (via `google-cloud-storage`)

### Data Flow
1.  **Spider** (`homyscrapy/spiders/*`): Extracts data from HTML/JSON.
2.  **Item** (`homyscrapy/items.py`): Validates data into a strict `PropertyItem` schema.
3.  **Pipeline** (`homyscrapy/pipelines.py`):
    *   Accomulates items in memory.
    *   Converts to Pandas DataFrame.
    *   Uploads raw JSON to GCS bucket `web-scraper-data`.

## 2. Spider Implementation Strategies

We use two distinct patterns depending on the target site complexity.

### A. Static Spiders (Fast, Low Overhead)
Used for: `mls`, `recr`, `inmotico`.
- **Base Class**: `scrapy.Spider`
- **Method**: Standard HTTP requests (`requests` / `urllib` equivalent).
- **Extraction**: CSS/XPath selectors.
- **Pagination**: URL parameters (`?page=2`).

### B. Dynamic/Stealth Spiders (Heavy, Complex)
Used for: `century21`, `inhaus`, `encuentra24`.
- **Base Class**: `scrapy.Spider` (with `playwright=True` meta).
- **Configuration**:
    ```python
    custom_settings = {
        'PLAYWRIGHT_LAUNCH_OPTIONS': {
            'headless': True,
            'args': ['--disable-blink-features=AutomationControlled']
        }
    }
    ```
- **Strategy**:
    1.  **Century21**: Bypasses Cloudflare. waits for `.details-container`. Extracts data from *embedded JSON-LD* (highly reliable).
    2.  **InHaus**: Uses `SitemapSpider` logic to find URLs, then Playwright to hydrate the SSR React/Next.js page. Extracts from JSON-LD + CSS.
    3.  **Encuentra24**: Full browser rendering to handle aggressive JS obfuscation and infinite scroll.

## 3. Key Configuration Files

- **`data/keys.json`**: Registry of active spiders. Used to look up URLs and internal bot IDs (e.g., `C24`, `MLS`).
- **`.devcontainer/datalake-*.json`**: GCP Service Account credentials. **Critical for Pipeline success**.
- **`homyscrapy/items.py`**:
    *   `PropertyItem`: The canonical data model.
    *   **Fields**: `price`, `currency`, `bed`, `bath`, `area`, `lot_area`, `lat`, `lon`, `images` (list), `description` (normalized).

## 4. Development Workflow

### Adding a New Spider
1.  **Probe**: Use `curl` or a probe spider to check for JSON-LD availability.
2.  **Decide**: Static vs Playwright? (Prefer Static if possible).
3.  **Implement**: Create `homyscrapy/spiders/costa_rica/my_spider.py`.
4.  **Register**: Add entry to `data/keys.json`.
5.  **Test**: `scrapy crawl my_spider -O data/test.json -s CLOSESPIDER_ITEMCOUNT=5`.

### Common Issues
- **Cloudflare 403**: Switch to Playwright with `args=['--disable-blink-features=AutomationControlled']`.
- **Missing Data**: Check if the site is Client-Side Rendered (CSR). If `response.body` is empty but browser shows data, use Playwright.
- **Pipeline Errors**: Ensure `GOOGLE_APPLICATION_CREDENTIALS` matches the path in `.devcontainer/.env`.
