# Spider Reference

## Common Schema (`PropertyItem`)

All spiders output the same fields:

| Field | Type | Notes |
|---|---|---|
| `source` | str | Marketplace name (e.g. `"Encuentra24"`) |
| `country` | str | Always `"Costa Rica"` |
| `extraction_date` | str | `YYYY-MM-DD` in Costa Rica time (UTC-6) |
| `url` | str | Listing URL |
| `title` | str | Listing title |
| `price` | str | Raw price string (e.g. `"$ 250,000"`) |
| `location_pcd` | str | Province / Canton / District |
| `bedrooms` | str | Number of bedrooms |
| `bathrooms` | str | Number of bathrooms |
| `area` | str | Built area (e.g. `"180 m²"`) |
| `lot_area` | str | Lot size if available |
| `garage` | str | Parking spots |
| `images` | list[str] | Photo URLs |
| `description` | str | Full listing description |
| `features` | list[str] | Amenities / benefits |
| `metadata` | dict | Spider-specific extra fields |
| `external_id` | str | Marketplace's internal listing ID |

---

## encuentra24

- **URL**: encuentra24.com/costa-rica-es/bienes-raices-venta-de-propiedades-casas
- **Strategy**: Playwright (Chromium) + playwright-stealth
- **Proxy**: Required (residential rotating via Decodo)
- **Pagination**: SPA button-click — single browser session across all pages
- **Items/page**: 20
- **Total pages**: ~417 (casas only)
- **Est. full run**: ~50 min

**Key implementation notes:**
- The site uses SPA routing — the URL never changes between pages. Pagination is handled by clicking the "Siguiente" button in a `while` loop keeping the browser session alive.
- All data (including the full description via `card_description[title]`) is available on the list page — no detail-page requests are made.
- `max_pages` spider argument limits pages for testing: `scrapy crawl encuentra24 -a max_pages=10`

---

## mls

- **URL**: mls.re.cr/search/rs
- **Strategy**: Static Scrapy (no browser)
- **Proxy**: Not required
- **Pagination**: `?offset=N` query param
- **Items/page**: 25
- **Est. full run**: ~20 min

**Key implementation notes:**
- Listings are in an HTML table (`<tr>` rows with `td.title`).
- Detail pages are fetched for description and full metadata.

---

## recr

- **URL**: re.cr/en/costa-rica-real-estate-for-sale
- **Strategy**: Static Scrapy (no browser)
- **Proxy**: Not required
- **Pagination**: Page-based URL params
- **Est. full run**: ~25 min

---

## inhaus

- **URL**: inhaus.cr
- **Strategy**: Sitemap-based Scrapy (no browser)
- **Proxy**: Not required
- **Pagination**: Crawls via XML sitemap (`/propiedades/` URLs)
- **Est. full run**: ~10 min

---

## century21 ⚠️ Disabled

- **Status**: Blocked by reCAPTCHA v3
- **Why disabled**: The site detects automation and serves a CAPTCHA challenge that playwright-stealth cannot bypass.
- **To re-enable**: Integrate a CAPTCHA solving service (e.g. 2captcha, CapSolver) before running.

---

## Adding a New Spider

1. Extend `BasePropertySpider` (provides `output_date`, `make_item()`, `collapse_whitespace()`)
2. Set `name`, `source`, `allowed_domains`, `start_urls`
3. Implement `parse()` and optionally `parse_detail()`
4. Add to `SPIDERS` array in `run_job.sh` and increment `--tasks` in the Cloud Run Job
5. Capture a fixture HTML and add tests in `tests/test_spiders.py`
