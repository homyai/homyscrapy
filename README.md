# HomyScrapy

A robust, enterprise-grade web scraper for Costa Rican real estate marketplaces, built with **Scrapy** and **Playwright**.

## Features

- **Hybrid Crawling**: Supports both static (Requests) and dynamic (Playwright) websites.
- **Enterprise Pipelines**: Exports structured data (`JSON`/`Parquet`) directly to Google Cloud Storage.
- **Stealth Mode**: Includes browser fingerprinting evasion for Cloudflare-protected sites (Century21, InHaus).
- **Containerized**: Fully Dockerized development environment via VS Code Devcontainers.

## Supported Marketplaces

| Marketplace | Spider Name | Strategy | Features |
|-------------|-------------|----------|----------|
| **Century21** | `century21` | Playwright + JSON-LD | Cloudflare Bypass, Structured Schema |
| **InHaus** | `inhaus` | Sitemap + Playwright | SSR Hydration, JSON-LD |
| **Encuentra24** | `encuentra24` | Playwright | Dynamic Rendering, Infinite Scroll |
| **MLS Costa Rica** | `mls` | Static (Requests) | Fast, Table parsing |
| **Inmotico** | `inmotico` | Static (Requests) | Legacy System support |
| **RE.CR** | `recr` | Static (Requests) | Specialized Metadata |

## Installation

### Prerequisites
- Docker & VS Code (for Devcontainers)
- OR Python 3.10+ with `pip`

### 1. Environment Setup
Create a `.env` file in `.devcontainer/` (or root if local) with your Google Cloud credentials path:
```ini
GOOGLE_APPLICATION_CREDENTIALS=./.devcontainer/datalake-homyai-990ddbaa84ae.json
```

### 2. Configuration
Marketplaces are defined in `data/keys.json`. You can add or modify spider targets there.

### 3. Running Spiders
```bash
# Run a specific spider and save output
scrapy crawl century21 -O data/century21.json

# Run with a limit (for testing)
scrapy crawl encuentra24 -O data/test.json -s CLOSESPIDER_ITEMCOUNT=10
```

## Project Structure

- `homyscrapy/spiders/costa_rica/`: Spider implementations.
- `homyscrapy/items.py`: Unified `PropertyItem` schema.
- `homyscrapy/pipelines.py`: Data processing and GCS upload pipeline.
- `data/`: Local storage for test outputs and configuration.

## Documentation

For deep technical context, architecture decisions, and "how it works", see [Project Memory Bank](docs/project_memory_bank.md).
