# HomyScrapy

A scalable web scraping project for Real Estate marketplaces, powered by **Scrapy** and **Playwright**.

## Architecture

This project uses Scrapy for the crawling framework and Playwright for browser automation, enabling extraction from difficult sites with anti-bot protection.

### Spiders
- **Inmotico** (`inmotico`): Standard Scrapy spider.
- **Encuentra24** (`encuentra24`): Playwright-based spider.

## Setup

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   playwright install chromium
   ```

2. Run a spider:
   ```bash
   cd homyscrapy
   scrapy crawl inmotico
   # or
   scrapy crawl encuentra24
   ```

## Cloud Storage
Data is automatically uploaded to Google Cloud Storage if credentials are configured.
