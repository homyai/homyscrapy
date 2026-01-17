import os

BOT_NAME = 'homyscrapy'

SPIDER_MODULES = ['homyscrapy.spiders']
NEWSPIDER_MODULE = 'homyscrapy.spiders'

# Obey robots.txt rules
ROBOTSTXT_OBEY = False

# Scrapy-Playwright settings
DOWNLOAD_HANDLERS = {
    "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
    "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
}

TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"

PLAYWRIGHT_LAUNCH_OPTIONS = {
    "headless": True,
    "timeout": 20 * 1000,  # 20 seconds
}

CONCURRENT_REQUESTS = 4
DOWNLOAD_DELAY = 2
COOKIES_ENABLED = False

DEFAULT_REQUEST_HEADERS = {
   'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
   'Accept-Language': 'en',
}

ITEM_PIPELINES = {
    'homyscrapy.pipelines.HomyscrapyPipeline': 300,
}
