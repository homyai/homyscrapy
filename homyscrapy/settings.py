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
    "args": [
        "--disable-blink-features=AutomationControlled",
        "--no-sandbox",
        "--disable-setuid-sandbox",
        "--disable-infobars",
        "--window-position=0,0",
        "--ignore-certificate-errors",
        "--ignore-ssl-errors",
    ],
}

PLAYWRIGHT_CONTEXTS = {
    "default": {
        "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "viewport": {
            "width": 1920,
            "height": 1080,
        },
        "java_script_enabled": True,
        "ignore_https_errors": True,
    }
}

CONCURRENT_REQUESTS = 1
DOWNLOAD_DELAY = 10
RANDOMIZE_DOWNLOAD_DELAY = True
COOKIES_ENABLED = True

USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'


ITEM_PIPELINES = {
    'homyscrapy.pipelines.HomyscrapyPipeline': 300,
}
