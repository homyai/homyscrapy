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

PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT = 120 * 1000  # 120 seconds

PLAYWRIGHT_LAUNCH_OPTIONS = {
    "headless": True,
    "timeout": 60 * 1000,  # 60 seconds
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


CONCURRENT_REQUESTS = 1
DOWNLOAD_DELAY = 10
RANDOMIZE_DOWNLOAD_DELAY = True
# Retry settings
RETRY_ENABLED = True
RETRY_TIMES = 3

# Anti-Blocking Settings
DOWNLOADER_MIDDLEWARES = {
    'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
    'scrapy_fake_useragent.middleware.RandomUserAgentMiddleware': 400,
    # 'scrapy_fake_useragent.middleware.RetryUserAgentMiddleware': 401, # CAUSES CRASH
    'homyscrapy.middlewares.RandomProxyMiddleware': 410,
}

FAKEUSERAGENT_PROVIDERS = [
    'scrapy_fake_useragent.providers.FakerProvider',
    'scrapy_fake_useragent.providers.FixedUserAgentProvider',
]


ITEM_PIPELINES = {
    'homyscrapy.pipelines.HomyscrapyPipeline': 300,
}
