import scrapy
from homyscrapy.items import PropertyItem
from scrapy_playwright.page import PageMethod
from playwright_stealth import Stealth
from datetime import datetime
from parsel import Selector
import random
import os

_stealth = Stealth()
stealth_async = _stealth.apply_stealth_async

# Resource types the browser will never need for data extraction
BLOCKED_RESOURCE_TYPES = {'image', 'font', 'media'}

# Third-party domains that add zero data value but cost bandwidth
BLOCKED_DOMAINS = {
    'google-analytics.com',
    'googletagmanager.com',
    'doubleclick.net',
    'facebook.net',
    'connect.facebook.net',
    'hotjar.com',
    'clarity.ms',
    'intercom.io',
    'segment.com',
    'api.mapbox.com',
    'ads.pubmatic.com',
    'securepubads.g.doubleclick.net',
}


async def block_resources(route, request):
    if request.resource_type in BLOCKED_RESOURCE_TYPES:
        await route.abort()
    elif any(domain in request.url for domain in BLOCKED_DOMAINS):
        await route.abort()
    else:
        await route.continue_()


class Encuentra24Spider(scrapy.Spider):
    """Crawl Encuentra24 listing pages using SPA button-click pagination.

    The site renders listings client-side and paginates via a JS button (no URL
    change), so we keep the Playwright page open and click "Siguiente" in a loop.
    All data (including the full description via the card's title attribute) is
    available on list pages, so no detail-page requests are needed.
    """

    name = 'encuentra24'
    allowed_domains = ['encuentra24.com']
    start_urls = ['https://www.encuentra24.com/costa-rica-es/bienes-raices-venta-de-propiedades-casas/']

    def __init__(self, max_pages=0, start_page=1, output_date=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.max_pages = int(max_pages)  # 0 = unlimited
        self.start_page = int(start_page)
        self.current_page = 0
        self.output_date = output_date or datetime.today().strftime("%Y-%m-%d")
        self.logger.info(f"Starting scrape — max_pages: {self.max_pages}, start_page: {self.start_page}, date: {self.output_date}")

    proxy_config = None
    _proxy_server = os.getenv("PROXY_SERVER")
    _proxy_user = os.getenv("PROXY_USER")
    _proxy_password = os.getenv("PROXY_PASSWORD")
    if _proxy_server and _proxy_user and _proxy_password:
        # Embed credentials in the URL — avoids 407 challenge/response cycle
        _host = _proxy_server.split("://", 1)[-1]
        proxy_config = {"server": f"http://{_proxy_user}:{_proxy_password}@{_host}"}

    custom_settings = {
        'USE_PROXY': True,
        'ROBOTSTXT_OBEY': False,
        'DOWNLOAD_DELAY': 0,
        'RANDOMIZE_DOWNLOAD_DELAY': False,
        'CONCURRENT_REQUESTS': 1,
        'RETRY_TIMES': 3,
        'RETRY_HTTP_CODES': [502, 503, 504, 522, 524, 408, 429],
        'PLAYWRIGHT_LAUNCH_OPTIONS': {
            'headless': True,
            'args': [
                '--disable-blink-features=AutomationControlled',
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-infobars',
                '--disable-dev-shm-usage',
                '--disable-extensions',
                '--disable-gpu',
                '--window-position=0,0',
                '--ignore-certificate-errors',
                '--ignore-certificate-errors-spki-list',
            ],
        },
        'PLAYWRIGHT_CONTEXTS': {
            'default': {
                'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
                'viewport': {'width': 1920, 'height': 1080},
                'java_script_enabled': True,
                'ignore_https_errors': True,
                'bypass_csp': True,
            },
        },
        'HTTPERROR_ALLOWED_CODES': [403, 500],
        'FEED_EXPORT_ENCODING': 'utf-8',
    }

    async def init_page(self, page, request):
        """Block non-essential resources and apply stealth before navigation."""
        await page.route('**/*', block_resources)
        await stealth_async(page)

    def _playwright_meta(self, extra_methods=None):
        """Build standard Playwright meta dict, injecting proxy if configured."""
        meta = {
            'playwright': True,
            'playwright_context': 'default',
            'playwright_include_page': True,
            'playwright_page_init_callback': self.init_page,
            'playwright_page_goto_kwargs': {'wait_until': 'domcontentloaded'},
            'playwright_page_methods': extra_methods or [],
        }
        if self.proxy_config:
            meta['playwright_context_kwargs'] = {'proxy': self.proxy_config}
        return meta

    async def start(self):
        for url in self.start_urls:
            yield scrapy.Request(
                url,
                meta=self._playwright_meta([
                    PageMethod('wait_for_selector',
                               'a.item-card-link',
                               timeout=60000,
                               state='attached'),
                ]),
                errback=self.errback,
            )

    def _extract_item(self, ad, response):
        """Extract a PropertyItem from a listing card element (list page only)."""
        item = PropertyItem()
        item['source'] = 'Encuentra24'
        item['country'] = 'Costa Rica'
        item['extraction_date'] = datetime.today().strftime('%Y-%m-%d')

        relative_url = ad.attrib.get('href', '')
        item['url'] = response.urljoin(relative_url) if relative_url else ''
        item['title'] = (ad.css('h3.card_title::text').get() or '').strip()

        # Two price elements on page (mobile + desktop) — take only the first
        price_el = ad.css('span.card_price')
        item['price'] = ' '.join(t.strip() for t in price_el[0].css('::text').getall() if t.strip()) if price_el else ''

        item['location_pcd'] = (ad.css('p.card_subtitle::text').get() or '').strip()

        # Full description is stored in the `title` attribute of card_description
        item['description'] = ad.css('p.card_description::attr(title)').get('').strip()

        for spec in ad.css('span.card_spec'):
            spec_html = spec.get()
            # Text contains number + Spanish label (e.g. "3 Recámaras") — keep only digits/units
            texts = [t.strip() for t in spec.css('::text').getall() if t.strip()]
            if 'lucide-bed' in spec_html and texts:
                item['bedrooms'] = texts[0].split()[0]  # "3 Recámaras" → "3"
            elif 'lucide-bath' in spec_html and texts:
                item['bathrooms'] = texts[0].split()[0]  # "2.5 Baños" → "2.5"
            elif 'lucide-maximize' in spec_html and texts:
                item['area'] = texts[0]  # "118 m²" — keep full value

        images = []
        for img in ad.css('img.card_image'):
            src = img.attrib.get('src', '')
            if src and 'base64' not in src and 'no-img' not in src:
                images.append(src)
        item['images'] = images

        # External ID is the last numeric path segment of the URL
        path_parts = relative_url.rstrip('/').split('/')
        if path_parts and path_parts[-1].isdigit():
            item['external_id'] = path_parts[-1]

        item['features'] = []
        item['metadata'] = {}

        return item

    async def parse(self, response):
        """Paginate via button clicks in a single browser session.

        All list-page data (including full description) is extracted here.
        No detail-page requests are made.
        """
        page = response.meta['playwright_page']
        # Respect CLOSESPIDER_ITEMCOUNT if set (scrapy extension can't interrupt
        # the while loop, so we track it here)
        max_items = self.settings.getint('CLOSESPIDER_ITEMCOUNT', 0)
        items_yielded = 0

        try:
            while True:
                await page.evaluate('window.scrollTo(0, document.body.scrollHeight / 3)')
                await page.wait_for_timeout(random.randint(1200, 2800))
                await page.evaluate('window.scrollTo(0, document.body.scrollHeight / 2)')
                await page.wait_for_timeout(random.randint(800, 1800))

                html = await page.content()
                sel = Selector(text=html)

                ads = sel.css('a.item-card-link')
                self.logger.info(f"Found {len(ads)} ads on page {self.current_page}")

                if not ads:
                    self.logger.warning(f"No ads on page {self.current_page} — dumping HTML.")
                    os.makedirs('data', exist_ok=True)
                    with open(f'data/debug_page_{self.current_page}.html', 'w', encoding='utf-8') as f:
                        f.write(html)
                    break

                for ad in ads:
                    item = self._extract_item(ad, response)
                    if not item['url']:
                        continue
                    yield item
                    items_yielded += 1
                    if max_items and items_yielded >= max_items:
                        self.logger.info(f"Reached item limit ({max_items})")
                        return

                self.current_page += 1
                self.logger.info(f"Completed page {self.current_page}")

                if self.max_pages != 0 and self.current_page >= self.max_pages:
                    self.logger.info(f"Reached max_pages limit ({self.max_pages})")
                    break

                # Pagination: site uses a button click (URL does not change)
                next_btn = page.locator('button[aria-label="Página siguiente"]')
                next_count = await next_btn.count()
                if next_count > 0:
                    aria_disabled = await next_btn.get_attribute('aria-disabled')
                    if aria_disabled == 'true':
                        next_count = 0

                if next_count == 0:
                    self.logger.info("No more pages — reached end of listings")
                    break

                self.logger.info(f"Moving to page {self.current_page + 1}")
                await next_btn.click()

                try:
                    await page.wait_for_function(
                        'document.querySelectorAll("a.item-card-link").length > 0',
                        timeout=30000,
                    )
                except Exception:
                    self.logger.warning(f"Timeout waiting for listings on page {self.current_page + 1}")
                    break

                await page.wait_for_timeout(random.randint(2000, 4000))

        finally:
            await page.close()

    async def errback(self, failure):
        page = failure.request.meta.get('playwright_page')
        if page:
            try:
                await page.close()
            except Exception:
                pass
        self.logger.error(f"Request failed: {failure.request.url} — {failure.getErrorMessage()}")
