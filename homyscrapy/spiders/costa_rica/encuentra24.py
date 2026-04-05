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
BLOCKED_RESOURCE_TYPES = {'image', 'font', 'media', 'stylesheet'}

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
    name = 'encuentra24'
    allowed_domains = ['encuentra24.com', 'googleusercontent.com', 'webcache.googleusercontent.com']
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
        'DOWNLOAD_DELAY': 8,
        'RANDOMIZE_DOWNLOAD_DELAY': True,
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
                'proxy': proxy_config,
            },
        },
        'HTTPERROR_ALLOWED_CODES': [403, 500],
        'FEED_EXPORT_ENCODING': 'utf-8',
    }

    async def init_page(self, page, request):
        """Block non-essential resources and apply stealth before navigation."""
        await page.route('**/*', block_resources)
        await stealth_async(page)

    async def start(self):
        for url in self.start_urls:
            yield scrapy.Request(
                url,
                meta={
                    'playwright': True,
                    'playwright_context': 'default',
                    'playwright_include_page': True,
                    'playwright_page_init_callback': self.init_page,
                    'playwright_page_goto_kwargs': {'wait_until': 'domcontentloaded'},
                    'playwright_page_methods': [
                        PageMethod('wait_for_timeout', 20000),
                    ],
                },
                errback=self.errback,
            )

    async def parse(self, response):
        page = response.meta['playwright_page']

        await page.evaluate('window.scrollTo(0, document.body.scrollHeight / 3)')
        await page.wait_for_timeout(random.randint(1200, 2800))
        await page.evaluate('window.scrollTo(0, document.body.scrollHeight / 2)')
        await page.wait_for_timeout(random.randint(800, 1800))

        html = await page.content()
        await page.close()

        sel = Selector(text=html)

        # Two themes served by the site:
        # d3-* (SSR): div.d3-ad-tile
        # m3-* (CSR): div.card[data-adid]
        ads_d3 = sel.css('div.d3-ad-tile')
        ads_m3 = sel.css('div.card[data-adid]')
        ads = ads_d3 if ads_d3 else ads_m3
        theme = 'd3' if ads_d3 else 'm3'
        self.logger.info(f"Found {len(ads)} ads on page {self.current_page} (theme: {theme})")

        if not ads:
            self.logger.warning(f"No ads found on page {self.current_page} — possible selector change or bot block.")

        for ad in ads:
            item = PropertyItem()
            item['source'] = 'Encuentra24'
            item['country'] = 'Costa Rica'
            item['extraction_date'] = datetime.today().strftime('%Y-%m-%d')

            if theme == 'd3':
                relative_url = ad.css('a.d3-ad-tile__description::attr(href)').get()
                item['url'] = response.urljoin(relative_url) if relative_url else ''
                item['title'] = (ad.css('span.d3-ad-tile__title::text').get() or '').strip()
                location_texts = ad.css('div.d3-ad-tile__location span::text').getall()
                item['location_pcd'] = ' '.join(t.strip() for t in location_texts if t.strip())
                item['price'] = (ad.css('div.d3-ad-tile__price::text').get() or '').strip()
                images = []
                for img in ad.css('img.d3-photos-carousel__photo'):
                    src = img.css('::attr(data-src)').get() or img.css('::attr(src)').get()
                    if src and 'base64' not in src:
                        images.append(src)
                item['images'] = images
                for spec in ad.css('li.d3-ad-tile__details-item'):
                    text = ' '.join(t.strip() for t in spec.css('::text').getall() if t.strip())
                    icon_html = spec.get()
                    if '#resize"' in icon_html or '#size"' in icon_html:
                        item['area'] = text
                    elif '#bed"' in icon_html:
                        item['bedrooms'] = text
                    elif '#bath"' in icon_html:
                        item['bathrooms'] = text
                ext_id = ad.css('a.tool-favorite::attr(data-adid)').get()
            else:
                relative_url = ad.css('a.ad-name::attr(href)').get()
                item['url'] = response.urljoin(relative_url) if relative_url else ''
                item['title'] = (ad.css('a.ad-name::text').get() or '').strip()
                location_texts = ad.css('div.ad-location::text').getall()
                item['location_pcd'] = ' '.join(t.strip() for t in location_texts if t.strip())
                item['price'] = (ad.css('div.ad-price::text').get() or '').strip()
                images = []
                for img in ad.css('img.m3-photos-carousel__photo'):
                    src = img.css('::attr(data-src)').get() or img.css('::attr(src)').get()
                    if src and 'base64' not in src:
                        images.append(src)
                item['images'] = images
                for icon_item in ad.css('div.ad-icons-item'):
                    text = ' '.join(t.strip() for t in icon_item.css('::text').getall() if t.strip())
                    icon_html = icon_item.get()
                    if '#ic-resize"' in icon_html or '#ic-size"' in icon_html:
                        item['area'] = text
                    elif '#ic-bed"' in icon_html:
                        item['bedrooms'] = text
                    elif '#ic-bath"' in icon_html:
                        item['bathrooms'] = text
                ext_id = ad.attrib.get('data-adid')

            if ext_id:
                item['external_id'] = ext_id

            if not item['url']:
                self.logger.warning(f"Skipping ad with no URL: {item.get('title', 'unknown')}")
                continue

            detail_meta = {
                'item': item,
                'playwright': True,
                'playwright_context': 'default',
                'playwright_include_page': True,
                'playwright_page_init_callback': self.init_page,
                'playwright_page_goto_kwargs': {'wait_until': 'domcontentloaded'},
                'playwright_page_methods': [
                    PageMethod('wait_for_timeout', 10000),
                ],
            }
            current_context = response.meta.get('playwright_context_kwargs', {})
            if 'proxy' in current_context:
                detail_meta['playwright_context_kwargs'] = {'proxy': current_context['proxy']}

            yield response.follow(
                item['url'],
                callback=self.parse_detail,
                meta=detail_meta,
                errback=self.errback,
            )

        self.current_page += 1
        self.logger.info(f"Completed page {self.current_page}")

        next_page = sel.css('a.d3-pagination__arrow--next::attr(href)').get()
        should_continue = next_page and (self.max_pages == 0 or self.current_page < self.max_pages)

        if should_continue:
            self.logger.info(f"Moving to page {self.current_page + 1}")
            yield response.follow(
                next_page,
                callback=self.parse,
                meta={
                    'playwright': True,
                    'playwright_context': 'default',
                    'playwright_include_page': True,
                    'playwright_page_init_callback': self.init_page,
                    'playwright_page_goto_kwargs': {'wait_until': 'domcontentloaded'},
                    'playwright_page_methods': [
                        PageMethod('wait_for_timeout', 20000),
                    ],
                },
                errback=self.errback,
                dont_filter=True,
            )
        elif not next_page:
            self.logger.info("No more pages — reached end of listings")
        else:
            self.logger.info(f"Reached max_pages limit ({self.max_pages})")

    async def parse_detail(self, response):
        page = response.meta['playwright_page']
        html = await page.content()
        await page.close()

        sel = Selector(text=html)
        item = response.meta['item']

        # Description — d3-* or m3-* (product-comments) theme
        desc_lines = sel.css('.d3-property-about__text *::text, .cas-property-about__text *::text').getall()
        if not any(line.strip() for line in desc_lines):
            desc_lines = sel.css('.product-comments *::text, .product-comments::text').getall()
        item['description'] = '\n'.join(line.strip() for line in desc_lines if line.strip())

        if not item['description']:
            self.logger.warning(f"Empty description for {response.url}")

        # Features — d3-* or m3-* (product-extras-extra)
        benefits = sel.css('.d3-property-benefits__benefit::text, .cas-property-benefits__benefit::text').getall()
        if not benefits:
            benefits = sel.css('.product-extras-extra::text').getall()
        item['features'] = [f.strip() for f in benefits if f.strip()]

        # Specs — d3-* insight attributes or m3-* product-icons-icon
        attributes = sel.css('.d3-property-insight__attribute, .cas-property-insight__attribute')
        for attr in attributes:
            text = attr.css('.d3-property-insight__attribute-value::text, .cas-property-insight__attribute-value::text').get('').strip()
            icon_html = attr.get()
            if '#bed"' in icon_html:
                item['bedrooms'] = text
            elif '#bath"' in icon_html:
                item['bathrooms'] = text
            elif '#resize"' in icon_html or '#size"' in icon_html:
                item['area'] = text
            elif '#parking"' in icon_html:
                item['garage'] = text

        if not attributes:
            for icon_item in sel.css('div.product-icons-icon'):
                icon_html = icon_item.get()
                text = ' '.join(t.strip() for t in icon_item.css('::text').getall() if t.strip())
                if '#ic-bed"' in icon_html:
                    item['bedrooms'] = text
                elif '#ic-bath"' in icon_html:
                    item['bathrooms'] = text
                elif '#ic-resize"' in icon_html or '#ic-size"' in icon_html:
                    item['area'] = text
                elif '#ic-parking"' in icon_html:
                    item['garage'] = text

        # Metadata — d3-* detail labels or m3-* product-publication col-6 pairs
        meta = {}
        details = sel.css('.d3-property-details__detail-label, .cas-property-details__detail-label')
        for d in details:
            label = d.css('::text').get('').strip()
            value = d.css('p.d3-property-details__detail::text, p.cas-property-details__detail::text').get('').strip()
            if label and value:
                meta[label] = value

        if not meta:
            cols = sel.css('.product-publication .col-6')
            col_texts = [c.css('::text, strong::text').get('').strip() for c in cols]
            for i in range(0, len(col_texts) - 1, 2):
                label, value = col_texts[i], col_texts[i + 1]
                if label and value:
                    meta[label] = value

        item['metadata'] = meta
        yield item

    async def errback(self, failure):
        page = failure.request.meta.get('playwright_page')
        if page:
            try:
                await page.close()
            except Exception:
                pass
        self.logger.error(f"Request failed: {failure.request.url} — {failure.getErrorMessage()}")
