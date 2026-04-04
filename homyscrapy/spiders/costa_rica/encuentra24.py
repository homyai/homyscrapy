import scrapy
from homyscrapy.items import PropertyItem
from scrapy_playwright.page import PageMethod
from playwright_stealth import Stealth
_stealth = Stealth()
stealth_async = _stealth.apply_stealth_async
from datetime import datetime
from parsel import Selector
import random
import os

class Encuentra24Spider(scrapy.Spider):
    name = 'encuentra24'
    allowed_domains = ['encuentra24.com', 'googleusercontent.com', 'webcache.googleusercontent.com']
    start_urls = ['https://www.encuentra24.com/costa-rica-es/bienes-raices-venta-de-propiedades-casas/']
    
    # Configurable scraping parameters
    def __init__(self, max_pages=0, start_page=1, output_date=None, *args, **kwargs):
        super(Encuentra24Spider, self).__init__(*args, **kwargs)
        self.max_pages = int(max_pages)  # 0 = unlimited
        self.start_page = int(start_page)
        self.current_page = 0
        self.output_date = output_date or datetime.today().strftime("%Y-%m-%d")
        self.logger.info(f"Starting scrape - max_pages: {self.max_pages}, start_page: {self.start_page}, date: {self.output_date}")
    

    # Dynamic Proxy Configuration
    proxy_config = {}
    proxy_server = os.getenv("PROXY_SERVER")
    if proxy_server:
        proxy_config = {
            "server": proxy_server,
            "username": os.getenv("PROXY_USER"),
            "password": os.getenv("PROXY_PASSWORD")
        }


    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'DOWNLOAD_DELAY': 8,
        'RANDOMIZE_DOWNLOAD_DELAY': True,  # effective range: 4–12s
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
                '--start-maximized',
                '--window-position=0,0',
                '--ignore-certificate-errors',
                '--ignore-certificate-errors-spki-list',
            ]
        },
        'PLAYWRIGHT_CONTEXTS': {
             'default': {
                 'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
                 'viewport': {'width': 1920, 'height': 1080},
                 'java_script_enabled': True,
                 'ignore_https_errors': True,
                 'bypass_csp': True,
                 'proxy': proxy_config if proxy_server else None
             }
        },
        'HTTPERROR_ALLOWED_CODES': [403, 500],
        'FEED_EXPORT_ENCODING': 'utf-8'
    }


    async def init_page(self, page, request):
        """Apply stealth patches before any page script runs (pre-navigation)."""
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
                    # Stop navigation as soon as DOM is ready — avoids timeout on slow analytics/ad scripts
                    'playwright_page_goto_kwargs': {'wait_until': 'domcontentloaded'},
                    'playwright_page_methods': [
                        PageMethod("wait_for_timeout", 20000),
                    ],
                },
                errback=self.errback_save_screenshot,
            )

    async def parse(self, response):
        page = response.meta["playwright_page"]

        # Human-like scroll before extracting
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight / 3)")
        await page.wait_for_timeout(random.randint(1200, 2800))
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight / 2)")
        await page.wait_for_timeout(random.randint(800, 1800))

        # Snapshot content
        html = await page.content()
        await page.close()
        
        sel = Selector(text=html)

        # Support two themes served by the site:
        # d3-* (SSR, class=d3): div.d3-ad-tile
        # m3-* (CSR, class=m3): div.card[data-adid]
        ads_d3 = sel.css('div.d3-ad-tile')
        ads_m3 = sel.css('div.card[data-adid]')
        ads = ads_d3 if ads_d3 else ads_m3
        theme = 'd3' if ads_d3 else 'm3'
        self.logger.info(f"Found {len(ads)} ads on page {self.current_page} (theme: {theme})")

        # Always dump HTML for page 0; only on empty for subsequent pages
        if len(ads) == 0 or self.current_page == 0:
            debug_filename = f"data/debug_page_{self.current_page}.html"
            with open(debug_filename, 'w', encoding='utf-8') as f:
                f.write(html)
            if len(ads) == 0:
                self.logger.warning(f"No ads found on page {self.current_page}. HTML dumped to {debug_filename}")
            else:
                self.logger.info(f"Dumped page 0 HTML to {debug_filename} for inspection")

        for ad in ads:
            item = PropertyItem()
            item['source'] = 'Encuentra24'
            item['country'] = 'Costa Rica'
            item['extraction_date'] = datetime.today().strftime("%Y-%m-%d")

            if theme == 'd3':
                relative_url = ad.css('a.d3-ad-tile__description::attr(href)').get()
                item['url'] = response.urljoin(relative_url) if relative_url else ''
                title = ad.css('span.d3-ad-tile__title::text').get()
                item['title'] = title.strip() if title else ''
                location_texts = ad.css('div.d3-ad-tile__location span::text').getall()
                item['location_pcd'] = ' '.join(t.strip() for t in location_texts if t.strip())
                price = ad.css('div.d3-ad-tile__price::text').get()
                item['price'] = price.strip() if price else ''
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
                # m3-* theme
                relative_url = ad.css('a.ad-name::attr(href)').get()
                item['url'] = response.urljoin(relative_url) if relative_url else ''
                title = ad.css('a.ad-name::text').get()
                item['title'] = title.strip() if title else ''
                location_texts = ad.css('div.ad-location::text').getall()
                item['location_pcd'] = ' '.join(t.strip() for t in location_texts if t.strip())
                price = ad.css('div.ad-price::text').get()
                item['price'] = price.strip() if price else ''
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
            
            # Prepare meta for detail page
            detail_meta = {
                'item': item,
                'playwright': True,
                'playwright_context': 'default',
                'playwright_include_page': True,
                'playwright_page_init_callback': self.init_page,
                'playwright_page_goto_kwargs': {'wait_until': 'domcontentloaded'},
                'playwright_page_methods': [
                    PageMethod("wait_for_timeout", 10000),
                ],
            }
            
            # Pass the CURRENT proxy to the detail request to maintain session continuity
            current_context = response.meta.get('playwright_context_kwargs', {})
            if 'proxy' in current_context:
                detail_meta['playwright_context_kwargs'] = {'proxy': current_context['proxy']}
            
            # Follow to detail page
            if not item['url']:
                self.logger.warning(f"Skipping ad with no URL: {item.get('title', 'unknown')}")
                continue
            yield response.follow(
                item['url'],
                callback=self.parse_detail,
                meta=detail_meta,
                errback=self.errback_save_screenshot
            )

        # Pagination with page tracking
        self.current_page += 1
        self.logger.info(f"Completed page {self.current_page}")
        
        # Next page arrow link
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
                        PageMethod("wait_for_timeout", 20000),
                    ],
                },
                errback=self.errback_save_screenshot,
                dont_filter=True
            )
        else:
            if not next_page:
                self.logger.info("No more pages to scrape - reached end")
            else:
                self.logger.info(f"Reached max_pages limit ({self.max_pages})")

    async def parse_detail(self, response):
        page = response.meta["playwright_page"]

        html = await page.content()
        await page.close()
        
        sel = Selector(text=html)
        item = response.meta['item']
        
        # 1. Full Description — d3-* theme or m3-* (product-comments) theme
        desc_lines = sel.css('.d3-property-about__text *::text, .cas-property-about__text *::text').getall()
        if not any(line.strip() for line in desc_lines):
            desc_lines = sel.css('.product-comments *::text, .product-comments::text').getall()
        full_desc = "\n".join([line.strip() for line in desc_lines if line.strip()])
        item['description'] = full_desc

        if not full_desc:
            debug_filename = f"data/debug_detail_{datetime.now().strftime('%H%M%S')}.html"
            with open(debug_filename, 'w', encoding='utf-8') as f:
                f.write(html)
            self.logger.warning(f"Empty description for {response.url}. HTML dumped to {debug_filename}")

        # 2. Features / Amenities — d3-* or m3-* (product-extras-extra)
        benefits = sel.css('.d3-property-benefits__benefit::text, .cas-property-benefits__benefit::text').getall()
        if not benefits:
            benefits = sel.css('.product-extras-extra::text').getall()
        item['features'] = [f.strip() for f in benefits if f.strip()]

        # 3. Specs — d3-* insight attributes or m3-* product-icons-icon
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
                text_parts = [t.strip() for t in icon_item.css('::text').getall() if t.strip()]
                text = ' '.join(text_parts)
                if '#ic-bed"' in icon_html:
                    item['bedrooms'] = text
                elif '#ic-bath"' in icon_html:
                    item['bathrooms'] = text
                elif '#ic-resize"' in icon_html or '#ic-size"' in icon_html:
                    item['area'] = text
                elif '#ic-parking"' in icon_html:
                    item['garage'] = text

        # 4. Detailed Metadata — d3-* detail labels or m3-* product-publication col-6 pairs
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

    async def errback_save_screenshot(self, failure):
        self.logger.error(f"ERRBACK TRIGGERED for {failure.request.url if failure.request else 'unknown'}")
        self.logger.error(f"Request failed: {failure}")
        
        page = failure.request.meta.get("playwright_page")
        if not page:
             self.logger.error("NO PLAYWRIGHT PAGE IN META! Cannot take screenshot.")
             return
        
        filename = f"data/error_{datetime.now().strftime('%H%M%S')}.png"
        try:
             await page.screenshot(path=filename, full_page=True)
             self.logger.error(f"Captured error screenshot to {filename}")
        except Exception as e:
             self.logger.error(f"Failed to capture screenshot: {e}")
        
        # Ensure page is closed to avoid leaks
        try:
            await page.close()
        except Exception:
            pass
