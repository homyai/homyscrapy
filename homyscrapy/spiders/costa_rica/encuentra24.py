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
    allowed_domains = ['casas24.com', 'encuentra24.com', 'googleusercontent.com', 'webcache.googleusercontent.com']
    start_urls = ['https://www.casas24.com/costa-rica-es/propiedades-residenciales?q=withcat.propiedades-residenciales-venta-casas']
    
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

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(
                url,
                meta={
                    'playwright': True,
                    'playwright_context': 'default',
                    'playwright_include_page': True,
                    'playwright_page_init_callback': self.init_page,
                    'playwright_page_methods': [
                        PageMethod("wait_for_timeout", 20000),
                    ],
                    'errback': self.errback_save_screenshot,
                }
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
        
        # Support both 'cas-' (legacy) and 'd3-' (new theme) classes
        ads = sel.css('div.cas-ad-tile, div.d3-ad-tile')
        self.logger.info(f"Found {len(ads)} ads on page {self.current_page}")
        
        # Always dump HTML for page 0 to inspect Cloudflare status; only on empty for subsequent pages
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
            
            # Base info - try both selector styles
            relative_url = ad.css('a.cas-ad-tile__cover::attr(href), a.d3-ad-tile__cover::attr(href)').get()
            item['url'] = response.urljoin(relative_url)
            
            title = ad.css('.cas-ad-tile__title::text, .d3-ad-tile__title::text').get()
            item['title'] = title.strip() if title else ''
            
            location = ad.css('.cas-ad-tile__location::text, .d3-ad-tile__location::text').get()
            item['location_pcd'] = location.strip() if location else ''
            
            # Price
            price = ad.css('.cas-ad-tile__price::text, .d3-ad-tile__price::text').get()
            item['price'] = price.strip() if price else ''
            
            # Images - Collect from card but allow update from detail
            images = []
            # Try both legacy and new carousel classes
            imgs = ad.css('.cas-photos-carousel__photo, .d3-photos-carousel__photo')
            for img in imgs:
                src = img.css('::attr(data-src)').get() or img.css('::attr(src)').get()
                if src and 'base64' not in src:
                     images.append(src)
            item['images'] = images
            
            # Prepare meta for detail page
            detail_meta = {
                'item': item,
                'playwright': True,
                'playwright_context': 'default',
                'playwright_include_page': True,
                'playwright_page_init_callback': self.init_page,
                'playwright_page_methods': [
                    PageMethod("wait_for_timeout", 5000),
                ],
            }
            
            # Pass the CURRENT proxy to the detail request to maintain session continuity
            current_context = response.meta.get('playwright_context_kwargs', {})
            if 'proxy' in current_context:
                detail_meta['playwright_context_kwargs'] = {'proxy': current_context['proxy']}
            
            # Follow to detail page
            yield response.follow(
                item['url'],
                callback=self.parse_detail,
                meta=detail_meta,
                errback=self.errback_save_screenshot
            )

        # Pagination with page tracking
        self.current_page += 1
        self.logger.info(f"Completed page {self.current_page}")
        
        # Check if we should continue to next page
        next_page = sel.css('a.cas-pagination__arrow--next::attr(href), a.d3-pagination__arrow--next::attr(href)').get()
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
                    'playwright_page_methods': [
                        PageMethod("wait_for_selector", "div.cas-ad-tile, div.d3-ad-tile, .d3-pagination, .cas-pagination", timeout=60000),
                        PageMethod("evaluate", "window.scrollBy(0, 300)"),
                        PageMethod("wait_for_timeout", 2000),
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
        
        # 1. Full Description
        # .cas-property-about__text
        desc_lines = sel.css('.cas-property-about__text *::text').getall()
        full_desc = "\n".join([line.strip() for line in desc_lines if line.strip()])
        item['description'] = full_desc
        
        if not full_desc:
             self.logger.warning(f"Empty description for {response.url}. Dumping HTML.")
             debug_filename = f"data/debug_detail_{datetime.now().strftime('%H%M%S')}.html"
             with open(debug_filename, 'w', encoding='utf-8') as f:
                 f.write(html)
             self.logger.info(f"Saved debug HTML to {debug_filename}")
        
        # 2. Features / Amenities
        # .cas-property-benefits__benefit
        benefits = sel.css('.cas-property-benefits__benefit::text').getall()
        clean_features = [f.strip() for f in benefits if f.strip()]
        
        # Merge with any existing features inferred (none yet in this flow)
        item['features'] = clean_features
        
        # 3. Specs from Hero Insight (more accurate than card)
        # Bedrooms, Bathrooms, Area, Parking
        attributes = sel.css('.cas-property-insight__attribute, .d3-property-insight__attribute')
        for attr in attributes:
            text = attr.css('.cas-property-insight__attribute-value::text, .d3-property-insight__attribute-value::text').get('').strip()
            icon_html = attr.get() # Check SVG
            
            if 'sprites.svg#bed' in icon_html:
                item['bedrooms'] = text
            elif 'sprites.svg#bath' in icon_html:
                item['bathrooms'] = text
            elif 'sprites.svg#size' in icon_html:
                item['area'] = text
            elif 'sprites.svg#parking' in icon_html:
                item['garage'] = text

        # 4. Detailed Metadata
        meta = {}
        details = sel.css('.cas-property-details__detail-label, .d3-property-details__detail-label')
        for d in details:
            # The label text is strictly the text node of the parent, not the child <p>
            # But the structure is <div>Label <p>Value</p></div>
            # So d.css('::text').get() might be "Label "
            label = d.css('::text').get('').strip()
            value = d.css('p.cas-property-details__detail::text, p.d3-property-details__detail::text').get('').strip()
            
            if label and value:
                meta[label] = value
                
        # 5. Extract specific fields from metadata map to top-level if needed
        # e.g. Year Built, Maintenance Fee
        
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
