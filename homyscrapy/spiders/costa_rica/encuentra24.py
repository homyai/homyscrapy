import scrapy
from homyscrapy.items import PropertyItem
from scrapy_playwright.page import PageMethod
from datetime import datetime
from parsel import Selector
import random
import os

class Encuentra24Spider(scrapy.Spider):
    name = 'encuentra24'
    allowed_domains = ['casas24.com', 'encuentra24.com', 'googleusercontent.com', 'webcache.googleusercontent.com']
    start_urls = ['https://www.casas24.com/costa-rica-es/propiedades-residenciales?q=withcat.propiedades-residenciales-venta-casas']

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
        'DOWNLOAD_DELAY': 5,
        'CONCURRENT_REQUESTS': 1,
        'PLAYWRIGHT_LAUNCH_OPTIONS': {
            'headless': True,
            'args': [
                '--disable-blink-features=AutomationControlled',
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-infobars',
                '--window-position=0,0',
                '--ignore-certificate-errors',
                '--ignore-certificate-errors-spki-list',
            ]
        },
        'PLAYWRIGHT_CONTEXTS': {
             'default': {
                 'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                 'viewport': {'width': 1280, 'height': 720},
                 'java_script_enabled': True,
                 'ignore_https_errors': True,
                 'bypass_csp': True,
                 'proxy': proxy_config if proxy_server else None
             }
        },
        'HTTPERROR_ALLOWED_CODES': [403]
    }

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(
                url,
                meta={
                    'playwright': True,
                    'playwright_context': 'default',
                    'playwright_include_page': True, 
                    'playwright_page_methods': [
                        PageMethod("wait_for_selector", "div.cas-ad-tile", timeout=30000),
                        PageMethod("wait_for_timeout", 5000),
                    ],
                    'errback': self.errback_save_screenshot,
                }
            )

    async def parse(self, response):
        page = response.meta["playwright_page"]
        
        # Snapshot content
        html = await page.content()
        await page.close()
        
        sel = Selector(text=html)
        ads = sel.css('div.cas-ad-tile')
        self.logger.info(f"Found {len(ads)} ads.")
        
        for ad in ads:
            item = PropertyItem()
            item['source'] = 'Encuentra24'
            item['country'] = 'Costa Rica'
            item['extraction_date'] = datetime.today().strftime("%Y-%m-%d")
            
            # Base info
            relative_url = ad.css('a.cas-ad-tile__cover::attr(href)').get()
            item['url'] = response.urljoin(relative_url)
            item['title'] = ad.css('.cas-ad-tile__title::text').get('').strip()
            item['location_pcd'] = ad.css('.cas-ad-tile__location::text').get('').strip()
            
            # Price
            item['price'] = ad.css('.cas-ad-tile__price::text').get('').strip()
            
            # Images - Collect from card but allow update from detail
            images = []
            imgs = ad.css('.cas-photos-carousel__photo')
            for img in imgs:
                src = img.css('::attr(data-src)').get() or img.css('::attr(src)').get()
                if src and 'base64' not in src:
                     images.append(src)
            item['images'] = images
            
            # Follow to detail page
            yield response.follow(
                item['url'],
                callback=self.parse_detail,
                meta={
                    'item': item,
                    'playwright': True,
                    'playwright_context': f"detail_{random.randint(0, 100000)}",
                    'playwright_include_page': True,
                    'playwright_page_methods': [
                        PageMethod("wait_for_selector", "div.cas-property-details", timeout=30000), 
                        PageMethod("wait_for_timeout", 2000),
                    ],
                    'errback': self.errback_save_screenshot
                }
            )

        # Pagination
        next_page = sel.css('a.cas-pagination__arrow--next::attr(href)').get()
        if next_page:
            yield response.follow(
                next_page,
                callback=self.parse,
                meta={
                    'playwright': True,
                    'playwright_context': 'default',
                    'playwright_include_page': True,
                    'playwright_page_methods': [
                         PageMethod("wait_for_selector", "div.cas-ad-tile", timeout=30000),
                         PageMethod("evaluate", "window.scrollBy(0, 300)"),
                         PageMethod("wait_for_timeout", 2000), 
                    ],
                    'errback': self.errback_save_screenshot
                }
            )

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
        
        # 2. Features / Amenities
        # .cas-property-benefits__benefit
        benefits = sel.css('.cas-property-benefits__benefit::text').getall()
        clean_features = [f.strip() for f in benefits if f.strip()]
        
        # Merge with any existing features inferred (none yet in this flow)
        item['features'] = clean_features
        
        # 3. Specs from Hero Insight (more accurate than card)
        # Bedrooms, Bathrooms, Area, Parking
        # .cas-property-insight__attribute
        attributes = sel.css('.cas-property-insight__attribute')
        for attr in attributes:
            text = attr.css('.cas-property-insight__attribute-value::text').get('').strip()
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
        # .cas-property-details__content > .cas-property-details__detail-label
        meta = {}
        details = sel.css('.cas-property-details__detail-label')
        for d in details:
            # The label text is strictly the text node of the parent, not the child <p>
            # But the structure is <div>Label <p>Value</p></div>
            # So d.css('::text').get() might be "Label "
            label = d.css('::text').get('').strip()
            value = d.css('p.cas-property-details__detail::text').get('').strip()
            
            if label and value:
                meta[label] = value
                
        # 5. Extract specific fields from metadata map to top-level if needed
        # e.g. Year Built, Maintenance Fee
        
        item['metadata'] = meta
        
        yield item

    async def errback_save_screenshot(self, failure):
        page = failure.request.meta.get("playwright_page")
        if page:
            filename = f"data/error_{datetime.now().strftime('%H%M%S')}.png"
            await page.screenshot(path=filename, full_page=True)
            self.logger.error(f"Captured error screenshot to {filename}")
            await page.close()
        self.logger.error(f"Request failed: {failure}")
