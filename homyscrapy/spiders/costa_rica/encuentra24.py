import scrapy
from homyscrapy.items import PropertyItem
from scrapy_playwright.page import PageMethod
from datetime import datetime
from parsel import Selector
import random
import os

class Encuentra24Spider(scrapy.Spider):
    name = 'encuentra24'
    allowed_domains = ['casas24.com', 'encuentra24.com']
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
        'DOWNLOAD_DELAY': 5, # Conservative delay
        'CONCURRENT_REQUESTS': 1,
        'PLAYWRIGHT_CONTEXTS': {
             'default': {
                 'persistent': True,
                 'user_agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                 'viewport': {'width': 1920, 'height': 1080},
                 'java_script_enabled': True,
                 'ignore_https_errors': True,
                 'proxy': proxy_config if proxy_server else None
             }
        }
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
                        PageMethod("wait_for_selector", "div.cas-ad-tile"),
                        # Human-like scrolling
                        PageMethod("evaluate", "window.scrollBy(0, 300)"),
                        PageMethod("wait_for_timeout", 1000),
                        PageMethod("evaluate", "window.scrollBy(0, document.body.scrollHeight)"),
                        PageMethod("wait_for_timeout", random.randint(3000, 5000)), 
                    ],
                }
            )

    async def parse(self, response):
        page = response.meta["playwright_page"]
        
        # Snapshot content to avoid race conditions
        html = await page.content()
        await page.close()
        
        sel = Selector(text=html)
        ads = sel.css('div.cas-ad-tile')
        self.logger.info(f"Found {len(ads)} ads.")
        
        for ad in ads:
            relative_url = ad.css('a.cas-ad-tile__cover::attr(href)').get()
            
            if relative_url:
                yield response.follow(
                    relative_url, 
                    callback=self.parse_property,
                    meta={
                        'playwright': True,
                        'playwright_context': 'default', # Maintain session
                        'playwright_include_page': True,
                        'playwright_page_methods': [
                            PageMethod("wait_for_selector", "div.cas-property-details__content", timeout=15000),
                            PageMethod("wait_for_timeout", random.randint(2000, 5000)),
                        ],
                        'errback': self.errback_close_page,
                    }
                )

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
                         PageMethod("wait_for_selector", "div.cas-ad-tile"),
                         PageMethod("wait_for_timeout", 2000),
                    ]
                }
            )

    async def parse_property(self, response):
        page = response.meta["playwright_page"]
        
        try:
            html = await page.content()
            sel = Selector(text=html)
            
            # Anti-Bot Check
            title = sel.css('title::text').get('').lower()
            if 'attention' in title or 'security' in title or 'challenge' in title:
                self.logger.warning(f"Blocked on {response.url} (Title: {title})")
                return

            item = PropertyItem()
            item['url'] = response.url
            item['extraction_date'] = datetime.today().strftime("%Y-%m-%d")
            item['source'] = 'Encuentra24'
            item['country'] = 'Costa Rica'
            
            item['title'] = sel.css('h1.cas-property__title::text').get('').strip()
            item['price'] = sel.css('.cas-property-msg-container__price::text').get('').strip()
            item['location_pcd'] = " ".join(sel.css('.cas-property__location ::text').getall()).strip()
            
            attributes = sel.css('.cas-property-insight__attribute')
            for attr in attributes:
                text = "".join(attr.css('::text').getall()).lower()
                if 'dorm' in text:
                    item['bedrooms'] = text.replace('dorm', '').strip()
                elif 'baño' in text:
                    item['bathrooms'] = text.replace('baño', '').strip()
                elif 'm2' in text:
                    item['area'] = text.strip()

            item['description'] = " ".join(sel.css('.cas-property__description *::text').getall()).strip()
            
            yield item
            
        except Exception as e:
            self.logger.error(f"Error parsing property {response.url}: {e}")
        finally:
            await page.close()

    async def errback_close_page(self, failure):
        page = failure.request.meta.get("playwright_page")
        if page:
            await page.close()
        self.logger.error(f"Request failed: {failure}")
