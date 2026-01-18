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
            
            # Specs
            details = ad.css('.cas-ad-tile__details-item')
            for d in details:
                text = "".join(d.css('::text').getall()).strip()
                icon_href = d.xpath('.//use/@*[local-name()="href"]').get('')
                
                if 'bed' in icon_href:
                    item['bedrooms'] = text
                elif 'bath' in icon_href:
                    item['bathrooms'] = text
                elif 'size' in icon_href or 'm' in text: 
                     item['area'] = text
            
            # Images
            images = []
            imgs = ad.css('.cas-photos-carousel__photo')
            for img in imgs:
                src = img.css('::attr(data-src)').get() or img.css('::attr(src)').get()
                if src and 'base64' not in src:
                     images.append(src)
            item['images'] = images
            
            # Description & Feature Extraction
            desc = ad.css('.cas-ad-tile__short-description::text').get('').strip()
            item['description'] = desc
            
            # Inferred Features from Description
            features = []
            keywords = {
                'piscina': 'Piscina',
                'pool': 'Piscina',
                'terraza': 'Terraza',
                'terrace': 'Terraza',
                'balcon': 'Balcón',
                'balcony': 'Balcón',
                'seguridad': 'Seguridad 24/7',
                'security': 'Seguridad 24/7',
                'parqueo': 'Parqueo',
                'garage': 'Garage',
                'cochera': 'Cochera',
                'amueblado': 'Amueblado',
                'furniture': 'Amueblado',
                'linea blanca': 'Línea Blanca',
                'appliances': 'Línea Blanca',
                'gimnasio': 'Gimnasio',
                'gym': 'Gimnasio',
                'bodega': 'Bodega',
                'storage': 'Bodega'
            }
            
            desc_lower = desc.lower() + " " + item['title'].lower()
            for k, v in keywords.items():
                if k in desc_lower:
                    if v not in features:
                        features.append(v)
            
            item['features'] = features
            
            # Metadata (Inferred)
            meta = {}
            # Try to find fees
            import re
            img = re.search(r'(?:cuota|mantenimiento)\s*:?\s*\$?(\d+)', desc_lower)
            if img:
                 meta['Mantenimiento'] = img.group(1)
            
            item['metadata'] = meta

            yield item

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

    async def errback_save_screenshot(self, failure):
        page = failure.request.meta.get("playwright_page")
        if page:
            filename = f"data/error_{datetime.now().strftime('%H%M%S')}.png"
            await page.screenshot(path=filename, full_page=True)
            self.logger.error(f"Captured error screenshot to {filename}")
            await page.close()
        self.logger.error(f"Request failed: {failure}")
