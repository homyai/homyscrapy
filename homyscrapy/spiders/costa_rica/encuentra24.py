import scrapy
from homyscrapy.items import PropertyItem
from scrapy_playwright.page import PageMethod
from datetime import datetime

class Encuentra24Spider(scrapy.Spider):
    name = 'encuentra24'
    allowed_domains = ['casas24.com', 'encuentra24.com']
    start_urls = ['https://www.casas24.com/costa-rica-es/propiedades-residenciales?q=withcat.propiedades-residenciales-venta-casas']

    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'DOWNLOAD_DELAY': 5, # Slower for anti-bot
        'CONCURRENT_REQUESTS': 2,
        'PLAYWRIGHT_LAUNCH_OPTIONS': {
            'headless': True,
        }
    }

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(
                url,
                meta={
                    'playwright': True,
                    'playwright_include_page': True, 
                    'playwright_page_methods': [
                        PageMethod("wait_for_selector", "div.cas-ads-grid__item"),
                        # Scroll down to load lazy images or items
                        PageMethod("evaluate", "window.scrollBy(0, document.body.scrollHeight)"),
                        PageMethod("wait_for_timeout", 2000), 
                    ],
                }
            )

    async def parse(self, response):
        page = response.meta["playwright_page"]
        await page.close() # Close page to free resources
        
        # P1 -> div.cas-ads-grid__item
        ads = response.css('div.cas-ads-grid__item')
        
        for ad in ads:
            # Extract basic info from the card to avoid visiting if possible (speed), 
            # but usually we visit for full details.
            
            # Link is usually in an anchor tag inside the tile
            # P2 -> div.cas-ad-tile 
            # Check for anchor
            relative_url = ad.css('a.cas-ad-tile__link::attr(href)').get()
             # Fallback or specific class
            if not relative_url:
                relative_url = ad.css('a::attr(href)').get()
            
            if relative_url:
                yield response.follow(
                    relative_url, 
                    callback=self.parse_property,
                    meta={
                        'playwright': True,
                        'playwright_include_page': True,
                        'playwright_page_methods': [
                            PageMethod("wait_for_selector", "div.cas-property-details__content"),
                        ]
                    }
                )

        # Pagination (P3)
        # Button "Siguiente" or class "cas-pagination__arrow--next"
        next_page = response.css('a.cas-pagination__arrow--next::attr(href)').get()
        if next_page:
            yield response.follow(
                next_page,
                callback=self.parse,
                meta={
                    'playwright': True,
                    'playwright_include_page': True,
                    'playwright_page_methods': [
                         PageMethod("wait_for_selector", "div.cas-ads-grid__item"),
                    ]
                }
            )

    async def parse_property(self, response):
        page = response.meta["playwright_page"]
        await page.close()

        item = PropertyItem()
        item['url'] = response.url
        item['extraction_date'] = datetime.today().strftime("%Y-%m-%d")
        item['source'] = 'Encuentra24'
        item['country'] = 'Costa Rica'
        
        # Details logic based on P4..P10 or inspection
        # Title
        item['title'] = response.css('h1.cas-property__title::text').get('').strip()
        
        # Price
        item['price'] = response.css('.cas-property-msg-container__price::text').get('').strip()
        
        # Location
        # Breadcrumbs or specific fields
        item['location_pcd'] = " ".join(response.css('.cas-property__location ::text').getall()).strip()
        
        # Features (Bed/Bath)
        # Usually icons or lists. "cas-property-insight__attribute"
        attributes = response.css('.cas-property-insight__attribute')
        for attr in attributes:
            text = "".join(attr.css('::text').getall()).lower()
            if 'dorm' in text:
                item['bedrooms'] = text.replace('dorm', '').strip()
            elif 'baño' in text:
                item['bathrooms'] = text.replace('baño', '').strip()
            elif 'm2' in text:
                item['area'] = text.strip()

        # Description
        item['description'] = " ".join(response.css('.cas-property__description *::text').getall()).strip()
        
        yield item
