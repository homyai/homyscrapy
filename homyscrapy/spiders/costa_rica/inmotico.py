import scrapy
from homyscrapy.items import PropertyItem
from scrapy_playwright.page import PageMethod
from datetime import datetime

class InmoticoSpider(scrapy.Spider):
    name = 'inmotico'
    allowed_domains = ['inmotico.com', 'godaddy.com'] # Allow godaddy to follow redirect
    start_urls = ['https://www.inmotico.com/venta-de-casas-en-costa-rica-l0-3-0.html']
    
    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'DOWNLOAD_DELAY': 2,
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
                }
            )

    async def parse(self, response):
        page = response.meta["playwright_page"]
        title = await page.title()
        print(f"DEBUG: Page Title is: {title}")
        await page.close() 

        # ... rest of logic
