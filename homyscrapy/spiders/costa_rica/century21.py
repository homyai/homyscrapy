import scrapy
import json
from datetime import datetime
from homyscrapy.items import PropertyItem
from scrapy_playwright.page import PageMethod
from playwright_stealth import Stealth

_stealth = Stealth()
stealth_async = _stealth.apply_stealth_async

# NOTE: This spider is currently non-functional.
# The century21.com site gates all content behind an invisible reCAPTCHA v3
# challenge. The listing API only fires after obtaining a passing score, which
# headless Playwright cannot achieve without a CAPTCHA solving service (e.g.
# 2captcha or anti-captcha). The domain also moved from century21global.com to
# century21.com — the selectors and URL have been updated here, but the spider
# will not scrape any items until reCAPTCHA is handled.

class Century21Spider(scrapy.Spider):
    name = 'century21'
    allowed_domains = ['century21.com']
    base_url = 'https://www.century21.com/office/global/en/l/homes-for-sale/costa-rica'

    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'DOWNLOAD_DELAY': 3,
        'CONCURRENT_REQUESTS': 1,
        'PLAYWRIGHT_LAUNCH_OPTIONS': {
            'headless': True,
            'args': [
                '--disable-blink-features=AutomationControlled',
                '--no-sandbox',
                '--disable-setuid-sandbox',
            ]
        },
        'PLAYWRIGHT_CONTEXTS': {
            'default': {
                'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
                'viewport': {'width': 1920, 'height': 1080},
                'java_script_enabled': True,
                'ignore_https_errors': True,
            }
        },
    }

    async def init_page(self, page, request):
        await stealth_async(page)

    async def start(self):
        self.logger.error(
            "Century21 spider is disabled — blocked by reCAPTCHA v3. "
            "Integrate a CAPTCHA solving service before running this spider."
        )
        return
        yield  # keeps this a generator

        yield scrapy.Request(
            url=self.base_url,
            meta=dict(
                playwright=True,
                playwright_context='default',
                playwright_include_page=True,
                playwright_page_init_callback=self.init_page,
                playwright_page_goto_kwargs={'wait_until': 'domcontentloaded'},
                playwright_page_methods=[
                    PageMethod('wait_for_timeout', 30000),
                ],
                current_page=1,
            )
        )

    async def parse(self, response):
        page = response.meta['playwright_page']
        content = await page.content()
        title = await page.title()
        await page.close()

        if 'loading' in title.lower():
            self.logger.warning(
                "Page stuck in loading state — reCAPTCHA v3 challenge not solved. "
                "A CAPTCHA solving service is required for this spider to work."
            )
            return

        from parsel import Selector
        sel = Selector(text=content)
        json_LDs = sel.css('script[type="application/ld+json"]::text').getall()
        items_found = 0

        for json_str in json_LDs:
            try:
                data = json.loads(json_str)
                if isinstance(data, list):
                    for entry in data:
                        yield self.parse_item(entry)
                        items_found += 1
                elif isinstance(data, dict):
                    if '@type' in data and ('Product' in data['@type'] or 'SingleFamilyResidence' in data['@type']):
                        yield self.parse_item(data)
                        items_found += 1
            except json.JSONDecodeError:
                continue

        # Pagination
        current_page = response.meta.get('current_page', 1)
        if items_found > 0:
            next_page = current_page + 1
            next_url = f"{self.base_url}?page={next_page}"
            self.logger.info(f"Navigating to page {next_page}: {next_url}")
            yield scrapy.Request(
                next_url,
                meta=dict(
                    playwright=True,
                    playwright_context='default',
                    playwright_include_page=True,
                    playwright_page_init_callback=self.init_page,
                    playwright_page_goto_kwargs={'wait_until': 'domcontentloaded'},
                    playwright_page_methods=[PageMethod('wait_for_timeout', 30000)],
                    current_page=next_page,
                )
            )

    def parse_item(self, data):
        item = PropertyItem()
        item['extraction_date'] = datetime.today().strftime("%Y-%m-%d")
        item['source'] = 'Century21'
        item['country'] = 'Costa Rica'

        item['title'] = data.get('name')
        item['description'] = data.get('description')
        item['url'] = data.get('url')

        address = data.get('address', {})
        if isinstance(address, dict):
            parts = [
                address.get('streetAddress', ''),
                address.get('addressLocality', ''),
                address.get('addressRegion', '')
            ]
            item['city'] = ", ".join([p for p in parts if p]).strip()

        offers = data.get('offers')
        if isinstance(offers, list) and offers:
            offer = offers[0]
            price_spec = offer.get('priceSpecification', {})
            item['price'] = str(price_spec.get('price', ''))

        item['bedrooms'] = str(data.get('numberOfBedrooms', ''))
        item['bathrooms'] = str(data.get('numberOfBathroomsTotal', ''))

        floor_size = data.get('floorSize', {})
        if isinstance(floor_size, dict):
            item['area'] = str(floor_size.get('value', ''))

        image = data.get('image')
        if isinstance(image, str):
            item['images'] = [image]
        elif isinstance(image, list):
            item['images'] = image

        return item
