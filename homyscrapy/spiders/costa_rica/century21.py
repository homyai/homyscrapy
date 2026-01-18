import scrapy
import json
from datetime import datetime
from homyscrapy.items import PropertyItem

class Century21Spider(scrapy.Spider):
    name = 'century21'
    allowed_domains = ['century21global.com']
    base_url = 'https://www.century21global.com/en/l/homes-for-sale/costa-rica'

    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'DOWNLOAD_DELAY': 3,
        'CONCURRENT_REQUESTS': 2,
        'PLAYWRIGHT_LAUNCH_OPTIONS': {
            'headless': True,
            'args': ['--disable-blink-features=AutomationControlled']
        }
    }

    def start_requests(self):
        yield scrapy.Request(
            url=self.base_url,
            meta=dict(
                playwright=True,
                playwright_include_page=True,
                current_page=1
            )
        )

    async def parse(self, response):
        page = response.meta['playwright_page']
        # Wait for listings to load (target the visual card container)
        try:
            await page.wait_for_selector('.details-container', timeout=20000)
            # Give a small buffer for JSON-LD injection after DOM update
            await page.wait_for_timeout(2000)
        except:
            self.logger.warning("Timeout waiting for Listings")
            await page.close()
            return

        # Extract JSON-LD scripts
        content = await page.content()
        response_obj = response.replace(body=content.encode('utf-8'))
        
        json_LDs = response_obj.css('script[type="application/ld+json"]::text').getall()
        items_found = 0
        
        for json_str in json_LDs:
            try:
                data = json.loads(json_str)
                # Handle list of items or single item
                if isinstance(data, list):
                    for entry in data:
                        yield self.parse_item(entry)
                        items_found += 1
                elif isinstance(data, dict):
                    # Check if it's a Product/Residence
                    if '@type' in data and ('Product' in data['@type'] or 'SingleFamilyResidence' in data['@type']):
                        yield self.parse_item(data)
                        items_found += 1
            except json.JSONDecodeError:
                continue

        await page.close()

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
                    playwright_include_page=True,
                    current_page=next_page
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
        
        # Address/Location
        address = data.get('address', {})
        if isinstance(address, dict):
             parts = [
                 address.get('streetAddress', ''),
                 address.get('addressLocality', ''),
                 address.get('addressRegion', '')
             ]
             item['city'] = ", ".join([p for p in parts if p]).strip()
        
        # Price
        offers = data.get('offers')
        if isinstance(offers, list) and offers:
            offer = offers[0]
            price_spec = offer.get('priceSpecification', {})
            item['price'] = str(price_spec.get('price', ''))
            # We could store currency too if needed
            
        # Specs
        item['bedrooms'] = str(data.get('numberOfBedrooms', ''))
        item['bathrooms'] = str(data.get('numberOfBathroomsTotal', ''))
        
        floor_size = data.get('floorSize', {})
        if isinstance(floor_size, dict):
            item['area'] = str(floor_size.get('value', ''))
            
        # Images
        image = data.get('image')
        if isinstance(image, str):
            item['images'] = [image]
        elif isinstance(image, list):
            item['images'] = image
            
        return item
