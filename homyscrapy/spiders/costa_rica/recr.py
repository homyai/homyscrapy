import scrapy
from homyscrapy.items import PropertyItem
from datetime import datetime

class RECRSpider(scrapy.Spider):
    name = 'recr'
    allowed_domains = ['re.cr', 'www.re.cr']
    # Long URL provided by user
    start_urls = ['https://www.re.cr/en/costa-rica-real-estate-for-sale/search-properties?form.search-properties.buttons.search=&form.search-properties.widgets.object_type%3Alist=house&form.search-properties.widgets.object_type%3Alist=mobile&form.search-properties.widgets.object_type%3Alist=multiplex&form.search-properties.widgets.object_type%3Alist=townhouse&form.search-properties.widgets.listing_type=rs']

    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'DOWNLOAD_DELAY': 3,
        'CONCURRENT_REQUESTS': 2,
    }

    def parse(self, response):
        # The list view uses tiles
        tiles = response.css('div.tileItem')
        self.logger.info(f"Found {len(tiles)} tiles")
        
        for tile in tiles:
            item = PropertyItem()
            item['extraction_date'] = datetime.today().strftime("%Y-%m-%d")
            item['source'] = 'RE.CR'
            item['country'] = 'Costa Rica'
            
            # Extract basic info from tile
            # Title & URL
            title_link = tile.css('h2.tileHeadline a')
            item['title'] = title_link.css('::text').get('').strip()
            
            detail_url = title_link.css('::attr(href)').get()
            
            # Price
            item['price'] = tile.css('.listing__price dd::text').get('').strip()
            
            self.logger.info(f"Tile found: {item['title']} - {detail_url}")

            if detail_url:
                item['url'] = detail_url
                self.logger.info(f"Yielding follow request for {detail_url}")
                yield response.follow(detail_url, callback=self.parse_property, meta={'item': item})
        
        # Pagination
        next_page = response.css('.next a::attr(href)').get()
        if next_page:
            yield response.follow(next_page, callback=self.parse)

    def parse_property(self, response):
        item = response.meta['item']
        
        # Description
        raw_desc = " ".join(response.css('.documentDescription *::text').getall())
        item['description'] = " ".join(raw_desc.split())

        # Images (from data-nav="thumbs" container)
        item['images'] = response.css('div[data-nav="thumbs"] a::attr(href)').getall()

        # Metadata from tables
        item['metadata'] = {}
        for tr in response.css('tr'):
             key = tr.css('th::text').get('').strip()
             # Use xpath string() to get all text inside td (including links if any)
             val = tr.xpath('string(td)').get('').strip()
             if key and val:
                  item['metadata'][key] = val
        
        # Map fields
        meta = item['metadata']
        
        if 'bedrooms' not in item or not item['bedrooms']:
             item['bedrooms'] = meta.get('Bedrooms', '').strip() or meta.get('Number of Bedrooms', '').strip()
        
        if 'bathrooms' not in item or not item['bathrooms']:
             item['bathrooms'] = meta.get('Bathrooms', '').strip() or meta.get('Number of Full Bathrooms', '').strip()
        
        if 'city' not in item or not item['city']:
             item['city'] = meta.get('City', '').strip() or meta.get('City/Town', '').strip()
             
        if 'state' not in item or not item['state']:
             item['state'] = meta.get('State', '').strip() or meta.get('Provincia', '').strip()
        
        # Construct Location
        if item.get('city') and item.get('state'):
            item['location_pcd'] = f"{item['city']}, {item['state']}"
        elif item.get('city'):
            item['location_pcd'] = item['city']
            
        # Parse External ID
        if 'external_id' not in item:
             item['external_id'] = meta.get('Listing ID', '')
             
        # Areas
        # Use XPath to find headers containing specific text
        lot_area = response.xpath('//th[contains(text(), "Lot Size")]/following-sibling::td/text()').get()
        if lot_area: item['lot_area'] = lot_area.strip()
        
        living_area = response.xpath('//th[contains(text(), "Living Area")]/following-sibling::td/text()').get()
        if living_area: item['area'] = living_area.strip()

        yield item
