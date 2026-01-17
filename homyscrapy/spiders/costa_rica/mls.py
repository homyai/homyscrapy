import scrapy
from homyscrapy.items import PropertyItem
from datetime import datetime

class MLSSpider(scrapy.Spider):
    name = 'mls'
    allowed_domains = ['mls.re.cr']
    start_urls = ['https://mls.re.cr/search/rs?form.widgets.object_type%3Alist=house&batch-limit=25&offset=0']

    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'DOWNLOAD_DELAY': 3,
        'CONCURRENT_REQUESTS': 2,
    }

    def parse(self, response):
        rows = response.css('tr')
        self.logger.info(f"Found {len(rows)} rows")
        
        for row in rows:
            # Skip header or empty rows
            if not row.css('td.title'):
                continue
                
            item = PropertyItem()
            item['extraction_date'] = datetime.today().strftime("%Y-%m-%d")
            item['source'] = 'MLS'
            item['country'] = 'Costa Rica'
            
            # Extract basic info from table
            item['title'] = row.css('td.title a::text').get('').strip()
            item['price'] = row.css('td.price a::text').get('').strip()
            item['status'] = row.css('td.workflow_state a::text').get('').strip()
            item['bedrooms'] = row.css('td.bedrooms a::text').get('').strip()
            item['bathrooms'] = row.css('td.bathrooms a::text').get('').strip()
            item['city'] = row.css('td.city a::text').get('').strip()
            item['state'] = row.css('td.state a::text').get('').strip()
            item['external_id'] = row.css('td.listing_id a::text').get('').strip()
            
            # Construct Location
            item['location_pcd'] = f"{item['city']}, {item['state']}"

            detail_url = row.css('td.title a::attr(href)').get()
            
            if detail_url:
                item['url'] = detail_url
                yield response.follow(detail_url, callback=self.parse_property, meta={'item': item})
        
        # Pagination
        next_page = response.xpath("//a[contains(text(), 'Next')]/@href").get()
        if next_page:
            yield response.follow(next_page, callback=self.parse)

    def parse_property(self, response):
        item = response.meta['item']
        
        # Extract description mainly
        # Note: Excluding the h3 "Description" header by targeting the div
        raw_desc = " ".join(response.css('#tab-listing-description div *::text').getall())
        item['description'] = " ".join(raw_desc.split())
        
        yield item
