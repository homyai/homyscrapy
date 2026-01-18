import scrapy
import json
import re
from datetime import datetime
from homyscrapy.items import PropertyItem
from scrapy.spiders import SitemapSpider

class InHausSpider(SitemapSpider):
    name = 'inhaus'
    allowed_domains = ['inhauscr.com']
    sitemap_urls = ['https://www.inhauscr.com/sitemap.xml']
    sitemap_rules = [
        ('/propiedades/', 'parse_property'),
    ]

    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'DOWNLOAD_DELAY': 2,
        'CONCURRENT_REQUESTS': 2,
    }

    def parse_property(self, response):
        item = PropertyItem()
        item['url'] = response.url
        item['extraction_date'] = datetime.today().strftime("%Y-%m-%d")
        item['source'] = 'InHaus'
        item['country'] = 'Costa Rica'
        
        # 1. Title
        item['title'] = response.css('h1::text').get('').strip()
        
        # 2. Price
        # JSON-LD uses "price": 2100
        # Internal JSON might use "precio": ...
        script_text = response.text
        
        # Try finding schema price first
        price_match = re.search(r'"price"\s*:\s*([0-9.]+)', script_text)
        if price_match:
             item['price'] = price_match.group(1)
             
             # Check currency
             curr_match = re.search(r'"priceCurrency"\s*:\s*"(\w+)"', script_text)
             if curr_match and curr_match.group(1) == 'CRC':
                 # Maybe convert? For now just log or store. Item has no currency field explicitly, 
                 # but we assume USD usually. If CRC, value might be large.
                 # Let's append currency to price if not USD to be safe, or just store number.
                 pass
        
        if not item.get('price'):
            # Fallback to "precio"
            price_match = re.search(r'"precio"\s*:\s*"?(\$?[0-9,.]+)"?', script_text)
            if price_match:
                 item['price'] = price_match.group(1).replace('$', '').strip()
        
        # 3. Description
        item['description'] = "\n".join(response.css('.prose p::text').getall()).strip()
        
        # 4. ID
        id_match = re.search(r'#\s*(\d+)', response.text)
        if id_match:
            item['external_id'] = id_match.group(1)
            
        # 5. Metadata
        for grid_item in response.css('.grid.grid-cols-2.md\:grid-cols-3 .bg-muted\/50'):
             value_div = grid_item.css('.text-xl.font-bold ::text').get()
             label_div = grid_item.css('.text-sm.text-muted-foreground ::text').get()
             
             if value_div and label_div:
                 val = value_div.strip()
                 lbl = label_div.strip().lower()
                 
                 if 'habitaciones' in lbl:
                     item['bedrooms'] = val
                 elif 'baños' in lbl and 'medios' not in lbl:
                     item['bathrooms'] = val
                 elif 'construcción' in lbl:
                     item['area'] = val
                 elif 'terreno' in lbl:
                     item['lot_area'] = val
                     
        # 6. Images
        item['images'] = response.css('meta[property="og:image"]::attr(content)').getall()
        gallery_images = response.css('img[alt^="Propiedad"]::attr(src)').getall()
        if gallery_images:
            item['images'].extend(gallery_images) 
        item['images'] = list(set(item['images']))

        # 7. Location - Scoping to the h1 container to avoid "Similar Properties"
        # The structure is h1 -> div -> svg + span
        # We can find the h1, then the next sibling div
        loc_container = response.xpath('//h1/following-sibling::div[1]')
        if loc_container:
            loc_text = loc_container.css('span::text').get()
            if loc_text:
                item['city'] = loc_text.strip()

        yield item
