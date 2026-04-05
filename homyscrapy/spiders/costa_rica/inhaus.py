import re
from scrapy.spiders import SitemapSpider
from homyscrapy.spiders.base_spider import BasePropertySpider


class InHausSpider(BasePropertySpider, SitemapSpider):
    name = 'inhaus'
    source = 'InHaus'
    allowed_domains = ['inhauscr.com']
    sitemap_urls = ['https://www.inhauscr.com/sitemap.xml']
    sitemap_rules = [('/propiedades/', 'parse_property')]

    # Override global conservative defaults — InHaus is a low-traffic static site
    custom_settings = {
        'USE_PROXY': False,
        'DOWNLOAD_DELAY': 2,
        'CONCURRENT_REQUESTS': 2,
    }

    def parse_property(self, response):
        item = self.make_item()
        item['url'] = response.url
        item['title'] = response.css('h1::text').get('').strip()

        # Price — try JSON-LD schema first, fall back to "precio" field
        script_text = response.text
        price_match = re.search(r'"price"\s*:\s*([0-9.]+)', script_text)
        if price_match:
            item['price'] = price_match.group(1)
        else:
            fallback = re.search(r'"precio"\s*:\s*"?(\$?[0-9,.]+)"?', script_text)
            if fallback:
                item['price'] = fallback.group(1).replace('$', '').strip()

        item['description'] = '\n'.join(response.css('.prose p::text').getall()).strip()

        id_match = re.search(r'#\s*(\d+)', script_text)
        if id_match:
            item['external_id'] = id_match.group(1)

        for grid_item in response.css('.grid.grid-cols-2.md\\:grid-cols-3 .bg-muted\\/50'):
            val = grid_item.css('.text-xl.font-bold ::text').get()
            lbl = grid_item.css('.text-sm.text-muted-foreground ::text').get()
            if not (val and lbl):
                continue
            val, lbl = val.strip(), lbl.strip().lower()
            if 'habitaciones' in lbl:
                item['bedrooms'] = val
            elif 'baños' in lbl and 'medios' not in lbl:
                item['bathrooms'] = val
            elif 'construcción' in lbl:
                item['area'] = val
            elif 'terreno' in lbl:
                item['lot_area'] = val

        # Images — og:image + gallery; dict.fromkeys preserves order while deduplicating
        images = response.css('meta[property="og:image"]::attr(content)').getall()
        images += response.css('img[alt^="Propiedad"]::attr(src)').getall()
        item['images'] = list(dict.fromkeys(images))

        # Location from the div immediately following the h1
        loc_container = response.xpath('//h1/following-sibling::div[1]')
        if loc_container:
            loc_text = loc_container.css('span::text').get()
            if loc_text:
                item['city'] = loc_text.strip()

        yield item
