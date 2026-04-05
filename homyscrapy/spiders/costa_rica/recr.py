from homyscrapy.spiders.base_spider import BasePropertySpider


class RECRSpider(BasePropertySpider):
    name = 'recr'
    source = 'RE.CR'
    allowed_domains = ['re.cr', 'www.re.cr']
    start_urls = [
        'https://www.re.cr/en/costa-rica-real-estate-for-sale/search-properties'
        '?form.search-properties.buttons.search='
        '&form.search-properties.widgets.object_type%3Alist=house'
        '&form.search-properties.widgets.object_type%3Alist=mobile'
        '&form.search-properties.widgets.object_type%3Alist=multiplex'
        '&form.search-properties.widgets.object_type%3Alist=townhouse'
        '&form.search-properties.widgets.listing_type=rs'
    ]

    # Override global conservative defaults — RE.CR is a low-traffic static site
    custom_settings = {
        'USE_PROXY': False,
        'DOWNLOAD_DELAY': 3,
        'CONCURRENT_REQUESTS': 2,
    }

    def parse(self, response):
        tiles = response.css('div.tileItem')
        self.logger.info(f"Found {len(tiles)} tiles")

        for tile in tiles:
            title_link = tile.css('h2.tileHeadline a')
            detail_url = title_link.css('::attr(href)').get()
            if not detail_url:
                continue

            item = self.make_item()
            item['title'] = title_link.css('::text').get('').strip()
            item['price'] = tile.css('.listing__price dd::text').get('').strip()
            item['url'] = detail_url
            yield response.follow(detail_url, callback=self.parse_property, meta={'item': item})

        next_page = response.css('.next a::attr(href)').get()
        if next_page:
            yield response.follow(next_page, callback=self.parse)

    def parse_property(self, response):
        item = response.meta['item']

        raw_desc = ' '.join(response.css('.documentDescription *::text').getall())
        item['description'] = self.collapse_whitespace(raw_desc)

        item['images'] = response.css('div[data-nav="thumbs"] a::attr(href)').getall()

        item['metadata'] = {}
        for tr in response.css('tr'):
            key = tr.css('th::text').get('').strip()
            val = tr.xpath('string(td)').get('').strip()
            if key and val:
                item['metadata'][key] = val

        meta = item['metadata']
        item['bedrooms'] = meta.get('Bedrooms', '').strip() or meta.get('Number of Bedrooms', '').strip()
        item['bathrooms'] = meta.get('Bathrooms', '').strip() or meta.get('Number of Full Bathrooms', '').strip()
        item['city'] = meta.get('City', '').strip() or meta.get('City/Town', '').strip()
        item['state'] = meta.get('State', '').strip() or meta.get('Provincia', '').strip()
        item['external_id'] = meta.get('Listing ID', '')

        if item.get('city') and item.get('state'):
            item['location_pcd'] = f"{item['city']}, {item['state']}"
        elif item.get('city'):
            item['location_pcd'] = item['city']

        lot_area = response.xpath('//th[contains(text(), "Lot Size")]/following-sibling::td/text()').get()
        if lot_area:
            item['lot_area'] = lot_area.strip()

        living_area = response.xpath('//th[contains(text(), "Living Area")]/following-sibling::td/text()').get()
        if living_area:
            item['area'] = living_area.strip()

        yield item
