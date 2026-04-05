from homyscrapy.spiders.base_spider import BasePropertySpider


class MLSSpider(BasePropertySpider):
    name = 'mls'
    source = 'MLS'
    allowed_domains = ['mls.re.cr']
    start_urls = ['https://mls.re.cr/search/rs?form.widgets.object_type%3Alist=house&batch-limit=25&offset=0']

    # Override global conservative defaults — MLS is a low-traffic static site
    custom_settings = {
        'USE_PROXY': False,
        'DOWNLOAD_DELAY': 3,
        'CONCURRENT_REQUESTS': 2,
    }

    def parse(self, response):
        rows = response.css('tr')
        self.logger.info(f"Found {len(rows)} rows")

        for row in rows:
            if not row.css('td.title'):
                continue

            item = self.make_item()
            item['title'] = row.css('td.title a::text').get('').strip()
            item['price'] = row.css('td.price a::text').get('').strip()
            item['status'] = row.css('td.workflow_state a::text').get('').strip()
            item['bedrooms'] = row.css('td.bedrooms a::text').get('').strip()
            item['bathrooms'] = row.css('td.bathrooms a::text').get('').strip()
            item['city'] = row.css('td.city a::text').get('').strip()
            item['state'] = row.css('td.state a::text').get('').strip()
            item['external_id'] = row.css('td.listing_id a::text').get('').strip()
            item['location_pcd'] = f"{item['city']}, {item['state']}"

            detail_url = row.css('td.title a::attr(href)').get()
            if detail_url:
                item['url'] = detail_url
                yield response.follow(detail_url, callback=self.parse_property, meta={'item': item})

        next_page = response.xpath("//a[contains(text(), 'Next')]/@href").get()
        if next_page:
            yield response.follow(next_page, callback=self.parse)

    def parse_property(self, response):
        item = response.meta['item']

        raw_desc = ' '.join(response.css('#tab-listing-description div *::text').getall())
        item['description'] = self.collapse_whitespace(raw_desc)

        item['images'] = response.css('#tab-pictures a::attr(href)').getall()

        lot_area = response.xpath('//dt[contains(text(), "Total Lot Size")]/following-sibling::dd[1]/text()').get()
        if lot_area:
            item['lot_area'] = lot_area.strip()

        living_area = response.xpath('//dt[contains(text(), "Total Living Area")]/following-sibling::dd[1]/text()').get()
        if living_area:
            item['area'] = living_area.strip()

        item['metadata'] = {}
        target_tabs = ['#listing-details', '#geography', '#features', '#infrastructure', '#financial-legal-information']
        for tab in target_tabs:
            for dt in response.css(f'{tab} dl dt'):
                key = dt.css('::text').get('').strip()
                if not key:
                    continue
                val = dt.xpath('following-sibling::dd[1]/text()').get()
                if val:
                    item['metadata'][key] = val.strip()

        yield item
