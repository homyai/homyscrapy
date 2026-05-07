import scrapy

from homyscrapy.spiders.base_spider import BasePropertySpider

_SALE_BASE = "https://www.re.cr/en/costa-rica-real-estate-for-sale/search-properties"
_RENT_BASE = "https://www.re.cr/en/costa-rica-rental-properties"
_SALE_PREFIX = "?form.search-properties.buttons.search="
_RENT_PREFIX = "?form.search-properties.buttons.search="

# Each entry: (url, property_category, status)
# Rental URL uses the confirmed base: /en/costa-rica-rental-properties
# The search param structure for rentals mirrors sales (listing_type=ra vs rs).
_LISTING_URLS = [
    # Sale: residential types
    (
        f"{_SALE_BASE}{_SALE_PREFIX}"
        "&form.search-properties.widgets.object_type%3Alist=house"
        "&form.search-properties.widgets.object_type%3Alist=mobile"
        "&form.search-properties.widgets.object_type%3Alist=multiplex"
        "&form.search-properties.widgets.object_type%3Alist=townhouse"
        "&form.search-properties.widgets.listing_type=rs",
        "house",
        "sale",
    ),
    # Sale: apartments/condos
    (
        f"{_SALE_BASE}{_SALE_PREFIX}"
        "&form.search-properties.widgets.object_type%3Alist=apartment"
        "&form.search-properties.widgets.listing_type=rs",
        "apartment",
        "sale",
    ),
    # Sale: land/lots
    (
        f"{_SALE_BASE}{_SALE_PREFIX}"
        "&form.search-properties.widgets.object_type%3Alist=land"
        "&form.search-properties.widgets.listing_type=rs",
        "land",
        "sale",
    ),
    # Sale: commercial
    (
        f"{_SALE_BASE}{_SALE_PREFIX}"
        "&form.search-properties.widgets.object_type%3Alist=commercial"
        "&form.search-properties.widgets.listing_type=rs",
        "commercial",
        "sale",
    ),
    # Rent: catch-all (confirmed base URL; property_category inferred from listing)
    (
        f"{_RENT_BASE}{_RENT_PREFIX}&form.search-properties.widgets.listing_type=ra",
        None,
        "rent",
    ),
]


class RECRSpider(BasePropertySpider):
    name = "recr"
    source = "RE.CR"
    allowed_domains = ["re.cr", "www.re.cr"]

    # Override global conservative defaults — RE.CR is a low-traffic static site
    custom_settings = {
        "USE_PROXY": False,
        "DOWNLOAD_DELAY": 3,
        "CONCURRENT_REQUESTS": 2,
    }

    async def start(self):
        for url, property_category, status in _LISTING_URLS:
            yield scrapy.Request(
                url,
                callback=self.parse,
                cb_kwargs={"property_category": property_category, "status": status},
            )

    def parse(self, response, property_category=None, status="sale"):
        tiles = response.css("div.tileItem")
        self.logger.info(f"[{property_category}/{status}] Found {len(tiles)} tiles")

        for tile in tiles:
            title_link = tile.css("h2.tileHeadline a")
            detail_url = title_link.css("::attr(href)").get()
            if not detail_url:
                continue

            item = self.make_item()
            item["title"] = title_link.css("::text").get("").strip()
            item["price"] = tile.css(".listing__price dd::text").get("").strip()
            item["url"] = detail_url
            item["status"] = status
            # Only set if known; catch-all rental segment leaves it to dbt title parsing
            if property_category is not None:
                item["property_category"] = property_category
            yield response.follow(detail_url, callback=self.parse_property, meta={"item": item})

        next_page = response.css(".next a::attr(href)").get()
        if next_page:
            yield response.follow(
                next_page,
                callback=self.parse,
                cb_kwargs={"property_category": property_category, "status": status},
            )

    def parse_property(self, response):
        item = response.meta["item"]

        raw_desc = " ".join(response.css(".documentDescription *::text").getall())
        item["description"] = self.collapse_whitespace(raw_desc)

        item["images"] = response.css('div[data-nav="thumbs"] a::attr(href)').getall()

        item["metadata"] = {}
        for tr in response.css("tr"):
            key = tr.css("th::text").get("").strip()
            val = tr.xpath("string(td)").get("").strip()
            if key and val:
                item["metadata"][key] = val

        meta = item["metadata"]
        item["bedrooms"] = meta.get("Bedrooms", "").strip() or meta.get("Number of Bedrooms", "").strip()
        item["bathrooms"] = meta.get("Bathrooms", "").strip() or meta.get("Number of Full Bathrooms", "").strip()
        item["city"] = meta.get("City", "").strip() or meta.get("City/Town", "").strip()
        item["state"] = meta.get("State", "").strip() or meta.get("Provincia", "").strip()
        item["external_id"] = meta.get("Listing ID", "")

        if item.get("city") and item.get("state"):
            item["location_pcd"] = f"{item['city']}, {item['state']}"
        elif item.get("city"):
            item["location_pcd"] = item["city"]

        lot_area = response.xpath('//th[contains(text(), "Lot Size")]/following-sibling::td/text()').get()
        if lot_area:
            item["lot_area"] = lot_area.strip()

        living_area = response.xpath('//th[contains(text(), "Living Area")]/following-sibling::td/text()').get()
        if living_area:
            item["area"] = living_area.strip()

        yield item
