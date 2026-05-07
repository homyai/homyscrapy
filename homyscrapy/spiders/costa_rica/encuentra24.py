import os
from datetime import timedelta, timezone

import scrapy

from homyscrapy.spiders.base_spider import BasePropertySpider

_CR_TZ = timezone(timedelta(hours=-6))

# All active listing segments: (base_url, property_category, status)
# Sale URLs use the per-type pattern: bienes-raices-venta-de-propiedades-{type}
# Rental URL is a single catch-all: bienes-raices-alquiler (no per-type sub-paths confirmed)
# property_category for rentals is left to dbt title-based normalization.
_LISTING_URLS = [
    ("https://www.encuentra24.com/costa-rica-es/bienes-raices-venta-de-propiedades-casas", "house", "sale"),
    ("https://www.encuentra24.com/costa-rica-es/bienes-raices-venta-de-propiedades-apartamentos", "apartment", "sale"),
    ("https://www.encuentra24.com/costa-rica-es/bienes-raices-venta-de-propiedades-terrenos", "land", "sale"),
    (
        "https://www.encuentra24.com/costa-rica-es/bienes-raices-venta-de-propiedades-locales-comerciales",
        "commercial",
        "sale",
    ),
    ("https://www.encuentra24.com/costa-rica-es/bienes-raices-alquiler", None, "rent"),
]

# Standard browser headers — enough to pass Cloudflare without a real browser
_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "es-CR,es;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate",
    "DNT": "1",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
}


class Encuentra24Spider(BasePropertySpider):
    """Crawl Encuentra24 using plain HTTP — no browser needed.

    The site server-side renders listing cards, so a plain GET request with
    browser-like headers is enough to retrieve full listing data. This avoids
    Cloudflare browser-fingerprint detection entirely.

    Each page has a direct URL: <base>.N (e.g. .../casas.2 for page 2).
    All property types and both transaction types (sale and rent) are crawled.
    """

    name = "encuentra24"
    source = "Encuentra24"
    allowed_domains = ["encuentra24.com"]

    # Decodo proxy — single session IP for the full run so Cloudflare cookies stay valid
    _proxy_url = None
    _ps = os.getenv("PROXY_SERVER", "").split("://")[-1]
    _pu = os.getenv("PROXY_USER", "")
    _pp = os.getenv("PROXY_PASSWORD", "")
    if _ps and _pu and _pp:
        _proxy_url = f"http://{_pu}:{_pp}@{_ps}"

    custom_settings = {
        "USE_PROXY": False,  # disable RandomProxyMiddleware (would rotate IPs mid-session)
        "ROBOTSTXT_OBEY": False,
        "DOWNLOAD_DELAY": 3,
        "RANDOMIZE_DOWNLOAD_DELAY": True,
        "CONCURRENT_REQUESTS": 1,
        "RETRY_TIMES": 3,
        "RETRY_HTTP_CODES": [429, 500, 502, 503, 504],
        "HTTPERROR_ALLOWED_CODES": [403],
        "FEED_EXPORT_ENCODING": "utf-8",
        "DOWNLOAD_HANDLERS": {},  # plain HTTP, no Playwright
    }

    def __init__(self, max_pages=0, output_date=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.max_pages = int(max_pages)  # 0 = unlimited
        if output_date:
            self.output_date = output_date
        self.logger.info(
            f"Starting scrape — max_pages: {self.max_pages}, proxy: {'yes' if self._proxy_url else 'no'}, date: {self.output_date}"
        )

    @staticmethod
    def _page_url(base_url, page_num):
        return base_url if page_num == 1 else f"{base_url}.{page_num}"

    def _meta(self):
        m = {}
        if self._proxy_url:
            m["proxy"] = self._proxy_url
        return m

    async def start(self):
        for base_url, property_category, status in _LISTING_URLS:
            self.logger.info(f"Queuing segment: {property_category}/{status}")
            yield scrapy.Request(
                self._page_url(base_url, 1),
                headers=_HEADERS,
                meta=self._meta(),
                callback=self.parse,
                cb_kwargs={
                    "base_url": base_url,
                    "property_category": property_category,
                    "status": status,
                    "page_num": 1,
                },
                errback=self.errback,
            )

    def parse(self, response, base_url, property_category, status, page_num=1):
        ads = response.css("a.item-card-link")
        self.logger.info(f"[{property_category}/{status}] Page {page_num}: {len(ads)} ads found")

        if not ads:
            self.logger.warning(f"[{property_category}/{status}] Page {page_num}: no ads — stopping.")
            return

        for ad in ads:
            item = self._extract_item(ad, response, property_category=property_category, status=status)
            if item["url"]:
                yield item

        if self.max_pages != 0 and page_num >= self.max_pages:
            self.logger.info(f"[{property_category}/{status}] Reached max_pages limit ({self.max_pages})")
            return

        next_btn = response.css('button[aria-label="Página siguiente"]')
        if not next_btn or next_btn.attrib.get("aria-disabled") == "true":
            self.logger.info(f"[{property_category}/{status}] No more pages — finished.")
            return

        next_num = page_num + 1
        self.logger.info(f"[{property_category}/{status}] Queuing page {next_num}")
        yield scrapy.Request(
            self._page_url(base_url, next_num),
            headers=_HEADERS,
            meta=self._meta(),
            callback=self.parse,
            cb_kwargs={
                "base_url": base_url,
                "property_category": property_category,
                "status": status,
                "page_num": next_num,
            },
            errback=self.errback,
        )

    def _extract_item(self, ad, response, property_category="house", status="sale"):
        item = self.make_item()

        relative_url = ad.attrib.get("href", "")
        item["url"] = response.urljoin(relative_url) if relative_url else ""
        item["title"] = (ad.css("h3.card_title::text").get() or "").strip()

        price_el = ad.css("span.card_price")
        item["price"] = " ".join(t.strip() for t in price_el[0].css("::text").getall() if t.strip()) if price_el else ""

        item["location_pcd"] = (ad.css("p.card_subtitle::text").get() or "").strip()
        item["description"] = ad.css("p.card_description::attr(title)").get("").strip()

        for spec in ad.css("span.card_spec"):
            spec_html = spec.get()
            texts = [t.strip() for t in spec.css("::text").getall() if t.strip()]
            if "lucide-bed" in spec_html and texts:
                item["bedrooms"] = texts[0].split()[0]
            elif "lucide-bath" in spec_html and texts:
                item["bathrooms"] = texts[0].split()[0]
            elif "lucide-maximize" in spec_html and texts:
                item["area"] = texts[0]

        images = []
        for img in ad.css("img.card_image"):
            src = img.attrib.get("src", "")
            if src and "base64" not in src and "no-img" not in src:
                images.append(src)
        item["images"] = images

        path_parts = relative_url.rstrip("/").split("/")
        if path_parts and path_parts[-1].isdigit():
            item["external_id"] = path_parts[-1]

        item["features"] = []
        item["metadata"] = {}

        # status is always known from the crawl segment
        item["status"] = status
        # property_category is known for sale segments (typed URLs); for the catch-all
        # rental URL it is None — dbt will infer from title
        if property_category is not None:
            item["property_category"] = property_category

        return item

    async def errback(self, failure):
        self.logger.error(f"Request failed: {failure.request.url} — {failure.getErrorMessage()}")
