import re

import scrapy

from homyscrapy.spiders.base_spider import BasePropertySpider

SALE_BASE = "https://www.fazwaz.co.cr/propiedad-en-venta/costa-rica"
RENT_BASE = "https://www.fazwaz.co.cr/propiedad-en-alquiler/costa-rica"

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "es-CR,es;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate",
    "DNT": "1",
    "Upgrade-Insecure-Requests": "1",
}

# Matches both sale and rental detail URLs
_DETAIL_RE = re.compile(r"/propiedad-e-Inmueble-(venta|alquiler)/")


class FazWazSpider(BasePropertySpider):
    """Crawl FazWaz Costa Rica — sale and rental listings.

    Two-stage crawl:
      1. List pages (?page=N) → collect detail-page URLs.
      2. Each detail page → extract full property data.

    Plain HTTP only — no Playwright needed.
    """

    name = "fazwaz"
    source = "FazWaz"
    allowed_domains = ["fazwaz.co.cr", "cdn.fazwaz.com"]

    custom_settings = {
        "USE_PROXY": False,
        "ROBOTSTXT_OBEY": False,
        "DOWNLOAD_DELAY": 2,
        "RANDOMIZE_DOWNLOAD_DELAY": True,
        "CONCURRENT_REQUESTS": 2,
        "RETRY_TIMES": 3,
        "RETRY_HTTP_CODES": [429, 500, 502, 503, 504],
        "FEED_EXPORT_ENCODING": "utf-8",
        "DOWNLOAD_HANDLERS": {},  # plain HTTP, no Playwright
    }

    def __init__(self, max_pages=0, output_date=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.max_pages = int(max_pages)  # 0 = unlimited
        if output_date:
            self.output_date = output_date
        self.logger.info(f"Starting — max_pages: {self.max_pages}, date: {self.output_date}")

    async def start(self):
        for base, listing_type in [(SALE_BASE, "sale"), (RENT_BASE, "rent")]:
            yield scrapy.Request(
                f"{base}?page=1",
                headers=_HEADERS,
                callback=self.parse_list,
                cb_kwargs={"base": base, "listing_type": listing_type, "page": 1},
                errback=self.errback,
            )

    def parse_list(self, response, base, listing_type, page):
        # Collect all detail-page links on this list page
        links = response.css('a[href*="/propiedad-e-Inmueble-"]::attr(href)').getall()
        seen = set()
        for href in links:
            url = response.urljoin(href)
            if url not in seen:
                seen.add(url)
                yield scrapy.Request(
                    url,
                    headers=_HEADERS,
                    callback=self.parse_detail,
                    cb_kwargs={"listing_type": listing_type},
                    errback=self.errback,
                )

        self.logger.info(f"[{listing_type}] Page {page}: {len(seen)} listings queued")

        if not seen:
            self.logger.info(f"[{listing_type}] Empty page {page} — finished.")
            return

        if self.max_pages and page >= self.max_pages:
            self.logger.info(f"[{listing_type}] Reached max_pages ({self.max_pages})")
            return

        # Pagination uses ?page=N — navigate directly (next-page links are JS-rendered)
        yield scrapy.Request(
            f"{base}?page={page + 1}",
            headers=_HEADERS,
            callback=self.parse_list,
            cb_kwargs={"base": base, "listing_type": listing_type, "page": page + 1},
            errback=self.errback,
        )

    def parse_detail(self, response, listing_type="sale"):
        item = self.make_item()
        item["url"] = response.url
        item["status"] = listing_type

        # Title
        item["title"] = response.css("h1::text").get("").strip()

        # Description — extract early so we can parse specs from it
        desc_parts = response.css(".unit-view-description ::text").getall()
        desc_text = " ".join(t.strip() for t in desc_parts if t.strip())
        item["description"] = desc_text

        # Price — "a partir de $180,000" in the description
        price_raw = self._re_first(r"partir de (\$[\d,]+)", desc_text)
        if not price_raw:
            # Fallback: largest dollar amount (avoids /m² noise from smaller numbers)
            amounts = re.findall(r"\$([\d,]+)", desc_text)
            if amounts:
                price_raw = "$" + max(amounts, key=lambda x: int(x.replace(",", "")))
        item["price"] = price_raw or ""

        # External ID from "Referencia: U6180209"
        ref_text = response.css('*:contains("Referencia")').re_first(r"[Uu](\d+)")
        if not ref_text:
            # Fall back to URL segment
            m = re.search(r"-u(\d+)$", response.url, re.IGNORECASE)
            if m:
                ref_text = m.group(1)
        item["external_id"] = ref_text or ""

        # Location from .project-location (strip the img element text)
        loc_texts = response.css(".project-location ::text").getall()
        location = " ".join(t.strip() for t in loc_texts if t.strip())
        item["location_pcd"] = location

        # Parse city/state from "City, Province, Costa Rica"
        loc_parts = [p.strip() for p in location.split(",") if p.strip()]
        if len(loc_parts) >= 3:
            item["city"] = loc_parts[0]
            item["state"] = loc_parts[1]
        elif len(loc_parts) == 2:
            item["city"] = loc_parts[0]
            item["state"] = loc_parts[1]
        elif loc_parts:
            item["city"] = loc_parts[0]

        # Specs — parse from description: "...de 180 m² con 2 habitaciones y 2 baños..."
        item["bedrooms"] = self._re_first(r"(\d+)\s+habitaci[oó]n(?:es)?", desc_text)
        item["bathrooms"] = self._re_first(r"(\d+(?:\.\d+)?)\s+ba[nñ]os?", desc_text)
        item["area"] = self._re_first(r"de\s+([\d,.]+\s*m²)\s+con", desc_text)

        # Lot area — appears as a standalone "12,141 m²" later in page text
        specs = self._parse_specs(response)
        item["lot_area"] = specs.get("tamaño de la parcela", specs.get("tamano de la parcela", ""))
        item["garage"] = specs.get("aparcamiento", specs.get("garage", specs.get("estacionamiento", "")))

        # Property type from URL slug (e.g. "casa", "apartamento", "lote")
        prop_type = self._re_first(r"/propiedad-e-Inmueble-(?:venta|alquiler)/\d+-\w+-(\w+)-en-", response.url) or ""
        if not prop_type:
            prop_type = specs.get("tipo de propiedad", "")
        item["property_category"] = self._categorize(prop_type.lower())

        # Images from cdn.fazwaz.com
        images = []
        for src in response.css('img[src*="cdn.fazwaz.com"]::attr(src)').getall():
            if src not in images:
                images.append(src)
        # Also data-src lazy-loaded images
        for src in response.css('img[data-src*="cdn.fazwaz.com"]::attr(data-src)').getall():
            if src not in images:
                images.append(src)
        item["images"] = images

        item["features"] = []
        item["metadata"] = {
            "listing_type": listing_type,
            "tipo_propiedad": specs.get("tipo de propiedad", ""),
            "floors": specs.get("número de plantas", specs.get("numero de plantas", "")),
            "year_built": specs.get("año de construcción", specs.get("ano de construccion", "")),
            "amenities": specs.get("instalaciones", []),
        }

        yield item

    def _parse_specs(self, response) -> dict:
        """Extract key-value pairs from the property details table, headings, and page text."""
        specs = {}

        # Lot area — find "12,141 m²" that appears after the interior area context
        # Look for the second distinct m² value in page text (first is interior area)
        all_areas = re.findall(r"([\d,.]+\s*m²)", response.text)
        seen_areas = []
        for a in all_areas:
            norm = re.sub(r"\s", "", a)
            if norm not in seen_areas:
                seen_areas.append(norm)
        if len(seen_areas) >= 2:
            specs["tamaño de la parcela"] = seen_areas[1]

        # Table rows: <td>Label</td><td>Value</td> or <th>Label</th><td>Value</td>
        for row in response.css("table tr"):
            cells = row.css("th, td")
            if len(cells) >= 2:
                label = cells[0].css("::text").get("").strip().lower()
                value = " ".join(cells[1].css("::text").getall()).strip()
                if label and value:
                    specs[label] = value

        # h2 headings like "2 Habitaciones", "2 Baños", "180 m² Superficie interior"
        for h2 in response.css("h2::text").getall():
            h2 = h2.strip()
            m = re.match(r"^(\d+[\d,.]*\s*m²)\s*(.*)$", h2)
            if m:
                desc = m.group(2).lower()
                if "interior" in desc or "superficie" in desc or not desc:
                    specs.setdefault("superficie interior", m.group(1).strip())
                elif "parcela" in desc or "terreno" in desc:
                    specs.setdefault("tamaño de la parcela", m.group(1).strip())
            m2 = re.match(r"^(\d+)\s+(habitaciones?|baños?|banos?)$", h2, re.IGNORECASE)
            if m2:
                specs.setdefault(m2.group(2).lower().rstrip("s") + "s", m2.group(1))

        # .header-data-topic spans (older layout)
        for header in response.css(".header-data-topic::text").getall():
            header = header.strip()
            m = re.match(r"^(\d+[\d,.]*\s*m²)\s+(.+)$", header)
            if m:
                desc = m.group(2).lower()
                if "interior" in desc or "superficie" in desc:
                    specs.setdefault("superficie interior", m.group(1))
                elif "parcela" in desc or "terreno" in desc:
                    specs.setdefault("tamaño de la parcela", m.group(1))
            m2 = re.match(r"^(\d+)\s+(habitaciones?|baños?)$", header, re.IGNORECASE)
            if m2:
                specs.setdefault(m2.group(2).lower(), m2.group(1))

        return specs

    @staticmethod
    def _re_first(pattern: str, text: str) -> str:
        m = re.search(pattern, text, re.IGNORECASE)
        return m.group(1).strip() if m else ""

    @staticmethod
    def _categorize(prop_type: str) -> str:
        if "apartamento" in prop_type or "condo" in prop_type:
            return "apartment"
        if "casa" in prop_type or "villa" in prop_type:
            return "house"
        if "lote" in prop_type or "terreno" in prop_type or "parcela" in prop_type:
            return "land"
        if "local" in prop_type or "comercial" in prop_type or "oficina" in prop_type:
            return "commercial"
        return prop_type or "other"

    async def errback(self, failure):
        self.logger.error(f"Request failed: {failure.request.url} — {failure.getErrorMessage()}")
