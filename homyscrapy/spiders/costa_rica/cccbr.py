import re
import scrapy
from homyscrapy.spiders.base_spider import BasePropertySpider

API_BASE = 'https://www.camara.cr/wp-json/wp/v2/cccbr-propiedades'
PER_PAGE = 100  # max allowed by WP REST API


class CamaraSpider(BasePropertySpider):
    """Scrape Cámara Costarricense de Corredores de Bienes Raíces via WP REST API.

    All listing data (fields, images, location) is available directly from the
    API — no browser rendering needed. Pagination uses ?page=N.
    """

    name = 'cccbr'
    source = 'Cámara CCCBR'
    allowed_domains = ['camara.cr']

    custom_settings = {
        'USE_PROXY': False,
        'ROBOTSTXT_OBEY': False,
        'DOWNLOAD_DELAY': 1,
        'RANDOMIZE_DOWNLOAD_DELAY': True,
        'CONCURRENT_REQUESTS': 2,
        'RETRY_TIMES': 3,
        'RETRY_HTTP_CODES': [429, 500, 502, 503, 504],
        'FEED_EXPORT_ENCODING': 'utf-8',
        'DOWNLOAD_HANDLERS': {},  # plain HTTP, no Playwright
    }

    def __init__(self, max_pages=0, output_date=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.max_pages = int(max_pages)
        if output_date:
            self.output_date = output_date
        self.logger.info(f"Starting scrape — max_pages: {self.max_pages}, date: {self.output_date}")

    async def start(self):
        yield self._api_request(1)

    def _api_request(self, page):
        return scrapy.Request(
            f'{API_BASE}?per_page={PER_PAGE}&page={page}&_embed=1',
            headers={'Accept': 'application/json'},
            callback=self.parse,
            cb_kwargs={'page': page},
            errback=self.errback,
        )

    def parse(self, response, page=1):
        listings = response.json()
        if not listings:
            self.logger.info(f"Page {page}: empty — finished.")
            return

        self.logger.info(f"Page {page}: {len(listings)} listings")
        for listing in listings:
            item = self._extract(listing)
            if item:
                yield item

        # Pagination: WP REST API returns X-WP-TotalPages header
        total_pages = int(response.headers.get('X-WP-TotalPages', 1))
        if self.max_pages:
            total_pages = min(total_pages, self.max_pages)

        if page < total_pages:
            yield self._api_request(page + 1)

    def _extract(self, l):
        item = self.make_item()
        meta = l.get('meta', {})

        item['url'] = l.get('link', '')
        item['external_id'] = str(l.get('id', ''))
        item['title'] = l.get('title', {}).get('rendered', '').strip()

        # Description: prefer Spanish content, fall back to English ACF field
        content_html = l.get('content', {}).get('rendered', '')
        desc_es = re.sub(r'<[^>]+>', ' ', content_html).strip()
        desc_en = re.sub(r'<[^>]+>', ' ', meta.get('descripcion-en-ingles', '') or '').strip()
        item['description'] = desc_es or desc_en

        # Price
        price_sale = meta.get('precio-de-venta') or ''
        price_rent = meta.get('precio-de-alquiler') or ''
        currency = meta.get('moneda') or 'USD'
        if price_sale:
            item['price'] = f'{currency} {price_sale}'.strip()
            item['status'] = 'sale'
        elif price_rent:
            item['price'] = f'{currency} {price_rent} (alquiler)'.strip()
            item['status'] = 'rent'
        else:
            item['status'] = 'sale'  # default for listings without price info

        # Property details
        item['bedrooms'] = str(meta.get('habitaciones') or '')
        item['bathrooms'] = str(meta.get('banos') or '')
        item['garage'] = str(meta.get('garage') or '')
        item['area'] = str(meta.get('construccion') or '')       # built area m²
        item['lot_area'] = str(meta.get('terreno') or '')        # land area m²

        # Extra metadata
        item['metadata'] = {
            'tipo_propiedad': meta.get('tipo-de-propiedad') or '',
            'estatus': meta.get('estatus-propiedad') or [],
            'uso_suelo': meta.get('uso-de-suelo') or [],
            'exclusividad': meta.get('exclusividad') or [],
            'plantas': str(meta.get('plantas') or ''),
            'ano_construccion': str(meta.get('ano-de-construccion') or ''),
            'numero_finca': meta.get('numero-de-finca') or '',
            'mapa': meta.get('mapa') or '',
            'video': meta.get('video_propiedad') or '',
            'amenidades': meta.get('amenidades') or [],
            'descripcion_en': desc_en,
            'precio_venta_raw': str(price_sale),
            'precio_alquiler_raw': str(price_rent),
            'fecha_publicacion': l.get('date', ''),
            'fecha_modificacion': l.get('modified', ''),
        }

        # property_category derived from tipo
        tipo = (meta.get('tipo-de-propiedad') or '').lower()
        if 'apartamento' in tipo:
            item['property_category'] = 'apartment'
        elif 'casa' in tipo:
            item['property_category'] = 'house'
        elif 'lote' in tipo or 'terreno' in tipo:
            item['property_category'] = 'land'
        elif 'local' in tipo or 'comercial' in tipo or 'oficina' in tipo:
            item['property_category'] = 'commercial'
        else:
            item['property_category'] = tipo

        # Images: featured media + any gallery images
        images = []
        embedded = l.get('_embedded', {})
        featured = embedded.get('wp:featuredmedia', [])
        if featured and featured[0].get('source_url'):
            images.append(featured[0]['source_url'])
        # imagenes field is sometimes null, sometimes a list of objects
        gallery = meta.get('imagenes')
        if isinstance(gallery, list):
            for img in gallery:
                if isinstance(img, dict):
                    url = img.get('url') or img.get('source_url') or ''
                elif isinstance(img, str):
                    url = img
                else:
                    url = ''
                if url and url not in images:
                    images.append(url)
        item['images'] = images

        item['features'] = list(meta.get('amenidades') or [])

        # Location from ubicacion-propiedad taxonomy (term IDs resolved by WP)
        ubicacion_terms = l.get('ubicacion-propiedad', [])
        if ubicacion_terms:
            item['metadata']['ubicacion_term_ids'] = ubicacion_terms

        return item

    async def errback(self, failure):
        self.logger.error(f"Request failed: {failure.request.url} — {failure.getErrorMessage()}")
