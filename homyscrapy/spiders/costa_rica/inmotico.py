import scrapy
from homyscrapy.items import PropertyItem
from datetime import datetime

class InmoticoSpider(scrapy.Spider):
    name = 'inmotico'
    allowed_domains = ['inmotico.com']
    start_urls = ['https://www.inmotico.com/venta-de-casas-en-costa-rica-l0-3-0.html']
    
    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'DOWNLOAD_DELAY': 2,
    }

    def parse(self, response):
        # 1. Extract property links
        # Original: P1 -> div#resultadosRight -> div.anunciondii
        # Then inside: P2 -> div.contenido_anuncio -> h2 -> a
        
        # We can select directly:
        # div#resultadosRight div.anunciondii div.contenido_anuncio h2 a
        links = response.css('#resultadosRight .anunciondii .contenido_anuncio h2 a::attr(href)').getall()
        
        for link in links:
            yield response.follow(link, callback=self.parse_property)
            
        # 2. Pagination
        # Original: P3 (div#resultadosRight -> div#paginacion) -> P4 (a, last item) checks if text is "Siguiente"
        # Let's verify this logic. Usually "next" class or text contains "Siguiente"
        
        next_page = response.css('#paginacion a:last-child')
        if next_page:
            text = next_page.css('::text').get('').strip()
            href = next_page.css('::attr(href)').get()
            
            if text == "Siguiente" or "Siguiente" in text:
                 yield response.follow(href, callback=self.parse)
            # Alternatively look for any link with text "Siguiente"
            else:
                 next_by_text = response.xpath("//a[contains(text(), 'Siguiente')]/@href").get()
                 if next_by_text:
                     yield response.follow(next_by_text, callback=self.parse)

    def parse_property(self, response):
        item = PropertyItem()
        item['url'] = response.url
        item['extraction_date'] = datetime.today().strftime("%Y-%m-%d")
        item['source'] = 'Inmotico'
        item['country'] = 'Costa Rica'
        
        # Price (P5 -> h5.precio)
        item['price'] = response.css('#info_container .mas_info_ii h5.precio::text').get('').strip()
        
        # Primary Details (P6 -> .details_info)
        # These are usually "Bedrooms: 3", "Bathrooms: 2"
        details = response.css('#info_container #details_add .details_info::text').getall()
        
        # Extract bedrooms/bathrooms/area from details text
        for detail in details:
            d = detail.strip()
            if not d: continue
            
            # Simple parsing logic based on observation or standard terms
            lower_d = d.lower()
            if "dorm" in lower_d or "habitaci" in lower_d: # Dormitorios/Habitaciones
                parts = d.split(':')
                if len(parts) > 1:
                    item['bedrooms'] = parts[1].strip()
            elif "baño" in lower_d: 
                parts = d.split(':')
                if len(parts) > 1:
                    item['bathrooms'] = parts[1].strip()
            elif "construc" in lower_d or "m2" in lower_d:
                 parts = d.split(':')
                 if len(parts) > 1:
                    item['area'] = parts[1].strip()

        # Secondary Details (P7, P8)
        # Location / Remarks
        # P7: #info_anuncio_superior span.descripcion_2
        # This seems to contain a lot of text or html
        raw_desc = response.css('#info_anuncio_superior span.descripcion_2').get()
        # Clean tags if needed, or store raw HTML as remarks
        # response.css('#info_anuncio_superior span.descripcion_2 *::text').getall() to get all text
        item['remarks'] = " ".join(response.css('#info_anuncio_superior span.descripcion_2 *::text').getall()).strip()
        
        # Location from logic: P7 find h3 find a
        item['location_pcd'] = response.css('#info_anuncio_superior span.descripcion_2 h3 a::text').get('').strip()

        # P8: .detalles_descripcion2 (Features)
        features = response.css('.detalles_descripcion2::text').getall()
        # We can store this as a list or string
        # item['features'] = features # We don't have a features field in PropertyItem yet, but 'remarks' implies description. 
        # We should probably extend the Item or just put it in description.
        # Let's put it in remarks for now or ignore specific matching.
        
        # Map (P9 -> #mapa_google input value)
        # <div id="mapa_google"><input value="9.123,-84.123"></div>
        lat_lon = response.css('#mapa_google input::attr(value)').get()
        if lat_lon:
            parts = lat_lon.split(',')
            if len(parts) >= 2:
                item['lat'] = parts[0]
                item['lon'] = parts[1]
                
        # Category/Breadcrumbs (P10)
        # #seccion_superior .breadcrumbs_container
        breadcrumbs = response.css('#seccion_superior .breadcrumbs_container ul li a::text').getall()
        item['property_category'] = breadcrumbs[-1] if breadcrumbs else None
        
        yield item
