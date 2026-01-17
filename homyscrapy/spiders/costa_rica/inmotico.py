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
        links = response.css('#resultadosRight .anunciondii .contenido_anuncio h2 a::attr(href)').getall()
        for link in links:
            yield response.follow(link, callback=self.parse_property)
            
        next_page = response.css('#paginacion a:last-child')
        if next_page:
            text = next_page.css('::text').get('').strip()
            href = next_page.css('::attr(href)').get()
            
            if text == "Siguiente" or "Siguiente" in text:
                 yield response.follow(href, callback=self.parse)
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
        
        item['price'] = response.css('#info_container .mas_info_ii h5.precio::text').get('').strip()
        
        details = response.css('#info_container #details_add .details_info::text').getall()
        for detail in details:
            d = detail.strip()
            if not d: continue
            lower_d = d.lower()
            if "dorm" in lower_d or "habitaci" in lower_d:
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

        item['remarks'] = " ".join(response.css('#info_anuncio_superior span.descripcion_2 *::text').getall()).strip()
        item['location_pcd'] = response.css('#info_anuncio_superior span.descripcion_2 h3 a::text').get('').strip()

        lat_lon = response.css('#mapa_google input::attr(value)').get()
        if lat_lon:
            parts = lat_lon.split(',')
            if len(parts) >= 2:
                item['lat'] = parts[0]
                item['lon'] = parts[1]
                
        breadcrumbs = response.css('#seccion_superior .breadcrumbs_container ul li a::text').getall()
        item['property_category'] = breadcrumbs[-1] if breadcrumbs else None
        
        yield item
