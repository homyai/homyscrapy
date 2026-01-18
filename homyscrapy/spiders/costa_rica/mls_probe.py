import scrapy

class MLSProbeSpider(scrapy.Spider):
    name = 'mls_probe'
    allowed_domains = ['mls.re.cr']
    start_urls = ['https://mls.re.cr/agencies/bluezonerealty/listings/rs2500010']

    custom_settings = {
        'ROBOTSTXT_OBEY': False,
    }

    def parse(self, response):
        # Save HTML for analysis
        filename = 'data/mls_probe.html'
        with open(filename, 'wb') as f:
            f.write(response.body)
        self.logger.info(f"Saved {filename}")
