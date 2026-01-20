import random
from scrapy import signals

class HomyscrapySpiderMiddleware:
    @classmethod
    def from_crawler(cls, crawler):
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        return None

    def process_spider_output(self, response, result, spider):
        for i in result:
            yield i

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)

class HomyscrapyDownloaderMiddleware:
    @classmethod
    def from_crawler(cls, crawler):
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        return None

    def process_response(self, request, response, spider):
        return response

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)

class RandomProxyMiddleware:
    def __init__(self, settings):
        self.proxies = []
        proxy_file = settings.get('PROXY_LIST_FILE', 'proxies.txt')
        try:
            with open(proxy_file, 'r') as f:
                self.proxies = [line.strip() for line in f if line.strip()]
        except FileNotFoundError:
            pass
        self.stats = None

    @classmethod
    def from_crawler(cls, crawler):
        o = cls(crawler.settings)
        o.stats = crawler.stats
        return o

    def process_request(self, request, spider):
        if not self.proxies:
            return

        proxy = random.choice(self.proxies)
        
        # Set Playwright proxy
        if request.meta.get('playwright'):
            context_kwargs = request.meta.setdefault('playwright_context_kwargs', {})
            # Playwright handles auth in server URL
            context_kwargs['proxy'] = {
                'server': proxy
            }
        else:
            # Set standard scrapy proxy for non-Playwright requests
            request.meta['proxy'] = proxy

        if self.stats:
            self.stats.inc_value('proxies/request_count')
