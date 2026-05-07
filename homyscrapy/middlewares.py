import random


class RandomProxyMiddleware:
    """Apply a random proxy from proxies.txt to every request.

    Handles both standard Scrapy requests and Playwright requests.
    """

    def __init__(self, settings):
        self.proxies = []
        proxy_file = settings.get("PROXY_LIST_FILE", "proxies.txt")
        try:
            with open(proxy_file, "r") as f:
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

        if not spider.settings.getbool("USE_PROXY", True):
            return

        proxy = random.choice(self.proxies)

        if request.meta.get("playwright"):
            context_kwargs = request.meta.setdefault("playwright_context_kwargs", {})
            context_kwargs["proxy"] = {"server": proxy}
        else:
            request.meta["proxy"] = proxy

        if self.stats:
            self.stats.inc_value("proxies/request_count")
