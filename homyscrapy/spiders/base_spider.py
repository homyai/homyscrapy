import scrapy
from datetime import datetime
from homyscrapy.items import PropertyItem


class BasePropertySpider(scrapy.Spider):
    """Base class for all Homy property spiders.

    Provides shared helpers so subclasses don't repeat boilerplate.
    """

    country = 'Costa Rica'

    def make_item(self) -> PropertyItem:
        """Return a PropertyItem pre-populated with shared fields."""
        item = PropertyItem()
        item['source'] = getattr(self, 'source', self.name.capitalize())
        item['country'] = self.country
        item['extraction_date'] = datetime.today().strftime('%Y-%m-%d')
        return item

    @staticmethod
    def collapse_whitespace(text: str) -> str:
        """Collapse runs of whitespace to a single space."""
        return ' '.join(text.split())
