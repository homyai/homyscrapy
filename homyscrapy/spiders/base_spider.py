import scrapy
from datetime import datetime, timezone, timedelta
from homyscrapy.items import PropertyItem

# Costa Rica is UTC-6 year-round (no DST)
_CR_TZ = timezone(timedelta(hours=-6))


class BasePropertySpider(scrapy.Spider):
    """Base class for all Homy property spiders.

    Provides shared helpers so subclasses don't repeat boilerplate.
    """

    country = 'Costa Rica'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Capture the run date in Costa Rica local time at spider start.
        # Used by the pipeline for file naming so runs that cross UTC midnight
        # still produce a file dated correctly for Costa Rica.
        if not hasattr(self, 'output_date') or not self.output_date:
            self.output_date = datetime.now(_CR_TZ).strftime('%Y-%m-%d')

    def make_item(self) -> PropertyItem:
        """Return a PropertyItem pre-populated with shared fields."""
        item = PropertyItem()
        item['source'] = getattr(self, 'source', self.name.capitalize())
        item['country'] = self.country
        item['extraction_date'] = datetime.now(_CR_TZ).strftime('%Y-%m-%d')
        return item

    @staticmethod
    def collapse_whitespace(text: str) -> str:
        """Collapse runs of whitespace to a single space."""
        return ' '.join(text.split())
