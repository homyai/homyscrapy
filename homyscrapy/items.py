import scrapy

class PropertyItem(scrapy.Item):
    # Standard fields for all properties
    url = scrapy.Field()
    extraction_date = scrapy.Field()
    price = scrapy.Field()
    title = scrapy.Field()
    description = scrapy.Field()
    
    # Location details
    location_pcd = scrapy.Field() # Province, Canton, District
    lat = scrapy.Field()
    lon = scrapy.Field()
    
    # Features
    bedrooms = scrapy.Field()
    bathrooms = scrapy.Field()
    area = scrapy.Field()
    lot_area = scrapy.Field()
    features = scrapy.Field()
    garage = scrapy.Field()
    
    # Additional data
    remarks = scrapy.Field()
    images = scrapy.Field()
    property_category = scrapy.Field()
    
    # Metadata
    source = scrapy.Field()
    country = scrapy.Field()
    status = scrapy.Field()
    city = scrapy.Field()
    state = scrapy.Field()
    external_id = scrapy.Field()
    metadata = scrapy.Field()
