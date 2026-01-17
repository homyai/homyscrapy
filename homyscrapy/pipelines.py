from itemadapter import ItemAdapter
import pandas as pd
import logging
from homyscrapy.common.google_cloud_tools.google_cloud_tools import gcs_upload_file_pd, date_manager

class HomyscrapyPipeline:
    def open_spider(self, spider):
        self.items = []

    def process_item(self, item, spider):
        self.items.append(ItemAdapter(item).asdict())
        return item

    def close_spider(self, spider):
        if not self.items:
            spider.logger.info("No items to upload.")
            return

        df = pd.DataFrame(self.items)
        current_date = date_manager()
        
        # Determine bot key for path
        # Map spider names to keys used in bucket
        bot_key_map = {
            'inmotico': 'INT',
            'encuentra24': 'C24'
        }
        key_bot = bot_key_map.get(spider.name, spider.name.upper())
        
        # Path logic: {BOT_KEY}/sales/houses/raw-data/
        path = f"{key_bot}/sales/houses/raw-data/"
        
        spider.logger.info(f"Uploading {len(df)} items to GCS bucket 'web-scraper-data' at path '{path}'...")
        
        try:
            gcs_upload_file_pd(
                df=df,
                bucket_name='web-scraper-data',
                file_name=current_date + ".json",
                extension=".json",
                path=path
            )
            spider.logger.info("Upload successful.")
        except Exception as e:
            spider.logger.error(f"Upload failed: {e}")
            # Identify if it's credential issue
            if "credentials" in str(e).lower():
                 spider.logger.warning("GCS Credentials missing or invalid. Data was NOT uploaded.")
