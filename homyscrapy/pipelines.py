from itemadapter import ItemAdapter
import pandas as pd
import os
from datetime import datetime
from homyscrapy.common.google_cloud_tools.google_cloud_tools import gcs_upload_file_pd, date_manager

class HomyscrapyPipeline:
    def open_spider(self, spider):
        self.items = []

    def process_item(self, item, spider):
        self.items.append(ItemAdapter(item).asdict())
        return item

    def close_spider(self, spider):
        if not self.items:
            spider.logger.info("No items to save.")
            return

        df = pd.DataFrame(self.items)
        current_date = datetime.today().strftime("%Y-%m-%d")

        bot_key_map = {
            'inmotico': 'INT',
            'encuentra24': 'C24'
        }
        key_bot = bot_key_map.get(spider.name, spider.name.upper())
        path = f"{key_bot}/sales/houses/raw-data/"
        file_name = f"{current_date}.json"

        # Always save locally
        local_dir = os.path.join("data", path)
        os.makedirs(local_dir, exist_ok=True)
        local_path = os.path.join(local_dir, file_name)
        df.to_json(local_path, orient="records", force_ascii=False, indent=2)
        spider.logger.info(f"Saved {len(df)} items locally to {local_path}")

        # Upload to GCS if credentials are configured
        if os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
            spider.logger.info(f"Uploading {len(df)} items to GCS bucket 'web-scraper-data' at path '{path}'...")
            try:
                gcs_upload_file_pd(
                    df=df,
                    bucket_name='web-scraper-data',
                    file_name=file_name,
                    extension=".json",
                    path=path
                )
                spider.logger.info("GCS upload successful.")
            except Exception as e:
                spider.logger.error(f"GCS upload failed: {e}")
