from itemadapter import ItemAdapter
import pandas as pd
import os
from datetime import datetime, timezone, timedelta
from google.cloud import storage

_CR_TZ = timezone(timedelta(hours=-6))


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
        # Use the spider's start date (set at init) so runs crossing midnight
        # don't produce a file dated the following day.
        run_date = getattr(spider, 'output_date', datetime.now(_CR_TZ).strftime("%Y-%m-%d"))
        gcs_path = f"raw/{spider.name}/{run_date}.json"
        file_name = f"{run_date}.json"

        # Always save locally under data/raw/<spider>/
        local_dir = os.path.join("data", "raw", spider.name)
        os.makedirs(local_dir, exist_ok=True)
        local_path = os.path.join(local_dir, file_name)
        df.to_json(local_path, orient="records", lines=True, force_ascii=False)
        spider.logger.info(f"Saved {len(df)} items locally to {local_path}")

        # Upload to GCS if credentials are configured
        if os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
            spider.logger.info(f"Uploading {len(df)} items to gs://web-scraper-data/{gcs_path}")
            try:
                client = storage.Client()
                bucket = client.bucket("web-scraper-data")
                blob = bucket.blob(gcs_path)
                blob.upload_from_string(
                    df.to_json(orient="records", lines=True, force_ascii=False),
                    content_type="application/json",
                )
                spider.logger.info("GCS upload successful.")
            except Exception as e:
                spider.logger.error(f"GCS upload failed: {e}")
