import os
import json
import sys
import time
from datetime import datetime
import random
import logging

import pandas as pd
import scrapy
from scrapy.crawler import CrawlerProcess

FILE_PATH = os.path.dirname(os.path.realpath(__file__))
PACKAGE_PATH = os.path.normpath(f"{FILE_PATH}/../")
sys.path.append(PACKAGE_PATH)

from common.soup_functions import ScrapTool
from common.google_cloud_tools import get_last_file_from_bucket, gcs_upload_file_pd, date_manager
from common.scrapy_tools import get_properties_page_url, get_process_steps, scrape_urls_from_properties_page, preserve_b_items_if_common

contador = 1
key_bot = "int"
current_date = date_manager()
first_iter = True
class ScrapyINT(scrapy.Spider):
    """
    Spider to scrape properties from inmotico.com
    """
    name = key_bot.upper()
    start_urls = get_properties_page_url(key_bot.upper())
    custom_settings = {
        "DOWNLOADER_MIDDLEWARES": {
            "scrapy.downloadermiddlewares.useragent.UserAgentMiddleware": None,
            "scrapy_user_agents.middlewares.RandomUserAgentMiddleware": 400,
            "rotating_proxies.middlewares.RotatingProxyMiddleware": 800,
            "rotating_proxies.middlewares.BanDetectionMiddleware": 800,
        },
        "DOWNLOAD_DELAY": 2,
    }

    def parse(self, response):
        """
        Main function to scrape.
        """
        time.sleep(round(random.randint(1, 2) * random.random(), 2))
        time.sleep(10)
            
        steps = get_process_steps(key_bot)
        scrap_tool = ScrapTool(response)
        soup = scrap_tool.soup_creation()
        url_list = scrape_urls_from_properties_page(scrap_tool, soup, steps)
        last_page_as = scrap_tool.search_nest(soup, steps["P3"]) # List of following pages
        last_page_list = scrap_tool.search_nest(last_page_as, steps["P4"])[-1] # Last item in the list
        next_page_link = last_page_list.get("href") # Link to the last item in the list
        next_page_name = last_page_list.get_text().strip() # Name of the last it in the list
        if next_page_name == "Siguiente":
            yield response.follow(next_page_link, callback=self.parse)
        else:
            last_url_collection_list = get_last_file_from_bucket(
                project_id = "datalake-homyai",
                bucket_name = "web-scraper-data",
                extension = ".json",
                bucket_path = key_bot + "/sales/houses/url-list/"
            )['scrap_links'].values.tolist()
            url_collection_list = preserve_b_items_if_common(last_url_collection_list, url_list)
            if len(url_collection_list) > 0:
                logging.warning("------ Scraped %s links today -----" % str(len(url_collection_list)))
                logging.warning("----- Uploading the new Collection of URLs to GCS -----")
                df = pd.DataFrame({"scrap_links": url_collection_list})
                gcs_upload_file_pd(
                    df = df,
                    bucket_name= 'web-scraper-data',
                    file_name = current_date + ".json",
                    extension= ".json",
                    path = key_bot + "/sales/houses/url-list/"
                )
                logging.warning("----- Starting to scrap the properties from the Collection of URLs -----")
                time.sleep(3)

                for page in url_collection_list:
                    yield response.follow(
                        page,
                        callback=self.int_logic,
                        meta={
                            "url": page,
                            "url_count": len(url_collection_list),
                            "date": datetime.today().strftime("%d/%m/%Y"),
                        },
                    )
            else:
                logging.warning("----- No new URLs to scrap today -----")
    
    def int_logic(self, response):
        # For All Bots
        global contador
        rand_secs = round(random.randint(1, 2) * random.random(), 2)
        time.sleep(rand_secs)
        url = response.meta.get("url")
        list_size = response.meta.get("url_count")
        scrap_date = response.meta.get("date")

        dataset_1 = {}
        dataset_2 = {}
        dataset_3 = {}
        # For All Bots
        # add url key and date to database
        url_dataset = {"url": url, "extraction_date": scrap_date}

        procesos = "data/procesos.json"
        with open(procesos) as step_file:
            steps = json.load(step_file)
        scrap_tool = ScrapTool(response)
        soup = scrap_tool.soup_creation()
        # -------------------scrapy code----------------
        # step 1: primary details
        price = scrap_tool.search_nest(soup, steps["INT"]["P5"]).get_text()
        primary_details = scrap_tool.search_nest(soup, steps["INT"]["P6"])
        title_ds1 = []
        values_ds1 = []
        for i in primary_details:
            detail = i.get_text().strip(" \n")
            title_ds1.append(detail.split(":")[0])
            values_ds1.append((detail.split(":")[1]).strip(" \n"))

        zip_prim_feat = zip(title_ds1, values_ds1)
        dataset_1 = dict(zip_prim_feat)
        dataset_1 = {"price": price} | dataset_1
        # step 2: secondary details
        sec_details_div = scrap_tool.search_nest(soup, steps["INT"]["P7"])
        try:
            location_pcd = sec_details_div.find("h3").find("a").get_text()
        except:
            location_pcd = ""
        secondary_features_div = scrap_tool.search_nest(
            sec_details_div, steps["INT"]["P8"]
        )
        remarks = str(sec_details_div).split("</h3>")[1]
        remarks = remarks.split("<br/>")[0]
        title_ds2 = []
        values_ds2 = []
        try:
            for i in secondary_features_div:
                feature = i.get_text()
                title_ds2.append(feature.split(":")[0])
                try:
                    values_ds2.append(feature.split(":")[1])
                except:
                    values_ds2.append(1)

            zip_sec_feat = zip(title_ds2, values_ds2)
            dataset_2 = dict(zip_sec_feat)
        except:
            pass
        dataset_2 = dataset_2 | {"location_pcd": location_pcd} | {"remarks": remarks}

        # step 3: lat & long
        try:
            lat_lon = scrap_tool.search_nest(soup, steps["INT"]["P9"])

            latitud = lat_lon.find("input").get("value").split(",")[0]
            longitud = lat_lon.find("input").get("value").split(",")[1]
            dataset_3 = {"latitud": latitud, "longitud": longitud}
        except:
            pass
        # dataset of PCD Breadcum and property type & category
        try:
            div_breadcrumbs = scrap_tool.search_nest(soup, steps["INT"]["P10"])
            property_category = div_breadcrumbs.find("span").get_text()
            location_breadcrumbs_list = scrap_tool.search_nest(
                div_breadcrumbs, steps["INT"]["P11"]
            )
            location_breadcrumbs = ""
            for x in location_breadcrumbs_list:
                if location_breadcrumbs == "":
                    location_breadcrumbs = str(x.get_text().strip("\n"))
                else:
                    location_breadcrumbs = (
                        location_breadcrumbs + "_" + str(x.get_text().strip("\n"))
                    )

            dataset_4 = {
                "property_category": property_category,
                "location_breadcrumbs": location_breadcrumbs,
            }
        except:
            dataset_4 = {}


        properties = url_dataset | dataset_4 | dataset_1 | dataset_3 | dataset_2
        # append to dictionary
        json_file = []
        json_file.append(properties)
        # for all bots
        data_file = pd.DataFrame(json_file)

        time.sleep(2)
        logging.warning("pagina numero " + str(contador) + " de " + str(list_size))
        list_size_2 = round((list_size * 0.98), 0)
        url_collection_file_name = current_date + ".json"
        if contador >= list_size_2:
            logging.warning("writing properties to cloud storage")
            time.sleep(5)

            gcs_upload_file_pd(
                data_file,
                "web-scraper-data",
                url_collection_file_name,
                ".json",
                key_bot + "/sales/houses/raw-data/",
            )
        contador = contador + 1

if __name__ == "__main__":
    time.sleep(50)
    start = time.time()
    process = CrawlerProcess()
    process.crawl(ScrapyINT)
    process.start()
    stopt = time.time()
    logging.warning("duracion: " + str((stopt - start) / 60) + " minutos")
