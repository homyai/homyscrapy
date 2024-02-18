import os
import json
import sys
import time
from datetime import datetime
import random
import logging

import pandas as pd

# crawl tools
import scrapy
from scrapy.crawler import CrawlerProcess

# Homy Libraries
FILE_PATH = os.path.dirname(os.path.realpath(__file__))
PACKAGE_PATH = os.path.normpath(f"{FILE_PATH}/../")
sys.path.append(PACKAGE_PATH)
from common.soup_functions import ScrapTool
from common.google_cloud_tools import CloudTools

# Global variables
json_file = []
url_list = []
contador = 1
key_bot = "int"
pagina_borrar = 1
cr_flag = True


class SpiderRES(scrapy.Spider):
    """
    This page won't give us lat and longitude data, but got a solid ubication structure
    """

    # -------Keys for web scraping-------------
    file_name = "data/keys.json"
    with open(file_name) as json_file:
        json_data = json.load(json_file)
    name = json_data["INT"]["name"]
    start_urls = json_data["INT"]["url"]
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
        main in wich the scrapy spider runs
        """
        rand_secs = round(random.randint(1, 2) * random.random(), 2)
        time.sleep(rand_secs)
        global json_file
        global key_bot
        global pagina_borrar
        global url_list
        global cr_flag
        gcs_tool = CloudTools()
        date_manager = gcs_tool.date_manager()
        self.web_scrap_links = date_manager + ".json"
        self.output_file = date_manager + ".json"

        try:
            if cr_flag:
                logging.warning("reading last url list file ....")
                time.sleep(2)
                self.last_file_scrap_links = gcs_tool.gcs_get_last_file(
                    ".json", key_bot + "/sales/houses/url-list"
                ) 
                self.last_file_df_links = gcs_tool.gcs_read_file_pd(
                    self.last_file_scrap_links, key_bot + "/sales/houses/url-list/"
                )
                self.last_file_list_links = self.last_file_df_links[
                    "scrap_links"
                ].values.tolist()
                cr_flag = False
                logging.warning("file found!!!")
                logging.warning("last week links")
                logging.warning(len(self.last_file_list_links))
                time.sleep(10)
        except:
            logging.warning("no file to read!!!")
            self.last_file_list_links = []
            cr_flag = False
            logging.warning("last week links")
            logging.warning(len(self.last_file_list_links))
            time.sleep(10)

        # Read step by step file
        procesos = "data/procesos.json"
        with open(procesos) as step_file:
            steps = json.load(step_file)

        # *******************STARTS-MAIN-CODE**********************
        logging.warning('Starting to scrap')
        scrap_tool = ScrapTool(response)
        soup = scrap_tool.soup_creation()
        # Read key file instructions for scraping
        url_divs = scrap_tool.search_nest(soup, steps["INT"]["P1"])

        # Logic for getting all urls

        for link in url_divs:
            url = (
                (scrap_tool.search_nest(link, steps["INT"]["P2"]))
                .find("h2")
                .find("a")
                .get("href")
            )
            url_list.append(url)
            # logging.warning('----------------')
        logging.warning("urls a scrapear: " + str(len(url_list)))

        # SCRAPPEAR
        # Logic for iterating over pages
        last_page_as = scrap_tool.search_nest(soup, steps["INT"]["P3"])
        last_page_list = scrap_tool.search_nest(last_page_as, steps["INT"]["P4"])[-1]
        next_page_link = last_page_list.get("href")
        next_page_name = last_page_list.get_text()
        logging.warning(next_page_name)
        if next_page_name == "Siguiente ":
            logging.warning("pagina numero: " + str(pagina_borrar))
            yield response.follow(next_page_link, callback=self.parse)
            pagina_borrar = pagina_borrar + 1
            logging.warning("vamos por la pag: " + str(pagina_borrar))
        else:
            df = pd.DataFrame({"scrap_links": url_list})
            logging.warning("extracted links today: ")
            logging.warning(df.shape[0])
            # df.to_json(self.web_scrap_links,orient="records", lines=True)
            # step1. Upload todays links as Json to gcs
            gcs_tool.gcs_upload_file_pd(
                df, self.web_scrap_links, ".json", key_bot + "/sales/houses/url-list/"
            )
            # step2. Check wether is or not a last link file to know what sites to scrap
            scrap_links_today = []
            if self.last_file_list_links == []:
                scrap_links_today = url_list
            else:
                for url in url_list:
                    if url not in self.last_file_list_links:
                        scrap_links_today.append(url)
            scrap_links_today = list(dict.fromkeys(scrap_links_today))
            if len(scrap_links_today) != 0:
                # step3. scrap the links
                time.sleep(2)
                logging.warning("Scrapping " + str(len(scrap_links_today)) + " links today")
                # step3. scrap the links
                lista_len = len(scrap_links_today)
                logging.warning("tamano lista")
                logging.warning(lista_len)
                scrap_date = datetime.today().strftime("%d/%m/%Y")

                for page in scrap_links_today:
                    yield response.follow(
                        page,
                        callback=self.int_logic,
                        meta={
                            "enlace": page,
                            "tool": gcs_tool,
                            "list_size": lista_len,
                            "scrap_date": scrap_date,
                        },
                    )
            else:
                logging.warning("No Links to scrap today, code is over!!!!")

            # *******************STOPS-MAIN-CODE***********************

    def int_logic(self, response):
        # ----------------START_SCRAP_PROCEDURE-------------------------------------
        # For All Bots
        rand_secs = round(random.randint(1, 2) * random.random(), 2)
        time.sleep(rand_secs)
        global json_file
        global contador
        global key_bot
        my_url = response.meta.get("enlace")
        gcs_tool = response.meta.get("tool")
        list_size = response.meta.get("list_size")
        scrap_date = response.meta.get("scrap_date")

        dataset_1 = {}
        dataset_2 = {}
        dataset_3 = {}
        # For All Bots
        # add url key and date to database
        url_dataset = {"url": my_url, "extraction_date": scrap_date}

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

        # logging.warning(data_set1)
        properties = url_dataset | dataset_4 | dataset_1 | dataset_3 | dataset_2
        # append to dictionary
        json_file.append(properties)
        # for all bots
        data_file = pd.DataFrame(json_file)
        # data_file.to_json(self.output_file,orient="records", lines=True)
        time.sleep(2)
        logging.warning("pagina numero " + str(contador) + " de " + str(list_size))
        list_size_2 = round((list_size * 0.98), 0)
        if contador >= list_size_2:
            logging.warning("writing properties to cloud storage")
            time.sleep(5)
            # data_file.to_json(self.output_file,orient="records", lines=True)
            gcs_tool.gcs_upload_file_pd(
                data_file,
                self.output_file,
                ".json",
                key_bot + "/sales/houses/raw-data/",
            )
        contador = contador + 1
        # ----------------STOP_SCRAP_PROCEDURE-------------------------------------


time.sleep(50)
start = time.time()
process = CrawlerProcess()
process.crawl(SpiderRES)
process.start()
stopt = time.time()
logging.warning("duracion: " + str((stopt - start) / 60) + " minutos")
