import os
import json
from gcloud import storage
from datetime import datetime
import pandas as pd
import unicodedata
import re
import pytz
import json

# from dotenv import load_dotenv


class CloudTools:
    def __init__(self):
        self.project_id = "datalake-homyai"
        self.bucket_name = "web-scraper-data"

    # function to upload data
    def gcs_upload_file(self, file, path=""):
        """
        Upload plain file to a determine bucket
        """

        self.client = storage.Client(self.project_id)
        self.bucket = self.client.get_bucket(self.bucket_name)
        self.blob = self.bucket.blob(str(path) + file)
        self.blob.upload_from_filename(file)
        print("uploading in progress....")

    def gcs_upload_file_pd(self, df, file_name, extension, path=""):
        """
        Upload plain file to a determine bucket
        """
        real_path = "gs://" + self.bucket_name + "/" + path + file_name
        real_path_log = (
            "gs://" + self.bucket_name + "/" + path + "new-cols-log/" + file_name
        )
        # added nico's code
        df.columns = df.columns.str.replace(" ", "_")
        df.columns = df.columns.str.replace("/", "_")
        df.columns = df.columns.str.replace("-", "_")
        df.columns = df.columns.str.lower()

        columns_lista = []

        for column in df.columns:
            columns_lista.append(self.text_to_id(column))

        df.columns = columns_lista

        # added nico's code
        # si el codigo solo tiene una columna

        # si es scrap links (solo una columna)
        if len(df.columns.tolist()) == 1:
            if extension == ".csv":
                df.to_csv(real_path, index=False)
            if extension == ".json":
                df.to_json(real_path, orient="records", lines=True)
        # si estamos subiendo una data base
        else:
            df_real, df_news = self.only_listed_cols(df)
            if extension == ".csv":
                df_real.to_csv(real_path, index=False)
                if len(df_news.columns.tolist()) > 1:
                    df_news.to_csv(real_path_log, index=False)
            if extension == ".json":
                df_real.to_json(real_path, orient="records", lines=True)
                if len(df_news.columns.tolist()) > 1:
                    df_news.to_json(real_path_log, orient="records", lines=True)
        print("uploading in progress....")

    def gcs_read_file_pd(self, file_name, path=""):
        """
        Read file from a certain bucket
        """
        extension_to_read = "." + file_name.split(".")[1]
        if path == "":
            read_path = "gs://" + str(self.bucket_name) + "/" + file_name
        else:
            read_path = "gs://" + str(self.bucket_name) + "/" + str(path) + file_name
        print(read_path)
        if extension_to_read == ".csv":
            db = pd.read_csv(read_path)
        if extension_to_read == ".json":
            db = pd.read_json(read_path, orient="records", lines=True)
        return db

    def gcs_get_last_file(self, extension, bucket_path=""):
        """
        Get all files in the seame bucket
        """
        # os.environ[
        #     "GOOGLE_APPLICATION_CREDENTIALS"
        # ] = "data/datalake-homyai-990ddbaa84ae.json"
        self.client = storage.Client(self.project_id)
        print(self.bucket_name + "/" + str(bucket_path))
        self.bucket = self.client.get_bucket(self.bucket_name)
        self.archivos = self.bucket.list_blobs(prefix=bucket_path)
        self.files_list = []
        for file in self.archivos:
            self.files_list.append(file.name)
        self.last_file_name = self.last_file(self.files_list, extension)
        return self.last_file_name

    def last_file(self, lista, extension):
        # filter_lista = list(filter(lambda root : root_3_letters in root, lista))
        filter_lista = lista
        dates_list = []
        try:
            for element in filter_lista:
                elemento = element.split("/")[-1]
                dates_list.append(int(elemento[:8]))
        except:
            for element in filter_lista[1:]:
                elemento = element.split("/")[-1]
                dates_list.append(int(elemento[:8]))
        last_date = max(dates_list)
        last_file = str(last_date) + extension
        return last_file

    def date_manager(self):
        date_time_now = datetime.now(pytz.timezone("America/Costa_Rica"))
        day = date_time_now.day
        month = date_time_now.month
        year = date_time_now.year
        if day < 10:
            day = "0" + str(day)
        if month < 10:
            month = "0" + str(month)
        date_str = str(year) + str(month) + str(day)
        return date_str

    def strip_accents(self, text):
        """
        Strip accents from input String.

        :param text: The input string.
        :type text: String.

        :returns: The processed String.
        :rtype: String.
        """
        try:
            text = str(text, "utf-8")
        except (TypeError, NameError):  # unicode is a default on python 3
            pass
        text = unicodedata.normalize("NFD", text)
        text = text.encode("ascii", "ignore")
        text = text.decode("utf-8")
        return str(text)

    def text_to_id(self, text):
        """
        Convert input text to id.
        Fix cases in which the first character is an int (BigQuery Requirement)

        :param text: The input string.
        :type text: String.

        :returns: The processed String.
        :rtype: String.
        """
        text = self.strip_accents(text.lower())
        text = re.sub("[ ]+", "_", text)
        text = re.sub("[^0-9a-zA-Z_-]", "", text)
        text = re.sub("1", "", text)

        # First number character fixing
        try:
            first_number = int(text[0])
            text = "_" + text
        except:
            pass
        return text

    def only_listed_cols(self, df):
        """
        Includes only validated columns to bigquery data base
        Includes missing cols on data updates
        Also stores missing fields if needed

        :param df: pandas dataframe
        :type df: pandas dataframe

        :returns: Two dataframes with validated cols and new cols
        :rtype: dataframe
        """
        file_name = "data/procesos.json"
        with open(file_name) as json_file:
            json_data = json.load(json_file)
        cols = json_data["columnas"]
        non_existing_cols = ["url"]
        # new columns
        for i in df.columns.tolist():
            if i not in (cols):
                non_existing_cols.append(i)
        df_new_cols = df[non_existing_cols]

        # df oficial columns
        df_oficial = df.reindex(columns=cols)
        return df_oficial, df_new_cols
