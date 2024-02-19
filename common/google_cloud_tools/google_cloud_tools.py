import os
import json
from gcloud import storage
from datetime import datetime
import pandas as pd
import unicodedata
import re
import pytz
import json

def get_list_files_from_bucket(project_id: str, bucket_name: str, bucket_path: str) -> list:
    """
    Returns a list of files in a bucket.
    """
    client = storage.Client(project_id)
    bucket = client.get_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=bucket_path)
    files_list = [blob.name for blob in blobs]
    return files_list

def get_last_file_name_from_list(files_list: list, extension: str) -> str:
    """
    Returns the last file in a list of files.
    """
    try:
        dates_list = [int(file.split("/")[-1][:8]) for file in files_list]
    except:
        dates_list = [int(file.split("/")[-1][:8]) for file in files_list[1:]]
    last_date = max(dates_list)
    last_file_name = str(last_date) + extension
    return last_file_name

def get_df_from_bucket(bucket_name: str, file_name: str, bucket_path: str) -> pd.DataFrame:
    """
    Reads a file from a bucket and returns a pandas DataFrame.
    """
    extension_to_read = "." + file_name.split(".")[1]
    if bucket_path == "":
        complete_bucket_path = f"gs://{bucket_name}/{file_name}"
    else:
        complete_bucket_path = f"gs://{bucket_name}/{bucket_path}{file_name}"
    if extension_to_read == ".csv":
        df = pd.read_csv(complete_bucket_path)
    if extension_to_read == ".json":
        df = pd.read_json(complete_bucket_path, orient="records", lines=True)
    return df

def get_last_file_from_bucket(project_id: str, bucket_name: str, extension: str, bucket_path: str) -> pd.DataFrame:
    """
    Returns the last file from a bucket as a pandas DataFrame.
    """
    files_list = get_list_files_from_bucket(project_id, bucket_name, bucket_path)
    last_file_name = get_last_file_name_from_list(files_list, extension)
    df = get_df_from_bucket(bucket_name, last_file_name, bucket_path)
    return df

def date_manager():
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

def gcs_upload_file_pd(df, bucket_name, file_name, extension, path=""):
    """
    Upload plain file to a determine bucket
    """
    real_path = "gs://" + bucket_name + "/" + path + file_name
    real_path_log = (
        "gs://" + bucket_name + "/" + path + "new-cols-log/" + file_name
    )
    # added nico's code
    df.columns = df.columns.str.replace(" ", "_")
    df.columns = df.columns.str.replace("/", "_")
    df.columns = df.columns.str.replace("-", "_")
    df.columns = df.columns.str.lower()

    columns_lista = []

    for column in df.columns:
        columns_lista.append(text_to_id(column))

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
        df_real, df_news = only_listed_cols(df)
        if extension == ".csv":
            df_real.to_csv(real_path, index=False)
            if len(df_news.columns.tolist()) > 1:
                df_news.to_csv(real_path_log, index=False)
        if extension == ".json":
            df_real.to_json(real_path, orient="records", lines=True)
            if len(df_news.columns.tolist()) > 1:
                df_news.to_json(real_path_log, orient="records", lines=True)
    print("uploading in progress....")

def strip_accents(text):
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

def text_to_id(text):
    """
    Convert input text to id.
    Fix cases in which the first character is an int (BigQuery Requirement)

    :param text: The input string.
    :type text: String.

    :returns: The processed String.
    :rtype: String.
    """
    text = strip_accents(text.lower())
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

def only_listed_cols(df):
    """
    Includes only validated columns to bigquery data base
    Includes missing cols on data updates
    Also stores missing fields if needed

    :param df: pandas dataframe
    :type df: pandas dataframe

    :returns: Two dataframes with validated cols and new cols
    :rtype: dataframe
    """
    file_name = "data/columns.json"
    with open(file_name, encoding='utf-8') as json_file:
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