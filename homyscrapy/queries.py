GET_URLS_QUERY = """
SELECT 
    distinct url
FROM datalake-homyai.gcs.sales_houses_{bot}
where url = ''
"""