import json

def get_properties_page_url(scrapy_name: str) -> str:
    """
    Returns the URL of the properties page.
    """
    with open("data/keys.json", encoding='utf-8') as json_file:
        keys = json.load(json_file)
    return keys[scrapy_name.upper()]["url"]

def get_process_steps(scrapy_name: str) -> json:
    """
    Returns the steps to scrape the properties page.
    """
    procesos = "data/procesos.json"
    with open(procesos, encoding='utf-8') as step_file:
        steps = json.load(step_file)
    return steps[scrapy_name.upper()]

def preserve_unique_items_from_b(list_a: list, list_b: list) -> list:    
    """
    Returns the unique items from List B if the lists have at least one common element.
    """
    set_a = set(list_a)
    set_b = set(list_b)
    return list(set_b - set_a) if bool(set_a & set_b) else []