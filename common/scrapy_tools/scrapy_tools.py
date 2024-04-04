import json

def get_properties_page_url(scrapy_name: str):
    """
    Returns the URL of the properties page.
    """
    with open("data/keys.json", encoding='utf-8') as json_file:
        keys = json.load(json_file)
    return keys[scrapy_name.upper()]["url"]

def get_process_steps(scrapy_name: str):
    """
    Returns the steps to scrape the properties page.
    """
    procesos = "data/procesos.json"
    with open(procesos, encoding='utf-8') as step_file:
        steps = json.load(step_file)
    return steps[scrapy_name.upper()]

def scrape_urls_from_properties_page(scrap_tool, soup, steps:json) -> list:
    """
    Scrapes the urls from the properties page.
    """
    urls_list = []
    urls_divs = scrap_tool.search_nest(soup, steps["P1"]) # Gets the section that contains the posts in the Propertie Page
    for link in urls_divs:
        url = (
            (scrap_tool.search_nest(link, steps["P2"])) # Gets the link of the post
            .find("h2")
            .find("a")
            .get("href")
        )
        urls_list.append(url) # Appends the link to the list of links
    return urls_list

def preserve_b_items_if_common(list_a: list, list_b: list) -> list:    
    """
    Returns True if the lists have at least one common element.
    """
    set_a = set(list_a)
    set_b = set(list_b)
    return list(set_b - set_a) if bool(set_a & set_b) else []