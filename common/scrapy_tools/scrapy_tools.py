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

def next_properties_page(scrapy_name: str, scrap_tool, soup, steps:json, response, parse):
    """
    Goes to the next properties page.
    """
    if scrapy_name == "int":
        last_page_as = scrap_tool.search_nest(soup, steps["P3"]) # List of following pages
        last_page_list = scrap_tool.search_nest(last_page_as, steps["P4"])[-1] # Last item in the list
        next_page_link = last_page_list.get("href") # Link to the last item in the list
        next_page_name = last_page_list.get_text() # Name of the last it in the list
        yield response.follow(next_page_link, callback=parse) if next_page_name == "Siguiente " else None # If the name of the last item is "Siguiente " then follow the link

def preserve_b_items_if_common(list_a: list, list_b: list) -> list:    
    """
    Returns True if the lists have at least one common element.
    """
    set_a = set(list_a)
    set_b = set(list_b)
    return list(set_b - set_a) if bool(set_a & set_b) else []