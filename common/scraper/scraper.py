from bs4 import BeautifulSoup
import pandas as pd
import json
import time

class Scraper():
    """
        Class used for BS4 Scrapping, that contains normal tools for reading elements
        suchs as tables, <ul>,<dl>, urls, etc...
    """
    def __init__(self, response):
        """
            Init class with soup response
        """
        self.response = response
        self.soup = BeautifulSoup(self.response.text,'lxml')

    def table_read(self, table_class = None):
        """
            Read whole table
        """
        try:
            if table_class==None:
                #find table
                table = self.soup.find('table')
            else:
                table = self.soup.find('table',{'class':table_class})
            #find table body 
            tbody = table.find('tbody')

            th = tbody.find_all('th')
            td = tbody.find_all('td')
            cols = []
            values = []
            for col,val in zip(th,td):
                cols.append(col.get_text())
                values.append(val.get_text())
            #create dictionary
            zip_it = zip(cols,values)
            data_dict = dict(zip_it)
        except:
            data_dict = {}
        return data_dict

    def table_read_col(self, table_col,tag,extract):
        """
            Return all values from a single colum
        """
        #find table
        table = self.soup.find('table')
        #find table body 
        rows = table.find('tbody')
        desired_col = rows.find_all('td', {'class':table_col})
        url_list = []
        for row in desired_col:
            url = row.find(tag).get(extract)
            url_list.append(url)
        return url_list

    def dl_read(self, dl_class=None):
        """
            Read Data List from HTML
        """
        col_names = []
        values = []
        if dl_class==None:
            data_list = self.soup.find_all('dl')
            for dl in data_list:
                data_names = dl.find_all('dt')
                data_values = dl.find_all('dd')
                for col, val in zip(data_names,data_values):
                    #print(col,val)
                    col_names.append(col.text)
                    values.append(val.text)
            zip_it = zip(col_names,values)
            data_dict = dict(zip_it)
        else:
            data_list = self.soup.find_all('dl', {'class':dl_class})
            for dl in data_list:
                data_names = dl.find_all('dt')
                data_values = dl.find_all('dd')
                for col, val in zip(data_names,data_values):
                    #print(col,val)
                    col_names.append(col.text)
                    values.append(val.text)
            zip_it = zip(col_names,values)
            data_dict = dict(zip_it)
        return data_dict

    def search_nest(self, str_dict, soup=None):
        """
            Nested search for any kind of element.
        """
        soup = self.soup if soup is None else soup
        try:
            for step in str_dict:
                if(str_dict[step]['function']=="find"):
                    response = soup.find(str_dict[step]['search'],
                                        {   str_dict[step]['key']:
                                            str_dict[step]['key_name']
                                        })
                else:
                    response = soup.find_all(str_dict[step]['search'],
                        {   str_dict[step]['key']:
                            str_dict[step]['key_name']
                        })
                new_soup = response
        except:
            for step in str_dict:
                if(str_dict[step]['function']=="find"):
                    response = soup.find(str_dict[step]['search'])
                else:
                    response = soup.find_all(str_dict[step]['search'])
                new_soup = response
        return new_soup
    
    def getatr_fromlist(self,list_,tag,attr):
        """
            DEF
        """
        if(attr=="text"):
            try:
                lista = []
                for i in list_:
                    lista.append(i.find(tag).get_text())
                return lista
            except:
                return list_.find(tag).get_text()
        else:
            try:
                lista = []
                for i in list_:
                    lista.append(i.find(tag).get(attr))
                return lista
            except:
                return list_.find(tag).get(attr)

    def next_page(self, nav_class, ul_class):
        """
            Find next url of an interation page
        """
        div_pages = self.soup.find('div', {'class' : nav_class})
        ul_item = div_pages.find('ul',{'class' : ul_class})
        last_li = ul_item.find_all('li')[-1].find('a').get('href')
        text_last_li =ul_item.find_all('li')[-1].find('a').get_text()
        return last_li,text_last_li

    def scrape_urls_from_properties_page(self, steps:json) -> list:
        """
        Scrapes the urls from the properties page.
        """
        urls_list = []
        urls_divs = self.search_nest(steps["P1"]) # Gets the section that contains the posts in the Propertie Page
        for link in urls_divs:
            url = (
                (self.search_nest(steps["P2"], link)) # Gets the link of the post
                .find("h2")
                .find("a")
                .get("href")
            )
            urls_list.append(url) # Appends the link to the list of links
        return urls_list