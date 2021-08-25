from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from time import sleep
from bs4 import BeautifulSoup
import re
import os
import logging
import threading
from queue import Queue
from datetime import datetime
from datetime import timedelta
from datetime import date
import pandas as pd
from concurrent.futures import as_completed
from requests_futures.sessions import FuturesSession
from io import StringIO
from io import BytesIO
import gzip
import io
import xml.etree.cElementTree as ET
from itertools import repeat
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from concurrent.futures import ThreadPoolExecutor

RUN_ID = datetime.today().strftime('%Y%m%d%H%M%S')
q = Queue()

class LOGGER_DB_STREAM(io.TextIOBase):
    def __init__(self, *args):
        self.driver = 'ODBC Driver 17 for SQL Server'
        self.server = 'lazy-june.com'
        self.port = '1433'
        self.database = 'lazy-june.com'
        self.username = 'lazy-june.com'
        self.password = 'lazy-june.com'
        self.connection_string = f'mssql+pyodbc://{self.username}:{self.password}@{self.server}:{self.port}/{self.database}?driver={self.driver.replace(" ", "+")}'
        self.engine = create_engine(self.connection_string, fast_executemany=True)
        self.columns = ['_timestamp', '_level', 'unit', 'msg']
        io.TextIOBase.__init__(self, *args)
        threading.Thread(target=self.worker, daemon=True).start()

    def write(self, x):
        global q
        row = re.split(" ; ", x.replace("\n", ""))
        if len(row) >= 4:
            row_dict = {'_timestamp': [row[0]], '_level': [row[1]], 'unit': [row[2]], 'msg': [row[3]]}
            df = pd.DataFrame(columns=self.columns)
            df = df.append(pd.DataFrame.from_dict(row_dict))
            q.put(df)
        else:
            print(f'Logger row is smaller than len(4): {row}')
        print(row)

    def worker(self):
        global q
        while True:
            df = q.get()
            df.to_sql(name='script_logs', schema='RetailData', if_exists='append', index=False, con=self.engine)
            q.task_done()

# Logger config:
logger = logging.getLogger('basic_logger')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler(LOGGER_DB_STREAM())
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s ; %(levelname)s ; %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

# SQL statements:
start_procedure = 'execute some procedure at startup'
chain_and_stores_query = 'load chain ids and store ids for parsing'
end_procedure = 'execute some procedure before closing the program'

cerebrus = 'https://url.publishedprices.co.il/login'
nibit = 'http://matrixcatalog.co.il/NBCompetitionRegulations.aspx'
shufersal = 'http://prices.shufersal.co.il/'
cookie_value = []

cerebrus_headers = {
        'Host': 'url.publishedprices.co.il',
        'Connection': 'keep-alive',
        'sec-ch-ua': '"Chromium";v="92", " Not A;Brand";v="99", "Google Chrome";v="92"',
        'sec-ch-ua-mobile': '?0',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-User': '?1',
        'Sec-Fetch-Dest': 'document',
        'Referer': 'https://url.publishedprices.co.il/file',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'en-US,en;q=0.9'
    }
nibit_headers = {
        'Accept' : 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Accept-Encoding' : 'gzip, deflate',
        'Accept-Language' : 'en,he-IL;q=0.9,he;q=0.8,en-US;q=0.7',
        'Connection' : 'keep-alive',
        'Host' : 'matrixcatalog.co.il',
        'Referer' : 'http://matrixcatalog.co.il/NBCompetitionRegulations.aspx',
        'Upgrade-Insecure-Requests' : '1',
        'User-Agent' : 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36'
    }

shufersal_headers = {
        'Host': 'pricesprodpublic.blob.core.windows.net',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Referer': 'http://prices.shufersal.co.il/',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'en,he-IL;q=0.9,he;q=0.8,en-US;q=0.7'
    }

js_websites = {
                'Rami Levi' : { 'url' : cerebrus,
                                'base_url' : cerebrus.replace('/login', ''),
                                'user': 'RamiLevi',
                                'pass': '',
                                'url_regex_extract_result' : ".*\/((.*)(full|Full)(\d{13})-(\d{3})-(\d{12}).gz)",
                                'url_regex_extract_result2' : ".*\/((.*)()(\d{13})-()(\d{12}).xml)"
                                },
                'Mahsanei Hashuk' : {
                                'url' : nibit,
                                'base_url' : nibit.replace('NBCompetitionRegulations.aspx', ''),
                                'chains button': '//*[@id="MainContent_chain"]',
                                'current chain' : '//*[@id="MainContent_chain"]/option[4]',
                                'categpry picker' : '//*[@id="MainContent_fileType"]',
                                'category' : [['promofull', '//*[@id="MainContent_fileType"]/option[3]'],
                                              ['pricefull', '//*[@id="MainContent_fileType"]/option[5]'],
                                              ['stores', '//*[@id="MainContent_fileType"]/option[6]']],
                                'submit' : '//*[@id="MainContent_btnSearch"]',
                                'url_regex_extract_result' : ".*\/((.*)(full|Full)(\d{13})-(\d{3})-(\d{12})-\d{3}.xml.gz)",
                                'url_regex_extract_result2' : "something_isn't_there",
                                },
                'H Cohen' : {
                                'url' : nibit,
                                'base_url' : nibit.replace('NBCompetitionRegulations.aspx', ''),
                                'chains button': '//*[@id="MainContent_chain"]',
                                'current chain' : '//*[@id="MainContent_chain"]/option[3]',
                                'categpry picker' : '//*[@id="MainContent_fileType"]',
                                'category' : [['promofull', '//*[@id="MainContent_fileType"]/option[3]'],
                                              ['pricefull', '//*[@id="MainContent_fileType"]/option[5]'],
                                              ['stores', '//*[@id="MainContent_fileType"]/option[6]']],
                                'submit' : '//*[@id="MainContent_btnSearch"]',
                                'url_regex_extract_result' : ".*\/((.*)(full|Full)(\d{13})-(\d{3})-(\d{12})-\d{3}.xml.gz)",
                                'url_regex_extract_result2' : "something_isn't_there",
                                },
                'Victory' : {
                                'url' : nibit,
                                'base_url' : nibit.replace('NBCompetitionRegulations.aspx', ''),
                                'chains button': '//*[@id="MainContent_chain"]',
                                'current chain' : '//*[@id="MainContent_chain"]/option[2]',
                                'categpry picker' : '//*[@id="MainContent_fileType"]',
                                'category' : [['promofull', '//*[@id="MainContent_fileType"]/option[3]'],
                                              ['pricefull', '//*[@id="MainContent_fileType"]/option[5]'],
                                              ['stores', '//*[@id="MainContent_fileType"]/option[6]']],
                                'submit' : '//*[@id="MainContent_btnSearch"]',
                                'url_regex_extract_result' : ".*\/((.*)(full|Full)(\d{13})-(\d{3})-(\d{12})-\d{3}.xml.gz)",
                                'url_regex_extract_result2' : "something_isn't_there",
                                },
                'Shufersal' : {
                                'url' : shufersal,
                                'base_url' : shufersal,
                                'categpry picker' : '//*[@id="ddlCategory"]',
                                'category' : [['promofull', '//*[@id="ddlCategory"]/option[5]'],
                                              ['pricefull', '//*[@id="ddlCategory"]/option[3]'],
                                              ['stores', '//*[@id="ddlCategory"]/option[6]']],
                                'sort time xpath': '//*[@id="gridContainer"]/table/thead/tr/th[2]/a',
                                'next page xpath' : '//*[@id="gridContainer"]/table/tfoot/tr/td/a[5]',
                                'url_regex_extract_result' : ".*\/((.*)(full|Full)(\d{13})-(\d{3})-(\d{12}).gz)",
                                'url_regex_extract_result2' : ".*\/((.*)()(\d{13})-(\d{3})-(\d{12}).gz)",
                                },

                'Hazi Hinam' : {'url' : cerebrus,
                                'base_url' : cerebrus.replace('/login', ''),
                                'user': 'HaziHinam',
                                'pass': '',
                                'url_regex_extract_result' : ".*\/((.*)(full|Full)(\d{13})-(\d{3})-(\d{12}).gz)",
                                'url_regex_extract_result2' : ".*\/((.*)()(\d{13})-()(\d{12}).xml)"
                                },
                'Yohananof' : { 'url' : cerebrus,
                                'base_url' : cerebrus.replace('/login', ''),
                                'user': 'yohananof',
                                'pass': '',
                                'url_regex_extract_result' : ".*\/((.*)(full|Full)(\d{13})-(\d{3})-(\d{12}).gz)",
                                'url_regex_extract_result2' : ".*\/((.*)()(\d{13})-()(\d{12}).xml)"
                                },
                'Osherad' : {   'url' : cerebrus,
                                'base_url' : cerebrus.replace('/login', ''),
                                'user': 'osherad',
                                'pass': '',
                                'url_regex_extract_result' : ".*\/((.*)(full|Full)(\d{13})-(\d{3})-(\d{12}).gz)",
                                'url_regex_extract_result2' : ".*\/((.*)()(\d{13})-()(\d{12}).xml)"
                                },
                'Salach Dabah':{'url' : cerebrus,
                                'base_url' : cerebrus.replace('/login', ''),
                                'user': 'SalachD',
                                'pass': '',
                                'url_regex_extract_result' : ".*\/((.*)(full|Full)(\d{13})-(\d{3})-(\d{12}).gz)",
                                'url_regex_extract_result2' : ".*\/((.*)()(\d{13})-()(\d{12}).xml)"
                                },
                'Freshmarket' :{'url' : cerebrus,
                                'base_url' : cerebrus.replace('/login', ''),
                                'user': 'freshmarket',
                                'pass': '',
                                'url_regex_extract_result' : ".*\/((.*)(full|Full)(\d{13})-(\d{3})-(\d{12}).gz)",
                                'url_regex_extract_result2' : ".*\/((.*)()(\d{13})-()(\d{12}).xml)"
                                },
                'Stop Market' :{'url' : cerebrus,
                                'base_url' : cerebrus.replace('/login', ''),
                                'user': 'Stop_Market',
                                'pass': '',
                                'url_regex_extract_result' : ".*\/((.*)(full|Full)(\d{13})-(\d{3})-(\d{12}).gz)",
                                'url_regex_extract_result2' : ".*\/((.*)()(\d{13})-()(\d{12}).xml)"
                                },
                'Bareket' : {
                                'url' : nibit,
                                'base_url' : nibit.replace('NBCompetitionRegulations.aspx', ''),
                                'chains button': '//*[@id="MainContent_chain"]',
                                'current chain' : '//*[@id="MainContent_chain"]/option[5]',
                                'categpry picker' : '//*[@id="MainContent_fileType"]',
                                'category' : [['promofull', '//*[@id="MainContent_fileType"]/option[3]'],
                                              ['pricefull', '//*[@id="MainContent_fileType"]/option[5]'],
                                              ['stores', '//*[@id="MainContent_fileType"]/option[6]']],
                                'submit' : '//*[@id="MainContent_btnSearch"]',
                                'url_regex_extract_result' : ".*\/((.*)(full|Full)(\d{13})-(\d{3})-(\d{12})-\d{3}.xml.gz)",
                                'url_regex_extract_result2' : "something_isn't_there",
                                }

               }

def login(driver, _user, _pass):
    username = driver.find_element_by_id("username")
    username.clear()
    username.send_keys(_user)
    password = driver.find_element_by_id("password")
    password.clear()
    password.send_keys(_pass)
    driver.find_element_by_id("login-button").click()

def press_button(driver, xpath, seconds=1, loop=1):
    for i in range(loop):
        try:
            driver.find_element_by_xpath(xpath).click()
        except:
            try:
                driver.execute_script("arguments[0].click();", driver.find_element_by_xpath(xpath))
            except:
                return True
        sleep(seconds)

def get_filenames(driver, chain_name, base_url, result_regex, result2_regex, df_filter,
                  user=None, relative=False, reverse_slash=False):
    global cookie_value
    sleep(3)
    if driver.get_cookie('cftpSID'):
        cookie_value = ['cftpSID', driver.get_cookie('cftpSID')['value']]
    elif driver.get_cookie('ASP.NET_SessionId'):
        cookie_value = ['ASP.NET_SessionId', driver.get_cookie('ASP.NET_SessionId')['value']]
    elif driver.get_cookie('ARRAffinity'):
        cookie_value = ['ARRAffinity', driver.get_cookie('ARRAffinity')['value']]
    else:
        cookie_value = None
    html = driver.page_source
    soup = BeautifulSoup(html, features="html.parser")
    downloads = {'chain_name' : [], '_type' : [], 'store_id' : [], 'chain_id' : [],  '_datetime' : [], 'filename' : [], 'd_url' : [], '_user': []}
    logger.info(f'Scraping ; Parsing links for chain_name: {chain_name}')
    for tag in soup.find_all('a', href=True):
        d_url = base_url + tag['href'] if relative else tag['href']
        d_url = d_url.replace("\\", "/") if reverse_slash else d_url
        result = re.match(result_regex, d_url)
        result_2 = re.match(result2_regex, d_url)
        if result:
            filename = result.group(1)
            type = result.group(2)
            chain_id = result.group(4)
            store_id = result.group(5)
            _datetime = datetime.strptime(result.group(6), "%Y%m%d%H%M")
            # Build dict for df:
            downloads['chain_name'].append(chain_name)
            downloads['_type'].append(type)
            downloads['store_id'].append(store_id)
            downloads['chain_id'].append(chain_id)
            downloads['_datetime'].append(_datetime)
            downloads['filename'].append(filename)
            downloads['d_url'].append(d_url)
            downloads['_user'].append(user)
        elif result_2:
            filename = result_2.group(1)
            type = result_2.group(2)
            chain_id = result_2.group(4)
            store_id = -1
            _datetime = datetime.strptime(result_2.group(6), "%Y%m%d%H%M")
            # Build dict for df:
            downloads['chain_name'].append(chain_name)
            downloads['_type'].append(type)
            downloads['store_id'].append(store_id)
            downloads['chain_id'].append(chain_id)
            downloads['_datetime'].append(_datetime)
            downloads['filename'].append(filename)
            downloads['d_url'].append(d_url)
            downloads['_user'].append(user)

    df = pd.DataFrame.from_dict(downloads)
    df = df[df.groupby(['chain_id', '_type', 'store_id'])['_datetime'].transform(max) == df['_datetime']]
    logger.info(f'Scraping ; Found {df.shape[0]} urls')
    if not df.empty:
        stores_df = df['_type'].str.lower() == 'stores'
        stores_df = df[stores_df]
        df[['chain_id', 'store_id']] = df[['chain_id', 'store_id']].astype(str)
        df_filter[['chain_id', 'store_id']] = df_filter[['chain_id', 'store_id']].astype(str)
        stores_chains_df = df.merge(df_filter, on=['chain_id', 'store_id'], how='inner')
        stores_chains_df = stores_chains_df.append(stores_df)
        logger.info(f'Scraping ; Retrieving {len(stores_chains_df)} urls after filter')
        return stores_chains_df
    else:
        logger.warning(f"Scraping ; Couldn't find URLs")
        return df


def download_files(urls, base_url=None):
    global cookie_value, cerebrus_headers, nibit_headers
    cookie = {
        cookie_value[0] : cookie_value[1]
    }
    if base_url == cerebrus.replace('/login', ''):
        headers = cerebrus_headers
    elif base_url == nibit.replace('NBCompetitionRegulations.aspx', ''):
        headers = nibit_headers
    elif base_url == shufersal:
        headers = shufersal_headers
    if urls:
        logger.info(f'Scraping ; Downloading {len(urls)} files')
        downloaded_files = []
        with FuturesSession(max_workers=20) as session:
            futures = [session.get(url, headers=headers, cookies=cookie) for url in urls]
            for future in as_completed(futures):
                response = future.result()
                downloaded_files.append(response)
        logger.info(f'Scraping ; Downloaded {len(urls)} files')
        return downloaded_files
    else:
        return []

def promo_xml_parser(zipped_f, url=None):
    result = re.match(".*\/.*-(\d{12}).", url)
    file_datetime = datetime.strptime(result.group(1), "%Y%m%d%H%M")
    ignored_tags = ['xmldocversion', 'root']
    promotions_start = False
    sales_start = False
    in_promotion = False
    in_sale = False
    in_items = False
    in_clubs = False
    items_counter = 0
    parent_cols = {}
    promotion_cols = {}
    item_cols = {}
    sale_cols = {}
    club_cols = {}
    all_cols = {}
    parsed_dict = {}

    buffer = BytesIO(zipped_f)
    with gzip.open(buffer, 'rb') as f:
        decoded_string = f.read().decode('UTF-8')
        xml_file = StringIO(decoded_string)
        for event, elem in ET.iterparse(xml_file, events=("start", "end")):
            if 'matrixcatalog' in url:
                start_tag = event == 'start'
                end_tag = event == 'end'
                elem_tag = elem.tag.lower().strip()
                elem_text = elem.text
                if start_tag and elem_tag == 'sale':
                    in_sale = True
                    sales_start = True
                elif end_tag and elem_tag == 'sale':
                    in_sale = False
                # Fill parent tags information:
                if start_tag and not sales_start and not len(elem) and elem_tag not in ignored_tags:
                    parent_cols[elem_tag] = elem.text
                # Fill sale information:
                if end_tag and in_sale:
                    if not len(elem) and elem.text is not None:
                        # Write all cols anyway:
                        all_cols[elem_tag] = '-1'
                        # Write to current loop cols:
                        sale_cols[elem_tag] = elem.text

                if end_tag and elem_tag == 'sale':
                    # populated first time:
                    if parsed_dict == {}:
                        for i in all_cols.keys():
                            parsed_dict[i] = []
                    unique_new_keys = list(set(all_cols) - set(parsed_dict.keys()))
                    old_keys_not_showing_in_items = list(set(all_cols) - set(sale_cols.keys()))
                    if unique_new_keys:
                        for unique_v in unique_new_keys:
                            parsed_dict[unique_v] = []
                            parsed_dict[unique_v].extend(repeat('-1', items_counter))
                    for sale_col in sale_cols.keys():
                        parsed_dict[sale_col].append(sale_cols[sale_col])
                    # balance rows:
                    items_counter += len(sale_cols['itemcode'])
                    # Make up for old keys not showing
                    for old_key in old_keys_not_showing_in_items:
                        parsed_dict[old_key].append('-1')
                    # Reset temp dicts:
                    sale_cols = {}

            else:
                start_tag = event == 'start'
                end_tag = event == 'end'
                elem_tag = elem.tag.lower().strip()
                elem_text = elem.text
                if start_tag and elem_tag == 'promotions':
                    promotions_start = True
                elif start_tag and elem_tag == 'promotion':
                    in_promotion = True
                elif end_tag and elem_tag == 'promotion':
                    in_promotion = False
                elif start_tag and elem_tag == 'promotionitems':
                    in_items = True
                elif end_tag and elem_tag == 'promotionitems':
                    in_items = False
                elif start_tag and elem_tag == 'clubs':
                    in_clubs = True
                elif end_tag and elem_tag == 'clubs':
                    in_clubs = False

                # Fill parent tags information:
                if start_tag and not promotions_start and not len(elem) and elem_tag not in ignored_tags:
                    parent_cols[elem_tag] = elem.text

                # Fill promotion information:
                if end_tag and in_promotion:
                    if not len(elem) and elem.text is not None:
                        # Write all cols anyway:
                        if 'clubid' == elem_tag:
                            all_cols[elem_tag] = '-1'
                        else:
                            all_cols[elem_tag] = '-1'
                        # Write parent promotion info:
                        if not in_items and not in_clubs:
                            promotion_cols[elem_tag] = elem.text
                        # Write items info:
                        elif in_items and len(item_cols.keys()) < 3:
                            item_cols[elem_tag] = [elem.text]
                        elif in_items and len(item_cols.keys()) >= 3:
                            item_cols[elem_tag].append(elem.text)
                        # Write clubs info
                        elif in_clubs and len(club_cols.keys()) == 0:
                            club_cols[elem_tag] = [elem.text]
                        elif in_clubs and len(club_cols.keys()) > 0:
                            club_cols[elem_tag].append(elem.text)

                if end_tag and elem_tag == 'promotion':
                    # populated first time:
                    if parsed_dict == {}:
                        for i in all_cols.keys():
                            parsed_dict[i] = []
                    unique_new_keys = list(set(all_cols) - set(parsed_dict.keys()))
                    old_keys_not_showing_in_promotion = list(set(all_cols) - set(promotion_cols.keys()))
                    if unique_new_keys:
                        for unique_v in unique_new_keys:
                            parsed_dict[unique_v] = []
                            parsed_dict[unique_v].extend(repeat('-1', items_counter))
                    for i_club, v_club in enumerate(club_cols['clubid']):
                        for i, v in enumerate(item_cols['itemcode']):
                            items_counter += 1
                            # Regular append of values to main parsed_dict
                            for i_promo in promotion_cols.keys():
                                parsed_dict[i_promo].append(promotion_cols[i_promo])
                            for item_attr in item_cols.keys():
                                parsed_dict[item_attr].append(item_cols[item_attr][i])
                            for club_attr in club_cols.keys():
                                parsed_dict[club_attr].append(club_cols[club_attr][i_club])
                            # Make up for old keys not showing in current promotion:
                            for old_key in old_keys_not_showing_in_promotion:
                                if 'item' not in old_key and 'clubid' not in old_key:
                                    parsed_dict[old_key].append('-1')
                    # Reset temp dicts:
                    promotion_cols = {}
                    item_cols = {}
                    club_cols = {}

    df = pd.DataFrame.from_dict(parsed_dict)
    for k, v in parent_cols.items():
        df[k] = v
    parsed_dict = parent_cols = {}
    df['origin_url'] = url
    df['file_datetime'] = file_datetime
    return df

def price_xml_parser(zipped_f, url=None):
    result = re.match(".*\/.*-(\d{12}).", url)
    file_datetime = datetime.strptime(result.group(1), "%Y%m%d%H%M")
    ignored_tags = ['xmldocversion', 'root']
    in_item = False
    items_start = False
    parent_cols = {}
    item_cols = {}
    all_cols = {}
    parsed_dict  = {}
    items_counter = 0

    buffer = BytesIO(zipped_f)
    with gzip.open(buffer, 'rb') as f:
        decoded_string = f.read().decode('UTF-8')
        xml_file = StringIO(decoded_string)
        for event, elem in ET.iterparse(xml_file, events=("start", "end")):
            start_tag = event == 'start'
            end_tag = event == 'end'
            elem_tag = elem.tag.lower().replace('products', 'items').replace('product', 'item')
            elem_text = elem.text
            if start_tag and elem_tag == 'items':
                in_item = True
                items_start = True
            elif end_tag and elem_tag == 'items':
                in_item = False

            # Fill parent tags information:
            if start_tag and not items_start and not len(elem) and elem_tag not in ignored_tags:
                parent_cols[elem_tag] = elem.text

            # Fill item information:
            if end_tag and in_item:
                if not len(elem) and elem.text is not None:
                    # Write all cols anyway:
                    all_cols[elem_tag] = '-1'
                    item_cols[elem_tag] = elem.text

            if end_tag and elem_tag == 'item':
                # populated first time:
                if parsed_dict == {}:
                    for i in all_cols.keys():
                        parsed_dict[i] = []
                unique_new_keys = list(set(all_cols) - set(parsed_dict.keys()))
                old_keys_not_showing_in_items = list(set(all_cols) - set(item_cols.keys()))
                if unique_new_keys:
                    for unique_v in unique_new_keys:
                        parsed_dict[unique_v] = []
                        parsed_dict[unique_v].extend(repeat('-1', items_counter))
                for item_col in item_cols.keys():
                    parsed_dict[item_col].append(item_cols[item_col])
                # balance rows:
                items_counter += 1
                # Make up for old keys not showing
                for old_key in old_keys_not_showing_in_items:
                    parsed_dict[old_key].append('-1')
                # Reset temp dicts:
                item_cols = {}

    df = pd.DataFrame.from_dict(parsed_dict)
    for k, v in parent_cols.items():
        df[k] = v
    df['origin_url'] = url
    df['file_datetime'] = file_datetime
    return df

def store_xml_parser(zipped_f, _zipped_file=True, url=None):
    result = re.match(".*\/.*-(\d{12}).", url)
    file_datetime = datetime.strptime(result.group(1), "%Y%m%d%H%M")
    ignored_tags = ['xmldocversion', 'root']
    in_store = False
    stores_start = False
    parent_cols = {}
    store_cols = {}
    all_cols = {}
    parsed_dict  = {}
    items_counter = 0

    try:
        buffer = BytesIO(zipped_f)
        with gzip.open(buffer, 'rb') as f:
            decoded_string = f.read().decode('UTF-8')
            xml_file = StringIO(decoded_string)
            for event, elem in ET.iterparse(xml_file, events=("start", "end")):
                start_tag = event == 'start'
                end_tag = event == 'end'
                elem_tag = elem.tag.lower()
                elem_text = elem.text
                if start_tag and elem_tag == 'store':
                    in_store = True
                    stores_start = True
                elif end_tag and elem_tag == 'store':
                    in_store = False
                # Fill parent tags information:
                if start_tag and not stores_start and not len(elem) and elem_tag not in ignored_tags:
                    parent_cols[elem_tag] = elem.text
                # Fill store information:
                if end_tag and in_store:
                    if not len(elem) and elem.text is not None:
                        # Write all cols anyway:
                        all_cols[elem_tag] = '-1'
                        store_cols[elem_tag] = elem.text
                if end_tag and elem_tag == 'store':
                    items_counter += 1
                    # populated first time:
                    if parsed_dict == {}:
                        for i in all_cols.keys():
                            parsed_dict[i] = []
                    unique_new_keys = list(set(all_cols) - set(parsed_dict.keys()))
                    old_keys_not_showing_in_items = list(set(all_cols) - set(store_cols.keys()))
                    if unique_new_keys:
                        for unique_v in unique_new_keys:
                            parsed_dict[unique_v] = []
                            parsed_dict[unique_v].extend(repeat('-1', items_counter))
                    for store_col in store_cols.keys():
                        parsed_dict[store_col].append(store_cols[store_col])
                    # Make up for old keys not showing
                    for old_key in old_keys_not_showing_in_items:
                        parsed_dict[old_key].append('-1')
                    # Reset temp dicts:
                    store_cols = {}
    # Plain XML read:
    except:
        buffer = BytesIO(zipped_f)
        for event, elem in ET.iterparse(buffer, events=("start", "end")):
            start_tag = event == 'start'
            end_tag = event == 'end'
            elem_tag = elem.tag.lower()
            elem_text = elem.text
            if start_tag and elem_tag == 'store':
                in_store = True
                stores_start = True
            elif end_tag and elem_tag == 'store':
                in_store = False
            # Fill parent tags information:
            if start_tag and not stores_start and not len(elem) and elem_tag not in ignored_tags:
                parent_cols[elem_tag] = elem.text
            # Fill store information:
            if end_tag and in_store:
                if not len(elem) and elem.text is not None:
                    # Write all cols anyway:
                    all_cols[elem_tag] = '-1'
                    store_cols[elem_tag] = elem.text
            if end_tag and elem_tag == 'store':
                items_counter += 1
                # populated first time:
                if parsed_dict == {}:
                    for i in all_cols.keys():
                        parsed_dict[i] = []
                unique_new_keys = list(set(all_cols) - set(parsed_dict.keys()))
                old_keys_not_showing_in_items = list(set(all_cols) - set(store_cols.keys()))
                if unique_new_keys:
                    for unique_v in unique_new_keys:
                        parsed_dict[unique_v] = []
                        parsed_dict[unique_v].extend(repeat('-1', items_counter))
                for store_col in store_cols.keys():
                    parsed_dict[store_col].append(store_cols[store_col])
                # Make up for old keys not showing
                for old_key in old_keys_not_showing_in_items:
                    parsed_dict[old_key].append('-1')
                # Reset temp dicts:
                store_cols = {}

    df = pd.DataFrame.from_dict(parsed_dict)
    for k, v in parent_cols.items():
        df[k] = v
    df['origin_url'] = url
    df['file_datetime'] = file_datetime
    return df

def db_executer(df=False, promos=False, prices=False, stores=False,
              filenames=False, query=False, start_procedure=False, end_procedure=False):
    driver = 'ODBC Driver 17 for SQL Server'
    server = 'lazy-june.com'
    port = '1433'
    database = 'lazy-june.com'
    username = 'lazy-june.com'
    password = 'lazy-june.com'
    connection_string = f'mssql+pyodbc://{username}:{password}@{server}:{port}/{database}?driver={driver.replace(" ", "+")}'
    engine = create_engine(connection_string, fast_executemany=True)
    chunksize = 10000
    rows = 0
    if promos:
        logger.info(f'Writing ; Writing file to promo_daily, len: {len(df)}')
        for start in range(0, len(df), chunksize):
            try:
                df[start:start + chunksize].to_sql(name='promo_daily', schema='RetailData',
                                                   if_exists='append', index=False, con=engine)
                rows += len(df[start:start + chunksize])
            except Exception as e:
                logger.warning(f'Writing ; Batch write failed on row: {rows}, msg: {e}')
                return rows
        return rows
    elif prices:
        logger.info(f'Writing ; Writing file to price_daily, len: {len(df)}')
        for start in range(0, len(df), chunksize):
            try:
                df[start:start + chunksize].to_sql(name='price_daily', schema='RetailData',
                                               if_exists='append', index=False, con=engine)
                rows += len(df[start:start + chunksize])
            except Exception as e:
                logger.warning(f'Writing ; Batch write failed on row: {rows}, msg: {e}')
                return rows
            return rows
    elif stores:
        logger.info(f'Writing ; Writing file to store_daily')
        for start in range(0, len(df), chunksize):
            try:
                df[start:start + chunksize].to_sql(name='store_daily', schema='RetailData',
                                               if_exists='append', index=False, con=engine)
                rows += len(df[start:start + chunksize])
            except Exception as e:
                logger.warning(f'Writing ; Batch write failed on row: {rows}, msg: {e}')
                return rows
            return rows
    elif filenames:
        logger.info(f'Writing ; Writing file to filenames')
        df.to_sql(name='filenames', schema='RetailData', if_exists='append', index=False, con=engine)
    elif query:
        def value_standard(row):
            if len(row) <= 2:
                return "00" + row if len(row) == 1 else "0" + row
            else:
                return row
        query = text(query)
        df = pd.read_sql(query, con=engine)
        df = df.dropna()
        df = df.astype({'chainid': 'int64', 'storeid':'int64'})
        df = df.astype({'chainid': 'str', 'storeid': 'str'})
        df['storeid'] = df['storeid'].apply(value_standard)
        df = df.rename(columns={'chainid': 'chain_id', 'storeid': 'store_id'})
        return df
    elif start_procedure:
        query = text(start_procedure)
        with engine.connect().execution_options(autocommit=True) as conn:
            conn.execution_options(stream_results=True).execute(query)
    elif end_procedure:
        query = text(end_procedure)
        with engine.connect().execution_options(autocommit=True) as conn:
            conn.execution_options(stream_results=True).execute(query)
    engine.dispose()

def file_operator(responses, df_filenames):
    for i, v in enumerate(responses):
        try:
            logger.info(
                f'Parsing ; Processing file from: {responses[i].url}, status_code: {responses[i].status_code}, file length: {len(responses[i].content)}')
            if not responses[i].ok:
                raise f"Downloaded file status code: {responses[i].status_code}"
            file = responses[i].content
            file_name = responses[i].url
            zipped_file = False if '.xml' in file_name else True
            result = re.match(".*\/(.*)\d{13}", file_name.lower())
            promo_file = 'promo' in result.group(1)
            prices_file = 'price' in result.group(1)
            stores_file = 'stores' in result.group(1)
            # New conditions for file types:
            if promo_file:
                parsed_dict = promo_xml_parser(file, url=responses[i].url)
                rows = db_executer(parsed_dict, promos=True)
                return_code = '1' if rows == len(parsed_dict) else '2'
                df_filenames['return_code'] = df_filenames['d_url'].map({responses[i].url : return_code})
                df_row = df_filenames[df_filenames['return_code'] == return_code]
                db_executer(df_row, filenames=True)
                logger.info(f'Writing ; Wrote {rows}/{len(parsed_dict)} to promo_file from: {responses[i].url}')
            elif prices_file:
                parsed_dict = price_xml_parser(file, url=responses[i].url)
                rows = db_executer(parsed_dict, prices=True)
                return_code = '1' if rows == len(parsed_dict) else '2'
                df_filenames['return_code'] = df_filenames['d_url'].map({responses[i].url: return_code})
                df_row = df_filenames[df_filenames['return_code'] == return_code]
                db_executer(df_row, filenames=True)
                logger.info(f'Writing ; Wrote {rows}/{len(parsed_dict)} to price_file from: {responses[i].url}')
            elif stores_file:
                if not zipped_file:
                    odd_file = file
                    parsed_dict = store_xml_parser(odd_file, _zipped_file=zipped_file, url=responses[i].url)
                elif zipped_file:
                    odd_file = file
                    parsed_dict = store_xml_parser(odd_file, _zipped_file=zipped_file, url=responses[i].url)
                # parsed_dict.to_csv('stores_out.csv', index=False)
                rows = db_executer(parsed_dict, stores=True)
                return_code = '1' if rows == len(parsed_dict) else '2'
                df_filenames['return_code'] = df_filenames['d_url'].map({responses[i].url: return_code})
                df_row = df_filenames[df_filenames['return_code'] == return_code]
                db_executer(df_row, filenames=True)
                logger.info(f'Writing ; Wrote {rows}/{len(parsed_dict)} to stores_file from: {responses[i].url}')
        except Exception as e:
            df_filenames['return_code'] = df_filenames['d_url'].map({responses[i].url: '0'})
            df_row = df_filenames[df_filenames['return_code'] == '0']
            db_executer(df_row, filenames=True)
            logger.error(f'Writing ; Response URL: {responses[i].url}, Error: {e}')



if __name__ == "__main__":
    chrome_options = Options()
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--window-size=1420,1080')
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--disable-gpu')

    df_filenames = pd.DataFrame(columns=['chain_name' , '_type', 'store_id', 'chain_id', '_datetime', 'filename', 'd_url', '_user'])
    files_to_parse = []
    # Run db procedure:
    db_executer(start_procedure=start_procedure)
    # Get Chains & store IDs needed to be fetched:
    chains_stores_filter = db_executer(query=chain_and_stores_query)
    # Make sure we're not running on empty:
    if chains_stores_filter.empty:
        # Shutdown:
        db_executer(end_procedure=end_procedure)
        logger.info(f'End process ; Not authorized to run, Shutting down, 200 seconds to go')
        q.join()
        sleep(200)
        os.system('systemctl poweroff')


    # Parse target websites
    for chain_name in js_websites.keys():
        driver = webdriver.Chrome(options=chrome_options)
        driver.implicitly_wait(15)
        logger.info(f'Scraping ; RUN_ID:{RUN_ID} Entering Chain: {chain_name}')
        # Cerebrus Strategy
        if 'user' in js_websites[chain_name]:
            _user = js_websites[chain_name]['user']
            _pass = js_websites[chain_name]['pass']
            driver.get(js_websites[chain_name]['url'])
            login(driver, _user, _pass)
            df_single_website_filenames = get_filenames(driver, chain_name, js_websites[chain_name]['base_url'],
                                                        js_websites[chain_name]['url_regex_extract_result'],
                                                        js_websites[chain_name]['url_regex_extract_result2'],
                                                        chains_stores_filter,
                                                        _user, relative=True)
            df_filenames = df_filenames.append(df_single_website_filenames, ignore_index=True)
            # Download files
            chain_df = df_single_website_filenames[df_single_website_filenames['chain_name'] == chain_name]
            temp_files_to_parse = download_files(chain_df['d_url'].to_list(),
                                                 base_url=js_websites[chain_name]['base_url'])
            for file in temp_files_to_parse:
                duplicated = False
                for existing_f in files_to_parse:
                    if file.url == existing_f.url:
                        duplicated = True
                if not duplicated:
                    files_to_parse.append(file)
        # Shufersal strategy:
        elif chain_name == 'Shufersal':
            today = date.today()
            today = today - timedelta(days=0)
            driver.get(js_websites[chain_name]['url'])
            for category, xpath in js_websites[chain_name]['category']:
                logger.info(f'Scraping ; Entering category: {category}')
                counter = 0
                press_button(driver, js_websites[chain_name]['categpry picker'], seconds=1)
                press_button(driver, xpath, seconds=5)
                sleep(6)
                if category != 'stores':
                    press_button(driver, js_websites[chain_name]['sort time xpath'], seconds=4, loop=2)
                while True:
                    logger.info(f'Scraping ; Page {counter}')
                    df_single_website_filenames = get_filenames(driver, chain_name, js_websites[chain_name]['base_url'],
                                                                js_websites[chain_name]['url_regex_extract_result'],
                                                                js_websites[chain_name]['url_regex_extract_result2'],
                                                                chains_stores_filter)
                    df_filenames = df_filenames.append(df_single_website_filenames, ignore_index=True)
                    df_single_website_filenames['_datetime'] = pd.to_datetime(df_single_website_filenames['_datetime'], errors='coerce')
                    mask = (df_single_website_filenames['_datetime'].dt.date < today)
                    masked_df = df_single_website_filenames.loc[mask].empty
                    compensate_for_bug = 1
                    if masked_df and category != 'stores':
                        counter += 1
                        # Download files
                        chain_df = df_single_website_filenames[df_single_website_filenames['chain_name'] == chain_name]

                        no_next_page = press_button(driver, js_websites[chain_name]['next page xpath'], seconds=2)
                        if no_next_page:
                            break
                        # Make sure you don't iterate on too many pages:
                        elif counter > 25 and df_single_website_filenames.empty:
                            logger.warning(
                                f'Scraping ; Explored too many pages, exiting category {category}, on page {counter}')
                            break
                        temp_files_to_parse = download_files(chain_df['d_url'].to_list(),
                                                             base_url=js_websites[chain_name]['base_url'])
                        for file in temp_files_to_parse:
                            duplicated = False
                            for existing_f in files_to_parse:
                                if file.url == existing_f.url:
                                    duplicated = True
                            if not duplicated:
                                files_to_parse.append(file)
                    elif category == 'stores':
                        if df_single_website_filenames.empty:
                            df_single_website_filenames = get_filenames(driver, chain_name,
                                                                        js_websites[chain_name]['base_url'],
                                                                        js_websites[chain_name][
                                                                            'url_regex_extract_result'],
                                                                        js_websites[chain_name][
                                                                            'url_regex_extract_result2'],
                                                                        chains_stores_filter)
                            df_filenames = df_filenames.append(df_single_website_filenames, ignore_index=True)
                        counter += 1
                        # Download files
                        chain_df = df_single_website_filenames[df_single_website_filenames['chain_name'] == chain_name]
                        temp_files_to_parse = download_files(chain_df['d_url'].to_list(),
                                                             base_url=js_websites[chain_name]['base_url'])
                        for file in temp_files_to_parse:
                            duplicated = False
                            for existing_f in files_to_parse:
                                if file.url == existing_f.url:
                                    duplicated = True
                            if not duplicated:
                                files_to_parse.append(file)
                        break
                    elif counter < 1 and not masked_df:
                        press_button(driver, js_websites[chain_name]['sort time xpath'], seconds=4)
                        counter += 1
                    else:
                        break

        # Nibit strategy:
        elif js_websites[chain_name]['url'] == nibit:
            driver.get(js_websites[chain_name]['url'])
            for category, xpath in js_websites[chain_name]['category']:
                logger.info(f'Scraping ; Entering category: {category}')
                press_button(driver, js_websites[chain_name]['chains button'], seconds=2)
                press_button(driver, js_websites[chain_name]['current chain'], seconds=0.5)
                press_button(driver, js_websites[chain_name]['categpry picker'], seconds=0.5)
                press_button(driver, xpath, seconds=0.5)
                press_button(driver, js_websites[chain_name]['submit'], seconds=3)
                df_single_website_filenames = get_filenames(driver, chain_name, js_websites[chain_name]['base_url'],
                                                            js_websites[chain_name]['url_regex_extract_result'],
                                                            js_websites[chain_name]['url_regex_extract_result2'],
                                                            chains_stores_filter,
                                                            relative=True, reverse_slash=True)
                df_filenames = df_filenames.append(df_single_website_filenames, ignore_index=True)
                # Download files
                chain_df = df_single_website_filenames[df_single_website_filenames['chain_name'] == chain_name]
                temp_files_to_parse = download_files(chain_df['d_url'].to_list(),
                                                     base_url=js_websites[chain_name]['base_url'])
                for file in temp_files_to_parse:
                    duplicated = False
                    for existing_f in files_to_parse:
                        if file.url == existing_f.url:
                            duplicated = True
                    if not duplicated:
                        files_to_parse.append(file)

        driver.close()
        #break
    # Remove duplicates:
    df_filenames = df_filenames.drop_duplicates(subset='filename')
    # Async parse & write param:
    logger.info(f'Parsing ; Processing {len(files_to_parse)} files')
    workers = 14
    n_work_to_do = 3
    n_list = []
    for i in range(0, len(files_to_parse), n_work_to_do):
        n_list.append(files_to_parse[i:i + n_work_to_do])
    # Async executer:
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for row in n_list:
            pool.submit(file_operator, row, df_filenames)
    logger.info(f'End process ; Done processing all')
    # Shutdown:
    db_executer(end_procedure=end_procedure)
    logger.info(f'End process ; Shutting down, 200 seconds to go')
    q.join()
    sleep(200)
    os.system('systemctl poweroff')
