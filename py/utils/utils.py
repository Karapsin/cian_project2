from py.constants.constants import SEARCH_PAGE_JSON_TEMPLATE, OFFER_JSON_TEMPLATE
from bs4 import BeautifulSoup
import json as js
import os 
import pandas as pd
import datetime
import random
import time
import shutil

def get_current_date(output='text'):
    return str(datetime.datetime.now())[0:10] if output=='text' else datetime.datetime.now()


def get_current_datetime(output='text'):
    now = datetime.datetime.now()
    return now.strftime('%Y-%m-%d %H:%M') if output == 'text' else now

def time_print(string):
    return print((get_current_datetime()+' '+string))

def save_as_txt(object, file_name):
    with open(f'{file_name}.txt', 'w', encoding='utf-8') as file:
        file.write(object)

def forced_mkdir(path):
    try:
        os.mkdir(path)
    except FileExistsError:
        shutil.rmtree(path) 
        os.mkdir(path)

def random_sleep(mean, var):
    sleep_duration = random.normalvariate(mean, var)
    sleep_duration = max(0.5, sleep_duration)

    time_print(f"sleeping for {sleep_duration:.2f} seconds")
    time.sleep(sleep_duration)

def get_url_based_name(url, postfix):
    result = (url
                .replace('https://www.cian.ru/', '')
                .replace('/', '') 
             )
    result = f"{result}_{postfix}_{get_current_date()}"

    return result


def load_html(scraper,
              url, 
              save_to = None
    ):
    
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36'}
    html = scraper.get(url, headers = headers)
    html = html.text  

    if save_to is not None:
        save_as_txt(html, f"{save_to}\\html_content")

    return html


# since cian is written on react + ssr
# everytime offer page is created, 
# its json state is also created automatically and stored in the html somewhere
# inspired by: https://habr.com/ru/articles/494544/#comment_21436556

# that is very unlikely that the structure of that json will change in the future

# that function just returns that json from the loaded html
def parse_offer_json(html, 
                     start_json_template,
                     save_to = None             
    ):

    # we get such error if and only if we have been blocked
    if start_json_template not in html:
        print(html)
        raise ValueError("Json not found!!!")

    start = html.index(start_json_template) + len(start_json_template)
    end = html.index('</script>', start)
    json_raw = html[start:end].strip()[:-1]

    parsed_json_list = js.loads(json_raw[json_raw.index('concat(') :-1].replace('concat(', ''))

    # many internal json files contain mostly irrelevant tech info
    # we need only one which contains data about the ads
    if start_json_template == OFFER_JSON_TEMPLATE:
        needed_key = 'defaultState'

    elif start_json_template == SEARCH_PAGE_JSON_TEMPLATE:
        needed_key = 'initialState'

    else:
        raise ValueError("Unknown json template")

    offer_json = list(filter(lambda x: x['key'] == needed_key, 
                             parsed_json_list
                 ))[0]['value']

    if save_to is not None:
        with open(f'{save_to}\\offer_meta.json', 'w', encoding='utf-8') as file:
            js.dump(offer_json, file, ensure_ascii = False)

    return offer_json


def load_offer_json(scraper,
                    url,
                    page_type,
                    save_to = None
    ):

    if page_type == 'offer_page':
        start_json_template = OFFER_JSON_TEMPLATE
    elif page_type == 'search_page':
        start_json_template = SEARCH_PAGE_JSON_TEMPLATE
    else:
        raise ValueError("Unknown page")

    html = load_html(scraper, url, save_to)
    offer_json = parse_offer_json(html, 
                                  start_json_template,
                                  save_to
                 )

    return offer_json


def add_json_values(df, 
                    json, 
                    keys_list,
                    col_names_map = None
    ):

    for single_key in keys_list:
        value = (json[single_key] 
                 if single_key in json.keys() 
                 else None
                )
        
        if isinstance(value, dict) or isinstance(value, list) or isinstance(value, tuple):
            value = str(value)

        if col_names_map is None:
            col_name = single_key
        else:
            col_name = col_names_map[single_key] if single_key in col_names_map.keys() else single_key

        df[col_name] = [value]

    return df 