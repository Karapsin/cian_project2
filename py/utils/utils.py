import json as js
import os 
import pandas as pd
import datetime
import random
import time
import shutil
import asyncio
from bs4 import BeautifulSoup
from py.constants.constants import SEARCH_PAGE_JSON_TEMPLATE, OFFER_JSON_TEMPLATE

def get_current_date(output='text'):
    return str(datetime.datetime.now())[0:10] if output=='text' else datetime.datetime.now()

def get_current_datetime(output='text'):
    now = datetime.datetime.now()
    return now.strftime('%Y-%m-%d %H:%M') if output == 'text' else now

def dttm_to_seconds(dttm):
    if isinstance(dttm, str):
        dttm = datetime.datetime.strptime(dttm, '%Y-%m-%d %H:%M')
    return time.mktime(dttm.timetuple())

def days_between_dttms(dttm1, dttm2):
    seconds = abs(dttm_to_seconds(dttm1) - dttm_to_seconds(dttm2))
    seconds_in_day = 60*60*24
    return seconds/seconds_in_day

def time_print(string):
    return print((get_current_datetime()+' '+string))

def save_file(save_type, path, file_name, object):
    
    os.makedirs(path, exist_ok=True)
    save_path = f'{path}//{file_name}'

    if save_type == 'txt':
        with open(f'{save_path}.txt', 'w', encoding='utf-8') as file:
            file.write(object)

    elif save_type == 'json':
        with open(f'{save_path}.json', 'w', encoding='utf-8') as file:
            js.dump(object, file, ensure_ascii = False)

    elif save_type == 'image':
        with open(f"{save_path}", "wb") as file:
            file.write(object.content)


async def random_sleep(mean, var, prefix = None, async_sleep = True):
    sleep_duration = random.normalvariate(mean, var)
    sleep_duration = max(max(random.normalvariate(0.5, 1), 0), sleep_duration)

    str_print = f"sleeping for {sleep_duration:.2f} seconds"
    if prefix is not None:
        str_print = f"{prefix} {str_print}"

    time_print(str_print)

    if async_sleep:
        await asyncio.sleep(sleep_duration)
    else:
        time.sleep(sleep_duration)

def get_url_based_name(url):
    result = (url
                .replace('https://www.cian.ru/', '')
                .replace('/', '') 
             )
    result = f"{result}_{get_current_date()}"

    return result


def load_html(scraper,
              url, 
              save_to = None
    ):
    
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36'}
    html = scraper.get(url, headers = headers)
    html = html.text  

    if save_to is not None:
        save_file('txt', f'{save_to}', 'html_content', html)

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
        
        if 'cdn.cian.site/frontend/frontend-status-pages/404.svg' in html:
            raise ValueError('Error 404, page not found')

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
        save_file('json', f'{save_to}', 'offer_meta', offer_json)

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
