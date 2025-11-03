import asyncio
import traceback
import pandas as pd
from math import ceil
from random import shuffle, choice
from py.constants.json_keys_lists import search_keys_meta_list, search_offer_cols_map
from py.utils.SleepyScraper import SleepyScraper
from py.utils.proxy_utils import get_unique_proxy, release_proxy
from py.utils.utils import (
    time_print,
    random_sleep,
    load_offer_json,
    add_json_values
)


MAX_PAGE_ERROR = "max_page is too high!"

def process_single_offer_card(offer):
    single_ad_df = pd.DataFrame()

    #from json_keys_lists import keys_meta_list
    json_list = [offer, 
                 offer['geo'], 
                 offer['building'], 
                 offer['bargainTerms']
                ]

    for json, keys_list in zip(json_list, search_keys_meta_list):
        single_ad_df = add_json_values(single_ad_df, json, keys_list, col_names_map = search_offer_cols_map)

    single_ad_df['cian_price_range'] = offer['goodPrice']['estimationRange'] if 'goodPrice' in offer.keys() else None
    single_ad_df['photo_url_list'] = str([x['fullUrl'] for x in offer['photos']])
    single_ad_df['moderation_info_search'] = str(offer["moderationInfo"])

    return single_ad_df


async def get_offer_json_and_max_page(scraper, url, ignore_page_limit = False):

    offers_json = await load_offer_json(scraper, url, 'search_page')
    offers_json = offers_json['results']
    max_page = ceil(offers_json['aggregatedOffers']/28)

    critical_page_num = 54
    if max_page > critical_page_num and not ignore_page_limit:
        raise ValueError(MAX_PAGE_ERROR)

    if ignore_page_limit:
        max_page = critical_page_num

    return offers_json, max_page


async def try_load_offer_json(scraper, 
                        url,
                        try_cnt = 1,
                        sleep_minutes = 10
    ):

    try:
        res = await load_offer_json(scraper, url, 'search_page')
        return res['results']

    except Exception as e:
        time_print(f"an error occured, try_cnt = {try_cnt}, url = {url}, proxy = {scraper.proxy}: {str(e)}")
        if try_cnt <= 5:
            
            old_proxy = scraper.proxy 
            new_proxy = await get_unique_proxy()
            await release_proxy(old_proxy)

            new_scraper = SleepyScraper(mean_sleep = scraper.mean_sleep, 
                                        var_sleep = scraper.var_sleep, 
                                        proxy = new_proxy
                          )
            scraper.close()

            time_print("changed {old_proxy} to {new_proxy}")
            

            time_print(f"{url}, {new_proxy}: retrying after {sleep_minutes} minutes sleep")
            await random_sleep(60 * sleep_minutes, 5)

            return await try_load_offer_json(new_scraper,
                                             url,
                                             try_cnt = try_cnt + 1,
                                             sleep_minutes = sleep_minutes + 10
                         )

        else:
            time_print(f"{url}, {scraper.proxy}: all retries have failed")
            raise


async def parse_all_search_results(scraper, url, ignore_page_limit = False):
    offers_json, max_page = await get_offer_json_and_max_page(scraper, url, ignore_page_limit)
    if max_page == 0:
        time_print("nothing to parse")
        return None

    time_print(f"starting search page parsing, {max_page} pages in total")
    page_df_list = list()
    pages_list = list(range(max_page))
    shuffle(pages_list)

    break_num = choice([10, 11, 12, 13, 14, 15])
    for index, page_num in enumerate(pages_list):

        time_print(f"processing page {page_num+1}")
        offers_json = await try_load_offer_json(scraper, f"{url}&p={page_num+1}")
            
        # break the loop if the page is empty
        if len(offers_json['offers']) == 0:
            break

        df_list = [process_single_offer_card(offer) for offer in offers_json['offers']]
        current_page_df = pd.concat(df_list, ignore_index=True)
        
        page_df_list.append(current_page_df)
        time_print("finished")

        # some extra sleep if too many photos
        if index % break_num == 0:
            await random_sleep(30, 0.5, prefix = scraper.proxy.split("@", 1)[1])

        await random_sleep(scraper.mean_sleep, scraper.var_sleep, prefix = scraper.safe_proxy)

    if len(page_df_list) == 0:
        return None    
    
    full_df = pd.concat(page_df_list, ignore_index=True)
    
    return full_df


async def try_parse_search(scraper, url):
    try:
        return await parse_all_search_results(scraper, url)

    except ValueError as e:
        if str(e) != MAX_PAGE_ERROR:
            raise

        time_print("max_page is too high, trying to find a valid split")
        room_filters = ['room1=1', 'room2=1', 'room3=1', 'room4=1', 'room5=1&room6=1&room7=1&room9=1']
        
        # shortcut
        time_print("parsing district using the room based split")
        async def parse_short(url, single_filter):
            time_print(f"parsing search results for filter {single_filter}")
            res = await parse_all_search_results(scraper, url = f"{url}&{single_filter}", ignore_page_limit = True)
            return res

        df_list = list() 
        for single_filter in room_filters:
            res = await parse_short(url, single_filter)
            df_list.append(res)
            
        df = pd.concat(df_list, ignore_index=True)

        return df
