import pandas as pd
from json_keys_lists import search_keys_meta_list, search_offer_cols_map
from math import ceil
from random import shuffle
from utils import (
    time_print,
    random_sleep,
    load_offer_json,
    add_json_values
)

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

def get_offer_json_and_max_page(scraper, url):

    offers_json = load_offer_json(scraper, url, 'search_page')['results']
    max_page = ceil(offers_json['aggregatedOffers']/28)
    if max_page > 54:
        raise ValueError("max_page is higher than 54!")

    return offers_json, max_page

def parse_all_search_results(scraper, url):
    offers_json, max_page = get_offer_json_and_max_page(scraper, url)
    if max_page == 0:
        time_print("nothing to parse")
        return None

    time_print(f"starting search page parsing, {max_page} pages in total")
    page_df_list = list()
    pages_list = list(range(max_page))
    shuffle(pages_list)
    for i in pages_list:

        time_print(f"processing page {i+1}")
        offers_json = load_offer_json(scraper, f"{url}&p={i+1}", 'search_page')['results']
            
        # break the loop if the page is empty
        if len(offers_json['offers']) == 0:
            break

        current_page_df = pd.concat([process_single_offer_card(offer) 
                                     for offer in offers_json['offers']
                                    ], 
                                    ignore_index=True
                          )
        page_df_list.append(current_page_df)
        time_print("finished")

        random_sleep(scraper.mean_sleep, scraper.var_sleep)

    full_df = pd.concat(page_df_list, ignore_index=True)
    
    return full_df

def try_parse_search(scraper, url):
    try:
        return parse_all_search_results(scraper, url)

    except ValueError as e:
        if str(e) != "max_page is higher than 54!":
            raise

        time_print("max_page is higher than 54, trying to find a valid split")
        room_filters = ['room1=1', 'room2=1', 'room3=1', 'room4=1', 'room5=1&room6=1&room7=1&room9=1']
        
        #checking max_page for room based split
        def parse_short(scraper, url, single_filter):
            return get_offer_json_and_max_page(scraper, f"{url}&{single_filter}")
        
        # if max_page is higher than 54, we should get an error here
        [parse_short(scraper, url, single_filter) for single_filter in room_filters]

        # if no errors on the previous step, we can proceed
        time_print("parsing district using the room based split")
        def parse_short(url, single_filter):
            time_print(f"parsing search results for filter {single_filter}")
            return parse_all_search_results(scraper, url = f"{url}&{single_filter}")

        df_list = [parse_short(url, single_filter) for single_filter in room_filters]
        df = pd.concat(df_list, ignore_index=True)

        return df