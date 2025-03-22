import pandas as pd
from random import shuffle 
from py.constants.constants import DISTRICTS_LISTS, CIAN_SEARCH_URLS
from py.parsing_funs.search_parsing_logic import try_parse_search
from py.utils.utils import time_print, get_current_datetime, random_sleep


def get_search_url(deal_type, district):
    return f"{CIAN_SEARCH_URLS[deal_type]}&{district}"

def get_search_pages_dict(deal_type):
    initial_dict = {district: get_search_url(deal_type, district) for district in DISTRICTS_LISTS}
    items_list = list(initial_dict.items())
    shuffle(items_list)
    shuffled_dict = dict(items_list)

    return shuffled_dict

def search_single_deal_type(deal_type, scraper, parsed_urls, progress_df):
    time_print(f"parsing of {deal_type}")

    n_parsed = 0
    search_dict = get_search_pages_dict(deal_type)
    parsed_districts = set(progress_df['search_alias'])
    for search_alias, url in search_dict.items():

        if search_alias in parsed_districts:
            n_parsed += 1
            continue

        time_print(f"processing {search_alias}")
        time_print(f"url: {url}")
        df = try_parse_search(scraper, url)

        if df is None:
            time_print("no ads for that district, going to the next one")
            n_parsed += 1
            random_sleep(scraper.mean_sleep, scraper.var_sleep)
            continue

        df = df.query('url not in @parsed_urls')
        if df.shape[0] == 0:
            n_parsed += 1
            time_print("no new ads for that district, going to the next one")
            continue

        time_print(f"found {df.shape[0]} new ads")

        df['search_alias'] = search_alias
        df['search_page_load_dttm'] = get_current_datetime()
        df['ad_deal_type'] =  deal_type
        parsed_urls = parsed_urls | set(df['url'])

        df.to_csv("data_load\\search_page_parsed.csv", mode='a', header=False, index=False)
        
        (
        pd.DataFrame({"search_alias": [search_alias], 
                      "ad_deal_type":[deal_type]
                     })
           .to_csv("data_load\\search_parsing_progress.csv", 
                    mode='a', 
                    header=False, 
                    index=False
            )
        )

        n_parsed += 1
        time_print(f"already parsed: {round((n_parsed/len(search_dict))*100, 1)}%")
    
    time_print(f"parsing of {deal_type} is finished")
    time_print("------------------------------------------------------------------------------------------------------------------------------------------------------")

def search_all_deals_type(scraper, deal_types_to_check = CIAN_SEARCH_URLS.keys()):
    parsed_urls = set(pd.read_csv("data_load\\search_page_parsed.csv", usecols=['url'])['url'])
    progress_df = pd.read_csv("data_load\\search_parsing_progress.csv")

    [search_single_deal_type(deal_type, scraper, parsed_urls, progress_df.query("ad_deal_type == @deal_type")) 
    for deal_type in CIAN_SEARCH_URLS
    if deal_type in deal_types_to_check
    ]
    pd.DataFrame({"search_alias": [], "ad_deal_type": []}).to_csv("data_load\\search_parsing_progress.csv", index= False)