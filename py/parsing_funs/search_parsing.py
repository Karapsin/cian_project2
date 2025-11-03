import random
import traceback
import pandas as pd
import asyncio
import nest_asyncio
from random import shuffle
from py.constants.constants import DISTRICTS_LISTS, CIAN_SEARCH_URLS
from py.parsing_funs.search_parsing_logic import try_parse_search
from py.utils.SleepyScraper import SleepyScraper
from py.utils.utils import time_print, get_current_datetime, random_sleep
from py.utils.db_utils import insert_df, query_table, delete_from_table, update_last_seen_entries
from py.utils.proxy_utils import get_unique_proxy, release_proxy, get_proxy_list

nest_asyncio.apply()

def get_search_url(deal_type, district):
    return f"{CIAN_SEARCH_URLS[deal_type]}&{district}"


def get_search_pages_dict(deal_type):
    initial_dict = {district: get_search_url(deal_type, district) for district in DISTRICTS_LISTS}
    items_list = list(initial_dict.items())
    shuffle(items_list)
    shuffled_dict = dict(items_list)

    return shuffled_dict


async def parse_single_district(semaphore,
                                deal_type,
                                scraper,
                                parsed_urls,
                                search_alias,
                                url
    ):

    async with semaphore:
        proxy = await get_unique_proxy()
        scraper = SleepyScraper(mean_sleep = scraper.mean_sleep, 
                                var_sleep = scraper.var_sleep, 
                                proxy = proxy
                  )

        try: 
            time_print(f"processing {search_alias}")
            time_print(f"url: {url}")
            df = await try_parse_search(scraper, url)
            scraper.close()

            progress_df_upd = pd.DataFrame({"search_alias": [search_alias], "ad_deal_type":[deal_type]})
            insert_df(progress_df_upd, "search_parsing_progress")

            if df is None:
                time_print("no ads for that district, going to the next one")
                await random_sleep(scraper.mean_sleep, scraper.var_sleep)
                return None

            df['search_page_load_dttm'] = get_current_datetime()

            old_urls_df = df.query("url in @parsed_urls")[['url', 'search_page_load_dttm']]
            old_urls_df = old_urls_df.rename(columns={'search_page_load_dttm': 'last_seen_dttm'})
            update_last_seen_entries(old_urls_df)

            df = df.query('url not in @parsed_urls')


            if df.shape[0] == 0:
                time_print("no new ads for that district, going to the next one")
                return None

            time_print(f"found {df.shape[0]} new ads")

            df['search_alias'] = search_alias
            df['ad_deal_type'] =  deal_type
            parsed_urls.update(set(df['url']))

            insert_df(df, "search_page_parsed")

        except Exception as e:
            time_print(f"parse_single_district: error with {proxy}, url {url}, error text:  {str(e)}, traceback: {traceback.format_exc()}")
            raise

        finally:
            await release_proxy(scraper.proxy)

async def search_single_deal_type(deal_type, 
                                  scraper, 
                                  parsed_urls, 
                                  search_dict
    ):

    time_print(f"parsing of {deal_type}")
    
    semaphore = asyncio.Semaphore(len(get_proxy_list()))
    tasks = [parse_single_district(semaphore, deal_type, scraper, parsed_urls, search_alias, url) 
             for search_alias, url in search_dict.items()
            ]

    await asyncio.gather(*tasks)
    
    time_print(f"parsing of {deal_type} is finished")
    time_print("------------------------------------------------------------------------------------------------------------------------------------------------------")

def search_all_deals_type(scraper, 
                          deal_types_to_check = CIAN_SEARCH_URLS.keys(),
                          districts_limit_per_deal_type = None
    ):
    
    parsed_urls = query_table("search_page_parsed", columns_dict = {"url": 1, "_id": 0})

    if parsed_urls.empty:
        parsed_urls = set()
    else:
        parsed_urls = set(parsed_urls['url'])

    more_parsed_urls = query_table("search_clean", columns_dict = {"url": 1, "_id": 0})
    if more_parsed_urls.empty:
        more_parsed_urls = set()
    else:
        more_parsed_urls = set(more_parsed_urls['url'])

    parsed_urls.update(more_parsed_urls)

    progress_df = query_table("search_parsing_progress")
    if progress_df.empty:
        progress_df = pd.DataFrame({"search_alias": [], "ad_deal_type": []})

    for deal_type in CIAN_SEARCH_URLS:
        if deal_type not in deal_types_to_check:
            continue
        
        search_dict = get_search_pages_dict(deal_type)
        parsed_districts = set(progress_df.query("ad_deal_type == @deal_type")['search_alias'])

        # remove already parsed districts from search_dict
        search_dict = {district: url for district, url in search_dict.items() if district not in parsed_districts}

        # limiting number of districts which will be parsed
        if districts_limit_per_deal_type is not None:
            random_districts = random.sample(list(search_dict), districts_limit_per_deal_type)
            search_dict = {district: search_dict[district] for district in random_districts}

        asyncio.run(search_single_deal_type(deal_type, 
                                            scraper, 
                                            parsed_urls, 
                                            search_dict
        ))

    delete_from_table("search_parsing_progress")
