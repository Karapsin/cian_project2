import pandas as pd
import requests
import asyncio
from random import shuffle
from py.utils.SleepyScraper import SleepyScraper
from py.parsing_funs.offer_parsing_logic import parse_offer_page
from py.utils.db_utils import insert_df, query_table, delete_from_table
from py.utils.proxy_utils import get_unique_proxy, release_proxy, get_proxy_list
from py.utils.utils import(
     get_current_datetime, 
     random_sleep, 
     time_print
)
pd.options.mode.copy_on_write = True


async def try_parse_offer_page(scraper, 
                               url, 
                               photos_url, 
                               visited_before, 
                               deal_type, 
                               try_cnt = 1, 
                               sleep_minutes = 10
    ):

    try:
        return await parse_offer_page(scraper, 
                                      url, 
                                      photos_url, 
                                      visited_before, 
                                      deal_type
                    )

    except Exception as e:

        time_print(str(e))
        if try_cnt <= 3:
            time_print(f"retrying after {sleep_minutes} minutes sleep")
            await random_sleep(60 * sleep_minutes, 5)

            return await try_parse_offer_page(scraper, 
                                              url, 
                                              photos_url, 
                                              visited_before, 
                                              deal_type, 
                                              try_cnt = try_cnt + 1, 
                                              sleep_minutes = sleep_minutes + 10
                         )

        time_print("all retries have failed")

        raise

async def parse_single_url(semaphore, scraper, url, visited_before):
    async with semaphore:
        proxy = await get_unique_proxy()
        scraper = SleepyScraper(mean_sleep = scraper.mean_sleep, 
                                var_sleep = scraper.var_sleep, 
                                proxy = proxy
                )

        try:
            time_print(f"starting {url} with proxy {proxy.split("@", 1)[1]}")
            old_df = query_table("search_clean", query_dict = {"url": url})

            photos_url = list()
            if len(old_df['photo_url_list']) > 0:
                photos_url = list(eval(old_df['photo_url_list'].tolist()[0])) 

            new_df = await try_parse_offer_page(scraper, 
                                                url, 
                                                photos_url, 
                                                visited_before, 
                                                old_df['ad_deal_type'].to_list()[0]
                            )

            old_df['offer_page_load_dttm'] = get_current_datetime()

            for col in new_df.columns:
                
                new_value = new_df[col].to_list()[0]
                if col in old_df.columns and (new_value is None or pd.isna(new_value)):
                    continue

                if col == 'description' and new_value == 'Объявление снято с публикации, поищите ещё что-нибудь':
                    continue

                old_df[col] = new_value

            insert_df(old_df, "offers_parsed")

            delete_from_table("offers_to_parse", {"url": url})

        finally:
            await release_proxy(proxy)

async def parse_offers(scraper):
    
    urls_to_parse = set(query_table("offers_to_parse",  columns_dict = {"url": 1, "_id": 0})['url'])

    all_visited = query_table("offers_parsed",  columns_dict = {"url": 1, "_id": 0})
    if all_visited.empty:
        all_visited = set()
    else:
        all_visited = set(all_visited['url'])

    semaphore = asyncio.Semaphore(len(get_proxy_list()))

    tasks = [parse_single_url(semaphore, scraper, url, url in all_visited) for url in urls_to_parse]

    await asyncio.gather(*tasks)
    