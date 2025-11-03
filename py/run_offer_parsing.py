import asyncio
from random import shuffle
import traceback
import pandas as pd
import nest_asyncio
import time 
from datetime import timedelta

from py.parsing_funs.offer_parsing import parse_offers
from py.utils.SleepyScraper import SleepyScraper
from py.utils.bot_funs import send_telegram_message
from py.utils.utils import (
    time_print, 
    get_current_datetime,
    days_between_dttms
)
from py.utils.db_utils import (
    insert_df, 
    query_table, 
    delete_from_table,
    update_finish_dttm,
    get_finish_dttm
)

def run_parsing():
    send_telegram_message("Starting offers parsing", 'reports')
    try:
        if query_table("offers_to_parse").empty:

            time_print("sampling urls")
            parsed_urls_df = query_table("search_clean", columns_dict = {"url": 1, "last_seen_dttm": 1, "_id": 0})
            parsed_urls_df['last_seen_dttm'] = pd.to_datetime(parsed_urls_df['last_seen_dttm'].apply(lambda x: x[1] if isinstance(x, list) else x))

            # ads which are almost sure closed
            last_dt = parsed_urls_df['last_seen_dttm'].max()
            cutoff_dt = last_dt - timedelta(days = 14)
            all_closed_urls = set(parsed_urls_df.query("last_seen_dttm < @cutoff_dt or last_seen_dttm.isnull()")['url'].tolist())

            # ads which were labeled as closed
            closed_urls_to_exclude = set(query_table('offers_parsed', query_dict={'ad_is_closed': True}, columns_dict={'url': 1, '_id': 0})['url'])

            # urls of closed ads are excluded
            # + extra check that ad was not reopened
            closed_urls = all_closed_urls & closed_urls_to_exclude
            parsed_urls = set(parsed_urls_df['url'].to_list())
            empty_urls = set(query_table("empty_offers_blacklist", columns_dict = {"url": 1, "_id": 0})['url'])

            # URLs which were opened before parsing started
            # + URLs which were excluded during previous iterations
            urls_blacklist = set(pd.read_csv("urls_to_exclude.csv")["url"])


            full_blacklist = closed_urls | urls_blacklist | closed_urls | empty_urls
            pd.DataFrame({"url": list(full_blacklist)}).to_csv("urls_to_exclude.csv")


            # population cleaning    
            parsed_urls = list(parsed_urls - full_blacklist) 
            clean_population_size = len(parsed_urls)

            # actual sampling
            shuffle(parsed_urls)
            urls_to_parse = parsed_urls[:50_000]

            send_telegram_message(f"{len(urls_to_parse)} out of {clean_population_size} URLs were sample for parsing", 'reports') 
            send_telegram_message(f"Note: {len(urls_blacklist)} URLs from blacklist and {len(closed_urls)} closed URLs were excluded from population before sampling", 'reports')
            insert_df(pd.DataFrame({"url": urls_to_parse}), "offers_to_parse")

        send_telegram_message(f"{query_table('offers_to_parse').shape[0]} offers are left", 'reports')
        asyncio.run(parse_offers(SleepyScraper(mean_sleep = 3)))

        update_finish_dttm('offers')
        delete_from_table("offers_to_parse")
        send_telegram_message("Offers parsing finished", 'reports')


    except Exception as e:
        send_telegram_message("An error occured during offer page parisng:")
        send_telegram_message(f"{e}\n{traceback.format_exc()}")
        raise

if days_between_dttms(get_finish_dttm('offers'), get_current_datetime()) >= 2:
    nest_asyncio.apply()
    run_parsing()
else:
    time_print("waiting for 2 days to pass...")
    time.sleep(60*60*5)
