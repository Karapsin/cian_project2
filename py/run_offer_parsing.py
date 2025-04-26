import asyncio
from random import shuffle
import traceback
import pandas as pd
import nest_asyncio
import time 

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
            parsed_urls = query_table("search_clean", columns_dict = {"url": 1, "_id": 0})['url'].tolist()
            parsed_urls = list(set(parsed_urls))
            shuffle(parsed_urls)
            urls_to_parse = parsed_urls[:50_000]

            insert_df(pd.DataFrame({"url": urls_to_parse}), "offers_to_parse")

        asyncio.run(parse_offers(SleepyScraper(mean_sleep = 3)))

        update_finish_dttm('offers')
        delete_from_table("offers_to_parse")
        send_telegram_message("Offers parsing finished", 'reports')

    except Exception as e:
        send_telegram_message("An error occured during offer page parisng:")
        send_telegram_message(f"{e}\n{traceback.format_exc()}")
        raise

if days_between_dttms(get_finish_dttm('offers'), get_current_datetime()) >= 3:
    nest_asyncio.apply()
    run_parsing()
else:
    time_print("waiting for 3 days to pass...")
    time.sleep(60*60*5)
