import asyncio
from random import shuffle
import traceback
import pandas as pd
import os
import nest_asyncio

from py.parsing_funs.offer_parsing import parse_offers
from py.utils.SleepyScraper import SleepyScraper
from py.utils.db_utils import insert_df, query_table, delete_from_table
from py.utils.bot_funs import send_telegram_message
from py.utils.utils import time_print

nest_asyncio.apply()

send_telegram_message("Starting offers parsing")
try:
    if query_table("offers_to_parse").empty:
        time_print("sampling urls")
        parsed_urls = query_table("search_clean", columns_dict = {"url": 1, "_id": 0})['url'].tolist()
        parsed_urls = list(set(parsed_urls))
        shuffle(parsed_urls)
        urls_to_parse = parsed_urls[:50_000]

        insert_df(pd.DataFrame({"url": urls_to_parse}), "offers_to_parse")

    asyncio.run(parse_offers(SleepyScraper(mean_sleep = 3)))

    delete_from_table("offers_to_parse")

except Exception as e:
    send_telegram_message("An error occured during offer page parisng:")
    send_telegram_message(f"{e}\n{traceback.format_exc()}")
    raise
