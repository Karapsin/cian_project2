import time
import pandas as pd
import traceback
import nest_asyncio
from py.parsing_funs.search_parsing import search_all_deals_type
from py.utils.SleepyScraper import SleepyScraper
from py.utils.bot_funs import send_telegram_message
from py.utils.data_cleaning_utils import clean_parsed_search_results, filter_clean_data
from py.utils.utils import time_print, get_current_datetime, days_between_dttms
from py.utils.db_utils import (
    insert_df, 
    query_table, 
    delete_from_table,
    update_finish_dttm,
    get_finish_dttm
)

def run_parsing():
    send_telegram_message("Starting search pages parsing")
    try:
        
        search_all_deals_type(SleepyScraper(mean_sleep = 10))

        df = query_table("search_page_parsed")
        clean_df = filter_clean_data(clean_parsed_search_results(df))
        
        insert_df(clean_df, "search_clean")
        insert_df(clean_df, "search_clean_backup")

        delete_from_table("search_page_parsed")
        update_finish_dttm('search')
            

    except Exception as e:
        print(e)
        send_telegram_message("An error occured during searh pages parisng:")
        send_telegram_message(f"{e}\n{traceback.format_exc()}")
        raise

    send_telegram_message("Search pages parsing is finished")

if days_between_dttms(get_finish_dttm('search'), get_current_datetime()) >= 0.1:
    pd.options.mode.copy_on_write = True
    nest_asyncio.apply()
    run_parsing()
else:
    time_print("waiting for 1 day to pass...")
    time.sleep(60*60*5)
