import pandas as pd
import traceback
from py.parsing_funs.search_parsing import search_all_deals_type
from py.utils.SleepyScraper import SleepyScraper
from py.utils.bot_funs import send_telegram_message
from py.utils.data_cleaning_utils import clean_parsed_search_results, filter_clean_data
from py.utils.db_utils import insert_df, query_table, delete_from_table

pd.options.mode.copy_on_write = True

send_telegram_message("Starting search pages parsing")
try:
    
    search_all_deals_type(SleepyScraper(mean_sleep = 10), districts_limit_per_deal_type = 20)

    df = query_table("search_page_parsed")
    clean_df = filter_clean_data(clean_parsed_search_results(df))
    
    insert_df(clean_df, "search_clean")
    insert_df(clean_df, "search_clean_backup")

    delete_from_table("search_page_parsed")

except Exception as e:
    print(e)
    send_telegram_message("An error occured during searh pages parisng:")
    send_telegram_message(f"{e}\n{traceback.format_exc()}")
    raise

send_telegram_message("Search pages parsing is finished")
