import pandas as pd
from offer_parsing_logic import parse_offer_page
import requests
from utils import get_current_datetime, random_sleep, time_print
pd.options.mode.copy_on_write = True

def robust_value_extraction(df, col):
    return df.get(col, pd.Series([None])).tolist()[0]

def combine_df(df_old, df_new):
    for col in df_old.columns:
        val1 = robust_value_extraction(df_old, col)
        val2 = robust_value_extraction(df_new, col)
        df_new[col] = val1 if val2 is None else val2
    
    return df_new  

def try_parse_offer_page(scraper, url, photos_url, deal_type, try_cnt = 0):
    try:
        return parse_offer_page(scraper, url, photos_url, deal_type)

    except (requests.exceptions.ConnectionError, KeyError) as e:
            
        is_con_error = 'Remote end closed connection without response' in str(e) 
        is_key_agent_error = str(e) == "'agent'"
        time_print(f"error: {str(e)}")

        if (is_con_error or is_key_agent_error) and try_cnt < 1:
            time_print("retrying after some sleep")
            random_sleep(0.5, 5)
            return try_parse_offer_page(scraper, url, photos_url, deal_type, try_cnt = try_cnt + 1)

        time_print("no handling for this error")
        raise

def parse_offers(scraper):
    already_done = set(pd.read_csv("data_load\\offer_page_parsed.csv", usecols=['url'])['url'])
    for old_df in pd.read_csv("data_load\\search_page_parsed.csv", chunksize=1):
        
        url = robust_value_extraction(old_df, 'url')
        deal_type = robust_value_extraction(old_df, 'ad_deal_type')
        if url in already_done:
            continue

        time_print(f"starting {url}")
        photos_url = set(eval(old_df['photo_url_list'].tolist()[0])) 
        
        new_df = try_parse_offer_page(scraper, url, photos_url, deal_type)
        new_df['url'] = url
        new_df['offer_page_load_dttm'] = get_current_datetime()

        combine_df(old_df, new_df).to_csv("data_load\\offer_page_parsed.csv", mode='a', header=False, index=False)
        already_done.add(url)