import pandas as pd
import requests
from random import shuffle
from py.parsing_funs.offer_parsing_logic import parse_offer_page
from py.utils.csv_utils import append_df_to_splitted_csv
from py.utils.utils import(
     get_current_datetime, 
     random_sleep, 
     time_print
)
pd.options.mode.copy_on_write = True

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
    urls_to_parse = set(pd.read_csv("data_load\\offers_to_parse.csv")['url'])
    already_done = set(pd.read_csv("data_load\\offers_parsed.csv")['url'])
    search_df = pd.read_csv("data_load\\search_results_to_parse.csv")
    for url in urls_to_parse:
        
        if url in already_done:
            continue

        time_print(f"starting {url}")
        old_df = search_df.query("url == @url")

        photos_url = list(eval(old_df['photo_url_list'].tolist()[0])) 
        new_df = try_parse_offer_page(scraper, url, photos_url, old_df['ad_deal_type'].to_list()[0])
        old_df['offer_page_load_dttm'] = get_current_datetime()
        for col in new_df.columns:
            
            new_value = new_df[col].to_list()[0]
            if col in old.df.columns and (new_value is None or pd.isna(new_value)):
                continue

            old_df[col] = new_df[col].to_list()[0]

        append_df_to_splitted_csv(old_df, "data_load\\csv_offer_pages_parsed", name_pattern = "offers_parsed")

        already_done.add(url)
        pd.DataFrame({"url": list(already_done)}).to_csv("data_load\\offers_parsed.csv", index = False)

        urls_to_parse.remove(url)
        pd.DataFrame({"url": list(urls_to_parse)}).to_csv("data_load\\offers_to_parse.csv", index = False)

