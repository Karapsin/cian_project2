from py.parsing_funs.offer_parsing import parse_offers
from py.utils.SleepyScraper import SleepyScraper
from py.utils.csv_utils import get_set_from_splitted_csv, get_set_from_splitted_csv
from random import shuffle
import pandas as pd

send_telegram_message("Starting offers parsing")
urls_csv_path = "data_load\\offers_to_parse.csv"

try:
    
    offers_to_parse = pd.read_csv(urls_csv_path)
    if offers_to_parse.shape[0] == 0:
        parsed_urls = list(get_set_from_splitted_csv("data_load\\csv_search_clean", column = "url"))
        shuffle(parsed_urls)
        urls_to_parse = parsed_urls[:20_000]

        pd.DataFrame({"url": []}).to_csv("data_load\\offers_parsed.csv", index = False)
        pd.DataFrame({"url": urls_to_parse}).to_csv(urls_csv_path, index = False)

        search_df = query_splitted_csv("data_load\\csv_search_clean", "url in @urls_to_parse")
        search_df.to_csv("data_load\\search_results_to_parse.csv", index = False)


    parse_offers(SleepyScraper(mean_sleep = 2.8))

    pd.DataFrame({"url": []}).to_csv(urls_csv_path, index = False)

except Exception as e:
    send_telegram_message("An error occured during offer page parisng:")
    send_telegram_message(f"{e}\n{traceback.format_exc()}")
    raise
