from py.parsing_funs.search_parsing import search_all_deals_type
from py.utils.SleepyScraper import SleepyScraper
from py.utils.bot_funs import send_telegram_message
import pandas as pd
import traceback
pd.options.mode.copy_on_write = True

send_telegram_message("Starting search pages parsing")
try:
    search_all_deals_type(SleepyScraper(mean_sleep = 10))

except Exception as e:
    print(e)
    send_telegram_message("An error occured during searh pages parisng:")
    send_telegram_message(f"{e}\n{traceback.format_exc()}")
    raise
send_telegram_message("Search pages parsing is finished")



#######################################################################3
# DO NOT RUN WIP

from py.utils.csv_utils import split_csv, read_splitted_csv
split_csv_file("data_load\\csv_offer_pages_parsed\\offer_page_parsed.csv",
               "data_load\\csv_offer_pages_parsed",
               "offer_pages_parsed",
               40,
               1000
)

df = read_splitted_csv("data_load\\csv_offer_pages_parsed")


import pandas as pd
df2 = pd.read_csv("offer_page_parsed.csv")



import hashlib
hashlib.sha256(df.sort_values(by=df.columns.tolist()).reset_index(drop=True).to_csv(index=False).encode('utf-8')).hexdigest()
hashlib.sha256(df2.sort_values(by=df2.columns.tolist()).reset_index(drop=True).to_csv(index=False).encode('utf-8')).hexdigest()

author_info_all_keys = df['author_info'].dropna().apply(lambda x: eval(x).keys()).drop_duplicates()


df['author_type'] = df['author_info'].apply(lambda x: None 
                                                      if pd.isna(x) 
                                                      else 
                                                          'owner' if not(eval(x)['isAgent'])
                                                           else eval(x)['accountType']
                                       )



# Initialize an empty set
unique_values = set()

# Iterate over the Series and update the set with elements from each tuple
for tup in author_info_all_keys:
    unique_values.update(tup)



df[['lat', 'lng']] = df['coordinates'].apply(eval).apply(pd.Series)

df.columns

df[["url", "lat", "lng", "totalArea", "roomsCount", "livingArea", "kitchenArea", "floorNumber"]]
