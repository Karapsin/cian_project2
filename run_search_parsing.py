from search_parsing import search_all_deals_type
from SleepyScraper import SleepyScraper
import pandas as pd
from bot_funs import send_telegram_message
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


df = pd.read_csv("data_load\\search_page_parsed.csv")


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
