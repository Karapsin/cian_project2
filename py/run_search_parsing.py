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

import re
import pandas as pd
from py.utils.csv_utils import read_splitted_csv

def contains_cyrillic(input_string):
    return bool(re.compile('[\u0400-\u04FF]').search(input_string))


def parse_author_type(x):
    if pd.isna(x):
        return None
    
    if not(eval(x)['isAgent']):
        return 'owner'

    return eval(x)['accountType']

def eval_and_get_key(x, key, use_get = False):
    
    if use_get:
        return eval(x).get(key) if not(pd.isna(x)) else None
    
    else:
        return eval(x)[key] if not(pd.isna(x)) else None

df = read_splitted_csv("data_load\\csv_search_page_parsed")

df['author_type'] = df['author_info'].apply(parse_author_type)
df['probably_fraud'] = df['moderationInfo'].apply(contains_cyrillic)

df['rosreestrCheck'] = df['rosreestrCheck'].apply(lambda x: eval_and_get_key(x, 'status))
df['parking'] = df['parking'].apply(lambda x: eval_and_get_key(x, 'type'))
df['deadline_quarter']= df['deadline'].apply(lambda x: eval_and_get_key(x, 'quarterEnd'))

df[['lat', 'lng']] = df['coordinates'].apply(eval).apply(pd.Series)

utilities_map = {'includedInPrice': 'in_price', 'flowMetersNotIncludedInPrice': 'flow_not_in_price', 'price': 'price'}
for key in utilities_map:
    df[f"utilities_{utilities_map[key]}"]= df['utilitiesTerms'].apply(lambda x: eval_and_get_key(x, key, use_get = True))

reward_map = {'currency': 'currency', 'paymentType': 'payment_type', 'value': 'price'}
for key in reward_map:
    df[f"agent_reward_{reward_map[key]}"]= df['agentBonus'].apply(lambda x: eval_and_get_key(x, key, use_get = True))



[ 
"ad_deal_type",
"url", 
"creationDate",
'author_type',
"isApartments",
"probably_fraud",

"parsed_address", "lat", "lng", 

"floor_number", "roomsCount", "loggiasCount", "balconiesCount",
"totalArea", "livingArea", "kitchenArea",
"decoration", "hasFurniture",

"buildYear", "floorsCount", "materialType", 
"passengerLiftsCount", "cargoLiftsCount",
"demolishedInMoscowProgramm",

"priceTotal", "currency", "sale_terms", "bargainAllowed", "mortgageAllowed",
'utilities_price', 'utilities_in_price', 'utilities_flow_not_in_price', 

'agent_reward_currency', 'agent_reward_payment_type', 'agent_reward_price', 'agentFee', 'deposit',

'title', 'description',

'isAuction', 'cian_price_range',

"photo_url_list", "search_alias", "search_page_load_dttm"
]

