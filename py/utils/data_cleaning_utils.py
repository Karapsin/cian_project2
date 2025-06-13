import re
import pandas as pd
from py.utils.csv_utils import read_splitted_csv
from py.constants.cols_order import clean_search_results_col_order
from py.constants.filtering_criterias import filtering_criterias

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

def clean_parsed_search_results(df):

    df['author_type'] = df['author_info'].apply(parse_author_type)
    df['probably_fraud'] = df['moderation_info_search'].apply(contains_cyrillic)

    df['rosreestrCheck'] = df['rosreestrCheck'].apply(lambda x: eval_and_get_key(x, 'status'))
    #df['parking'] = df['parking'].apply(lambda x: eval_and_get_key(x, 'type'))
    df['deadline_quarter']= df['deadline'].apply(lambda x: eval_and_get_key(x, 'quarterEnd', True))

    df[['lat', 'lng']] = df['coordinates'].apply(eval).apply(pd.Series)

    utilities_map = {'includedInPrice': 'in_price', 'flowMetersNotIncludedInPrice': 'flow_not_in_price', 'price': 'price'}
    for key in utilities_map:
        df[f"utilities_{utilities_map[key]}"]= df['utilitiesTerms'].apply(lambda x: eval_and_get_key(x, key, use_get = True))

    reward_map = {'currency': 'currency', 'paymentType': 'payment_type', 'value': 'price'}
    for key in reward_map:
        df[f"agent_reward_{reward_map[key]}"]= df['agentBonus'].apply(lambda x: eval_and_get_key(x, key, use_get = True))

    return df[clean_search_results_col_order]


def filter_clean_data(df):

    df = df[~pd.isna(df['roomsCount'])]
    df['roomsCount'] = df['roomsCount'].astype(int)
    df['totalArea'] = df['totalArea'].astype(float)

    filtered_dfs_list = list()
    for deal_type in filtering_criterias:

        
        if deal_type in {'long_rent', 'short_rent'}:
            multiplier = 1_000
        else:
            multiplier = 1_000_000

        deal_type_criterias = filtering_criterias[deal_type]
        for room_number in deal_type_criterias:
            ranges_dict = deal_type_criterias[room_number]
            room_number = int(room_number)

            lower_price, upper_price = ranges_dict['price']
            lower_price, upper_price = lower_price * multiplier, upper_price * multiplier

            lower_area, upper_area = ranges_dict['area']

            filtered_df = df.query("""\
                                            ad_deal_type == @deal_type \
                                        and roomsCount == @room_number \
                                        and priceTotal >= @lower_price and priceTotal <= @upper_price \
                                        and totalArea >= @lower_area and totalArea <= @upper_area \
                                        and probably_fraud == False
                                   """
                             )
            
            filtered_dfs_list.append(filtered_df)

    return pd.concat(filtered_dfs_list, ignore_index = True).drop_duplicates(subset=['url']).drop('probably_fraud', axis = 1)
