import re
import pandas as pd
from py.utils.csv_utils import read_splitted_csv
from py.constants.cols_order import clean_search_results_col_order

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
    df['probably_fraud'] = df['moderationInfo'].apply(contains_cyrillic)

    df['rosreestrCheck'] = df['rosreestrCheck'].apply(lambda x: eval_and_get_key(x, 'status'))
    df['parking'] = df['parking'].apply(lambda x: eval_and_get_key(x, 'type'))
    df['deadline_quarter']= df['deadline'].apply(lambda x: eval_and_get_key(x, 'quarterEnd'))

    df[['lat', 'lng']] = df['coordinates'].apply(eval).apply(pd.Series)

    utilities_map = {'includedInPrice': 'in_price', 'flowMetersNotIncludedInPrice': 'flow_not_in_price', 'price': 'price'}
    for key in utilities_map:
        df[f"utilities_{utilities_map[key]}"]= df['utilitiesTerms'].apply(lambda x: eval_and_get_key(x, key, use_get = True))

    reward_map = {'currency': 'currency', 'paymentType': 'payment_type', 'value': 'price'}
    for key in reward_map:
        df[f"agent_reward_{reward_map[key]}"]= df['agentBonus'].apply(lambda x: eval_and_get_key(x, key, use_get = True))

    return df[clean_search_results_col_order]