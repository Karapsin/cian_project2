import pymongo as pm
import pandas as pd
from py.utils.utils import get_current_datetime, dttm_to_seconds

DB_URI = "mongodb://localhost:27018/"
DB_NAME = "cian_project"

def insert_df(df, table_name):
    with pm.MongoClient(DB_URI) as connection:
        (
            connection
            [DB_NAME]
            [table_name]
            .insert_many(df.to_dict("records"))
        )
 
def query_table(table_name,
                query_dict = {}, # e.g. {"col1": "some x"}
                columns_dict = {"_id": 0} # e.g. {"col2": 1, "col3": 0}
    ):
    with pm.MongoClient(DB_URI) as connection:
        data = connection[DB_NAME][table_name].find(query_dict, columns_dict)
        df = pd.DataFrame(list(data)) 
        
    return df

# deletes all the data by default
def delete_from_table(table_name, query_dict = {}): 
    with pm.MongoClient(DB_URI) as connection:
        connection[DB_NAME][table_name].delete_many(query_dict)

def count_entries(table_name):
    with pm.MongoClient(DB_URI) as connection:
        entries = connection[DB_NAME][table_name].count_documents({})
    
    return entries

def update_finish_dttm(parsing_type):
    delete_from_table("parsing_finish_dttms", {"parsing_type": parsing_type})
    df = pd.DataFrame({"parsing_type": parsing_type, "last_finish_dttm": get_current_datetime()}, index = [0])
    insert_df(df, "parsing_finish_dttms")
    
def get_finish_dttm(parsing_type):
    return query_table('parsing_finish_dttms', {"parsing_type": parsing_type})['last_finish_dttm'][0]

def update_last_seen_entries(df, table_name = "search_clean"):
    with pm.MongoClient(DB_URI) as connection:
        col = connection[DB_NAME][table_name]
        ops = [
            pm.UpdateOne(
                {"url": row.url},                
                {"$set": {"last_seen_dttm": row}} 
            )
            for row in df.itertuples(index=False)
        ]

        # slice into chunks if the DataFrame is huge
        batch_size = 50_000                     # -- tune for your workload
        for i in range(0, len(ops), batch_size):
            col.bulk_write(ops[i : i + batch_size], ordered=False)
