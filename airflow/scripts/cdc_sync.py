from google.cloud import bigquery
from sqlalchemy import create_engine
import pandas as pd
import os

PG_CONNECTION = os.getenv("PG_CONNECTION")
PG_SOURCE_TABLE = os.getenv("PG_SOURCE_TABLE")
BQ_TARGET_TABLE_ID = os.getenv("BQ_TARGET_TABLE_ID")

def get_deleted_data():

    engine = create_engine(PG_CONNECTION)
    
    # Get the data only for status = 'DONE'
    query_deleted = f"SELECT id FROM {PG_SOURCE_TABLE} WHERE last_status = 'DONE'"
    deleted_data = pd.read_sql_query(query_deleted, engine)
    engine.dispose()
    return deleted_data["id"].tolist()

def delete_data_from_dw(deleted_ids):

    client = bigquery.Client()

    query = f"DELETE FROM `{BQ_TARGET_TABLE_ID}` WHERE id IN UNNEST({deleted_ids})"
    client.query(query).result()
    print("Deleted data synchronized in BigQuery.")

def run_cdc():

    deleted_ids = get_deleted_data()
    if deleted_ids:
        delete_data_from_dw(deleted_ids)
