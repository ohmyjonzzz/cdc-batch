import pandas as pd
from google.cloud import bigquery
import os

BQ_STAGING_TABLE_ID = os.getenv("BQ_STAGING_TABLE_ID")
BQ_TARGET_TABLE_ID = os.getenv("BQ_TARGET_TABLE_ID")

def load_data_to_staging(**kwargs):

    client = bigquery.Client()

    data_pull = kwargs.get("data_pull")

    data = pd.read_json(data_pull)
    data["id"] = data["id"].astype(str)
    data["customer_id"] = data["customer_id"].astype(str)
    data["last_status"] = data["last_status"].astype(str)
    data["pos_origin"] = data["pos_origin"].astype(str)
    data["pos_destination"] = data["pos_destination"].astype(str)
    data["created_at"] = pd.to_datetime(data["created_at"])
    data["updated_at"] = pd.to_datetime(data["updated_at"])
    data["deleted_at"] = pd.to_datetime(data["deleted_at"])

    schema = [
      bigquery.SchemaField("id", "STRING"), 
      bigquery.SchemaField("customer_id", "STRING"), 
      bigquery.SchemaField("last_status", "STRING"),
      bigquery.SchemaField("pos_origin", "STRING"),
      bigquery.SchemaField("pos_destination", "STRING"),
      bigquery.SchemaField("created_at", "DATETIME"),
      bigquery.SchemaField("updated_at", "DATETIME"),
      bigquery.SchemaField("deleted_at", "DATETIME") 
    ]

    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", schema=schema)  # Overwrite staging table
    job = client.load_table_from_dataframe(data, BQ_STAGING_TABLE_ID, job_config=job_config)
    job.result()
    print("Data loaded to staging table in BigQuery.")

def get_merge_query():

    merge_query = f"""
    CREATE TABLE IF NOT EXISTS `{BQ_TARGET_TABLE_ID}` ( 
        id STRING, 
        customer_id STRING, 
        last_status STRING, 
        pos_origin STRING, 
        pos_destination STRING, 
        created_at DATETIME, 
        updated_at DATETIME, 
        deleted_at DATETIME 
    );

    MERGE `{BQ_TARGET_TABLE_ID}` AS target
    USING `{BQ_STAGING_TABLE_ID}` AS source
    ON target.id = source.id
    WHEN MATCHED THEN 
      UPDATE SET 
        target.customer_id = source.customer_id,
        target.last_status = source.last_status,
        target.pos_origin = source.pos_origin,
        target.pos_destination = source.pos_destination,
        target.created_at = source.created_at,
        target.updated_at = source.updated_at,
        target.deleted_at = source.deleted_at
    WHEN NOT MATCHED THEN 
      INSERT (id, customer_id, last_status, pos_origin, pos_destination, created_at, updated_at, deleted_at)
      VALUES(source.id, source.customer_id, source.last_status, source.pos_origin, source.pos_destination, source.created_at, source.updated_at, source.deleted_at);
    """
    return merge_query
