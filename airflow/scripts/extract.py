import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

load_dotenv()

PG_SOURCE_TABLE = os.getenv("PG_SOURCE_TABLE")


def extract_data(**kwargs):

    PG_CONNECTION = os.getenv("PG_CONNECTION")

    # Defined last_run_time using Airflow 'prev_execution_date' context
    last_run_time = kwargs.get("prev_execution_date").strftime("%Y-%m-%d %H:%M:%S") or (datetime.now() - timedelta(weeks=156)).strftime("%Y-%m-%d %H:%M:%S")
    print(last_run_time)

    # Create Postgres connection
    engine = create_engine(PG_CONNECTION)
    
    # Get the new or updated data
    query = f"""
    SELECT 
        * 
    FROM 
        {PG_SOURCE_TABLE} 
    WHERE 
        last_status != 'DONE' AND 
        (updated_at > %s OR created_at > %s)
    """
    data = pd.read_sql_query(query, engine, params=(last_run_time, last_run_time))
    engine.dispose()
    print("Extracted data from source table in PostgreSQL.")
    
    data_push = kwargs.get("ti").xcom_push(key="dataframe", value=data.to_json())

    return data_push
