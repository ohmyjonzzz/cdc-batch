import os
import argparse
import pandas as pd
from sqlalchemy import create_engine, types


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    csv_file = params.csv_file

    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")

    dtype = {
        "id": types.VARCHAR(255),
        "customer_id": types.VARCHAR(255),
        "last_status": types.VARCHAR(255),
        "pos_origin": types.VARCHAR(255),
        "pos_destination": types.VARCHAR(255),
        "created_at": types.TIMESTAMP,
        "updated_at": types.TIMESTAMP,
        "deleted_at": types.TIMESTAMP
        
    }

    df = pd.read_csv(csv_file)

    df.to_sql(name=table_name, con=engine, index=False, if_exists="replace", dtype=dtype)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest CSV data to Postgres")

    parser.add_argument("--user", required=True, help="user name for postgres")
    parser.add_argument("--password", required=True, help="password for postgres")
    parser.add_argument("--host", required=True, help="host for postgres")
    parser.add_argument("--port", required=True, help="port for postgres")
    parser.add_argument("--db", required=True, help="database name for postgres")
    parser.add_argument("--table_name", required=True, help="name of the table")
    parser.add_argument("--csv_file", required=True, help="name of the csv file")

    args = parser.parse_args()

    main(args)
