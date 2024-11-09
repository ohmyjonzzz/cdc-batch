from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime, timedelta
from scripts.extract import extract_data
from scripts.load import load_data_to_staging, get_merge_query
from scripts.cdc_sync import run_cdc

default_args = {
    "owner": "ohmyjons",
    "depends_on_past": False,
    "start_date": datetime(2024, 11, 10),
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="cdc_batch",
    default_args=default_args,
    description="CDC Batch pipeline from PostgreSQL to BigQuery",
    schedule_interval= "@hourly",
) as dag:

    extract_task = PythonOperator(
        task_id="extract_transactions_data",
        python_callable=extract_data,
        provide_context=True,
    )


    load_task = PythonOperator(
        task_id="load_transactions_data_to_staging",
        python_callable=load_data_to_staging,
        provide_context=True,
        op_kwargs={"data_pull": "{{ ti.xcom_pull(key='dataframe', task_ids='extract_transactions_data') }}" }
    )

    upsert_task = BigQueryInsertJobOperator(
        task_id="merge_staging_to_target",
        configuration={
            "query": {
                "query": get_merge_query(),
                "useLegacySql": False,
            }
        },
    )

    cdc_task = PythonOperator(
        task_id="cdc_sync",
        python_callable=run_cdc,
    )

    extract_task >> load_task >> upsert_task >> cdc_task
