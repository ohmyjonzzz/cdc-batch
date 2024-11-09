# CDC Batch Pipeline

This repository contains an ETL pipeline designed for Change Data Capture (CDC) in batch mode, using PostgreSQL as the source database and Google BigQuery as the target data warehouse. The pipeline ensures that any updates or deletions in the source data are synchronized in the data warehouse, preventing data duplication and maintaining consistency.

## Features
- Batch Data Extraction: Extracts new and updated data based on the last sync time, minimizing load on the source database.
- Data Transformation: Transforms data to match the structure required in BigQuery.
- Upsert and Delete Sync: Uses BigQuery's `MERGE` statement to handle data upsert (insert/update) and delete synchronization, maintaining a single source of truth.
- Orchestration with Apache Airflow: Automates the pipeline with scheduled runs and DAG management.

## Architecture
- Extract: Data is extracted from PostgreSQL, limited to records that have changed since the last pipeline run.
- Transform: Data is transformed to ensure compatibility with the data warehouse schema.
- Load: Data is loaded into a staging table in BigQuery and then merged into the main table, updating existing rows and adding new rows.
- Delete Sync: Records with a filled `deleted_at` column in PostgreSQL are removed from the target table in BigQuery.

## Note on XCom, Pandas, and Large Data
This pipeline uses Pandas within Airflow tasks to test XCom push and pull functions for passing data between tasks. However, Pandas with XCom is not recommended for large datasets due to memory inefficiency and potential performance issues.

### Alternative Solutions for Large Data
For larger datasets, consider one of the following options:

- Direct SQL Processing: Execute SQL queries directly within the database to handle data transformations and filtering without loading data into Airflow’s memory.
- PySpark: Use PySpark for distributed data processing. PySpark’s integration with Airflow and its ability to handle large-scale data in a distributed fashion make it highly efficient for big data tasks.

## Prerequisites
- BigQuery
- PostgreSQL
- Apache Airflow
- Python
- Docker

## Screenshots

### Airflow
![Airflow](assets/[cdc-batch]%20Airflow.png)

### PostgreSQL
![Postgres](assets/[cdc-batch]%20PostgreSQL.png)

### BigQuery (Target Table)
![BigQuery target](assets/[cdc-batch]%20BigQuery%20Target%20Table.png)

### BigQuery (Staging Table)
![BigQuery staging](assets/[cdc-batch]%20BigQuery%20Staging%20Table.png)
