from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

import pandas as pd
from sqlalchemy import inspect

from datetime import datetime, timedelta
import os
from collections import namedtuple

NamedTable = namedtuple("NamedTable", "name data")

INPUT_CONN_ID = 'northwind-db'
LFS_PATH = '/var/lib/indicium-lfs/data'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 22),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'northwind_to_lfs',
    default_args=default_args,
    schedule_interval='@daily'
)

def extract_tables():
    tables = []
    db_hook = PostgresHook(postgres_conn_id=INPUT_CONN_ID)
    engine = db_hook.get_sqlalchemy_engine()
    inspector = inspect(engine)
    table_names = inspector.get_table_names()
    for table_name in table_names:
        table = NamedTable(
            name=table_name,
            data=pd.read_sql_table(table_name, engine.connect())
        )
        tables.append(table)
    return tables

def tables_to_files(**context):
    tables = context['task_instance'].xcom_pull(task_ids='extract')
    logical_date = datetime.today() - timedelta(days=1)
    folder_path = f"{LFS_PATH}/postgres/{logical_date.strftime('%Y-%m-%d')}"
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
    for table in tables:
        csv_path = f"{folder_path}/{table.name}.csv"
        pd.DataFrame(table.data).to_csv(csv_path, index=False)

extract = PythonOperator(
    task_id='extract',
    python_callable=extract_tables,
    dag=dag
)

load = PythonOperator(
    task_id='load',
    python_callable=tables_to_files,
    op_kwargs={'tables': extract.output},
    dag=dag
)

extract >> load