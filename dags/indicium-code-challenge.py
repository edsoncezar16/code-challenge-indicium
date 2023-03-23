from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import os
from collections import namedtuple

NamedTable = namedtuple("NamedTable", "name data")

INPUT_CONN_ID = 'northwind-db'
LFS_PATH = '/indicium-lfs'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 22),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'northwind_to_lfs',
    default_args=default_args,
    schedule_interval='@daily'
)

def extract_tables():
    tables = []
    conn = PostgresHook(conn_name_attr=INPUT_CONN_ID).get_conn()
    cur = conn.cursor()
    get_table_names_query = '''
    SELECT table_name 
    FROM information_schema.tables
    WHERE table_schema='public'
    '''
    cur.execute(get_table_names_query)
    table_names = [row[0] for row in cur.fetchall()]
    cur.close()
    for table_name in table_names:
        table = NamedTable(table_name, pd.read_sql_table(table_name, conn))
        tables.append(table)
    return tables

def tables_to_files(tables):
    logical_date = (datetime.today() - timedelta(days=1)).date()
    folder_path = f"{LFS_PATH}/postgres/{logical_date}"
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
    for table in tables:
        csv_path = f"{folder_path}/{table.name}.csv"
        table.data.to_csv(csv_path, index=False)

extract = PythonOperator(
    task_id='extract',
    python_callable=extract_tables,
    dag=dag
)

load = PythonOperator(
    task_id='load',
    python_callable=tables_to_files,
    opt_kw={'tables': extract.output},
    dag=dag
)

extract >> load