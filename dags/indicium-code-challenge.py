from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

import pandas as pd
from sqlalchemy import inspect

from datetime import datetime, timedelta
import os
from collections import namedtuple

NamedTable = namedtuple("NamedTable", "name data")

INPUT_CONN_ID = 'northwind-db'
OUTPUT_CONN_ID = 'analytics-db'
LFS_PATH = '/var/lib/indicium-lfs/data'
CSV_PATH = '/home/data/order_details.csv'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 22),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup':False
}

dag = DAG(
    'indicium-code-challenge',
    default_args=default_args,
    schedule_interval='@daily'
)

def extract_tables():
    """Connects to the northwind db and extracts data from all tables.

    Args:
        None

    Returns: 
        tables: list of NamedTables for all tables in the northwind db.
    """
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

def extract_csv():
    """Gets the order_details table data from the provided csv file.

    Args:
        None
    
    Returns:
        order_details: NamedTable corresponding to the data in the csv file.
    """
    order_details = NamedTable(
        name='order_details',
        data=pd.read_csv(CSV_PATH)
    )
    return order_details
    
def tables_to_lfs(**context):
    """Writes the data extracted from the northwind db to a local file system.

    Args:
        context: Airflow context dictionary.
    
    Returns:
        None
    """
    tables = context['task_instance'].xcom_pull(task_ids='extract.tables')
    logical_date = datetime.today() - timedelta(days=1)
    folder_path = f"{LFS_PATH}/postgres/{logical_date.strftime('%Y-%m-%d')}"
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
    for table in tables:
        csv_path = f"{folder_path}/{table.name}.csv"
        pd.DataFrame(table.data).to_csv(csv_path, index=False)

def csv_to_lfs(**context):
    """Writes the data extracted from the csv file to a local file system.

    Args:
        context: Airflow context dictionary.
    
    Returns:
        None
    """
    order_details = context['task_instance'].xcom_pull(task_ids='extract.csv')
    logical_date = datetime.today() - timedelta(days=1)
    folder_path = f"{LFS_PATH}/csv/{logical_date.strftime('%Y-%m-%d')}"
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
    output_csv_path = f"{folder_path}/{order_details.name}.csv"
    pd.DataFrame(order_details.data).to_csv(output_csv_path, index=False)

extract = TaskGroup(group_id='extract', dag=dag)

extract_tables = PythonOperator(
    task_id='tables',
    python_callable=extract_tables,
    task_group=extract,
    dag=dag
)

extract_csv = PythonOperator(
    task_id ='csv',
    python_callable=extract_csv,
    task_group=extract,
    dag=dag
)

load_to_lfs = TaskGroup(group_id='load-to-lfs', dag=dag)

load_tables = PythonOperator(
    task_id='tables',
    python_callable=tables_to_lfs,
    task_group=load,
    dag=dag
)

load_csv = PythonOperator(
    task_id='csv',
    python_callable=csv_to_lfs,
    task_group=load,
    dag=dag
)

extract >> load_to_lfs