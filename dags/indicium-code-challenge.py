from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

import pandas as pd
from sqlalchemy import inspect, text
from sqlalchemy import text, inspect, Column, ForeignKey
from sqlalchemy import Integer, Float, String, Date
from sqlalchemy.orm import relationship, declarative_base, sessionmaker

from datetime import datetime, timedelta
import os
import sys
from collections import namedtuple

NamedTable = namedtuple("NamedTable", "name data")

INPUT_CONN_ID = "northwind-db"
OUTPUT_CONN_ID = "analytics-db"
LFS_PATH = "/var/lib/indicium-lfs/data"
CSV_PATH = "/home/data/order_details.csv"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 3, 22),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "catchup": False,
}

indicium_code_challenge = DAG(
    "indicium-code-challenge", default_args=default_args, schedule_interval="@daily"
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
            name=table_name, data=pd.read_sql_table(table_name, engine.connect())
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
    order_details = NamedTable(name="order_details", data=pd.read_csv(CSV_PATH))
    return order_details


def get_folder_path(name):
    """Returns the path of the local file system folder for reading and writing operations.

    Args:
        name: "postgres", "csv" or "results", the folder name.

    Returns:
        folder_path: string with the requested path.
    """
    logical_date = datetime.today() - timedelta(days=1)
    folder_path = f"{LFS_PATH}/{name}/{logical_date.strftime('%Y-%m-%d')}"
    return folder_path


def tables_to_lfs(**context):
    """Writes the data extracted from the northwind db to a local file system.

    Args:
        context: Airflow context dictionary.

    Returns:
        None
    """
    tables = context["task_instance"].xcom_pull(task_ids="extract.tables")
    folder_path = get_folder_path("postgres")
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
    order_details = context["task_instance"].xcom_pull(task_ids="extract.csv")
    folder_path = get_folder_path("csv")
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
    output_csv_path = f"{folder_path}/{order_details.name}.csv"
    pd.DataFrame(order_details.data).to_csv(output_csv_path, index=False)


def clean_data():
    """Performs the necessary data cleansing before data loading into the analytical environment.

    Args: None

    Returns:
        None
    """
    folder_path = get_folder_path("postgres")
    if not os.path.exists(folder_path):
        print(
            """
        There is no data in the local disk for the provided date.
        Plese consider extracting the respective date before attempting
        to transform it.
        """
        )
        sys.exit(1)

    # make necessary data transformations
    orders_data = pd.read_csv(f"{folder_path}/orders.csv")
    orders_data["shipped_date"] = pd.to_datetime(
        orders_data["shipped_date"], errors="coerce"
    )
    orders_data["shipped_date"].fillna(
        pd.Timestamp("1970-01-01 00:00:00"), inplace=True
    )
    orders_data.to_csv(f"{folder_path}/orders.csv", index=False)


def lfs_to_analytics():
    """Loads data from the local file system into the analytics db. Previously existing data is excluded.

    Args:
        None

    Returns:
        None
    """
    pg_folder_path = get_folder_path("postgres")
    csv_folder_path = get_folder_path("csv")
    loading_data_available = (os.path.exists(pg_folder_path)) and (
        os.path.exists(csv_folder_path)
    )
    if not loading_data_available:
        print(
            """There is no data in the local file system for the provided date.
        Plese consider extracting the respective date before attempting
        to load it to the analytics database.
        """
        )
        sys.exit(1)
    orders_data = pd.read_csv(f"{pg_folder_path}/orders.csv")
    order_details_data = pd.read_csv(f"{csv_folder_path}/order_details.csv")
    db_hook = PostgresHook(postgres_conn_id=OUTPUT_CONN_ID)
    engine = db_hook.get_sqlalchemy_engine()
    inspector = inspect(subject=engine)
    table_names = inspector.get_table_names()
    conn = engine.connect()
    for table_name in table_names:
        sql = text(f"drop table {table_name} cascade")
        conn.execute(sql)
    conn.close()
    Base = declarative_base()

    class Orders(Base):
        __tablename__ = "orders"
        order_id = Column("order_id", Integer, primary_key=True)
        customer_id = Column("customer_id", String, nullable=False)
        employee_id = Column("employee_id", String, nullable=False)
        order_date = Column("order_date", Date)
        required_date = Column("required_date", Date)
        shipped_date = Column("shipped_date", Date)
        ship_via = Column("ship_via", Integer)
        freight = Column("freight", Float)
        ship_name = Column("ship_name", String)
        ship_address = Column("ship_address", String)
        ship_city = Column("ship_city", String)
        ship_region = Column("ship_region", String)
        ship_postal_code = Column("ship_postal_code", String)
        ship_country = Column("ship_country", String)
        order_details = relationship(
            "OrderDetails", back_populates="orders", cascade="all, delete-orphan"
        )

    class OrderDetails(Base):
        __tablename__ = "order_details"
        order_id = Column(
            "order_id",
            Integer,
            ForeignKey("orders.order_id", ondelete="CASCADE"),
            primary_key=True,
        )
        product_id = Column("product_id", Integer, primary_key=True)
        unit_price = Column("unit_price", Float)
        quantity = Column("quantity", Integer)
        discount = Column("discount", Float)
        orders = relationship("Orders", back_populates="order_details")

    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    orders = [Orders(**row_data) for _, row_data in orders_data.iterrows()]
    session.add_all(orders)
    session.commit()
    session.close()
    session = Session()
    order_details = [
        OrderDetails(**row_data) for _, row_data in order_details_data.iterrows()
    ]
    session.add_all(order_details)
    session.commit()
    session.close()
    engine.dispose()


def query_and_store_results():
    """Queries the alanytics database to show all orders and their details, and saves the result to a csv file.

    Args:
        None

    Returns:
        None
    """
    db_hook = PostgresHook(postgres_conn_id=OUTPUT_CONN_ID)
    engine = db_hook.get_sqlalchemy_engine()
    query = text(
        """
        SELECT * 
        FROM ORDERS
        JOIN ORDER_DETAILS
        ON ORDERS.ORDER_ID = ORDER_DETAILS.ORDER_ID
        """
    )
    try:
        conn = engine.connect()
        results = pd.read_sql_query(query, conn)
        conn.close()
        engine.dispose()
        results = results.T.drop_duplicates().T
        folder_path = get_folder_path("results")
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
        results.to_csv(f"{folder_path}/query_result.csv", index=False)
    except:  # output database is not set
        print(
            """
            Output database is not set. Please consider loading data to output
            database before querying.
            """
        )
        sys.exit(1)


extract = TaskGroup(group_id="extract", dag=indicium_code_challenge)

extract_tables = PythonOperator(
    task_id="tables",
    python_callable=extract_tables,
    task_group=extract,
    dag=indicium_code_challenge,
)

extract_csv = PythonOperator(
    task_id="csv",
    python_callable=extract_csv,
    task_group=extract,
    dag=indicium_code_challenge,
)

load_to_lfs = TaskGroup(group_id="load-to-lfs", dag=indicium_code_challenge)

load_tables = PythonOperator(
    task_id="tables",
    python_callable=tables_to_lfs,
    task_group=load_to_lfs,
    dag=indicium_code_challenge,
)

load_csv = PythonOperator(
    task_id="csv",
    python_callable=csv_to_lfs,
    task_group=load_to_lfs,
    dag=indicium_code_challenge,
)

transform = PythonOperator(
    task_id="transform", python_callable=clean_data, dag=indicium_code_challenge
)

load_to_analytics = PythonOperator(
    task_id="load-to-analytics",
    python_callable=lfs_to_analytics,
    dag=indicium_code_challenge,
)

query = PythonOperator(
    task_id="query",
    python_callable=query_and_store_results,
    dag=indicium_code_challenge,
)

extract >> load_to_lfs >> transform >> load_to_analytics >> query
