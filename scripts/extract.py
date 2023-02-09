#!/usr/bin/env python3

import pandas as pd
from sqlalchemy import create_engine, inspect, MetaData
import sys
import os
from utils import get_db_credentials, get_operation_date

CREDENTIALS_PATH = os.environ["CREDENTIALS_PATH"]
CSV_PATH = os.environ["CSV_PATH"]

# Get the operation date and create a folder to store respective data
operation_date = get_operation_date()
operation_date_str = operation_date.strftime("%Y-%m-%d")
date_folder_path = f"data/postgres/{operation_date_str}"
csv_folder_path = f"data/csv/{operation_date_str}"
if not os.path.exists(date_folder_path):
    os.makedirs(date_folder_path)
if not os.path.exists(csv_folder_path):
    os.makedirs(csv_folder_path)
    
# extract data from the postgres database
db_name, user, password, port = get_db_credentials(CREDENTIALS_PATH)
engine = create_engine(f"postgresql://{user}:{password}@localhost:{port}/{db_name}")
inspector = inspect(engine)
table_names = inspector.get_table_names()
total_tables = len(table_names)
print("Extracting data from the database ...")
for index, table_name in enumerate(table_names):
    print(f"Getting data from table {table_name} ({index + 1}/{total_tables})")
    table = pd.read_sql_table(table_name, engine.connect())
    output_name = f"{date_folder_path}/{table_name}.csv"
    print(f"Writting data from table {table_name} to local disk.")
    table.to_csv(output_name, index=False)
engine.dispose()  # to prevent resource leaks
print("Done.\n")

# extract data from the provided csv file
print("Extracting data from the csv file ...")
order_details = pd.read_csv(CSV_PATH)
csv_output_path = f"{csv_folder_path}/order_details.csv"
print(f"Writting data from csv file to local disk.")
order_details.to_csv(csv_output_path, index=False)
print("Done.")
