#!/usr/bin/env python3

import pandas as pd
from sqlalchemy import create_engine, inspect, MetaData
import sys
import os
from datetime import datetime
from get_credentials import CREDENTIALS_PATH, get_db_credentials



ORDER_DETAILS_PATH = 'data/order_details.csv'

# Get the extraction date and create a folder to store respective data
if len(sys.argv) == 1:
    extraction_date = datetime.today()
else:
    try:
        extraction_date = datetime.strptime(sys.argv[1], '%Y-%m-%d')
    except:
        print('''
        Please provide a date in the format 'YYYY-MM-DD' or no date at 
        all to extract today's data.
        ''')
        sys.exit(1)
extraction_date_str = extraction_date.strftime('%Y-%m-%d')
date_folder_path = f'data/postgres/{extraction_date_str}'
csv_folder_path = f'data/csv/{extraction_date_str}'
if not os.path.exists(date_folder_path):
    os.makedirs(date_folder_path)
if not os.path.exists(csv_folder_path):
    os.makedirs(csv_folder_path)
	
 

# extract data from the postgres database
db_name, user, password, port = get_db_credentials(CREDENTIALS_PATH)
engine = create_engine(
    f"postgresql://{user}:{password}@'localhost':{port}/{db_name}"
)
inspector = inspect(engine)
table_names = inspector.get_table_names()
for table_name in table_names:
        table = pd.read_sql_table(table_name, engine.connect())
        output_name = f'{date_folder_path}/{table_name}.csv'
        table.to_csv(output_name)
engine.dispose() # to prevent resource leaks

# extract data from the provided csv file
order_details = pd.read_csv(ORDER_DETAILS_PATH)
csv_output_path = f'{csv_folder_path}/order_details.csv'
order_details.to_csv(csv_output_path)
