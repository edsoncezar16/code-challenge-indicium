#!/usr/bin/env python3

# treats the column 'shipped date' in orders table
# which has 'nan' values that cannot be converted to a date type
import sys
import os
import pandas as pd
from datetime import datetime

# Get the extraction date and check if the data is present
if len(sys.argv) == 1:
    extraction_date = datetime.today()
else:
    try:
        extraction_date = datetime.strptime(sys.argv[1], '%Y-%m-%d')
    except:
        print('''
        Please provide a date in the format 'YYYY-MM-DD' or no date at 
        all to transform today's data.
        ''')
        sys.exit(1)
extraction_date_str = extraction_date.strftime('%Y-%m-%d')
date_folder_path = f'data/postgres/{extraction_date_str}'
if not os.path.exists(date_folder_path):
    print('''
    There is no data in the local disk for the provided date.
    Plese consider extracting the respective date before attempting
    to transform it.
    ''')
    sys.exit(1)

orders_data = pd.read_csv(
    f'{date_folder_path}/orders.csv'
)
orders_data['shipped_date'] = pd.to_datetime(
    orders_data['shipped_date'], errors='coerce'
)
orders_data['shipped_date'].fillna(
    pd.Timestamp('1970-01-01 00:00:00'), inplace=True
)
orders_data.to_csv(
    f'{date_folder_path}/orders.csv', index=False
)
